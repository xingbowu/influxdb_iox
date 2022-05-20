//! ParquetFile cache

use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::{
        lru::{LruBackend, ResourcePool},
        resource_consumption::FunctionEstimator,
        ttl::{SharedBackend, TtlBackend, ValueTtlProvider},
    },
    driver::Cache,
    loader::{metrics::MetricsLoader, FunctionLoader},
};
use data_types::{ParquetFileWithMetadata, SequenceNumber, TableId};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use parquet_file::chunk::DecodedParquetFile;
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, mem::size_of_val, sync::Arc, time::Duration};

use super::ram::RamSize;

/// Duration to keep parquet files cached
pub const TTL: Duration = Duration::from_secs(60);

const CACHE_ID: &str = "parquet_file";

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("CatalogError refreshing parquet file cache: {}", source))]
    Catalog {
        source: iox_catalog::interface::Error,
    },
}

/// A specialized `Error` for errors (needed to make Backoff happy for some reason)
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
/// Holds decoded catalog information about a parquet file
pub struct CachedParquetFiles {
    /// TODO maximum seen persisted parquet file

    /// Parquet catalog information and decoded metadata
    pub files: Arc<Vec<Arc<DecodedParquetFile>>>,
}

impl CachedParquetFiles {
    fn new(parquet_files_with_metadata: Vec<ParquetFileWithMetadata>) -> Self {
        let files: Vec<_> = parquet_files_with_metadata
            .into_iter()
            .map(DecodedParquetFile::new)
            .map(Arc::new)
            .collect();

        Self {
            files: Arc::new(files),
        }
    }

    /// return the underying files as a new Vec
    pub(crate) fn vec(&self) -> Vec<Arc<DecodedParquetFile>> {
        self.files.as_ref().clone()
    }

    /// Estimate the memory consumption of this object and its contents
    fn size(&self) -> usize {
        // todo
        42
    }

    /// Returns the greatest parquet sequence number stored in this cache entry
    pub(crate) fn max_parquet_sequence_number(&self) -> Option<SequenceNumber> {
        self.files
            .iter()
            .map(|f| f.parquet_file.max_sequence_number)
            .max()
    }
}

/// Cache for parquet file information with metadata.
///
/// DOES NOT CACHE the actual parquet bytes from object store
#[derive(Debug)]
pub struct ParquetFileCache {
    cache: Cache<TableId, Arc<CachedParquetFiles>>,

    /// Handle that allows clearing entries for existing cache entries
    backend: SharedBackend<TableId, Arc<CachedParquetFiles>>,
}

impl ParquetFileCache {
    /// Create new empty cache.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        ram_pool: Arc<ResourcePool<RamSize>>,
    ) -> Self {
        let loader = Box::new(FunctionLoader::new(move |table_id: TableId| {
            let catalog = Arc::clone(&catalog);
            let backoff_config = backoff_config.clone();

            async move {
                Backoff::new(&backoff_config)
                    .retry_all_errors("get parquet_files", || async {
                        // TODO refreshing all parquet files for the entire table is likely to be quite wasteful for large tables.
                        // Ways this code could be more efficeints:
                        // 1. incrementally fetch only NEW parquet files
                        // 2. track time ranges needed for queries and limit files fetched
                        let parquet_files_with_metadata: Vec<_> = catalog
                            .repositories()
                            .await
                            .parquet_files()
                            .list_by_table_not_to_delete_with_metadata(table_id)
                            .await
                            .context(CatalogSnafu)?;

                        Ok(Arc::new(CachedParquetFiles::new(
                            parquet_files_with_metadata,
                        ))) as std::result::Result<_, Error>
                    })
                    .await
                    .expect("retry forever")
            }
        }));
        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            metric_registry,
        ));

        // entries expire after TTL
        let backend = Box::new(TtlBackend::new(
            Box::new(HashMap::new()),
            Arc::new(ValueTtlProvider::new(TTL)),
            Arc::clone(&time_provider),
        ));

        // add to memory pool
        let backend = Box::new(LruBackend::new(
            backend as _,
            Arc::clone(&ram_pool),
            CACHE_ID,
            Arc::new(FunctionEstimator::new(
                |k: &TableId, v: &Arc<CachedParquetFiles>| {
                    RamSize(size_of_val(k) + size_of_val(v) + v.size())
                },
            )),
        ));

        // get a direct handle so we can clear out entries as needed
        let backend = SharedBackend::new(backend);

        let cache = Cache::new(loader, Box::new(backend.clone()));

        Self { cache, backend }
    }

    /// Get list of cached parquet files, by table id
    pub async fn get(&self, table_id: TableId) -> Arc<CachedParquetFiles> {
        self.cache.get(table_id).await
    }

    /// Mark the entry for table_id as expired (and needs a refresh)
    pub fn expire(&self, table_id: TableId) {
        self.backend.remove_if(&table_id, |_| true);
    }

    /// Clear the parquet file cache if it does not know about data up
    /// to max_parquet_sequence_number.
    ///
    /// If it does not know about max_parquet_sequence_number it means
    /// the ingester has written new data to the catalog and we need
    /// to update our knowledge of that.
    ///
    /// Returns true if the cache was cleared.
    pub fn expire_if_unknown(
        &self,
        table_id: TableId,
        max_parquet_sequence_number: Option<SequenceNumber>,
    ) -> bool {
        if let Some(max_parquet_sequence_number) = max_parquet_sequence_number {
            // check backend cache to see if the maximum sequence
            // number desired is less than what we know about
            self.backend.remove_if(&table_id, |maybe_cached_file| {
                let max_cached = maybe_cached_file.and_then(|f| f.max_parquet_sequence_number());

                if let Some(max_cached) = max_cached {
                    max_cached < max_parquet_sequence_number
                } else {
                    false
                }
            })
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iox_tests::util::TestCatalog;

    use crate::cache::ram::test_util::test_ram_pool;

    #[tokio::test]
    async fn test_parquet_chunks() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let table1 = ns.create_table("table1").await;
        let sequencer1 = ns.create_sequencer(1).await;

        let partition1 = table1
            .with_sequencer(&sequencer1)
            .create_partition("k")
            .await;

        let file = partition1.create_parquet_file("table1 foo=1 11").await;

        let cache = ParquetFileCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        );

        let cached_files = cache.get(table1.table.id).await.vec();

        assert_eq!(cached_files.len(), 1);
        let (expected_parquet_file, _meta) = file.parquet_file.split_off_metadata();
        assert_eq!(cached_files[0].parquet_file, expected_parquet_file);

        // TODO: validate a second request doens't result in a catalog request
    }

    // TODO tests for multiple tables
    // TODO tests for size

    // TODO tests for errors (table doesn't exist..)

    // TODO: test expire logic
    // max persisted seuqence
}
