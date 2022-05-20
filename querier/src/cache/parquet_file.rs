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
use data_types::{ParquetFileWithMetadata, TableId};
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
pub struct CachedParquetFile {
    /// Parquet catalog information and decoded metadata
    pub file: DecodedParquetFile,
}

impl CachedParquetFile {
    fn new(parquet_file_with_metadata: ParquetFileWithMetadata) -> Self {
        Self {
            file: DecodedParquetFile::new(parquet_file_with_metadata),
        }
    }

    fn size(&self) -> usize {
        // todo
        42
    }
}

/// Cache for parquet file information with metadata.
///
/// DOES NOT CACHE the actual parquet bytes from object store
#[derive(Debug)]
pub struct ParquetFileCache {
    cache: Cache<TableId, Vec<Arc<CachedParquetFile>>>,

    /// Handle that allows clearing entries for existing cache entries
    backend: SharedBackend<TableId, Vec<Arc<CachedParquetFile>>>,
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
                        let parquet_files: Vec<_> = catalog
                            .repositories()
                            .await
                            .parquet_files()
                            .list_by_table_not_to_delete_with_metadata(table_id)
                            .await
                            .context(CatalogSnafu)?
                            .into_iter()
                            .map(CachedParquetFile::new)
                            .map(Arc::new)
                            .collect();

                        Ok(parquet_files) as std::result::Result<_, Error>
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
                |k: &TableId, v: &Vec<Arc<CachedParquetFile>>| {
                    RamSize(
                        size_of_val(k)
                            + size_of_val(v)
                            + v.len()
                            + v.iter().map(|f| size_of_val(f) + f.size()).sum::<usize>(),
                    )
                },
            )),
        ));

        // get a direct handle so we can clear out entries as needed
        let backend = SharedBackend::new(backend);

        let cache = Cache::new(loader, Box::new(backend.clone()));

        Self { cache, backend }
    }

    /// Get list of cached parquet files, by table id
    pub async fn files(&self, table_id: TableId) -> Vec<Arc<CachedParquetFile>> {
        self.cache.get(table_id).await
    }

    /// Mark the entry for table_id as expired (and needs a refresh)
    pub fn expire(&self, table_id: TableId) {
        self.backend.force_remove(&table_id)
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

        let cached_files = cache.files(table1.table.id).await;

        assert_eq!(cached_files.len(), 1);
        let (expected_parquet_file, _meta) = file.parquet_file.split_off_metadata();
        assert_eq!(cached_files[0].file.parquet_file, expected_parquet_file);

        // TODO: validate a second request doens't result in a catalog request
    }

    // TODO tests for multiple tables
    // TODO tests for size

    // TODO tests for errors (table doesn't exist..)

    // TODO: test expire logic
}
