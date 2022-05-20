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
use data_types::{SequenceNumber, TableId, Tombstone};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, mem::size_of_val, sync::Arc, time::Duration};

use super::ram::RamSize;

/// Duration to keep tombstones cached
pub const TTL: Duration = Duration::from_secs(60);

const CACHE_ID: &str = "tombstone";

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("CatalogError refreshing tombstone cache: {}", source))]
    Catalog {
        source: iox_catalog::interface::Error,
    },
}

/// A specialized `Error` for errors (needed to make Backoff happy for some reason)
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Holds decoded catalog information about a parquet file
#[derive(Debug, Clone)]
pub struct CachedTombstones {
    /// Tombstones that were cached in the catalog
    pub tombstones: Vec<Arc<Tombstone>>,
}
impl CachedTombstones {
    fn new(tombstones: Vec<Tombstone>) -> Self {
        let tombstones = tombstones.into_iter().map(Arc::new).collect();

        Self { tombstones }
    }

    fn size(&self) -> usize {
        // todo
        42
    }

    fn into_inner(self) -> Vec<Arc<Tombstone>> {
        self.tombstones
    }

    /// Returns the greatest tombstone sequence number stored in this cache entry
    pub(crate) fn max_tombstone_sequence_number(&self) -> Option<SequenceNumber> {
        self.tombstones.iter().map(|f| f.sequence_number).max()
    }
}

/// Cache for tombstones for a particular table
#[derive(Debug)]
pub struct TombstoneCache {
    cache: Cache<TableId, CachedTombstones>,
    /// Handle that allows clearing entries for existing cache entries
    backend: SharedBackend<TableId, CachedTombstones>,
}

impl TombstoneCache {
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
                    .retry_all_errors("get tombstones", || async {
                        let cached_tombstone = CachedTombstones::new(
                            catalog
                                .repositories()
                                .await
                                .tombstones()
                                .list_by_table(table_id)
                                .await
                                .context(CatalogSnafu)?,
                        );

                        Ok(cached_tombstone) as std::result::Result<_, Error>
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
                |k: &TableId, v: &CachedTombstones| {
                    RamSize(size_of_val(k) + size_of_val(v) + v.size())
                },
            )),
        ));

        let backend = SharedBackend::new(backend);

        let cache = Cache::new(loader, Box::new(backend.clone()));

        Self { cache, backend }
    }

    /// Get list of cached tombstones, by table id
    pub async fn get(&self, table_id: TableId) -> Vec<Arc<Tombstone>> {
        self.cache.get(table_id).await.into_inner()
    }

    /// Mark the entry for table_id as expired / needs a refresh
    pub fn expire(&self, table_id: TableId) {
        self.backend.remove_if(&table_id, |_| true);
    }

    /// Clear the cache if it does not know about data up
    /// to max_tombstone_sequence_number.
    ///
    /// If it does not know about max_tombstone_sequence_number it means
    /// the ingester has written new data to the catalog and we need
    /// to update our knowledge of that.
    ///
    /// Returns true if the cache was cleared.
    pub fn expire_if_unknown(
        &self,
        table_id: TableId,
        max_tombstone_sequence_number: Option<SequenceNumber>,
    ) -> bool {
        if let Some(max_tombstone_sequence_number) = max_tombstone_sequence_number {
            // check backend cache to see if the maximum sequence
            // number desired is less than what we know about
            self.backend.remove_if(&table_id, |maybe_cached_file| {
                let max_cached = maybe_cached_file.and_then(|f| f.max_tombstone_sequence_number());

                if let Some(max_cached) = max_cached {
                    max_cached < max_tombstone_sequence_number
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
    async fn test_tombstones() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;
        let table1 = ns.create_table("table1").await;
        let sequencer1 = ns.create_sequencer(1).await;

        let table_and_sequencer = table1.with_sequencer(&sequencer1);

        let tombstone1 = table_and_sequencer
            .create_tombstone(7, 1, 100, "foo=1")
            .await;

        let cache = TombstoneCache::new(
            catalog.catalog(),
            BackoffConfig::default(),
            catalog.time_provider(),
            &catalog.metric_registry(),
            test_ram_pool(),
        );

        let cached_tombstones = cache.get(table1.table.id).await;

        assert_eq!(cached_tombstones.len(), 1);
        assert_eq!(cached_tombstones[0].as_ref(), &tombstone1.tombstone);
    }

    // TODO tests for multiple tombestones
    // TODO tests for size

    // TODO tests for errors (table doesn't exist..)
}
