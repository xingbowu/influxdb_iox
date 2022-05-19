//! ParquetFile cache

use backoff::{Backoff, BackoffConfig};
use cache_system::{
    backend::{
        lru::{LruBackend, ResourcePool},
        resource_consumption::FunctionEstimator,
        ttl::{TtlBackend, ValueTtlProvider},
    },
    driver::Cache,
    loader::{metrics::MetricsLoader, FunctionLoader},
};
use data_types::{TableId, Tombstone};
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
}

/// Cache for tombstones for a particular table
#[derive(Debug)]
pub struct TombstoneCache {
    cache: Cache<TableId, CachedTombstones>,
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

        let cache = Cache::new(loader, backend);

        Self { cache }
    }

    /// Get list of cached tombstones, by table id
    pub async fn tombstones(&self, table_id: TableId) -> Vec<Arc<Tombstone>> {
        self.cache.get(table_id).await.into_inner()
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

        let cached_tombstones = cache.tombstones(table1.table.id).await;

        assert_eq!(cached_tombstones.len(), 1);
        assert_eq!(cached_tombstones[0].as_ref(), &tombstone1.tombstone);
    }

    // TODO tests for multiple tombestones
    // TODO tests for size

    // TODO tests for errors (table doesn't exist..)
}
