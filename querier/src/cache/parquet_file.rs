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
use data_types::{TableId, ParquetFileWithMetadata};
use iox_catalog::interface::Catalog;
use iox_time::TimeProvider;
use parquet_file::chunk::DecodedParquetFile;
use std::{collections::HashMap, mem::size_of_val, sync::Arc, time::Duration};
use snafu::{Snafu, ResultExt};

use super::ram::RamSize;

/// Duration to keep parquet files cached
pub const TTL: Duration = Duration::from_secs(60);


const CACHE_ID: &str = "parquet_file";

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("{}", source))]
    Catalog { source: iox_catalog::interface::Error },
}

/// A specialized `Error` for errors (needed to make Backoff happy for some reason)
pub type Result<T, E = Error> = std::result::Result<T, E>;


#[derive(Debug)]
/// Holds decoded catalog information about a parquet file
pub struct CachedParquetFile {
    file: DecodedParquetFile
}

impl CachedParquetFile {
    fn new(parquet_file_with_metadata: ParquetFileWithMetadata) -> Self {
        Self {
            file: DecodedParquetFile::new(parquet_file_with_metadata)
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
                let parquet_files = Backoff::new(&backoff_config)
                    .retry_all_errors("get parquet_files", || async {

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
                    .expect("retry forever");

                parquet_files
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

        let cache = Cache::new(loader, backend);

        Self { cache }
    }

    /// Get list of cached parquet files, by table id
    pub async fn files(&self, table_id: TableId) ->  Vec<Arc<CachedParquetFile>> {
        self.cache.get(table_id).await
    }
}

#[cfg(test)]
mod tests {


}
