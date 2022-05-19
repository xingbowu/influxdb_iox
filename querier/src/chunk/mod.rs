//! Querier Chunk

use crate::cache::CatalogCache;
use arrow::record_batch::RecordBatch;
use data_types::{
    ChunkId, ChunkOrder, DeletePredicate, ParquetFileId, PartitionId, SequenceNumber, SequencerId,
    TimestampMinMax,
};
use futures::StreamExt;
use iox_catalog::interface::Catalog;
use iox_query::{exec::IOxSessionContext, QueryChunk};
use iox_time::TimeProvider;
use parquet_file::{
    chunk::{ChunkMetrics as ParquetChunkMetrics, DecodedParquetFile, ParquetChunk},
    storage::ParquetStorage,
};
use schema::{selection::Selection, sort::SortKey};
use std::sync::Arc;
use uuid::Uuid;

mod query_access;

/// Immutable metadata attached to a [`QuerierChunk`].
#[derive(Debug)]
pub struct ChunkMeta {
    /// The ID of the chunk
    chunk_id: ChunkId,

    /// Table name
    table_name: Arc<str>,

    /// Chunk order.
    order: ChunkOrder,

    /// Sort key.
    sort_key: Option<SortKey>,

    /// Partition sort key
    partition_sort_key: Option<SortKey>,

    /// Sequencer that created the data within this chunk.
    sequencer_id: SequencerId,

    /// Partition ID.
    partition_id: PartitionId,

    /// The minimum sequence number within this chunk.
    min_sequence_number: SequenceNumber,

    /// The maximum sequence number within this chunk.
    max_sequence_number: SequenceNumber,
}

impl ChunkMeta {
    /// Chunk order.
    pub fn order(&self) -> ChunkOrder {
        self.order
    }

    /// Sort key.
    pub fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    /// Partition sort key
    pub fn partition_sort_key(&self) -> Option<&SortKey> {
        self.partition_sort_key.as_ref()
    }

    /// Sequencer that created the data within this chunk.
    pub fn sequencer_id(&self) -> SequencerId {
        self.sequencer_id
    }

    /// Partition ID.
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// The minimum sequence number within this chunk.
    pub fn min_sequence_number(&self) -> SequenceNumber {
        self.min_sequence_number
    }

    /// The maximum sequence number within this chunk.
    pub fn max_sequence_number(&self) -> SequenceNumber {
        self.max_sequence_number
    }
}

/// Determines how the chunk data is currently accessible.
#[derive(Debug)]
pub enum ChunkStorage {
    /// Data is currently available via parquet file within the object store.
    Parquet {
        /// ID of the parquet file if the chunk
        parquet_file_id: ParquetFileId,
        /// Chunk of the parquet file
        chunk: Arc<ParquetChunk>,
    },
}

/// Chunk representation for the querier.
///
/// These chunks are usually created on-demand. The querier cache system does not really have a notion of chunks (rather
/// it knows about parquet files, local FS caches, ingester data, cached read buffers) but we need to combine all that
/// knowledge into chunk objects because this is what the query engine (DataFusion and InfluxRPC) expect.
#[derive(Debug)]
pub struct QuerierChunk {
    /// How the data is currently structured / available for query.
    storage: ChunkStorage,

    /// Immutable metadata.
    meta: Arc<ChunkMeta>,

    /// Delete predicates of this chunk
    delete_predicates: Vec<Arc<DeletePredicate>>,
}

impl QuerierChunk {
    /// Create new parquet-backed chunk (object store data).
    pub fn new_parquet(
        parquet_file_id: ParquetFileId,
        chunk: Arc<ParquetChunk>,
        meta: Arc<ChunkMeta>,
    ) -> Self {
        Self {
            storage: ChunkStorage::Parquet {
                parquet_file_id,
                chunk,
            },
            meta,
            delete_predicates: Vec::new(),
        }
    }

    /// Set delete predicates of the given chunk.
    pub fn with_delete_predicates(self, delete_predicates: Vec<Arc<DeletePredicate>>) -> Self {
        Self {
            storage: self.storage,
            meta: self.meta,
            delete_predicates,
        }
    }

    /// Get metadata attached to the given chunk.
    pub fn meta(&self) -> &ChunkMeta {
        self.meta.as_ref()
    }

    /// Parquet file ID if this chunk is backed by a parquet file.
    pub fn parquet_file_id(&self) -> Option<ParquetFileId> {
        match &self.storage {
            ChunkStorage::Parquet {
                parquet_file_id, ..
            } => Some(*parquet_file_id),
        }
    }

    /// Return time range
    pub fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        match &self.storage {
            ChunkStorage::Parquet { chunk, .. } => chunk.timestamp_min_max(),
        }
    }
}

/// Adapter that can create chunks.
#[derive(Debug)]
pub struct ParquetChunkAdapter {
    /// Cache
    catalog_cache: Arc<CatalogCache>,

    /// Object store.
    store: ParquetStorage,

    /// Metric registry.
    metric_registry: Arc<metric::Registry>,

    /// Time provider.
    #[allow(dead_code)]
    time_provider: Arc<dyn TimeProvider>,
}

impl ParquetChunkAdapter {
    /// Create new adapter with empty cache.
    pub fn new(
        catalog_cache: Arc<CatalogCache>,
        store: ParquetStorage,
        metric_registry: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            catalog_cache,
            store,
            metric_registry,
            time_provider,
        }
    }

    /// Get underlying catalog cache.
    pub fn catalog_cache(&self) -> &Arc<CatalogCache> {
        &self.catalog_cache
    }

    /// Get underlying catalog.
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        self.catalog_cache.catalog()
    }

    /// Create parquet chunk.
    ///
    /// Returns `None` if some data required to create this chunk is already gone from the catalog.
    async fn new_parquet_chunk(
        &self,
        decoded_parquet_file: &DecodedParquetFile,
    ) -> Option<ParquetChunk> {
        let metrics = ParquetChunkMetrics::new(self.metric_registry.as_ref());

        Some(ParquetChunk::new(
            decoded_parquet_file,
            metrics,
            self.store.clone(),
        ))
    }

    /// Create new querier chunk.
    ///
    /// Returns `None` if some data required to create this chunk is already gone from the catalog.
    pub async fn new_querier_chunk(
        &self,
        decoded_parquet_file: &DecodedParquetFile,
    ) -> Option<QuerierChunk> {
        //let decoded_parquet_file = DecodedParquetFile::new(parquet_file_with_metadata);
        let parquet_file = &decoded_parquet_file.parquet_file;
        let chunk = Arc::new(self.new_parquet_chunk(decoded_parquet_file).await?);
        let chunk_id = ChunkId::from(Uuid::from_u128(parquet_file.id.get() as _));
        let table_name = self
            .catalog_cache
            .table()
            .name(parquet_file.table_id)
            .await?;

        let iox_metadata = &decoded_parquet_file.iox_metadata;

        // Somewhat hacky workaround because of implicit chunk orders, use min sequence number and
        // hope it doesn't overflow u32. Order is non-zero, se we need to add 1.
        let order = ChunkOrder::new(1 + iox_metadata.min_sequence_number.get() as u32)
            .expect("cannot be zero");

        // Read partition sort key
        let partition_sort_key = self
            .catalog_cache()
            .partition()
            .sort_key(iox_metadata.partition_id)
            .await;

        let meta = Arc::new(ChunkMeta {
            chunk_id,
            table_name,
            order,
            sort_key: iox_metadata.sort_key.clone(),
            partition_sort_key,
            sequencer_id: iox_metadata.sequencer_id,
            partition_id: iox_metadata.partition_id,
            min_sequence_number: parquet_file.min_sequence_number,
            max_sequence_number: parquet_file.max_sequence_number,
        });

        Some(QuerierChunk::new_parquet(parquet_file.id, chunk, meta))
    }
}

/// collect data for the given chunk
pub async fn collect_read_filter(chunk: &QuerierChunk) -> Vec<RecordBatch> {
    chunk
        .read_filter(
            IOxSessionContext::default(),
            &Default::default(),
            Selection::All,
        )
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect()
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use arrow_util::assert_batches_eq;
    use iox_query::QueryChunkMeta;
    use iox_tests::util::TestCatalog;
    use schema::{builder::SchemaBuilder, sort::SortKeyBuilder};

    #[tokio::test]
    async fn test_create_record() {
        let catalog = TestCatalog::new();

        let adapter = ParquetChunkAdapter::new(
            Arc::new(CatalogCache::new(
                catalog.catalog(),
                catalog.time_provider(),
                catalog.metric_registry(),
                usize::MAX,
            )),
            ParquetStorage::new(catalog.object_store()),
            catalog.metric_registry(),
            catalog.time_provider(),
        );

        // set up catalog
        let lp = vec![
            "table,tag1=WA field_int=1000i 8000",
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");
        let ns = catalog.create_namespace("ns").await;
        let sequencer = ns.create_sequencer(1).await;
        let table = ns.create_table("table").await;
        let parquet_file = table
            .with_sequencer(&sequencer)
            .create_partition("part")
            .await
            .create_parquet_file(&lp)
            .await
            .parquet_file;

        // create chunk
        let chunk = adapter
            .new_querier_chunk(&DecodedParquetFile::new(parquet_file))
            .await
            .unwrap();

        // check chunk schema
        let expected_schema = SchemaBuilder::new()
            .field("field_int", DataType::Int64)
            .tag("tag1")
            .timestamp()
            .build()
            .unwrap();
        let actual_schema = chunk.schema();
        assert_eq!(actual_schema.as_ref(), &expected_schema);

        // check sort key
        let expected_sort_key = SortKeyBuilder::new()
            .with_col("tag1")
            .with_col("time")
            .build();
        let actual_sort_key = chunk.sort_key().unwrap();
        assert_eq!(actual_sort_key, &expected_sort_key);

        // check if chunk can be queried
        let batches = collect_read_filter(&chunk).await;
        assert_batches_eq!(
            &[
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
                "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
                "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
                "+-----------+------+-----------------------------+",
            ],
            &batches
        );
    }
}
