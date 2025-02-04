//! Helpers of the Compactor

use crate::query::QueryableParquetChunk;
use data_types::{
    ParquetFile, ParquetFileId, ParquetFileParams, PartitionId, TableSchema, Timestamp, Tombstone,
    TombstoneId,
};
use datafusion::physical_plan::SendableRecordBatchStream;
use observability_deps::tracing::*;
use parquet_file::{
    chunk::ParquetChunk,
    metadata::{IoxMetadata, IoxParquetMetaData},
    storage::ParquetStorage,
};
use schema::{sort::SortKey, Schema};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

/// Wrapper of a group of parquet files and their tombstones that overlap in time and should be
/// considered during compaction.
#[derive(Debug)]
pub struct GroupWithTombstones {
    /// Each file with the set of tombstones relevant to it
    pub(crate) parquet_files: Vec<ParquetFileWithTombstone>,
    /// All tombstones relevant to any of the files in the group
    pub(crate) tombstones: Vec<Tombstone>,
}

impl GroupWithTombstones {
    /// Return all tombstone ids
    pub fn tombstone_ids(&self) -> HashSet<TombstoneId> {
        self.tombstones.iter().map(|t| t.id).collect()
    }
}

/// Wrapper of group of parquet files with their min time and total size
#[derive(Debug, Clone, PartialEq)]
pub struct GroupWithMinTimeAndSize {
    /// Parquet files and their metadata
    pub(crate) parquet_files: Vec<ParquetFile>,

    /// min time of all parquet_files
    pub(crate) min_time: Timestamp,

    /// total size of all file
    pub(crate) total_file_size_bytes: i64,

    /// true if this group was split from a group of many overlapped files
    pub(crate) overlapped_with_other_groups: bool,
}

impl GroupWithMinTimeAndSize {
    /// Make GroupWithMinTimeAndSize for a given set of parquet files
    pub fn new(files: Vec<ParquetFile>, overlaped: bool) -> Self {
        let mut group = Self {
            parquet_files: files,
            min_time: Timestamp::new(i64::MAX),
            total_file_size_bytes: 0,
            overlapped_with_other_groups: overlaped,
        };

        assert!(
            !group.parquet_files.is_empty(),
            "invalid empty group for computing min time and total size"
        );

        for file in &group.parquet_files {
            group.min_time = group.min_time.min(file.min_time);
            group.total_file_size_bytes += file.file_size_bytes;
        }

        group
    }
}

/// Wrapper of a parquet file and its tombstones
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct ParquetFileWithTombstone {
    data: Arc<ParquetFile>,
    tombstones: Vec<Tombstone>,
}

impl std::ops::Deref for ParquetFileWithTombstone {
    type Target = Arc<ParquetFile>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl ParquetFileWithTombstone {
    /// Pair a [`ParquetFile`] with the specified [`Tombstone`] instances.
    pub fn new(data: Arc<ParquetFile>, tombstones: Vec<Tombstone>) -> Self {
        Self { data, tombstones }
    }

    /// Return all tombstone ids
    pub fn tombstone_ids(&self) -> HashSet<TombstoneId> {
        self.tombstones.iter().map(|t| t.id).collect()
    }

    /// Return true if there is no tombstone
    pub fn no_tombstones(&self) -> bool {
        self.tombstones.is_empty()
    }

    /// Return ID of this parquet file
    pub fn parquet_file_id(&self) -> ParquetFileId {
        self.data.id
    }

    /// Return the tombstones as a slice of [`Tombstone`].
    pub fn tombstones(&self) -> &[Tombstone] {
        &self.tombstones
    }

    /// Return all tombstones as a map keyed by tombstone ID.
    pub fn tombstone_map(&self) -> BTreeMap<TombstoneId, Tombstone> {
        self.tombstones
            .iter()
            .map(|ts| (ts.id, ts.clone()))
            .collect()
    }

    /// Add more tombstones
    pub fn add_tombstones(&mut self, tombstones: Vec<Tombstone>) {
        self.tombstones.extend(tombstones);
    }

    /// Convert to a QueryableParquetChunk
    pub fn to_queryable_parquet_chunk(
        &self,
        store: ParquetStorage,
        table_name: String,
        table_schema: &TableSchema,
        partition_sort_key: Option<SortKey>,
    ) -> QueryableParquetChunk {
        let column_id_lookup = table_schema.column_id_map();
        let selection: Vec<_> = self
            .column_set
            .iter()
            .flat_map(|id| column_id_lookup.get(id).copied())
            .collect();
        let table_schema: Schema = table_schema
            .clone()
            .try_into()
            .expect("table schema is broken");
        let schema = table_schema
            .select_by_names(&selection)
            .expect("schema in-sync");
        let pk = schema.primary_key();
        let sort_key = partition_sort_key.as_ref().map(|sk| sk.filter_to(&pk));

        let parquet_chunk = ParquetChunk::new(Arc::clone(&self.data), Arc::new(schema), store);

        trace!(
            parquet_file_id=?self.id,
            parquet_file_sequencer_id=?self.sequencer_id,
            parquet_file_namespace_id=?self.namespace_id,
            parquet_file_table_id=?self.table_id,
            parquet_file_partition_id=?self.partition_id,
            parquet_file_object_store_id=?self.object_store_id,
            "built parquet chunk from metadata"
        );

        QueryableParquetChunk::new(
            table_name,
            self.data.partition_id,
            Arc::new(parquet_chunk),
            &self.tombstones,
            self.data.min_sequence_number,
            self.data.max_sequence_number,
            self.data.min_time,
            self.data.max_time,
            sort_key,
            partition_sort_key,
        )
    }
}

/// Struct holding output of a compacted stream
pub struct CompactedData {
    pub(crate) data: SendableRecordBatchStream,
    pub(crate) meta: IoxMetadata,
    pub(crate) tombstones: BTreeMap<TombstoneId, Tombstone>,
}

impl CompactedData {
    /// Initialize compacted data
    pub fn new(
        data: SendableRecordBatchStream,
        meta: IoxMetadata,
        tombstones: BTreeMap<TombstoneId, Tombstone>,
    ) -> Self {
        Self {
            data,
            meta,
            tombstones,
        }
    }
}

/// Information needed to update the catalog after compacting a group of files
#[derive(Debug)]
pub struct CatalogUpdate {
    #[allow(dead_code)]
    pub(crate) meta: IoxMetadata,
    pub(crate) tombstones: BTreeMap<TombstoneId, Tombstone>,
    pub(crate) parquet_file: ParquetFileParams,
}

impl CatalogUpdate {
    /// Initialize with data received from a persist to object storage
    pub fn new(
        partition_id: PartitionId,
        meta: IoxMetadata,
        file_size: usize,
        md: IoxParquetMetaData,
        tombstones: BTreeMap<TombstoneId, Tombstone>,
        table_schema: &TableSchema,
    ) -> Self {
        let parquet_file = meta.to_parquet_file(partition_id, file_size, &md, |name| {
            table_schema.columns.get(name).expect("unknown column").id
        });
        Self {
            meta,
            tombstones,
            parquet_file,
        }
    }
}
