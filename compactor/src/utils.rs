//! Helpers of the Compactor

use arrow::record_batch::RecordBatch;
use data_types2::{
    FileMeta, ParquetFileParams, ParquetFileWithTombstone, PartitionId, SequencerId, TableId,
    Timestamp, Tombstone, TombstoneId,
};
use parquet_file::metadata::{IoxMetadata, IoxParquetMetaData};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

/// Wrapper of a group of parquet files and their tombstones that overlap in time and should be
/// considered during compaction.
pub struct GroupWithTombstones {
    /// Each file with the set of tombstones relevant to it
    pub(crate) parquet_files: Vec<ParquetFileWithTombstone>,
    /// All tombstones relevant to any of the files in the group
    pub(crate) tombstones: Vec<Tombstone>,
}

impl GroupWithTombstones {
    /// create from parquet files with tombstones
    pub fn new_from_parquet_files_with_tombstones(
        parquet_files: Vec<ParquetFileWithTombstone>,
    ) -> Self {
        let mut result = Self {
            parquet_files,
            tombstones: vec![],
        };

        // Build tombstones
        for file in &result.parquet_files {
            let tss = &file.tombstones;
            let to_add = tss
                .iter()
                .filter(|ts| !result.tombstones.iter().any(|t| t.id == ts.id))
                .cloned()
                .collect::<Vec<_>>();

            result.tombstones.extend(to_add);
        }

        result
    }

    /// Return all tombstone ids
    pub fn tombstone_ids(&self) -> HashSet<TombstoneId> {
        self.tombstones.iter().map(|t| t.id).collect()
    }
}

/// Wrapper of group of parquet files with their min time and total size
#[derive(Debug, Clone)]
pub struct GroupWithMinTimeAndSize {
    /// Parquet files
    pub(crate) parquet_files: Vec<Arc<dyn FileMeta>>,

    /// min time of all parquet_files
    pub(crate) min_time: Timestamp,

    /// total size of all file
    pub(crate) total_file_size_bytes: i64,
}

impl GroupWithMinTimeAndSize {
    /// Return true if the group inc;ude the given file
    pub fn contains(&self, file: &Arc<dyn FileMeta>) -> bool {
        self.parquet_files
            .iter()
            .any(|f| f.parquet_file_id() == file.parquet_file_id())
    }
}

impl PartialEq for GroupWithMinTimeAndSize {
    fn eq(&self, other: &Self) -> bool {
        if self.parquet_files.len() != other.parquet_files.len()
            || self.min_time != other.min_time
            || self.total_file_size_bytes != other.total_file_size_bytes
        {
            return false;
        }

        for f in &other.parquet_files {
            if !self.contains(f) {
                return false;
            }
        }
        true
    }
}

/// Struct holding output of a compacted stream
pub struct CompactedData {
    pub(crate) data: Vec<RecordBatch>,
    pub(crate) meta: IoxMetadata,
    pub(crate) tombstones: BTreeMap<TombstoneId, Tombstone>,
}

impl CompactedData {
    /// Initialize compacted data
    pub fn new(
        data: Vec<RecordBatch>,
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
    pub(crate) meta: IoxMetadata,
    pub(crate) tombstones: BTreeMap<TombstoneId, Tombstone>,
    pub(crate) parquet_file: ParquetFileParams,
}

impl CatalogUpdate {
    /// Initialize with data received from a persist to object storage
    pub fn new(
        meta: IoxMetadata,
        file_size: usize,
        md: IoxParquetMetaData,
        tombstones: BTreeMap<TombstoneId, Tombstone>,
    ) -> Self {
        let parquet_file = meta.to_parquet_file(file_size, &md);
        Self {
            meta,
            tombstones,
            parquet_file,
        }
    }
}

/// Partition with all tombstones-attched parquet files to be compacted
#[derive(Debug)]
pub struct PartitionWithParquetFiles {
    pub(crate) sequencer_id: SequencerId,
    pub(crate) table_id: TableId,
    pub(crate) partition_id: PartitionId,
    pub(crate) files_with_tombstones: Vec<ParquetFileWithTombstone>,
}
