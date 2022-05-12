----------------------------------------
-- sequencer
ALTER TABLE
  IF EXISTS sequencer
  RENAME TO shard
;

ALTER SEQUENCE
  IF EXISTS sequencer_id_seq
  RENAME TO shard_id_seq
;

ALTER TABLE
  IF EXISTS shard
  RENAME CONSTRAINT sequencer_unique
  TO shard_unique
;

ALTER TABLE
  IF EXISTS shard
  RENAME CONSTRAINT sequencer_kafka_topic_id_fkey
  TO shard_kafka_topic_id_fkey
;

----------------------------------------
-- parquet_file
DO
$$
BEGIN
  IF EXISTS(
    SELECT *
    FROM information_schema.columns
    WHERE table_name = 'parquet_file'
    AND column_name = 'sequencer_id'
  ) THEN
    ALTER TABLE
    IF EXISTS parquet_file
    RENAME sequencer_id
    TO shard_id;
  END IF;
END
$$;

ALTER TABLE
  IF EXISTS parquet_file
  RENAME CONSTRAINT parquet_file_sequencer_id_fkey
  TO parquet_file_shard_id_fkey
;

ALTER INDEX
  IF EXISTS parquet_file_sequencer_compaction_delete_idx
  RENAME TO parquet_file_shard_compaction_delete_idx
;

----------------------------------------
-- partition
DO
$$
BEGIN
  IF EXISTS(
    SELECT *
    FROM information_schema.columns
    WHERE table_name = 'partition'
    AND column_name = 'sequencer_id'
  ) THEN
    ALTER TABLE
    IF EXISTS partition
    RENAME sequencer_id
    TO shard_id;
  END IF;
END
$$;

ALTER TABLE
  IF EXISTS partition
  RENAME CONSTRAINT partition_sequencer_id_fkey
  TO partition_shard_id_fkey
;

----------------------------------------
-- tombstone
DO
$$
BEGIN
  IF EXISTS(
    SELECT *
    FROM information_schema.columns
    WHERE table_name = 'tombstone'
    AND column_name = 'sequencer_id'
  ) THEN
    ALTER TABLE
    IF EXISTS tombstone
    RENAME sequencer_id
    TO shard_id;
  END IF;
END
$$;

ALTER TABLE
  IF EXISTS tombstone
  RENAME CONSTRAINT tombstone_sequencer_id_fkey
  TO tombstone_shard_id_fkey
;
