-------------------------------------------
-- table parquet_file:

-- add new foreign key columns
ALTER TABLE
  IF EXISTS parquet_file
  ADD COLUMN kafka_topic_id BIGINT,
  ADD COLUMN kafka_partition INT;

-- insert foreign key values
UPDATE parquet_file
SET (kafka_topic_id, kafka_partition) =
  (SELECT kafka_topic_id, kafka_partition
   FROM sequencer
   WHERE parquet_file.sequencer_id = sequencer.id);

-- update constraints
ALTER TABLE
  IF EXISTS parquet_file
  ALTER COLUMN kafka_topic_id SET NOT NULL,
  ALTER COLUMN kafka_partition SET NOT NULL,
  ADD FOREIGN KEY (kafka_topic_id, kafka_partition)
      REFERENCES sequencer (kafka_topic_id, kafka_partition),
  DROP CONSTRAINT IF EXISTS parquet_file_sequencer_id_fkey,
  DROP COLUMN IF EXISTS sequencer_id;

-------------------------------------------
-- table partition:

-- add new foreign key columns
ALTER TABLE
  IF EXISTS partition
  ADD COLUMN kafka_topic_id BIGINT,
  ADD COLUMN kafka_partition INT;

-- insert foreign key values
UPDATE partition
SET (kafka_topic_id, kafka_partition) =
  (SELECT kafka_topic_id, kafka_partition
   FROM sequencer
   WHERE partition.sequencer_id = sequencer.id);

-- update constraints
ALTER TABLE
  IF EXISTS partition
  ALTER COLUMN kafka_topic_id SET NOT NULL,
  ALTER COLUMN kafka_partition SET NOT NULL,
  ADD FOREIGN KEY (kafka_topic_id, kafka_partition)
      REFERENCES sequencer (kafka_topic_id, kafka_partition),
  DROP CONSTRAINT IF EXISTS partition_sequencer_id_fkey,
  DROP COLUMN IF EXISTS sequencer_id;

-------------------------------------------
-- table tombstone:

-- add new foreign key columns
ALTER TABLE
  IF EXISTS tombstone
  ADD COLUMN kafka_topic_id BIGINT,
  ADD COLUMN kafka_partition INT;

-- insert foreign key values
UPDATE tombstone
SET (kafka_topic_id, kafka_partition) =
  (SELECT kafka_topic_id, kafka_partition
   FROM sequencer
   WHERE tombstone.sequencer_id = sequencer.id);

-- update constraints
ALTER TABLE
  IF EXISTS tombstone
  ALTER COLUMN kafka_topic_id SET NOT NULL,
  ALTER COLUMN kafka_partition SET NOT NULL,
  ADD FOREIGN KEY (kafka_topic_id, kafka_partition)
      REFERENCES sequencer (kafka_topic_id, kafka_partition),
  DROP CONSTRAINT IF EXISTS tombstone_sequencer_id_fkey,
  DROP COLUMN IF EXISTS sequencer_id;
