-- 068_output_only_fields: output-only timestamps the emulator previously
-- omitted, tracked as AUD6-5.
--
-- Cloud Datastore: EntityResult.create_time (and MutationResult.create_time).
-- Real Datastore records the entity's creation time once and preserves it
-- across updates; the emulator carried only update_time.
--
-- Cloud Logging: LogEntry.receive_timestamp (output only) — the time Logging
-- received the entry, stamped at write and returned on reads.
--
-- The columns are added nullable and backfilled from the closest existing
-- timestamp (create_time from the row's update_time; receive_timestamp from
-- the entry's timestamp) rather than from now(), so a backfilled create_time
-- can never sit after the row's update_time (AUD6-5 review).

ALTER TABLE jc_datastore_entities ADD COLUMN IF NOT EXISTS create_time TIMESTAMPTZ;
UPDATE jc_datastore_entities SET create_time = update_time WHERE create_time IS NULL;
ALTER TABLE jc_datastore_entities ALTER COLUMN create_time SET NOT NULL;
ALTER TABLE jc_datastore_entities ALTER COLUMN create_time SET DEFAULT now();

ALTER TABLE jc_log_entries ADD COLUMN IF NOT EXISTS receive_timestamp TIMESTAMPTZ;
UPDATE jc_log_entries SET receive_timestamp = timestamp WHERE receive_timestamp IS NULL;
ALTER TABLE jc_log_entries ALTER COLUMN receive_timestamp SET NOT NULL;
ALTER TABLE jc_log_entries ALTER COLUMN receive_timestamp SET DEFAULT now();
