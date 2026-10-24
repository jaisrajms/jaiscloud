-- Secret Manager delayed version destruction and secret expiration (DC70):
-- `versionDestroyTtl` delays destruction (destroy -> DISABLED with a
-- scheduled_destroy_time, then DESTROYED), and `expireTime`/`ttl` schedules the
-- secret's automatic deletion (which emits SECRET_DELETE deleteType
-- EXPIRATION).
-- version_destroy_ttl is NOT NULL DEFAULT '' so every existing row scans into
-- the Go string field; the backfill + SET NOT NULL also repairs a database that
-- received an earlier nullable form of this column.
ALTER TABLE jc_sm_secrets ADD COLUMN IF NOT EXISTS version_destroy_ttl TEXT NOT NULL DEFAULT '';
UPDATE jc_sm_secrets SET version_destroy_ttl = '' WHERE version_destroy_ttl IS NULL;
ALTER TABLE jc_sm_secrets ALTER COLUMN version_destroy_ttl SET NOT NULL;
ALTER TABLE jc_sm_secrets ADD COLUMN IF NOT EXISTS expire_time TIMESTAMPTZ;
ALTER TABLE jc_sm_versions ADD COLUMN IF NOT EXISTS scheduled_destroy_time TIMESTAMPTZ;
