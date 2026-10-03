-- 012_gcs_depth: GCS "depth" features — object versioning (generations),
-- object retention, and bucket lifecycle.
--
-- Bucket-level versioning/retentionPolicy/lifecycle live in the existing
-- jc_gcs_buckets.meta JSONB map (they round-trip through the meta column), so
-- no new bucket columns are required. Object versioning, by contrast, needs the
-- objects table to hold more than one generation per (bucket, name), so the
-- primary key widens to include generation and new retention/hold columns are
-- added.

-- Versioning: one row per generation. A non-live generation carries time_deleted.
ALTER TABLE jc_gcs_objects DROP CONSTRAINT jc_gcs_objects_pkey;
ALTER TABLE jc_gcs_objects ADD PRIMARY KEY (bucket, name, generation);

DROP INDEX IF EXISTS idx_jc_gcs_objects_bucket;
CREATE INDEX IF NOT EXISTS idx_jc_gcs_objects_bucket
    ON jc_gcs_objects (bucket, name, generation);

-- Object retention: retain_until is Object.retention.retainUntilTime;
-- retention_mode is Unlocked/Locked.
ALTER TABLE jc_gcs_objects ADD COLUMN IF NOT EXISTS retain_until TIMESTAMPTZ;
ALTER TABLE jc_gcs_objects ADD COLUMN IF NOT EXISTS retention_mode TEXT NOT NULL DEFAULT '';
ALTER TABLE jc_gcs_objects ADD COLUMN IF NOT EXISTS temporary_hold BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE jc_gcs_objects ADD COLUMN IF NOT EXISTS event_based_hold BOOLEAN NOT NULL DEFAULT FALSE;
-- time_deleted marks a non-live generation (Object.timeDeleted).
ALTER TABLE jc_gcs_objects ADD COLUMN IF NOT EXISTS time_deleted TIMESTAMPTZ;
