-- GCS soft delete: a generation removed by objects.delete while the bucket's
-- soft delete policy is in force stays restorable for the policy window.
-- soft_deleted distinguishes it from a noncurrent (archived) version, which
-- objects.restore rejects (412 objectNotSoftDeleted).
ALTER TABLE jc_gcs_objects ADD COLUMN IF NOT EXISTS soft_deleted BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE jc_gcs_objects ADD COLUMN IF NOT EXISTS hard_delete_time TIMESTAMPTZ;
