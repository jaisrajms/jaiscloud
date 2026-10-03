-- Secret Manager version destroy timestamp: output-only, present only when the
-- version state is DESTROYED.
ALTER TABLE jc_sm_versions ADD COLUMN IF NOT EXISTS destroy_time TIMESTAMPTZ;
