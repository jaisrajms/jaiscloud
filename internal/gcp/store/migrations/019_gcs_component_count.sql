-- 019_gcs_component_count: GCS composite objects (objects.compose). The number
-- of source objects concatenated into a composite object is persisted as
-- Object.componentCount.
ALTER TABLE jc_gcs_objects ADD COLUMN IF NOT EXISTS component_count BIGINT NOT NULL DEFAULT 0;
