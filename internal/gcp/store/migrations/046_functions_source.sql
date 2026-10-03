-- Cloud Functions source archive persistence: the deployed zip's revision
-- hash, size, and blobfs key (the "functions-source" namespace). Mirrors the
-- AWS Lambda code metadata columns added to the resource JSONB, but Cloud
-- Functions uses a dedicated table so the bytes live in blobfs and only the
-- pointer/revision live here. Existing rows default to "no source".
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS source_sha256    TEXT   NOT NULL DEFAULT '';
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS source_size      BIGINT NOT NULL DEFAULT 0;
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS source_blob_key  TEXT   NOT NULL DEFAULT '';
