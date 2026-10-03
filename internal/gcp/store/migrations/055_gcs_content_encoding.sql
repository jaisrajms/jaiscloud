-- GCS object content encoding (Object.contentEncoding, e.g. "gzip"). Distinct
-- from HTTP transport encoding; surfaces as x-goog-stored-content-encoding on
-- the XML API and contentEncoding on the JSON API (J69).
ALTER TABLE jc_gcs_objects ADD COLUMN IF NOT EXISTS content_encoding TEXT NOT NULL DEFAULT '';
