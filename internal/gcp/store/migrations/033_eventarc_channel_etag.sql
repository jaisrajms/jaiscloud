-- Eventarc channels gained an output-only etag used for optimistic
-- concurrency control (etag OCC): the provider recomputes a deterministic
-- content checksum on every create/update and rejects a stale etag on
-- patch/delete. The original 032_eventarc.sql table predates that field, so
-- add it here rather than editing an applied migration.
ALTER TABLE jc_eventarc_channels
    ADD COLUMN IF NOT EXISTS etag TEXT NOT NULL DEFAULT '';
