-- GCS bucket metadata (control-plane; bucket names are globally unique).
CREATE TABLE IF NOT EXISTS jc_gcs_buckets (
    name          TEXT        PRIMARY KEY,
    project_id    TEXT        NOT NULL,
    location      TEXT        NOT NULL DEFAULT 'US',
    storage_class TEXT        NOT NULL DEFAULT 'STANDARD',
    meta          JSONB       NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- GCS object metadata (data plane). Single-generation for now: an insert
-- overwrites the prior object. A later migration adds versioned generations
-- when multi-generation retention is implemented.
CREATE TABLE IF NOT EXISTS jc_gcs_objects (
    bucket         TEXT        NOT NULL,
    name           TEXT        NOT NULL,
    generation     TEXT        NOT NULL,
    metageneration TEXT        NOT NULL DEFAULT '1',
    content_type   TEXT        NOT NULL DEFAULT 'application/octet-stream',
    size           BIGINT      NOT NULL DEFAULT 0,
    md5_hash       TEXT        NOT NULL DEFAULT '',
    crc32c         TEXT        NOT NULL DEFAULT '',
    storage_class  TEXT        NOT NULL DEFAULT 'STANDARD',
    metadata       JSONB       NOT NULL DEFAULT '{}',
    time_created   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bucket, name)
);

CREATE INDEX IF NOT EXISTS idx_jc_gcs_objects_bucket
    ON jc_gcs_objects (bucket, name);

-- Resumable upload sessions (GCS resumable uploads). The upload body spills to
-- tmp_path once it exceeds the in-memory threshold.
CREATE TABLE IF NOT EXISTS jc_gcs_resumable_sessions (
    upload_id    TEXT        PRIMARY KEY,
    bucket       TEXT        NOT NULL,
    name         TEXT        NOT NULL,
    content_type TEXT        NOT NULL DEFAULT '',
    length       BIGINT      NOT NULL DEFAULT 0,
    tmp_path     TEXT        NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_access  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jc_gcs_resumable_last_access
    ON jc_gcs_resumable_sessions (last_access);
