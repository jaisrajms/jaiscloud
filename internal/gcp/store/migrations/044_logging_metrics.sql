-- Cloud Logging (v2) logs-based metrics. Project-scoped and keyed by the
-- decoded client-assigned metric id (the id may contain slashes, e.g.
-- "nginx/requests"). label_extractors and descriptor are JSONB; bucket_options
-- is the opaque canonical-JSON google.api.Distribution.BucketOptions (NULL when
-- unset). create_time/update_time are server-assigned.
CREATE TABLE IF NOT EXISTS jc_log_metrics (
    project_id       TEXT        NOT NULL,
    name             TEXT        NOT NULL,
    description      TEXT        NOT NULL DEFAULT '',
    filter           TEXT        NOT NULL DEFAULT '',
    disabled         BOOLEAN     NOT NULL DEFAULT FALSE,
    bucket_name      TEXT        NOT NULL DEFAULT '',
    value_extractor  TEXT        NOT NULL DEFAULT '',
    label_extractors JSONB       NOT NULL DEFAULT '{}',
    bucket_options   JSONB,
    descriptor       JSONB       NOT NULL DEFAULT '{}',
    create_time      TIMESTAMPTZ NOT NULL,
    update_time      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, name)
);
