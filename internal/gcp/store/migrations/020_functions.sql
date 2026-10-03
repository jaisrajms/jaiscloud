-- Cloud Functions v1 metadata. Functions are project+location scoped; the
-- canonical resource name is
-- projects/{project}/locations/{location}/functions/{function_id}.
CREATE TABLE IF NOT EXISTS jc_functions (
    project_id            TEXT        NOT NULL,
    location              TEXT        NOT NULL,
    function_id           TEXT        NOT NULL,
    runtime               TEXT        NOT NULL DEFAULT '',
    entry_point           TEXT        NOT NULL DEFAULT '',
    source_upload_url     TEXT        NOT NULL DEFAULT '',
    source_archive_url    TEXT        NOT NULL DEFAULT '',
    https_trigger_url     TEXT        NOT NULL DEFAULT '',
    event_trigger         JSONB,
    environment_variables JSONB       NOT NULL DEFAULT '{}',
    status                TEXT        NOT NULL DEFAULT 'ACTIVE',
    create_time           TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time           TIMESTAMPTZ NOT NULL DEFAULT now(),
    labels                JSONB       NOT NULL DEFAULT '{}',
    available_memory_mb   INT         NOT NULL DEFAULT 256,
    timeout               TEXT        NOT NULL DEFAULT '60s',
    description           TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (project_id, location, function_id)
);
