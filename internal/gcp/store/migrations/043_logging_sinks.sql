-- Cloud Logging (v2) config plane: sinks (export routes) and resource-level
-- exclusions. Both are project-scoped and keyed by their short client-assigned
-- id. A sink's inline exclusion filters are stored as a JSONB array; its
-- create/update timestamps are server-assigned.
CREATE TABLE IF NOT EXISTS jc_log_sinks (
    project_id       TEXT        NOT NULL,
    name             TEXT        NOT NULL,
    destination      TEXT        NOT NULL DEFAULT '',
    filter           TEXT        NOT NULL DEFAULT '',
    description      TEXT        NOT NULL DEFAULT '',
    disabled         BOOLEAN     NOT NULL DEFAULT FALSE,
    exclusions       JSONB       NOT NULL DEFAULT '[]',
    writer_identity  TEXT        NOT NULL DEFAULT '',
    include_children BOOLEAN     NOT NULL DEFAULT FALSE,
    create_time      TIMESTAMPTZ NOT NULL,
    update_time      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, name)
);

CREATE TABLE IF NOT EXISTS jc_log_exclusions (
    project_id  TEXT        NOT NULL,
    name        TEXT        NOT NULL,
    description TEXT        NOT NULL DEFAULT '',
    filter      TEXT        NOT NULL DEFAULT '',
    disabled    BOOLEAN     NOT NULL DEFAULT FALSE,
    create_time TIMESTAMPTZ NOT NULL,
    update_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, name)
);
