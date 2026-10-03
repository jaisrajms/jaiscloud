-- Cloud Workflows (management workflows + executions + operations). Workflows
-- are project+location scoped with the canonical name
-- projects/{project}/locations/{location}/workflows/{workflow}; executions nest
-- under their workflow. source_contents (YAML), argument, and result are JSON
-- string payloads kept byte-for-byte verbatim in TEXT columns; the structured
-- fields (labels, error, current_steps) are JSONB.

CREATE TABLE IF NOT EXISTS jc_workflows (
    project_id       TEXT        NOT NULL,
    location         TEXT        NOT NULL,
    workflow_id      TEXT        NOT NULL,
    description      TEXT        NOT NULL DEFAULT '',
    labels           JSONB       NOT NULL DEFAULT '{}',
    service_account  TEXT        NOT NULL DEFAULT '',
    source_contents  TEXT        NOT NULL DEFAULT '',
    state            TEXT        NOT NULL DEFAULT 'ACTIVE',
    revision_id      TEXT        NOT NULL DEFAULT '',
    create_time      TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time      TIMESTAMPTZ NOT NULL DEFAULT now(),
    call_log_level   TEXT        NOT NULL DEFAULT '',
    user_env_vars    JSONB       NOT NULL DEFAULT '{}',
    tags             JSONB       NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, location, workflow_id)
);

CREATE TABLE IF NOT EXISTS jc_workflow_executions (
    project_id            TEXT        NOT NULL,
    location              TEXT        NOT NULL,
    workflow_id           TEXT        NOT NULL,
    execution_id          TEXT        NOT NULL,
    state                 TEXT        NOT NULL DEFAULT 'ACTIVE',
    argument              TEXT        NOT NULL DEFAULT '',
    result                TEXT        NOT NULL DEFAULT '',
    error                 JSONB,
    start_time            TIMESTAMPTZ,
    end_time              TIMESTAMPTZ,
    duration              TEXT        NOT NULL DEFAULT '',
    workflow_revision_id  TEXT        NOT NULL DEFAULT '',
    call_log_level        TEXT        NOT NULL DEFAULT '',
    labels                JSONB       NOT NULL DEFAULT '{}',
    current_steps         JSONB       NOT NULL DEFAULT '[]',
    PRIMARY KEY (project_id, location, workflow_id, execution_id)
);

CREATE INDEX IF NOT EXISTS idx_jc_workflow_executions_start
    ON jc_workflow_executions (project_id, location, workflow_id, start_time DESC);

CREATE TABLE IF NOT EXISTS jc_workflow_operations (
    project_id   TEXT        NOT NULL,
    location     TEXT        NOT NULL,
    operation_id TEXT        NOT NULL,
    done         BOOL        NOT NULL DEFAULT TRUE,
    response     TEXT        NOT NULL DEFAULT '',
    verb         TEXT        NOT NULL DEFAULT '',
    target       TEXT        NOT NULL DEFAULT '',
    create_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    end_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, operation_id)
);
