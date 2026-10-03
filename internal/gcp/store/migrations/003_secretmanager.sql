-- Secret Manager secret metadata and versions (mirrors AWS jc_sm_secrets +
-- jc_sm_versions). Secret IDs are project-scoped, hence the project_id column.
CREATE TABLE IF NOT EXISTS jc_sm_secrets (
    project_id  TEXT        NOT NULL,
    secret_id   TEXT        NOT NULL,
    labels      JSONB       NOT NULL DEFAULT '{}',
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_ver    INT         NOT NULL DEFAULT 1,
    PRIMARY KEY (project_id, secret_id)
);

CREATE TABLE IF NOT EXISTS jc_sm_versions (
    project_id  TEXT        NOT NULL,
    secret_id   TEXT        NOT NULL,
    version_id  TEXT        NOT NULL,
    state       TEXT        NOT NULL DEFAULT 'ENABLED',
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    data        TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (project_id, secret_id, version_id)
);
