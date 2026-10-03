-- BigQuery resources. Resources are project-scoped; datasets
-- (projects/{project}/datasets/{id}), tables (…/datasets/{id}/tables/{tid}),
-- and jobs (projects/{project}/jobs/{id}) are stored records. The full wire
-- request body is stored verbatim as JSONB (config), with dataset/table labels
-- and the table schema extracted as structured JSONB. tabledata.insertAll
-- streams rows into jc_bq_rows (one row per JSONB object, ordered by a
-- per-table seq); tabledata.list reads them back. Query jobs are evaluated by
-- the in-process SQL engine (internal/gcp/queryengine, BQ1/BQ2) and persist
-- their statement statistics; jobs are reported DONE inline (synchronous LROs).

CREATE TABLE IF NOT EXISTS jc_bq_datasets (
    project_id  TEXT        NOT NULL,
    dataset_id  TEXT        NOT NULL,
    config      JSONB       NOT NULL DEFAULT '{}',
    labels      JSONB       NOT NULL DEFAULT '{}',
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, dataset_id)
);

CREATE TABLE IF NOT EXISTS jc_bq_tables (
    project_id  TEXT        NOT NULL,
    dataset_id  TEXT        NOT NULL,
    table_id    TEXT        NOT NULL,
    config      JSONB       NOT NULL DEFAULT '{}',
    schema      JSONB       NOT NULL DEFAULT '{}',
    labels      JSONB       NOT NULL DEFAULT '{}',
    num_rows    BIGINT      NOT NULL DEFAULT 0,
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, dataset_id, table_id)
);

CREATE TABLE IF NOT EXISTS jc_bq_jobs (
    project_id  TEXT        NOT NULL,
    job_id      TEXT        NOT NULL,
    config      JSONB       NOT NULL DEFAULT '{}',
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, job_id)
);

CREATE TABLE IF NOT EXISTS jc_bq_rows (
    project_id TEXT   NOT NULL,
    dataset_id TEXT   NOT NULL,
    table_id   TEXT   NOT NULL,
    seq        BIGINT NOT NULL,
    data       JSONB  NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, dataset_id, table_id, seq)
);
