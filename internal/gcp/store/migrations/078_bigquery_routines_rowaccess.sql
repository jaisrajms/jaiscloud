-- BigQuery routines and row access policies. Routines are dataset-scoped
-- (…/datasets/{id}/routines/{rid}); row access policies are table-scoped
-- (…/datasets/{id}/tables/{tid}/rowAccessPolicies/{pid}). Both store the wire
-- resource verbatim as JSONB config, the same metadata-only contract as
-- datasets and tables: routines are never executed and row access policies are
-- stored/echoed but never enforced (the emulator has no row-level IAM plane).

CREATE TABLE IF NOT EXISTS jc_bq_routines (
    project_id  TEXT        NOT NULL,
    dataset_id  TEXT        NOT NULL,
    routine_id  TEXT        NOT NULL,
    config      JSONB       NOT NULL DEFAULT '{}',
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, dataset_id, routine_id)
);

CREATE TABLE IF NOT EXISTS jc_bq_row_access_policies (
    project_id  TEXT        NOT NULL,
    dataset_id  TEXT        NOT NULL,
    table_id    TEXT        NOT NULL,
    policy_id   TEXT        NOT NULL,
    config      JSONB       NOT NULL DEFAULT '{}',
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, dataset_id, table_id, policy_id)
);
