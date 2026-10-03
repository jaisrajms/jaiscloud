-- Cloud Dataproc (clusters + jobs + long-running operations). Resources are
-- project+region scoped with canonical names
-- projects/{project}/regions/{region}/clusters/{name} (and .../jobs/{id},
-- .../operations/{id}). A cluster is a logical record only; config (including
-- initializationActions) is stored verbatim as JSONB, labels/status are
-- structured JSONB. job type_job holds the per-type job body (sparkJob/
-- pysparkJob/...) verbatim. operation metadata and response are rendered
-- wire-JSON and stored verbatim as TEXT (metadata carries its @type).

CREATE TABLE IF NOT EXISTS jc_dataproc_clusters (
    project_id     TEXT        NOT NULL,
    region         TEXT        NOT NULL,
    cluster_name   TEXT        NOT NULL,
    config         JSONB,
    labels         JSONB       NOT NULL DEFAULT '{}',
    status         JSONB       NOT NULL DEFAULT '{}',
    status_history JSONB       NOT NULL DEFAULT '[]',
    cluster_uuid   TEXT        NOT NULL DEFAULT '',
    create_time    TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, region, cluster_name)
);

CREATE TABLE IF NOT EXISTS jc_dataproc_jobs (
    project_id                 TEXT        NOT NULL,
    region                     TEXT        NOT NULL,
    job_id                     TEXT        NOT NULL,
    placement_cluster_name     TEXT        NOT NULL DEFAULT '',
    job_type                   TEXT        NOT NULL DEFAULT '',
    type_job                   JSONB,
    labels                     JSONB       NOT NULL DEFAULT '{}',
    status                     JSONB       NOT NULL DEFAULT '{}',
    status_history             JSONB       NOT NULL DEFAULT '[]',
    driver_output_resource_uri TEXT        NOT NULL DEFAULT '',
    driver_control_files_uri   TEXT        NOT NULL DEFAULT '',
    job_uuid                   TEXT        NOT NULL DEFAULT '',
    create_time                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, region, job_id)
);

CREATE INDEX IF NOT EXISTS idx_jc_dataproc_jobs_cluster
    ON jc_dataproc_jobs (project_id, region, placement_cluster_name);

CREATE TABLE IF NOT EXISTS jc_dataproc_operations (
    project_id   TEXT        NOT NULL,
    region       TEXT        NOT NULL,
    operation_id TEXT        NOT NULL,
    done         BOOL        NOT NULL DEFAULT TRUE,
    metadata     TEXT        NOT NULL DEFAULT '',
    response     TEXT        NOT NULL DEFAULT '',
    verb         TEXT        NOT NULL DEFAULT '',
    target       TEXT        NOT NULL DEFAULT '',
    create_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    end_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, region, operation_id)
);
