-- Cloud Run Admin v2 (run.googleapis.com). Services are project+location
-- scoped with canonical names
-- projects/{project}/locations/{location}/services/{service}; revisions nest
-- under a service; operations are the google.longrunning.Operation records under
-- projects/{project}/locations/{location}/operations/{operation}. Each record is
-- a single JSONB document so the caller-supplied template survives a round trip
-- and read-back echoes what the caller last observed; the id columns are the
-- lookup key.

CREATE TABLE IF NOT EXISTS jc_run_services (
    project_id TEXT  NOT NULL,
    location   TEXT  NOT NULL,
    service_id TEXT  NOT NULL,
    data       JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, location, service_id)
);

CREATE TABLE IF NOT EXISTS jc_run_revisions (
    project_id  TEXT  NOT NULL,
    location    TEXT  NOT NULL,
    service_id  TEXT  NOT NULL,
    revision_id TEXT  NOT NULL,
    data        JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, location, service_id, revision_id)
);

CREATE TABLE IF NOT EXISTS jc_run_operations (
    project_id   TEXT  NOT NULL,
    location     TEXT  NOT NULL,
    operation_id TEXT  NOT NULL,
    data         JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, location, operation_id)
);
