-- Cloud Scheduler v1. Jobs are project+location scoped with canonical names
-- projects/{project}/locations/{location}/jobs/{job}. The full job record
-- (target, schedule, retryConfig, state, output-only fields) is stored as a
-- single JSONB document so read-back echoes what the caller last observed;
-- project_id/location/job_name are the lookup key.

CREATE TABLE IF NOT EXISTS jc_scheduler_jobs (
    project_id  TEXT        NOT NULL,
    location    TEXT        NOT NULL,
    job_name    TEXT        NOT NULL,
    data        JSONB       NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, location, job_name)
);
