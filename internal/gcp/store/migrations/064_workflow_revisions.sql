-- Cloud Workflows revision history. Each workflow keeps an ordered list of
-- immutable revisions: one on create, plus a new one whenever source_contents
-- or service_account changes (the proto's revision_id contract). The current
-- workflow row in jc_workflows mirrors the newest revision plus the
-- workflow-wide fields (description, labels, create/update time). Workflows
-- created before this migration are backfilled with their current revision so
-- ListWorkflowRevisions reflects them.

CREATE TABLE IF NOT EXISTS jc_workflow_revisions (
    project_id           TEXT        NOT NULL,
    location             TEXT        NOT NULL,
    workflow_id          TEXT        NOT NULL,
    revision_id          TEXT        NOT NULL,
    revision_create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    state                TEXT        NOT NULL DEFAULT 'ACTIVE',
    source_contents      TEXT        NOT NULL DEFAULT '',
    service_account      TEXT        NOT NULL DEFAULT '',
    call_log_level       TEXT        NOT NULL DEFAULT '',
    user_env_vars        JSONB       NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, location, workflow_id, revision_id)
);

CREATE INDEX IF NOT EXISTS idx_jc_workflow_revisions_created
    ON jc_workflow_revisions (project_id, location, workflow_id, revision_create_time DESC);

-- The live workflow needs its own revision creation time: unlike update_time it
-- does not advance on a workflow-wide update (description, labels) that does not
-- mint a new revision. Existing rows are backfilled from update_time (the only
-- timestamp available for the revision they currently point at).
ALTER TABLE jc_workflows
    ADD COLUMN IF NOT EXISTS revision_create_time TIMESTAMPTZ NOT NULL DEFAULT now();

UPDATE jc_workflows SET revision_create_time = update_time;

INSERT INTO jc_workflow_revisions
    (project_id, location, workflow_id, revision_id, revision_create_time, state,
     source_contents, service_account, call_log_level, user_env_vars)
SELECT project_id, location, workflow_id, revision_id, update_time, state,
       source_contents, service_account, call_log_level, user_env_vars
FROM jc_workflows
WHERE revision_id <> ''
ON CONFLICT DO NOTHING;
