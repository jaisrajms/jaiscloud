-- Cloud Dataproc workflow templates (dataproc.v1.WorkflowTemplateService).
-- Project+region scoped with canonical names
-- projects/{project}/regions/{region}/workflowTemplates/{id}. `definition`
-- holds the full WorkflowTemplate wire object (jobs, placement, parameters,
-- labels, dagTimeout, ...) verbatim as JSONB. Only the latest version is
-- retained: create stores version 1 and each update bumps it in place (real GCP
-- keeps a version history; see README-GCP.md).
CREATE TABLE IF NOT EXISTS jc_dataproc_workflow_templates (
    project_id  TEXT        NOT NULL,
    region      TEXT        NOT NULL,
    template_id TEXT        NOT NULL,
    version     INTEGER     NOT NULL DEFAULT 1,
    definition  JSONB       NOT NULL DEFAULT '{}',
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, region, template_id)
);
