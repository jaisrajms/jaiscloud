-- Cloud Workflows executionHistoryLevel (workflows.v1.Workflow,
-- google.cloud.workflows.v1.ExecutionHistoryLevel): EXECUTION_HISTORY_BASIC /
-- EXECUTION_HISTORY_DETAILED, defaulting to EXECUTION_HISTORY_LEVEL_UNSPECIFIED.
-- The value is workflow-wide (not revision-scoped), so only jc_workflows needs
-- it; rows written before this migration read back as the unspecified default.

ALTER TABLE jc_workflows
    ADD COLUMN IF NOT EXISTS execution_history_level TEXT NOT NULL DEFAULT '';
