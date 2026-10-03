-- Cloud Monitoring incidents — the recorded state for fired alert policies.
--
--   jc_monitoring_incidents   OPEN/CLOSED incidents opened by the evaluator
--
-- GCP exposes incidents only on an internal API (not the public v3 client
-- library), so the emulator records them here for observability via the
-- monitoring store's snapshot/export surface and slog. At most one OPEN
-- incident exists per (project_id, policy_id); the evaluator enforces this
-- atomically, backed by the partial unique index below.

CREATE TABLE IF NOT EXISTS jc_monitoring_incidents (
    project_id     TEXT        NOT NULL,
    id             TEXT        NOT NULL,
    policy_id      TEXT        NOT NULL,
    condition_name TEXT        NOT NULL DEFAULT '',
    state          TEXT        NOT NULL DEFAULT 'OPEN',
    started_at     TIMESTAMPTZ,
    ended_at       TIMESTAMPTZ,
    reason         TEXT        NOT NULL DEFAULT '',
    notifications  JSONB       NOT NULL DEFAULT '[]',
    PRIMARY KEY (project_id, id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jc_monitoring_incidents_open
    ON jc_monitoring_incidents (project_id, policy_id)
    WHERE state = 'OPEN';
