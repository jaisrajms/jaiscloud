-- Cloud Logging (v2) data plane. Log entries are project-scoped and append-only;
-- queries order by (timestamp, id). id is a synthetic monotonic tie-breaker that
-- mirrors insert order when timestamps collide.
CREATE TABLE IF NOT EXISTS jc_log_entries (
    id            BIGSERIAL    PRIMARY KEY,
    project_id    TEXT         NOT NULL,
    log_name      TEXT         NOT NULL,
    resource_type TEXT         NOT NULL DEFAULT '',
    resource_labels JSONB      NOT NULL DEFAULT '{}',
    severity      INT          NOT NULL DEFAULT 0,
    payload_type  TEXT         NOT NULL DEFAULT '',
    text_payload  TEXT         NOT NULL DEFAULT '',
    json_payload  JSONB,
    timestamp     TIMESTAMPTZ  NOT NULL,
    insert_id     TEXT         NOT NULL DEFAULT '',
    labels        JSONB        NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_jc_log_entries_project
    ON jc_log_entries (project_id, timestamp, id);

CREATE INDEX IF NOT EXISTS idx_jc_log_entries_log
    ON jc_log_entries (project_id, log_name);
