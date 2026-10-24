-- Eventarc long-running operations (eventarc.googleapis.com/v1). Every
-- control-plane mutation completes synchronously, but real GCP still records
-- the done google.longrunning.Operation in the shared operations surface, where
-- operations.get/list/delete address it. Persisting it makes that surface real
-- instead of answering NotFound for every name.
--
-- jc_eventarc_operations — (project_id, location, operation_id) -> the done
-- operation's verb/target, the Any-wrapped rendered response body (kept after
-- the target resource is deleted, as real GCP keeps a completed delete
-- operation's response), and its create time.

CREATE TABLE IF NOT EXISTS jc_eventarc_operations (
    project_id   TEXT        NOT NULL,
    location     TEXT        NOT NULL,
    operation_id TEXT        NOT NULL,
    verb         TEXT        NOT NULL DEFAULT '',
    target       TEXT        NOT NULL DEFAULT '',
    response     JSONB       NOT NULL DEFAULT '{}',
    create_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, operation_id)
);
