-- Managed Kafka cluster long-running operations. Cluster create/update/delete
-- return a done google.longrunning.Operation under
-- projects/{project}/locations/{location}/operations/{id}; the operation is
-- persisted here so operations.get/list can read it back. Following the
-- Dataproc / Dataproc Metastore LRO shape, metadata and response are the
-- rendered wire-JSON stored verbatim as TEXT (metadata carries its @type).

CREATE TABLE IF NOT EXISTS jc_mk_operations (
    project_id   TEXT        NOT NULL,
    location     TEXT        NOT NULL,
    operation_id TEXT        NOT NULL,
    done         BOOL        NOT NULL DEFAULT TRUE,
    metadata     TEXT        NOT NULL DEFAULT '',
    response     TEXT        NOT NULL DEFAULT '',
    verb         TEXT        NOT NULL DEFAULT '',
    target       TEXT        NOT NULL DEFAULT '',
    create_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    end_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, operation_id)
);
