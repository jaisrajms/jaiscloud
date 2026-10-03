-- Cloud Functions long-running operations. Function create/update/delete return
-- a done google.longrunning.Operation under
-- projects/{project}/locations/{location}/operations/{id}; the operation is
-- persisted here so operations.get/list (and REST :wait) can read it back with
-- the typed response. Following the Managed Kafka LRO shape (037), but the
-- version-independent Function response snapshot is stored as JSONB rather than
-- pre-rendered JSON, so the shared renderer can emit the v1 or v2 wire shape on
-- read. `function` is NULL for a delete (response google.protobuf.Empty).

CREATE TABLE IF NOT EXISTS jc_functions_operations (
    project_id   TEXT        NOT NULL,
    location     TEXT        NOT NULL,
    operation_id TEXT        NOT NULL,
    done         BOOL        NOT NULL DEFAULT TRUE,
    verb         TEXT        NOT NULL DEFAULT '',
    target       TEXT        NOT NULL DEFAULT '',
    function     JSONB,
    create_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    end_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, operation_id)
);
