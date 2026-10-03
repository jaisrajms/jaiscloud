-- Dataproc Metastore control plane (Glue Data Catalog analogue). Resources are
-- project+location scoped with canonical names
-- projects/{project}/locations/{location}/services/{service} (and
-- .../services/{service}/backups/{backup},
-- .../services/{service}/metadataImports/{import}, and
-- .../locations/{location}/operations/{id}). A service is a logical record
-- only; its request body (hiveMetastoreConfig, networkConfig, ...) is stored
-- verbatim as JSONB and labels as structured JSONB. Backups and metadata
-- imports nest under a service and store their body (databaseDump, ...)
-- verbatim. Operations follow the Dataproc LRO shape: metadata and response are
-- rendered wire-JSON and stored verbatim as TEXT (metadata carries its @type).

CREATE TABLE IF NOT EXISTS jc_metastore_services (
    project_id    TEXT        NOT NULL,
    location      TEXT        NOT NULL,
    service_name  TEXT        NOT NULL,
    config        JSONB       NOT NULL DEFAULT '{}',
    labels        JSONB       NOT NULL DEFAULT '{}',
    state         TEXT        NOT NULL DEFAULT 'ACTIVE',
    state_history JSONB       NOT NULL DEFAULT '[]',
    create_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, service_name)
);

CREATE TABLE IF NOT EXISTS jc_metastore_backups (
    project_id   TEXT        NOT NULL,
    location     TEXT        NOT NULL,
    service_name TEXT        NOT NULL,
    backup_name  TEXT        NOT NULL,
    config       JSONB       NOT NULL DEFAULT '{}',
    description  TEXT        NOT NULL DEFAULT '',
    state        TEXT        NOT NULL DEFAULT 'ACTIVE',
    create_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    end_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, service_name, backup_name)
);

CREATE TABLE IF NOT EXISTS jc_metastore_metadata_imports (
    project_id   TEXT        NOT NULL,
    location     TEXT        NOT NULL,
    service_name TEXT        NOT NULL,
    import_name  TEXT        NOT NULL,
    config       JSONB       NOT NULL DEFAULT '{}',
    description  TEXT        NOT NULL DEFAULT '',
    state        TEXT        NOT NULL DEFAULT 'SUCCEEDED',
    create_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    end_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, service_name, import_name)
);

CREATE TABLE IF NOT EXISTS jc_metastore_operations (
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
