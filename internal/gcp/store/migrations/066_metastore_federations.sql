-- Dataproc Metastore federations (the separate
-- google.cloud.metastore.v1.DataprocMetastoreFederation surface). A federation
-- is project+location scoped with canonical name
-- projects/{project}/locations/{location}/federations/{federation}. Like a
-- service it is a logical record only: its request body (version,
-- backendMetastores, ...) is stored verbatim as JSONB and labels as structured
-- JSONB. State is output-only and always ACTIVE (mutations complete
-- synchronously).

CREATE TABLE IF NOT EXISTS jc_metastore_federations (
    project_id      TEXT        NOT NULL,
    location        TEXT        NOT NULL,
    federation_name TEXT        NOT NULL,
    config          JSONB       NOT NULL DEFAULT '{}',
    labels          JSONB       NOT NULL DEFAULT '{}',
    state           TEXT        NOT NULL DEFAULT 'ACTIVE',
    create_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, federation_name)
);
