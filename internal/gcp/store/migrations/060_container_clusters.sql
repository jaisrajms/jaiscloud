-- Google Kubernetes Engine (GKE) v1 metadata mock. Clusters are
-- project+location scoped with canonical names
-- projects/{project}/locations/{location}/clusters/{cluster}; operations are
-- google.container.v1.Operation records under
-- projects/{project}/locations/{location}/operations/{operation}. Each record is
-- stored as a single JSONB document so read-back echoes what the caller last
-- observed; project_id/location/name are the lookup key.

CREATE TABLE IF NOT EXISTS jc_container_clusters (
    project_id   TEXT        NOT NULL,
    location     TEXT        NOT NULL,
    cluster_name TEXT        NOT NULL,
    data         JSONB       NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, location, cluster_name)
);

CREATE TABLE IF NOT EXISTS jc_container_operations (
    project_id     TEXT        NOT NULL,
    location       TEXT        NOT NULL,
    operation_name TEXT        NOT NULL,
    data           JSONB       NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, location, operation_name)
);
