-- Apache Kafka for BigQuery (Managed Kafka). Resources are project+location
-- scoped with canonical names
-- projects/{project}/locations/{location}/clusters/{name} (and
-- .../clusters/{name}/topics/{topic}). A cluster is a logical record only; its
-- request body (capacityConfig, gcpConfig, ...) is stored verbatim as JSONB and
-- labels as structured JSONB. Topics store partitionCount/replicationFactor as
-- ints plus any additional wire fields verbatim. Consumer groups are not tracked.

CREATE TABLE IF NOT EXISTS jc_mk_clusters (
    project_id   TEXT        NOT NULL,
    location     TEXT        NOT NULL,
    cluster_name TEXT        NOT NULL,
    config       JSONB       NOT NULL DEFAULT '{}',
    labels       JSONB       NOT NULL DEFAULT '{}',
    create_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, cluster_name)
);

CREATE TABLE IF NOT EXISTS jc_mk_topics (
    project_id         TEXT        NOT NULL,
    location           TEXT        NOT NULL,
    cluster_name       TEXT        NOT NULL,
    topic_name         TEXT        NOT NULL,
    partition_count    INT         NOT NULL DEFAULT 0,
    replication_factor INT         NOT NULL DEFAULT 0,
    config             JSONB       NOT NULL DEFAULT '{}',
    create_time        TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, cluster_name, topic_name)
);
