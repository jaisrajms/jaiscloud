-- BigLake Metastore Iceberg REST Catalog. BigLake Metastore is GCP's
-- managed-Iceberg product, and its catalog is the standard Apache Iceberg REST
-- catalog surface. Unlike the project-scoped GCP v1 services, the catalog is
-- mounted at /iceberg/v1/... and addressed by warehouse prefix + namespace
-- (multi-level, levels joined with "/") rather than
-- projects/{project}/locations/{location}. Two logical tables:
--
--   jc_iceberg_namespaces — namespace -> properties map (JSONB).
--   jc_iceberg_tables     — (namespace, table_name) -> full TableMetadata JSON
--                            (JSONB) plus the metadata-location pointer, the
--                            table UUID, and a monotonically increasing version.
--
-- The catalog never writes metadata files to object storage (the client's
-- FileIO does that); it persists the TableMetadata JSON and its location
-- pointer only, and the commit path bumps `version` under a Serializable
-- SELECT ... FOR UPDATE transaction so concurrent commits to the same table
-- cannot lose updates.

CREATE TABLE IF NOT EXISTS jc_iceberg_namespaces (
    namespace  TEXT   NOT NULL,
    properties JSONB  NOT NULL DEFAULT '{}',
    PRIMARY KEY (namespace)
);

CREATE TABLE IF NOT EXISTS jc_iceberg_tables (
    namespace         TEXT    NOT NULL,
    table_name        TEXT    NOT NULL,
    metadata          JSONB   NOT NULL DEFAULT '{}',
    metadata_location TEXT    NOT NULL DEFAULT '',
    table_uuid        TEXT    NOT NULL DEFAULT '',
    version           INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace, table_name),
    -- Enforce namespace existence and "no tables → droppable" at the DB level,
    -- so DropNamespace/CreateTable/RenameTable are atomic against concurrent
    -- namespace drops (mirrors the OCC-fix series' atomic-write discipline).
    CONSTRAINT jc_iceberg_tables_ns_fk FOREIGN KEY (namespace)
        REFERENCES jc_iceberg_namespaces (namespace) ON DELETE RESTRICT
);
