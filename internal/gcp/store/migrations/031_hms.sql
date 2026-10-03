-- DPMS Hive Metastore serving plane (Thrift). This is the GCP analogue of the
-- AWS Glue table-metadata serving path, but over the real Apache Hive
-- Metastore protocol (hive_metastore.thrift, TBinaryProtocol over a raw
-- non-framed TSocket), not Glue's REST/JSON surface. Unlike the project-scoped
-- GCP v1 services, the serving plane is a single global catalog: databases and
-- tables are keyed by name only (the per-Service endpoint_uri emitted by the
-- control plane is cosmetic).
--
-- Three logical tables:
--
--   jc_hms_databases — (name) -> location_uri, parameters map (JSONB),
--                      description, owner.
--   jc_hms_tables    — (db_name, table_name) -> full Hive Table JSON (JSONB).
--                      The table is stored as the complete, round-trippable
--                      Table struct, not a decomposed column set, because
--                      Iceberg's alter_table is a full-Table round-trip (read
--                      whole table, mutate `parameters`, write whole table) and
--                      every field the client originally sent must be echoed
--                      back verbatim (F5).
--   jc_hms_locks     — (lock_id) -> lock state for lock/check_lock/unlock.
--
-- Partitions are deliberately NOT stored: the Iceberg-on-Hive critical path
-- never calls partition methods (Iceberg partitions its own metadata), so every
-- partition method is stubbed to empty / NoSuchObjectException at the handler
-- layer (D3).

CREATE TABLE IF NOT EXISTS jc_hms_databases (
    name         TEXT        NOT NULL,
    location_uri TEXT        NOT NULL DEFAULT '',
    parameters   JSONB       NOT NULL DEFAULT '{}',
    description  TEXT        NOT NULL DEFAULT '',
    owner        TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (name)
);

CREATE TABLE IF NOT EXISTS jc_hms_tables (
    db_name    TEXT  NOT NULL,
    table_name TEXT  NOT NULL,
    table_json JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (db_name, table_name),
    -- Enforce database existence and "no tables -> droppable" at the DB level,
    -- so DropDatabase/CreateTable/RenameTable are atomic against concurrent
    -- database drops (mirrors the 030_iceberg.sql FK and the OCC-fix series'
    -- atomic-write discipline).
    CONSTRAINT jc_hms_tables_db_fk FOREIGN KEY (db_name)
        REFERENCES jc_hms_databases (name) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS jc_hms_locks (
    lock_id     BIGSERIAL   NOT NULL,
    state       INTEGER     NOT NULL DEFAULT 1,
    db_name     TEXT        NOT NULL DEFAULT '',
    table_name  TEXT        NOT NULL DEFAULT '',
    user_name   TEXT        NOT NULL DEFAULT '',
    hostname    TEXT        NOT NULL DEFAULT '',
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (lock_id)
);
