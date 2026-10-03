-- DPMS Hive Metastore serving plane (Thrift) — partitions.
--
-- Partitions were deliberately omitted from 031_hms.sql while the Iceberg-on-
-- Hive critical path was the only target (Iceberg keeps its own partition
-- metadata and never calls the HMS partition methods). This migration adds the
-- store for the generic Hive/Hadoop-migration path, which does use them.
--
--   jc_hms_partitions — (db_name, table_name, part_key) -> values_json +
--                       full Hive Partition JSON (JSONB). Like jc_hms_tables,
--                       the partition is stored as the complete,
--                       round-trippable Partition struct, not a decomposed
--                       column set: get_partition -> alter_partition is a
--                       full-struct round-trip and every field the client
--                       originally sent must be echoed back verbatim.
--
-- part_key is the canonical JSON encoding of the ordered value tuple produced
-- by the store's PartitionKey helper, so both the memory and Postgres backends
-- address partitions identically (arbitrary value strings, including "/" and
-- "=", cannot collide across different tuples).

CREATE TABLE IF NOT EXISTS jc_hms_partitions (
    db_name     TEXT  NOT NULL,
    table_name  TEXT  NOT NULL,
    part_key    TEXT  NOT NULL,
    values_json JSONB NOT NULL DEFAULT '[]',
    part_json   JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (db_name, table_name, part_key),
    -- Enforce table existence and cascade on table/database drop at the DB
    -- level, mirroring jc_hms_tables' database FK.
    -- ON UPDATE CASCADE keeps a table rename (which re-keys jc_hms_tables)
    -- from dropping the partitions it owns; ON DELETE CASCADE still drops them
    -- with the table/database.
    CONSTRAINT jc_hms_partitions_table_fk FOREIGN KEY (db_name, table_name)
        REFERENCES jc_hms_tables (db_name, table_name) ON DELETE CASCADE ON UPDATE CASCADE
);
