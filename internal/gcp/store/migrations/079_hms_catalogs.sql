-- Per-Service Hive Metastore catalogs (NF32).
--
-- Before this migration the Hive Thrift serving plane was one global catalog:
-- databases and tables were keyed by name only, so any two Dataproc Metastore
-- Services (same project or not) collided on a database name, shared the
-- `default` database, and shared Iceberg commit locks.
--
-- Each Service now has its own catalog, identified by its canonical resource
-- name (projects/{p}/locations/{l}/services/{s}); the legacy shared catalog is
-- the empty string and still serves the base --hms-port listener. Every key
-- gains the catalog dimension:
--
--   jc_hms_databases  PRIMARY KEY (catalog, name)
--   jc_hms_tables     PRIMARY KEY (catalog, db_name, table_name),
--                     FK (catalog, db_name) -> databases
--   jc_hms_partitions PRIMARY KEY (catalog, db_name, table_name, part_key),
--                     FK (catalog, db_name, table_name) -> tables
--   jc_hms_locks      catalog column (lock ids stay globally unique)
--
-- The change is additive: every existing row defaults to the legacy catalog
-- (''), so a database created before per-Service catalogs is still reachable on
-- the base port, a pre-NF32 snapshot restores into that catalog, and no data is
-- dropped.

ALTER TABLE jc_hms_databases  ADD COLUMN IF NOT EXISTS catalog TEXT NOT NULL DEFAULT '';
ALTER TABLE jc_hms_tables     ADD COLUMN IF NOT EXISTS catalog TEXT NOT NULL DEFAULT '';
ALTER TABLE jc_hms_partitions ADD COLUMN IF NOT EXISTS catalog TEXT NOT NULL DEFAULT '';
ALTER TABLE jc_hms_locks      ADD COLUMN IF NOT EXISTS catalog TEXT NOT NULL DEFAULT '';

-- Widen the keys to include the catalog. The FKs are dropped first so the
-- primary keys can change underneath them.
ALTER TABLE jc_hms_partitions DROP CONSTRAINT IF EXISTS jc_hms_partitions_table_fk;
ALTER TABLE jc_hms_tables     DROP CONSTRAINT IF EXISTS jc_hms_tables_db_fk;
ALTER TABLE jc_hms_partitions DROP CONSTRAINT IF EXISTS jc_hms_partitions_pkey;
ALTER TABLE jc_hms_tables     DROP CONSTRAINT IF EXISTS jc_hms_tables_pkey;
ALTER TABLE jc_hms_databases  DROP CONSTRAINT IF EXISTS jc_hms_databases_pkey;

ALTER TABLE jc_hms_databases  ADD PRIMARY KEY (catalog, name);
ALTER TABLE jc_hms_tables     ADD PRIMARY KEY (catalog, db_name, table_name);
ALTER TABLE jc_hms_partitions ADD PRIMARY KEY (catalog, db_name, table_name, part_key);

ALTER TABLE jc_hms_tables
    ADD CONSTRAINT jc_hms_tables_db_fk FOREIGN KEY (catalog, db_name)
        REFERENCES jc_hms_databases (catalog, name) ON DELETE RESTRICT;

ALTER TABLE jc_hms_partitions
    ADD CONSTRAINT jc_hms_partitions_table_fk FOREIGN KEY (catalog, db_name, table_name)
        REFERENCES jc_hms_tables (catalog, db_name, table_name)
        ON DELETE CASCADE ON UPDATE CASCADE;

CREATE INDEX IF NOT EXISTS jc_hms_tables_catalog_idx ON jc_hms_tables (catalog);
CREATE INDEX IF NOT EXISTS jc_hms_partitions_catalog_idx ON jc_hms_partitions (catalog);
CREATE INDEX IF NOT EXISTS jc_hms_locks_catalog_idx ON jc_hms_locks (catalog);
