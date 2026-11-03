-- Per-Service Hive Metastore catalog ports (NF32).
--
-- A Dataproc Metastore Service's Hive Metastore Thrift endpoint is real, not
-- cosmetic: each Service has its own catalog served on its own port on the
-- emulator. The port is assigned once, at create time (base --hms-port + 1,
-- then upward), and is output-only thereafter.
--
-- Existing services are numbered here so their advertised endpoint is usable
-- immediately after the upgrade; new services continue after the highest port
-- already assigned. Ports are per-row state, so a restart (memory store aside)
-- keeps a Service's endpoint stable.

ALTER TABLE jc_metastore_services ADD COLUMN IF NOT EXISTS port INTEGER NOT NULL DEFAULT 0;

WITH ranked AS (
    SELECT project_id, location, service_name,
           row_number() OVER (ORDER BY project_id, location, service_name) AS rn
    FROM jc_metastore_services
    WHERE port = 0
)
UPDATE jc_metastore_services s
SET port = 9083 + ranked.rn
FROM ranked
WHERE s.project_id = ranked.project_id
  AND s.location = ranked.location
  AND s.service_name = ranked.service_name;
