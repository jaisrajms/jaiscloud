-- Cloud Functions v2 ServiceConfig instance/concurrency configuration (FD6).
--
-- min_instance_count / max_instance_count bound the number of coexisting
-- instances, max_instance_request_concurrency is the per-instance concurrent
-- request limit, and available_cpu is the CPU allocated to a single instance
-- (a Cloud Run quantity string such as "1" or "0.5"). They are v2-only on the
-- wire and render under serviceConfig. Zero/empty means the field was not
-- configured (max_instance_count 0 means "no configured limit"). They are
-- metadata only: the emulator validates their ranges but does not enforce a
-- quota or admission plane (see the FD11 no-fix deferral).
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS min_instance_count                INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS max_instance_count                INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS max_instance_request_concurrency  INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS available_cpu                     TEXT    NOT NULL DEFAULT '';
