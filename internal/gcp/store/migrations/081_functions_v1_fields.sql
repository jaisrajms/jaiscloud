-- Cloud Functions v1 fields the terraform-google-event-function module sends
-- (TGM3): ingress/egress placement, the v1 maxInstances limit, Secret Manager
-- bindings, image-build placement and the HTTPS trigger security level. They
-- were added to the wire model and the memory store; these columns give the
-- --dsn backend the same round-trip. Secret bindings are stored as JSONB in the
-- Discovery shape (secretEnvironmentVariables / secretVolumes).
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS ingress_settings            TEXT    NOT NULL DEFAULT '';
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS max_instances               INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS vpc_connector              TEXT    NOT NULL DEFAULT '';
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS vpc_connector_egress_settings TEXT  NOT NULL DEFAULT '';
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS secret_environment_variables JSONB;
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS secret_volumes             JSONB;
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS docker_registry            TEXT    NOT NULL DEFAULT '';
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS docker_repository          TEXT    NOT NULL DEFAULT '';
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS build_worker_pool          TEXT    NOT NULL DEFAULT '';
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS https_trigger_security_level TEXT  NOT NULL DEFAULT '';
