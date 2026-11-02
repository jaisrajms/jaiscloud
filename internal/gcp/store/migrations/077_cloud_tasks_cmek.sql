-- Cloud Tasks v2 CMEK config (locations.getCmekConfig/updateCmekConfig). The
-- config is location-scoped: one row per (project, location), holding the Cloud
-- KMS key resource name. It is metadata only — the emulator stores task
-- payloads in plaintext, so the key is recorded and echoed but never used for
-- at-rest encryption. An empty/missing row is the documented default ("By
-- default there is no kms_key configured").

CREATE TABLE IF NOT EXISTS jc_tasks_cmek_configs (
    project_id TEXT  NOT NULL,
    location   TEXT  NOT NULL,
    kms_key    TEXT  NOT NULL DEFAULT '',
    PRIMARY KEY (project_id, location)
);
