-- Cloud Monitoring alert policies: the `alertStrategy` message and the
-- `severity` enum (TGM5).
--
-- The CFT terraform-google-cloud-operations `simple-uptime-check` module emits
-- `alert_strategy` whenever auto_close / notification_rate_limit_period /
-- notification_channel_strategy is set, and the provider PATCHes it with
-- updateMask=alertStrategy. The policy message is stored opaquely, so the whole
-- object lives in one JSONB column; severity is the enum name.
--
-- Existing rows keep an empty strategy (NULL) and the unspecified severity ('')
-- so a policy that never set them still reads back unchanged.

ALTER TABLE jc_monitoring_alert_policies
    ADD COLUMN IF NOT EXISTS alert_strategy JSONB,
    ADD COLUMN IF NOT EXISTS severity TEXT NOT NULL DEFAULT '';
