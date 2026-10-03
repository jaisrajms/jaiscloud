-- Cloud Monitoring notification channels (v3) — the delivery targets referenced
-- by alert policies (the CloudWatch SNS-topic analogue).
--
--   jc_monitoring_notification_channels   delivery endpoints (pubsub/email/webhook/sms)
--
-- Project-scoped and keyed by a server-assigned channel id (the trailing
-- segment of projects/{project}/notificationChannels/{id}). The type-specific
-- configuration lives in labels (e.g. {"topic": "projects/p/topics/t"} for a
-- pubsub channel, {"email_address": "ops@example.com"} for email).

CREATE TABLE IF NOT EXISTS jc_monitoring_notification_channels (
    project_id          TEXT        NOT NULL,
    id                  TEXT        NOT NULL,
    type                TEXT        NOT NULL DEFAULT '',
    display_name        TEXT        NOT NULL DEFAULT '',
    description         TEXT        NOT NULL DEFAULT '',
    labels              JSONB       NOT NULL DEFAULT '{}',
    user_labels         JSONB       NOT NULL DEFAULT '{}',
    enabled             BOOL,
    verification_status INT         NOT NULL DEFAULT 0,
    create_time         TIMESTAMPTZ,
    update_time         TIMESTAMPTZ,
    PRIMARY KEY (project_id, id)
);
