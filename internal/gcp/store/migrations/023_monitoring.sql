-- Cloud Monitoring (v3) data plane — the CloudWatch metrics+alarms analogue.
--
--   jc_monitoring_metric_descriptors  the metric catalog (metric type schema)
--   jc_monitoring_time_series         the time-series data plane (points)
--   jc_monitoring_alert_policies      the alert-policy (alarm) registry
--
-- All three are project-scoped. Time series are keyed by series_key, a stable
-- JSON identity over (metric type, metric labels, resource type, resource
-- labels); points are stored as a JSONB array and concatenated on conflict so a
-- repeated CreateTimeSeries appends rather than overwrites.

CREATE TABLE IF NOT EXISTS jc_monitoring_metric_descriptors (
    project_id               TEXT    NOT NULL,
    type                     TEXT    NOT NULL,
    metric_kind              INT     NOT NULL DEFAULT 0,
    value_type               INT     NOT NULL DEFAULT 0,
    unit                     TEXT    NOT NULL DEFAULT '',
    description              TEXT    NOT NULL DEFAULT '',
    display_name             TEXT    NOT NULL DEFAULT '',
    labels                   JSONB   NOT NULL DEFAULT '[]',
    monitored_resource_types JSONB   NOT NULL DEFAULT '[]',
    PRIMARY KEY (project_id, type)
);

CREATE TABLE IF NOT EXISTS jc_monitoring_time_series (
    project_id      TEXT    NOT NULL,
    series_key      TEXT    NOT NULL,
    metric_type     TEXT    NOT NULL,
    metric_labels   JSONB   NOT NULL DEFAULT '{}',
    resource_type   TEXT    NOT NULL,
    resource_labels JSONB   NOT NULL DEFAULT '{}',
    metric_kind     INT     NOT NULL DEFAULT 0,
    value_type      INT     NOT NULL DEFAULT 0,
    unit            TEXT    NOT NULL DEFAULT '',
    points          JSONB   NOT NULL DEFAULT '[]',
    PRIMARY KEY (project_id, series_key)
);

CREATE INDEX IF NOT EXISTS idx_jc_monitoring_time_series_metric
    ON jc_monitoring_time_series (project_id, metric_type, resource_type);

CREATE TABLE IF NOT EXISTS jc_monitoring_alert_policies (
    project_id            TEXT    NOT NULL,
    id                    TEXT    NOT NULL,
    display_name          TEXT    NOT NULL DEFAULT '',
    combiner              INT     NOT NULL DEFAULT 0,
    enabled               BOOL,
    documentation         JSONB,
    conditions            JSONB   NOT NULL DEFAULT '[]',
    notification_channels JSONB   NOT NULL DEFAULT '[]',
    user_labels           JSONB   NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, id)
);
