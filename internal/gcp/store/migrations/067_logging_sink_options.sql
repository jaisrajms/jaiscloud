-- Cloud Logging (v2) sinks: the remaining writable LogSink fields modelled by
-- AUD2-1. `bigquery_options` is stored as nullable JSONB (a destination-dependent
-- message); the two scalars follow the table's NOT NULL + default convention.
ALTER TABLE jc_log_sinks
    ADD COLUMN IF NOT EXISTS intercept_children    BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS output_version_format TEXT    NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS bigquery_options      JSONB;
