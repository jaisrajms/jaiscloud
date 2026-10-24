-- Cloud Logging (v2) Admin registries: log buckets, views, BigQuery links,
-- log scopes, and the per-scope Settings/CMEK records. The emulator has no
-- per-bucket entry data plane, so every one of these is a metadata record; they
-- share one table keyed by (collection, scope, id) with the record persisted as
-- a JSONB document. collection is the registry kind (buckets, views, links,
-- logScopes, settings, cmekSettings); scope is the owning resource parent and
-- id the client-assigned short identifier (settings/cmek use a fixed id).
CREATE TABLE IF NOT EXISTS jc_log_admin_records (
    collection  TEXT        NOT NULL,
    scope       TEXT        NOT NULL,
    id          TEXT        NOT NULL,
    data        JSONB       NOT NULL DEFAULT '{}',
    create_time TIMESTAMPTZ,
    update_time TIMESTAMPTZ,
    PRIMARY KEY (collection, scope, id)
);
