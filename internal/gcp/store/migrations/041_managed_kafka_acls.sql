-- Managed Kafka ACLs. An ACL is a set of entries for one resource pattern,
-- keyed by the acl id (which encodes resource_type/resource_name/pattern_type).
-- entries is the caller-supplied list stored verbatim as JSON; etag backs the
-- optimistic-concurrency check on update. The resource_type/resource_name/
-- pattern_type columns hold the derived output-only fields.

CREATE TABLE IF NOT EXISTS jc_mk_acls (
    project_id    TEXT NOT NULL,
    location      TEXT NOT NULL,
    cluster_name  TEXT NOT NULL,
    acl_name      TEXT NOT NULL,
    entries       JSONB NOT NULL DEFAULT '[]',
    etag          TEXT NOT NULL DEFAULT '',
    resource_type TEXT NOT NULL DEFAULT '',
    resource_name TEXT NOT NULL DEFAULT '',
    pattern_type  TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (project_id, location, cluster_name, acl_name)
);
