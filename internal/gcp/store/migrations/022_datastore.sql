-- Cloud Datastore entity data plane. Entities are keyed by (project, kind,
-- name-or-id). name_or_id is the tagged identifier: "id:<decimal>" for
-- numeric-ID keys or "name:<value>" for name keys; the canonical key string is
-- "kind/name_or_id". properties is the raw map[string]Value JSON (Datastore
-- wire Value one-of shapes).
CREATE TABLE IF NOT EXISTS jc_datastore_entities (
    project    TEXT        NOT NULL,
    kind       TEXT        NOT NULL,
    name_or_id TEXT        NOT NULL,
    properties JSONB       NOT NULL DEFAULT '{}',
    PRIMARY KEY (project, kind, name_or_id)
);

CREATE INDEX IF NOT EXISTS idx_jc_datastore_entities_kind
    ON jc_datastore_entities (project, kind);

-- Per-project numeric ID allocator (monotonic counter backing AllocateIds).
CREATE TABLE IF NOT EXISTS jc_datastore_id_allocator (
    project_id TEXT   PRIMARY KEY,
    next_id    BIGINT NOT NULL DEFAULT 1
);
