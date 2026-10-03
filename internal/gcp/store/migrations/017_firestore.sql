-- Firestore document data plane. Documents are keyed by their full resource
-- name (globally unique, embeds owning project + database). collection_id and
-- parent_path are derived from name and persisted for efficient collectionGroup
-- and hierarchical queries without full-table scans. fields is the raw
-- map[string]Value JSON (Firestore wire Value shapes).
CREATE TABLE IF NOT EXISTS jc_firestore_documents (
    project       TEXT        NOT NULL,
    database      TEXT        NOT NULL,
    collection_id TEXT        NOT NULL,
    parent_path   TEXT        NOT NULL,
    name          TEXT        PRIMARY KEY,
    fields        JSONB       NOT NULL DEFAULT '{}',
    create_time   TIMESTAMPTZ NOT NULL,
    update_time   TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jc_firestore_documents_collection
    ON jc_firestore_documents (project, database, collection_id);

CREATE INDEX IF NOT EXISTS idx_jc_firestore_documents_parent
    ON jc_firestore_documents (project, database, parent_path);
