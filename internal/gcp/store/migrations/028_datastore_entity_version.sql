-- 028_datastore_entity_version: entity version + update_time, backing
-- Datastore's per-mutation optimistic-concurrency conflict detection
-- (Mutation.conflict_detection_strategy: base_version / update_time). Was
-- previously untracked entirely, so a client's conditional write was
-- silently applied unconditionally regardless of whether it actually
-- matched the server's current entity state.
ALTER TABLE jc_datastore_entities ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;
ALTER TABLE jc_datastore_entities ADD COLUMN IF NOT EXISTS update_time TIMESTAMPTZ NOT NULL DEFAULT now();
