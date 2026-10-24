-- Secret Manager replication policy: the whole `replication` oneof (automatic
-- vs user-managed, with replica locations and per-replica CMEK keys) is now
-- modelled instead of only the single CMEK key name. Records written before
-- this migration keep kms_key_name and read back as the automatic arm
-- (Secret.ReplicationPolicy), and are upgraded on their next write.
ALTER TABLE jc_sm_secrets ADD COLUMN IF NOT EXISTS replication JSONB;
