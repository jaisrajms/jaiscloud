-- Cloud Functions v2 revisions + 1st→2nd gen upgrade control plane (FD5).
--
-- revision is a 1-based monotonic counter bumped each time a new source
-- archive is deployed; the rendered serviceConfig.revision is derived from it
-- and the stored source hash. 0 means the function has no deployed revision
-- (the metadata-only case).
--
-- upgrade_* record the persisted UpgradeInfo state machine driven by the seven
-- v2 upgrade/traffic methods (setupFunctionUpgradeConfig,
-- redirect/rollbackFunctionUpgradeTraffic, commitFunctionUpgrade[AsGen2],
-- abortFunctionUpgrade, detachFunction). upgrade_state is the
-- UpgradeInfo.upgradeState enum value ('' = unspecified/none); the runtime and
-- maxInstanceCount are the Gen2 config overrides captured at setup, and
-- upgrade_traffic_gen2 is true once traffic has been redirected to the Gen2
-- copy.
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS revision              INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS upgrade_state         TEXT    NOT NULL DEFAULT '';
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS upgrade_runtime       TEXT    NOT NULL DEFAULT '';
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS upgrade_max_instances INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS upgrade_traffic_gen2  BOOL    NOT NULL DEFAULT FALSE;
