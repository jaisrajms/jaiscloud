-- Cloud Monitoring alert policies: the create/update clock the policy's
-- creationRecord/mutationRecord audit messages are rendered from (FC2-18).
--
-- Real Monitoring always returns both records on an AlertPolicy; the emulator
-- derives them from these two columns (and the documented default service
-- identity as mutated_by), so no second copy of the record is stored.
--
-- Policies written before this migration have no recorded clock, so they are
-- backfilled from now() (the table carries no earlier timestamp to derive from)
-- and the columns become NOT NULL: a policy row without a clock would fail the
-- time.Time scan and make a created record unrenderable.

ALTER TABLE jc_monitoring_alert_policies ADD COLUMN IF NOT EXISTS create_time TIMESTAMPTZ;
ALTER TABLE jc_monitoring_alert_policies ADD COLUMN IF NOT EXISTS update_time TIMESTAMPTZ;

UPDATE jc_monitoring_alert_policies SET create_time = now() WHERE create_time IS NULL;
UPDATE jc_monitoring_alert_policies SET update_time = now() WHERE update_time IS NULL;

ALTER TABLE jc_monitoring_alert_policies ALTER COLUMN create_time SET NOT NULL;
ALTER TABLE jc_monitoring_alert_policies ALTER COLUMN update_time SET NOT NULL;
ALTER TABLE jc_monitoring_alert_policies ALTER COLUMN create_time SET DEFAULT now();
ALTER TABLE jc_monitoring_alert_policies ALTER COLUMN update_time SET DEFAULT now();
