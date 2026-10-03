-- CryptoKey fidelity: user labels and the manual rotation schedule. GCP-native
-- (Cloud KMS crypto keys carry `labels`, `rotationPeriod`, and the derived
-- `nextRotationTime`; there is no AWS analog wired here). The emulator stores
-- the schedule and computes the next rotation time, but deliberately does not
-- execute rotations — see README-GCP.md's Cloud KMS Known Limitations.
ALTER TABLE jc_kms_cryptokeys ADD COLUMN IF NOT EXISTS labels JSONB;
ALTER TABLE jc_kms_cryptokeys ADD COLUMN IF NOT EXISTS rotation_period BIGINT;
ALTER TABLE jc_kms_cryptokeys ADD COLUMN IF NOT EXISTS next_rotation_time TIMESTAMPTZ;
