-- Asymmetric KMS keys have no symmetric key material; only private_key /
-- public_key are populated. key_material was declared NOT NULL in
-- 008_kms_versions.sql, which broke CreateCryptoKey (and its implicit primary
-- version) for ASYMMETRIC_SIGN / ASYMMETRIC_DECRYPT purposes on Postgres.
ALTER TABLE jc_kms_cryptokey_versions ALTER COLUMN key_material DROP NOT NULL;
