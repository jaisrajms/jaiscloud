-- Cloud Functions v2 fields the FC2-11/FC3-2 follow-up models: the function's
-- CMEK key (Function.kms_key_name), the runtime service account
-- (ServiceConfig.service_account_email) and the 1st-gen upload URL carried by
-- BuildConfig.Source.StorageSource.sourceUploadUrl.
--
-- The sourceUploadUrl member belongs to the same storageSource oneof alternative
-- as bucket/object, but is an auxiliary upload target rather than the archive
-- location, so it is stored separately from source_archive_url and rendered
-- back inside storageSource. All three columns default to '' so an unset field
-- stays absent from the wire response.
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS kms_key_name              TEXT NOT NULL DEFAULT '';
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS service_account_email    TEXT NOT NULL DEFAULT '';
ALTER TABLE jc_functions ADD COLUMN IF NOT EXISTS source_storage_upload_url TEXT NOT NULL DEFAULT '';
