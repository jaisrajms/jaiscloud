-- Secret Manager secret annotations: free-form client metadata rendered on the
-- Secret resource. Distinct from labels (annotations are not queryable/indexed).
ALTER TABLE jc_sm_secrets ADD COLUMN IF NOT EXISTS annotations JSONB;
