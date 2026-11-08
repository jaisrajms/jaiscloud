# Terraform differential report

Generated: 2026-11-08T03:20:21Z

- corpus: `tests/integration/gcp/terraform/differential/corpus.yaml`
- goldens: `tests/integration/gcp/terraform/differential/golden`
- emulator: `http://localhost:8080` (project `test-project`)

The goldens are recorded from real GCP by `tools/tfdifferential -mode record`;
this table is the emulator replay. Every difference is classified in
`classifications.yaml`; an unclassified difference fails the run.

## Cases

| case | status | differences | unclassified | re-plan |
|---|---|---|---|---|
| foundation | compared | 2 | 0 | no changes |
| cmek | compared | 6 | 0 | no changes |
| bootstrap | not-recorded (excluded) | 0 | 0 | — |
| events | not-recorded (excluded) | 0 | 0 | — |
| datalake | not-recorded (excluded) | 0 | 0 | — |
| multienv | not-recorded (excluded) | 0 | 0 | — |

## Differences

| case | path | real GCP | emulator | verdict | id | reason |
|---|---|---|---|---|---|---|
| cmek | `resources.google_bigquery_dataset.cmek.access[0].user_by_email` | `"\u003cuser\u003e"` | `"jaiscloud@example.iam.gserviceaccount.com"` | accepted-risk | TF6-5 | Real GCP adds the authenticated user to a dataset's default ACL; the emulator is unauthenticated and names jaiscloud@example.iam.gserviceaccount.com. Identity/authz is outside the local trust boundary. |
| cmek | `resources.google_bigquery_dataset.cmek.self_link` | `"\u003corigin\u003e/bigquery/v2/projects/\u003cproject\u003e/datasets/jc_tf_cmek_ds_\u003csuffix\u003e"` | `null` | emulator-gap | TF6-2 | datasets.get omits selfLink. Real GCP returns https://bigquery.googleapis.com/bigquery/v2/projects/{p}/datasets/{d}; the provider exposes it as a computed self_link. |
| cmek | `resources.google_kms_crypto_key.global.destroy_scheduled_duration` | `"2592000s"` | `""` | emulator-gap | TF6-1 | CryptoKey.get omits destroyScheduledDuration. Real GCP always returns the default 2592000s; the provider stores it as a computed attribute, so an import/refresh against the emulator sees an empty value. |
| cmek | `resources.google_kms_crypto_key.primary.destroy_scheduled_duration` | `"2592000s"` | `""` | emulator-gap | TF6-1 | CryptoKey.get omits destroyScheduledDuration. Real GCP always returns the default 2592000s; the provider stores it as a computed attribute, so an import/refresh against the emulator sees an empty value. |
| cmek | `resources.google_kms_crypto_key.rotated.destroy_scheduled_duration` | `"2592000s"` | `""` | emulator-gap | TF6-1 | CryptoKey.get omits destroyScheduledDuration. Real GCP always returns the default 2592000s; the provider stores it as a computed attribute, so an import/refresh against the emulator sees an empty value. |
| cmek | `resources.google_storage_bucket.cmek.versioning.length` | `0` | `1` | accepted-risk | TF6-4 | A bucket that never configured versioning is returned as versioning:{enabled:false} by the emulator; real GCP omits the block. Both are valid API shapes and Terraform reports no diff either way, so the omission is not chased. Recorded in docs/gcp/terraform-compatibility.md. |
| foundation | `resources.google_kms_crypto_key.diff.destroy_scheduled_duration` | `"2592000s"` | `""` | emulator-gap | TF6-1 | CryptoKey.get omits destroyScheduledDuration. Real GCP always returns the default 2592000s; the provider stores it as a computed attribute, so an import/refresh against the emulator sees an empty value. |
| foundation | `resources.google_storage_bucket_object.diff.media_link` | `"\u003corigin\u003e/download/storage/v1/b/jc-tf-diff-\u003csuffix\u003e/o/hello.txt?generation=\u003cgeneration\u003e\u0026alt=media"` | `"\u003corigin\u003e/download/storage/v1/b/jc-tf-diff-\u003csuffix\u003e/o/hello.txt?alt=media"` | emulator-gap | TF6-3 | The object mediaLink omits the generation query parameter. Real GCP returns ?generation=<n>&alt=media; the emulator returns ?alt=media. |

## Not recorded

| case | reason |
|---|---|
| bootstrap | Creates a real project and needs an organization/billing entitlement; a recorded run would leave a project in DELETE_REQUESTED and is not a cheap or reversible operator action. Environment-scoped, not an emulator claim. |
| events | Cloud Functions v2 deploys real build/run infrastructure and needs a source archive and Artifact Registry; the echo sink is a local process a real GCP trigger cannot reach. The composition is proven against the emulator by the patterns gate. |
| datalake | Needs a billable GKE cluster, a running Dataproc job and a real Spark driver; hours of compute for a control-plane differential. The Spark data plane is explicitly outside the trust boundary (terraform-compatibility). |
| multienv | The aliased provider targets <project>-shared, which does not exist on real GCP; workspaces are a Terraform feature already exercised by the emulator pattern, and a recorded run would need a second real project. |
