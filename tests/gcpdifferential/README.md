# GCP differential harness (record → replay → diff)

An offline-first differential (record/replay) conformance harness for the
jaiscloud GCP emulator. It records the responses of a curated set of requests
from **real GCP** into committed goldens, then replays the *same* requests
against the emulator and diffs the normalized responses. It is not the spec
(AGENTS.md source-of-truth order applies); it is a regression gate and a
divergence finder.

Everything here is behind the `gcp_differential` build tag and uses the
`gcpdifferential` package.

## Layout

| Directory | Set | Record target | Replay target |
| --- | --- | --- | --- |
| `testdata/golden/` | curated REST (`Scenarios`) | real GCP REST | emulator REST `:8080` |
| `testdata/golden-grpc/` | curated gRPC (`GRPCScenarios`) | real GCP gRPC | emulator gRPC `:8081` |
| `testdata/golden-tour/` | SDK-tour REST (`TourScenarios`) | real GCP REST | emulator REST `:8080` |
| `testdata/golden-tour-grpc/` | SDK-tour gRPC (`TourGRPCScenarios`) | real GCP gRPC | emulator gRPC `:8081` |
| `testdata/golden-bigquery/` | BigQuery SQL corpus (`BQSQLScenarios`) | real GCP REST | emulator REST `:8080` |

Each golden is one `Exchange` (`{index, service, op, method, path, transport,
status, request, response}`); `manifest.json` records a content hash per file.
Matching a golden to a scenario is by `(Service, Op)`, never by position, so the
scenario list may grow ahead of the next recording ("pending recording" is
logged and skipped; a golden with no scenario is a fatal orphan).

Reports are written per set under `testdata/report*/report.{json,md}`: one row
per divergence with the divergent field, the real-GCP value, the emulator value
and (for accepted divergences) the triage rule's reason.

## Make targets

```bash
make test-gcp-differential        # ephemeral emulator; replays every set (+tour)
make test-gcp-differential-tour   # ephemeral emulator; tour sets only
make test-gcp-differential-bigquery # ephemeral emulator; BigQuery SQL corpus
make record-gcp-differential      # curated REST   from real GCP (needs ADC)
make record-gcp-differential-grpc # curated gRPC   from real GCP (needs ADC)
make record-gcp-differential-tour # SDK-tour (REST + gRPC) from real GCP (needs ADC)
make record-gcp-differential-bigquery # BigQuery SQL corpus from real GCP (needs ADC)
```

The BigQuery SQL corpus classifies each query against a declared expectation
(`match`/`gap`/`bug`) instead of failing on every open divergence; see
[README-bigquery.md](README-bigquery.md). Its replay also emits the committed
gap/bug frontier at `testdata/report-bigquery/backlog.md`.

## CI coverage

`.github/workflows/ci.yml` runs two anchored differential steps against the
already-running emulator (strict, `GCP_DIFFERENTIAL_REQUIRE_GOLDENS=1`):

* the **REST** step replays the curated, SDK-tour, error-tour and BigQuery sets
  and runs their golden-hygiene/contract guards — `TestReplay*`,
  `Test*GoldensAreClean`, `Test*GoldenManifest`, `Test*ScenariosValid` for each
  set (the shared `assertGoldensClean` credential/identifier guard included);
* the **gRPC** step runs after the "Wait for gRPC listener" step and replays the
  curated and SDK-tour gRPC sets (`TestReplayGRPC`, `TestReplayTourGRPC`) with
  their guards.

Both `-run` regexes are anchored (`^(…)$`) so a gRPC-only replay cannot run
incidentally before the gRPC listener is up.

Record mode needs Application Default Credentials (`gcloud auth
application-default login`) and a project with the relevant APIs enabled
(`parity-diff-jaiscloud` by default; override with `GCP_DIFFERENTIAL_PROJECT` /
`GCP_DIFFERENTIAL_PROJECT_NUMBER`). No token is ever printed or written by this
package. Recordings clean up every created resource (KMS keys excepted: GCP
cannot delete them, so fixed reusable names are used).

## SDK-tour differential (demo/sdk-tour)

The SDK tour (`demo/sdk-tour`) runs one workflow through the official
Go/Python/Java/Node clients and asserts the emulator answers them. This set
turns that "does it work" into "does it match real GCP": it records the exact
operations the **official Go tour issues** and replays them, so the wire behavior
the SDKs depend on (resumable uploads, streaming/pagination, SDK-native request
shapes) is diffed field by field.

### Recorded REST operations (`testdata/golden-tour/`)

| Op | What it covers |
| --- | --- |
| `tour_bucket_create` | bucket create |
| `tour_resumable_start` / `tour_resumable_chunk` / `tour_resumable_finalize` | resumable (chunked) upload: session start → non-final 256 KiB chunk (308) → finalizing chunk (200 + object) |
| `tour_object_metadata` | created object's crc32c/md5/size |
| `tour_media_upload` / `tour_media_download` | small `alt=media` download round-trip |
| `tour_page_upload_00..06` / `tour_list_page_1..4` | `objects.list` pagination (`maxResults=2` over 7 objects → 2+2+2+1) |
| `tour_bq_dataset_create` / `tour_bq_table_create` / `tour_bq_insertall` / `tour_bq_query` | BigQuery `insertAll` + Standard SQL query |
| `tour_bq_load_object_upload` / `tour_bq_load_table_create` / `tour_bq_load_insert` / `tour_bq_load_poll` / `tour_bq_load_tabledata` | `gs://` load job to `DONE` + row read |
| `tour_dataproc_cluster_create` / `tour_dataproc_cluster_poll` / `tour_dataproc_cluster_get` | Dataproc cluster create `google.longrunning.Operation` (`done=false` at submit) → `operations.get` poll to `DONE` → settled `clusters.get` |

Chunk request bodies are 256 KiB binary and are deliberately **not** captured
(`NoRequestCapture`); the chunk status and the finalize response carry the
parity. The resumable session URI arrives in the `Location` response header and
is captured path-only (`SaveHeader`), so record (real GCP) and replay (emulator)
address the same session through their own origin.

### Recorded gRPC operations (`testdata/golden-tour-grpc/`)

| Op | What it covers |
| --- | --- |
| `tour_kms_encrypt` / `tour_kms_decrypt` | symmetric KMS round-trip |
| `tour_kms_asym_get_public_key` / `tour_kms_asym_sign` | asymmetric sign + public key (fixed reusable sign key) |
| `tour_secret_create` / `tour_secret_add_version` / `tour_secret_access` / `tour_secret_list_versions` | Secret Manager add/access/list |
| `tour_fs_tx_seed` / `tour_fs_tx_commit` | Firestore read-modify-write transaction (BeginTransaction → GetDocument → Commit) |
| `tour_fs_page_seed` / `tour_fs_query_page_1..3` | Firestore `RunQuery` cursor pagination (`StartAfter`) |
| `tour_log_write` / `tour_log_list` | `WriteLogEntries` + `ListLogEntries` |
| `tour_pubsub_topic_create` / `tour_pubsub_publish` | Pub/Sub topic + one batched `Publish` |
| `tour_pubsub_iam_get` / `tour_pubsub_iam_set` | topic IAM policy read-modify-write (`google.iam.v1.IAMPolicy` over gRPC) |

Firestore `RunQuery` is a server stream, so the scenario drains the stream and
captures the document frames as `{"frames":[...]}`.

### Normalization

Before diffing, both sides are normalized so goldens are stable across runs and
contain no project/run-specific strings or secrets. The tour reuses the shared
normalizer (`normalize.go`) and adds:

* run-suffixed resource names → `TourBucket`, `TourBQDataset`, `TourBQTable`,
  `TourBQJob`, `TourTopic`, `TourSecret`, `TourFSCounter`, `TourFSPage`,
  `TourDataproc` fold to the same `<bucket>/<dataset>/…` placeholders the
  curated sets use (so list-scoping rules apply);
* session/continuation query tokens in a path (`pageToken`, `upload_id`,
  `resumeToken`, `sessionToken`) → `<token>`;
* KMS asymmetric material (`pem`, `signature` and their CRCs) → placeholders,
  since the emulator and real GCP hold different keys;
* Firestore transaction ids → `<transaction>` (server-generated, echoed in the
  read/commit request, so folded on both sides like a cursor);
* the project **number** is folded to `<project>` in the gRPC half too (Secret
  Manager canonicalizes names to the number).

### Deferrals (documented, not silently skipped)

* **Firestore Listen, Logging TailLogEntries** — these are long-lived streams
  whose frame timing is not deterministic across real GCP and the emulator; a
  stable golden needs a bounded-capture contract that does not yet exist. They
  remain exercised by the live SDK tour (`demo/sdk-tour`).
* **Pub/Sub StreamingPull framing** is captured as a **bounded, message-level**
  contract rather than a raw frame sequence (`msg_grpc_streaming_pull`): the
  stream is drained until the published message arrives, then cancelled, and
  only the delivered messages are golden. The frame boundaries themselves are a
  documented deferral — real GCP emits a properties-only frame before the data
  frame, which the emulator combines.
* **Dataproc LRO create/poll** — the REST tour covers it (`tour_dataproc_cluster_*`:
  create → `operations.get` poll → settled `clusters.get`). Real GCP returns the
  create operation `done=false` and reports its metadata status `PENDING`; the
  emulator begins provisioning immediately (`RUNNING`) and settles on the first
  poll (`clusterReadyDelay`). Real provisioning is minutes-long and the parity
  project's Dataproc disk quota is small, so the scenario pins an `n1-standard-4`
  / `pd-standard` 100 GiB shape; the triage rules accept only the output-only
  materialized defaults (see `triage.go`), keeping the cluster identity and
  settled `status.state` value-checked.
* **Resumable multi-chunk** — the golden covers session start, one non-final
  chunk and the finalizing chunk (512 KiB total); larger transfers differ only
  in chunk count.

### Setup-driven captures

Some contract-bearing behavior depends on server-side state that is reached
through a best-effort sequence, so the sequence itself is not a stable golden.
A `Scenario.Setup` runs such a request in both record and replay but records no
golden and is not diffed; the captured scenario that follows asserts the
contract. The only current use is **Pub/Sub dead-letter republish** (PSM2,
`dlq_redelivered` REST / `msg_grpc_dead_letter` gRPC): the dead-letter and source
resources, the publish, and the nack loop that forces the forward are setup, and
only the final dead-letter pull is golden. That golden pins the four
`CloudPubSubDeadLetterSource*` attribute names/presence (source subscription
short id, project) plus the preserved publisher attributes; the numeric
`DeliveryCount` and the `TopicPublishTime` value both fold (real GCP forwards on
a best-effort schedule §"Track delivery attempts", and the harness normalizes
the timestamp), so the exact millisecond `+00:00` layout is pinned by the unit
tests rather than the golden.

## Error / retry tour differential (demo/sdk-tour errors mode)

`testdata/golden-tour-errors/` records the canonical **error** responses the same
operations produce on real GCP — the surface the happy-path tour cannot see —
and replays them against the emulator:

| Op | What it covers |
| --- | --- |
| `error_bucket_create` / `error_bucket_already_exists` | bucket create then duplicate → 409 ALREADY_EXISTS |
| `error_object_not_found` | missing object → 404 NOT_FOUND |
| `error_topic_create` / `error_iam_get` / `error_iam_set_ok` / `error_iam_set_stale` | Pub/Sub IAM etag-optimistic-concurrency read-modify-write; the stale replay → 409 ABORTED |
| `error_invalid_query` | malformed Standard SQL → 400 INVALID_ARGUMENT |
| `error_bq_dataset_create` / `error_bq_table_create` | BigQuery fixtures for the insertId case |
| `error_insertall_seed` / `error_insertall_duplicate` / `error_insertall_count` | `insertId` idempotency: the replay must not create a second row |

Targets: `make record-gcp-differential-errors` (ADC) and
`make test-gcp-differential-errors` (offline emulator). Like every other set
the replay is gated with `GCP_DIFFERENTIAL_STRICT=1`; every observed divergence
is either **open** (a real bug) or **accepted** with a rule and reason in
`triage.go`. The report lands under `testdata/report-tour-errors/`.

Findings from the first recording (structurally matching, prose-only accepted
divergences omitted):

* **Match:** error envelope shape, HTTP status and machine-readable
  `code`/`reason`/`status` for storage 409/404, Pub/Sub IAM 409 ABORTED, and
  BigQuery 400 INVALID_ARGUMENT.
* **Accepted (advisory):** BigQuery's per-error `location`/`locationType`
  fields are absent from the emulator's 400 body
  (`response.error.errors[0].location`/`.locationType`). No official client
  branches on them; `triage.go` accepts them with a reason.
* **Accepted (eventual consistency):** real GCP's `tabledata.list` `totalRows`
  lagged ("0") immediately after the insert while the emulator returned "1";
  the row list itself matched.
* **Additive/accepted:** the emulator reports the replayed `insertId` as an
  `insertErrors[].reason: "duplicate"`; real GCP silently accepted it (the
  stored row count is 1 on both). The demo harness asserts the row count, not
  the duplicate error, for exactly this reason.

## Divergence classification

Every divergence is either **open** (a real bug to investigate) or **accepted**
(by design, with a rule and a reason in `triage.go`). Accepted rules cover
output-only fields the emulator does not synthesize, cosmetic/vanity metadata,
documented default-enum equivalence, and superset fields. A regression in the
executed result itself (status, rows, schema, resource identity) stays open.
Run `make test-gcp-differential` (optionally with `GCP_DIFFERENTIAL_STRICT=1`) to
gate on the open set.
