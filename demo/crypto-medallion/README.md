# Crypto-medallion demo (jaiscloud-gcp)

A recorded, narrated how-to that follows one data product — a live crypto-trades
leaderboard — through **Provision → Ingest → Process → Serve → Observe →
Automate**, entirely against a local `jaiscloud-gcp` on k3d. It points real
Google clients and real Terraform at the emulator and runs real Spark, real
containers and a real Kafka broker.

The design is `plan_docs/gcp-demo-crypto-medallion-design.md` (deliberately not a
wave plan; excluded from the GCP status ledger). This directory is the build.

## What runs where

| Stage | Piece | What it uses |
|---|---|---|
| Provision | `terraform/` (project is created by `gcloud` — the provider cannot reach the Billing API) | GCS, IAM, Secret Manager, KMS, Pub/Sub |
| Provision | `scripts/democtl.py provision` | Managed Kafka, Dataproc Metastore, BigQuery, Firestore |
| Ingest | `images/bridge/` Deployment (`k8s/bridge.yaml`) | public trade WS → Managed Kafka + `gs://…/capture/` shadow JSONL |
| Process | `spark/stream_medallion.py` | one always-on Dataproc job: bronze/silver `foreachBatch`, gold windowed → Iceberg on HMS + `leaderboard/latest.json` |
| Process | `spark` + `democtl.py rollup` | a `sparkSqlJob` rollup → `gs://…/rollup/` |
| Serve | `images/publisher/` Cloud Run | Eventarc GCS trigger → Firestore docs + BigQuery `insertAll` + the `trades_per_min` metric |
| Serve | `images/leaderboard/` Cloud Run | the live leaderboard page |
| Serve | `images/submitter/` Deployment | Scheduler `httpTarget` → `dataproc.jobs.submit` → BigQuery `gs://` load job |
| Observe | `democtl.py observe` | Pub/Sub notification channel + `condition_threshold` alert policy |
| Record | `democtl.py record` | SPK7 LAN authority + `scripts/demo-record.sh` Xvfb/Chrome capture + narration |

Your app is only two of those: the bridge and the Cloud Run leaderboard (plus the
publisher that backs it).

## Prerequisites

- A running k3d emulator (`deploy/k8s/jaiscloud-gcp.yaml`) with the demo's
  combined config: `JAISCLOUD_SPARK_EXECUTOR_MODE=k8s`,
  `JAISCLOUD_KAFKA_BROKER_MODE=k8s`, `JAISCLOUD_CLOUDRUN_EXECUTOR_MODE=k8s`,
  `JAISCLOUD_DATAPROC_HMS_ENDPOINT`. `democtl.py provision` reconciles none of
  these — set them per `scripts/demo-spine-verify.sh` / README-GCP.md.
- `terraform`, `gcloud`, `kubectl`, `docker`, `ffmpeg`/`ffprobe`, `python3`, and
  (for the take) `google-chrome` + `Xvfb`.
- The Spark jars: `spark/download-jars.sh` (cached under `spark/jars/`).
- Narration uses **Google Cloud Text-to-Speech** by default (`make-narration.py
  --tts auto`, voice `en-US-Journey-F`) with the active `gcloud` credentials and
  the quota project from `--quota-project` / `$DEMO_TTS_QUOTA_PROJECT` / the ADC
  file / the active gcloud project. Enable `texttospeech.googleapis.com` on that
  project. If a call fails the whole track rebuilds with `edge-tts` instead
  (`python3 -m pip install --user --break-system-packages edge-tts`), so
  narration never mixes voices.

## Run

```bash
export PATH=/tmp/opencode/go/bin:$HOME/.local/bin:$PATH
make demo-crypto-jars        # fetch the Iceberg + Kafka connector jars (cached)
make demo-crypto-up          # provision -> bridge -> process -> serve -> observe
make demo-crypto-status      # endpoint, Kafka bootstrap, leaderboard URI
make demo-crypto-preflight   # assert every link is live before a take
make demo-crypto-rollup      # submit the Spark SQL rollup once by hand
make demo-crypto-take        # LAN authority + record + narration + mux -> out/take.mp4
make demo-crypto-take-split  # split screen: console (left) + leaderboard (right) -> out/take-split.mp4
make demo-crypto-reset       # delete demo resources (FULL=1 also POST /_jaiscloud/reset)
```

`demo-crypto-take-split` drives the console to each stage's resources as the
narration plays. The switch times come from `narration/timepoints.json` (each
resource's `<mark>` position estimated from its word offset in the beat, scaled
to the clip), so a page appears when its name is spoken rather than on an even
split; the schedule is anchored to the first captured frame so audio and video
line up. The pointer is choreographed with `xdotool`: the page switches first,
then the cursor glides to the sidebar link and clicks it (cosmetic). Pages shown:
Storage/KMS/Secret/Pub-Sub/IAM at *provision*, Managed Kafka cluster + topic and
Dataproc at *platform*, the topic + capture bucket at *ingest*, the Dataproc job
+ gold bucket at *process*, Eventarc/Cloud Run/Firestore/BigQuery at *serve*,
the metric/alert policy/Pub-Sub subscription at *observe*, and the Scheduler,
BigQuery jobs and Cloud Logging at *automate*.

Each stage is also a direct call, e.g.
`python3 demo/crypto-medallion/scripts/democtl.py process`.

## Notes

- **Replay fallback.** The bridge tries the live feed and falls back to the
  bundled capture (`feed/replay.jsonl.gz`, regenerate with
  `scripts/gen_replay.py`) if no trade arrives in `LIVENESS_SECONDS`. Force it
  with `SOURCE=replay` on the Deployment.
- **Images** are tagged by the content hash of their sources
  (`<registry>/crypto-medallion-*:demo-<hash>`) so a changed image always rolls
  a fresh pull instead of reusing a node-cached tag.
- **The gold table must live under `gs://…/gold/`.** If the demo is torn down by
  hand, drop the Iceberg table before recreating with a different warehouse:
  `DROP TABLE IF EXISTS medallion.gold_ohlc` through the Hive session catalog
  (an Iceberg-catalog drop fails if its metadata was deleted).
- The take is captured from a local Xvfb + Chrome against the emulator host's
  LAN address under a `run.<lan-ip>.nip.io` authority (SPK7), so a browser
  resolves the synthesized Host with no `/etc/hosts` edit.
- **The split take's console is served by the main emulator process**, not the
  separate `jaiscloud-gcp-ui` Deployment: the console mounts the same in-process
  providers, so it only shows the demo's resources when it runs where they live.
  `democtl.py record-split` idempotently adds `--ui --ui-port 4567`, exposes the
  port, and defaults the console to `crypto-medallion`. The console is navigated
  client-side (React Router `pushState`) because a full page reload is too slow
  to paint inside a beat's hold.
