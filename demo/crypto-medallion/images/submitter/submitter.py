#!/usr/bin/env python3
"""crypto-medallion submitter — Cloud Scheduler -> Dataproc -> BigQuery (design §5.6/§5.4).

Cloud Scheduler has no Dataproc target, so the demo's cron job is an
`httpTarget` pointing at this small in-cluster service. Each delivery carries
`X-CloudScheduler-JobName` / `-ScheduleTime` and an OIDC bearer; the job id is
derived from the schedule slot, so a retry of the same slot is idempotent (a
duplicate submit returns 409 and is treated as success).

On submit it answers the Scheduler immediately, then in the background waits for
the Spark SQL rollup to finish and runs a BigQuery `gs://` load job over
`gs://<rollup>/rollup/*.json` — the batch serving path (design §5.4).
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

EMULATOR = os.environ.get("EMULATOR", "http://jaiscloud-gcp.jaiscloud.svc.cluster.local:8080")
PROJECT = os.environ.get("PROJECT", "crypto-medallion")
REGION = os.environ.get("REGION", "us-central1")
CLUSTER = os.environ["CLUSTER"]
JARS = [j for j in os.environ.get("JARS", "").split(",") if j]
BQ_DATASET = os.environ.get("BQ_DATASET", "crypto_medallion")
BQ_TABLE = os.environ.get("BQ_TABLE", "trades_rollup")
ROLLUP_PREFIX = os.environ.get("ROLLUP_PREFIX", "")
# SQL and Spark confs travel base64-encoded so the Deployment stays single-line.
PROPERTIES = json.loads(base64.b64decode(os.environ.get("PROPERTIES_B64", "") or "e30="))
ROLLUP_SQL = base64.b64decode(os.environ["ROLLUP_SQL_B64"]).decode()
PORT = int(os.environ.get("PORT", "8080"))


def log(msg: str) -> None:
    print(f"[submitter] {msg}", flush=True)


def http(method: str, path: str, body=None) -> tuple[int, object]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(EMULATOR + path, data=data, method=method)
    req.add_header("Authorization", "Bearer demo-token")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = resp.read()
            return resp.status, (json.loads(payload) if payload else None)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode(errors="replace")


def submit_rollup(job_id: str) -> int:
    payload = {"job": {
        "reference": {"jobId": job_id},
        "placement": {"clusterName": CLUSTER},
        "sparkSqlJob": {
            "queryList": {"queries": [ROLLUP_SQL]},
            "jarFileUris": JARS,
            "properties": PROPERTIES,
        },
    }}
    status, resp = http("POST", f"/v1/projects/{PROJECT}/regions/{REGION}/jobs:submit", payload)
    log(f"submit rollup {job_id} -> {status}")
    return 0 if status in (200, 201) else 1


def load_bigquery(slot: str) -> None:
    job_id = "cm-load-" + hashlib.sha1(slot.encode()).hexdigest()[:12]
    body = {"jobReference": {"projectId": PROJECT, "jobId": job_id},
            "configuration": {"load": {
                "sourceUris": [f"{ROLLUP_PREFIX}/*.json"],
                "destinationTable": {"projectId": PROJECT, "datasetId": BQ_DATASET,
                                     "tableId": BQ_TABLE},
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_APPEND",
            }}}
    status, resp = http("POST", f"/bigquery/v2/projects/{PROJECT}/jobs", body)
    log(f"bigquery load {job_id} from {ROLLUP_PREFIX}/*.json -> {status} {resp}")


def rollup_then_load(job_id: str, slot: str) -> None:
    for _ in range(80):
        status, resp = http("GET", f"/v1/projects/{PROJECT}/regions/{REGION}/jobs/{job_id}")
        state = (resp or {}).get("status", {}).get("state") if isinstance(resp, dict) else ""
        if state == "DONE":
            log(f"rollup {job_id} DONE; loading into BigQuery")
            load_bigquery(slot)
            return
        if state in ("ERROR", "CANCELLED"):
            log(f"rollup {job_id} {state}; skipping BigQuery load")
            return
        time.sleep(3)
    log(f"rollup {job_id} did not finish in time")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        return

    def _reply(self, code):
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        self._reply(200)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        slot = self.headers.get("X-CloudScheduler-ScheduleTime", "")
        jobname = self.headers.get("X-CloudScheduler-JobName", "")
        rid = hashlib.sha1((slot or jobname).encode()).hexdigest()[:12]
        job_id = f"cm-rollup-{rid}"
        log(f"tick job={jobname} slot={slot} id={job_id}")
        if submit_rollup(job_id) == 0:
            threading.Thread(target=rollup_then_load, args=(job_id, slot or jobname),
                             daemon=True).start()
            self._reply(200)
        else:
            self._reply(500)


def main() -> None:
    log(f"listening on :{PORT} cluster={CLUSTER} rollup={ROLLUP_PREFIX}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
