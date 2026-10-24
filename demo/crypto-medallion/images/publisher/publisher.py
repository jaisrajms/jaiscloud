#!/usr/bin/env python3
"""crypto-medallion publisher — Eventarc -> Firestore + BigQuery + Monitoring (design §5.4).

Cloud Run service (your app, part 2 of 2's backend). An Eventarc GCS-finalize
trigger on `leaderboard/latest.json` invokes it; it reads the rollup object, then

  * upserts one Firestore document per symbol (the leaderboard read model),
  * appends the same rows to BigQuery with tabledata.insertAll (the live path),
  * writes the `trades_per_min` custom metric that the alert policy watches.

Pure stdlib; talks the emulator's REST APIs directly.
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

EMULATOR = os.environ.get("EMULATOR", "http://jaiscloud-gcp.jaiscloud.svc.cluster.local:8080")
PROJECT = os.environ.get("PROJECT", "crypto-medallion")
BQ_DATASET = os.environ.get("BQ_DATASET", "crypto_medallion")
BQ_TABLE = os.environ.get("BQ_TABLE", "trades_rollup")
COLLECTION = os.environ.get("FIRESTORE_COLLECTION", "leaderboard")
LEADERBOARD_BUCKET = os.environ.get("LEADERBOARD_BUCKET", "crypto-medallion-leaderboard")
METRIC = os.environ.get("MONITORING_METRIC", "custom.googleapis.com/crypto_medallion/trades_per_min")
PORT = int(os.environ.get("PORT", "8080"))


def log(msg: str) -> None:
    print(f"[publisher] {msg}", flush=True)


def http(method: str, path: str, body=None, *, ctype="application/json"):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(EMULATOR + path, data=data, method=method)
    req.add_header("Authorization", "Bearer demo-token")
    if data is not None:
        req.add_header("Content-Type", ctype)
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = resp.read()
        if not payload:
            return resp.status, None
        try:
            return resp.status, json.loads(payload)
        except json.JSONDecodeError:
            return resp.status, payload.decode(errors="replace")


def read_object(bucket: str, name: str) -> list[dict]:
    quoted = urllib.parse.quote(name, safe="")
    status, payload = http("GET", f"/storage/v1/b/{bucket}/o/{quoted}?alt=media")
    text = payload if isinstance(payload, str) else json.dumps(payload)
    rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    return rows


def firestore_fields(row: dict) -> dict:
    def num(v):
        return {"doubleValue": float(v)}

    def ts(v) -> str:
        v = v or ""
        if v and not (v.endswith("Z") or "+" in v[10:]):
            v += "Z"
        return v

    return {
        "symbol": {"stringValue": str(row["symbol"])},
        "vwap": num(row.get("vwap", 0)),
        "volume": num(row.get("volume", 0)),
        "trades": {"integerValue": str(int(row.get("trades", 0)))},
        "window_start": {"timestampValue": ts(row.get("window_start"))},
        "window_end": {"timestampValue": ts(row.get("window_end"))},
        "updated_at": {"timestampValue": ts(row.get("updated_at"))},
    }


def upsert_firestore(rows: list[dict]) -> None:
    for row in rows:
        doc = urllib.parse.quote(str(row["symbol"]), safe="")
        path = (f"/v1/projects/{PROJECT}/databases/(default)/documents/"
                f"{COLLECTION}/{doc}")
        http("PATCH", path, {"fields": firestore_fields(row)})
    log(f"firestore upserted {len(rows)} docs")


def insert_bigquery(rows: list[dict]) -> None:
    bq_rows = []
    for row in rows:
        bq_rows.append({
            "insertId": f"{row['symbol']}-{row.get('updated_at', '')}".replace(":", "-"),
            "json": {
                "symbol": row["symbol"],
                "vwap": row.get("vwap"),
                "volume": row.get("volume"),
                "trades": row.get("trades"),
                "window_start": row.get("window_start"),
                "window_end": row.get("window_end"),
                "updated_at": row.get("updated_at"),
            },
        })
    if not bq_rows:
        return
    status, resp = http("POST",
                        f"/bigquery/v2/projects/{PROJECT}/datasets/{BQ_DATASET}"
                        f"/tables/{BQ_TABLE}/insertAll", {"rows": bq_rows})
    errors = [e for e in (resp or {}).get("insertErrors", [])]
    log(f"bigquery insertAll rows={len(bq_rows)} insertErrors={len(errors)}")


def write_metric(trades: float) -> None:
    now = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"
    http("POST", f"/v3/projects/{PROJECT}/timeSeries", {"timeSeries": [{
        "metric": {"type": METRIC},
        "resource": {"type": "global", "labels": {"project_id": PROJECT}},
        "points": [{"interval": {"endTime": now}, "value": {"doubleValue": trades}}],
    }]})
    log(f"metric {METRIC} = {trades}")


def write_log(rows: list[dict], trades: float) -> None:
    """Structured app log to Cloud Logging (design §5.5)."""
    now = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"
    http("POST", "/v2/entries:write", {"entries": [{
        "logName": f"projects/{PROJECT}/logs/crypto-medallion",
        "resource": {"type": "global", "labels": {"project_id": PROJECT}},
        "severity": "INFO",
        "timestamp": now,
        "jsonPayload": {
            "event": "publisher.delivery",
            "object": "leaderboard/latest.json",
            "symbols": len(rows),
            "trades": trades,
            "top": (max(rows, key=lambda r: r.get("volume", 0))["symbol"]
                    if rows else None),
        },
    }]})
    log(f"logged publisher.delivery symbols={len(rows)} trades={trades}")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # keep the Cloud Run log clean
        return

    def _reply(self, code: int, body: bytes = b""):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_GET(self):
        self._reply(200, b'{"status":"ok"}')

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            body = {}
        bucket = body.get("bucket", LEADERBOARD_BUCKET)
        name = body.get("name", "latest.json")
        log(f"event object={name} bucket={bucket} ce-type={self.headers.get('ce-type')}")
        try:
            rows = read_object(bucket, name)
            upsert_firestore(rows)
            insert_bigquery(rows)
            trades = float(sum(r.get("trades", 0) for r in rows))
            write_metric(trades)
            write_log(rows, trades)
        except Exception as exc:  # a delivery must not wedge the trigger
            log(f"delivery error: {exc}")
            self._reply(500, json.dumps({"error": str(exc)}).encode())
            return
        self._reply(204)


def main() -> None:
    log(f"listening on :{PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
