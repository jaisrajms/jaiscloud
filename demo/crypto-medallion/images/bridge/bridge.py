#!/usr/bin/env python3
"""crypto-medallion bridge — live public trade feed -> Managed Kafka (design §5.2).

Your app, part 1 of 2. Connects to a public WebSocket trade feed, normalizes
each trade to `{symbol, price, size, ts, id}`, produces it to the demo's Managed
Kafka topic over the real Kafka wire, and appends the raw stream to a shadow
JSONL capture in GCS every run (design §9 "feed safety").

Modes (env SOURCE):
  live    connect to the feed and produce until killed;
  replay  stream the bundled capture at a fixed pace (off-line, deterministic);
  auto    (default) try live; if no trade arrives within LIVENESS seconds, fall
          back to replay for the rest of the run.

The feed endpoint comes from Secret Manager (design §5.1) when FEED_SECRET is
set, else from FEED_URL.
"""

from __future__ import annotations

import base64
import gzip
import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request

from kafka import KafkaProducer  # type: ignore

EMULATOR = os.environ.get("EMULATOR", "http://jaiscloud-gcp.jaiscloud.svc.cluster.local:8080")
PROJECT = os.environ.get("PROJECT", "crypto-medallion")
BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TOPIC = os.environ.get("KAFKA_TOPIC", "trades")
CAPTURE_BUCKET = os.environ.get("CAPTURE_BUCKET", "crypto-medallion-capture")
RUN_ID = os.environ.get("RUN_ID", "take")
SOURCE = os.environ.get("SOURCE", "auto").lower()
FEED_EXCHANGE = os.environ.get("FEED_EXCHANGE", "coinbase").lower()
FEED_URL = os.environ.get("FEED_URL", "wss://ws-feed.exchange.coinbase.com")
FEED_SECRET = os.environ.get("FEED_SECRET", "")
SYMBOLS = [s.strip() for s in os.environ.get(
    "SYMBOLS", "BTC-USD,ETH-USD,SOL-USD,XRP-USD,ADA-USD").split(",") if s.strip()]
REPLAY_FILE = os.environ.get("REPLAY_FILE", "/app/replay.jsonl.gz")
LIVENESS = float(os.environ.get("LIVENESS_SECONDS", "20"))
REPLAY_RATE = float(os.environ.get("REPLAY_TRADES_PER_SECOND", "120"))
FLUSH_INTERVAL = float(os.environ.get("CAPTURE_FLUSH_SECONDS", "5"))

_lock = threading.Lock()
_capture: list[str] = []
_last_event = 0.0
_active_source = ""      # "live" | "replay" — which producer's events count
_live_stop = threading.Event()
_shutdown = threading.Event()


def log(msg: str) -> None:
    print(f"[bridge] {msg}", flush=True)


def resolve_feed_url() -> str:
    if not FEED_SECRET:
        return FEED_URL
    url = (f"{EMULATOR}/v1/projects/{PROJECT}/secrets/{FEED_SECRET}"
           "/versions/latest:access")
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            payload = json.load(resp)
        data = base64.b64decode(payload["payload"]["data"]).decode().strip()
        log(f"feed url from Secret Manager: {data}")
        return data
    except Exception as exc:  # never die on the secret; fall back to the baked URL
        log(f"secret read failed ({exc}); using {FEED_URL}")
        return FEED_URL


def producer() -> KafkaProducer:
    log(f"kafka producer -> {BOOTSTRAP} topic={TOPIC}")
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode(),
        acks="all",
        linger_ms=50,
        retries=5,
    )


def record(normalized: dict) -> None:
    with _lock:
        _capture.append(json.dumps(normalized, separators=(",", ":")))


def flush_capture() -> None:
    with _lock:
        if not _capture:
            return
        body = ("\n".join(_capture) + "\n").encode()
        _capture.clear()
    name = f"captures/{RUN_ID}/raw.jsonl"
    url = (f"{EMULATOR}/upload/storage/v1/b/{CAPTURE_BUCKET}/o"
           f"?uploadType=media&name={urllib.parse.quote(name, safe='')}")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        urllib.request.urlopen(req, timeout=15).read()
        log(f"shadow capture flushed ({len(body)} bytes) -> gs://{CAPTURE_BUCKET}/{name}")
    except Exception as exc:
        log(f"capture flush failed: {exc}")


def capture_loop() -> None:
    while not _shutdown.is_set():
        time.sleep(FLUSH_INTERVAL)
        flush_capture()


def emit(prod: KafkaProducer, source: str, symbol: str, price: float, size: float,
         ts: int, trade_id: str) -> None:
    global _last_event
    if source != _active_source:
        return  # a superseded producer (live after fallback) is ignored
    normalized = {"symbol": symbol, "price": float(price), "size": float(size),
                  "ts": int(ts), "id": str(trade_id)}
    prod.send(TOPIC, normalized)
    _last_event = time.time()
    record(normalized)


def run_live(prod: KafkaProducer) -> None:
    import websockets.sync.client as ws_client  # type: ignore

    url = resolve_feed_url()
    log(f"connecting live feed {url} symbols={SYMBOLS} exchange={FEED_EXCHANGE}")
    while not _live_stop.is_set():
        try:
            with ws_client.connect(url, open_timeout=15, close_timeout=5) as ws:
                if FEED_EXCHANGE == "coinbase":
                    ws.send(json.dumps({"type": "subscribe", "product_ids": SYMBOLS,
                                        "channels": ["matches"]}))
                else:
                    params = [s.replace("-", "").lower() + "@trade" for s in SYMBOLS]
                    ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1}))
                log("live feed connected")
                for raw in ws:
                    if _live_stop.is_set():
                        return
                    msg = json.loads(raw)
                    if msg.get("type") == "match":
                        emit(prod, "live", msg["product_id"], msg["price"],
                             msg["size"], int(time.time() * 1000), msg.get("trade_id", ""))
                    elif msg.get("e") == "trade":  # binance shape
                        emit(prod, "live", msg["s"], msg["p"], msg["q"],
                             int(msg.get("T", time.time() * 1000)), msg.get("t", ""))
        except Exception as exc:
            if _live_stop.is_set():
                return
            log(f"live feed error: {exc}; reconnecting in 5s")
            time.sleep(5)


def run_replay(prod: KafkaProducer) -> None:
    global _last_event
    log(f"replay mode: {REPLAY_FILE} at {REPLAY_RATE}/s")
    opener = gzip.open if REPLAY_FILE.endswith(".gz") else open
    delay = 1.0 / max(REPLAY_RATE, 1.0)
    while True:
        with opener(REPLAY_FILE, "rt") as fh:
            for line in fh:
                if _live_stop.is_set():
                    return
                if not line.strip():
                    continue
                t = json.loads(line)
                emit(prod, "replay", t["symbol"], t["price"], t["size"],
                     int(time.time() * 1000), t["id"])
                time.sleep(delay)
        log("replay exhausted; looping")


def main() -> None:
    global _active_source
    prod = producer()
    threading.Thread(target=capture_loop, daemon=True).start()
    if SOURCE == "replay":
        _active_source = "replay"
        run_replay(prod)
        return
    if SOURCE == "live":
        _active_source = "live"
        run_live(prod)
        return
    _active_source = "live"
    threading.Thread(target=run_live, args=(prod,), daemon=True).start()
    deadline = time.time() + LIVENESS
    while time.time() < deadline and _last_event == 0.0:
        time.sleep(0.5)
    if _last_event > 0.0:
        log("live feed healthy; running live")
        while not _live_stop.is_set():
            time.sleep(1)
        return
    log(f"no live trade in {LIVENESS}s; falling back to replay")
    _live_stop.set()
    _active_source = "replay"
    run_replay(prod)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        _live_stop.set()
        _shutdown.set()
        sys.exit(0)
