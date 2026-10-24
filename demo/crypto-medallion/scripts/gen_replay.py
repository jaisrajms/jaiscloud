#!/usr/bin/env python3
"""Generate the deterministic replay capture the bridge falls back to.

The crypto-medallion demo reads a live public trade feed, but a recorded take
must never depend on the network (design §9 "feed safety": live + shadow capture,
replay fallback). This writes a small, reproducible JSONL capture in the same
shape the bridge produces — one JSON object per line,
`{symbol, price, size, ts, id}` — so `SOURCE=replay` reproduces the pipeline
exactly and off-line.

    python3 demo/crypto-medallion/scripts/gen_replay.py [out.jsonl.gz]

The output is committed under demo/crypto-medallion/feed/ and baked into the
bridge image.
"""

from __future__ import annotations

import gzip
import json
import random
import sys
import time
from pathlib import Path

SYMBOLS = {
    "BTC-USD": 68000.0,
    "ETH-USD": 3500.0,
    "SOL-USD": 165.0,
    "XRP-USD": 0.62,
    "ADA-USD": 0.45,
}
TRADES_PER_SYMBOL = 2400
START_MS = int(time.time() * 1000) - 30 * 60 * 1000


def main() -> None:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else \
        Path(__file__).resolve().parent.parent / "feed" / "replay.jsonl.gz"
    out.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(20261008)
    prices = dict(SYMBOLS)
    trade_id = {s: 1000000 for s in SYMBOLS}
    lines = []
    # Interleave symbols so replay looks like a multiplexed feed.
    for step in range(TRADES_PER_SYMBOL):
        for symbol, base in SYMBOLS.items():
            drift = rng.gauss(0, 1) * base * 0.00015
            prices[symbol] = max(base * 0.5, min(base * 1.5, prices[symbol] + drift))
            size = round(abs(rng.lognormvariate(-3.0, 1.1)), 6) or 0.0001
            ts = START_MS + step * 700 + rng.randint(0, 400)
            trade_id[symbol] += 1
            lines.append(json.dumps({
                "symbol": symbol,
                "price": round(prices[symbol], 2 if base > 100 else 4),
                "size": size,
                "ts": ts,
                "id": str(trade_id[symbol]),
            }, separators=(",", ":")))
    with gzip.open(out, "wt") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"wrote {len(lines)} trades to {out} ({out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
