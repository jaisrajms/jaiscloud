#!/usr/bin/env python3
"""Aggregate the four SDK-tour streaming legs into a cross-language matrix.

Reads results-streaming/<lang>.jsonl (one JSON record per scenario per
language), prints a scenario x language matrix, asserts cross-language
agreement on each scenario's normalized observable, and exits non-zero on:

  * any FAIL;
  * any scenario whose status differs from its pre-declared per-language
    expectation (an expected-OK surface that a language did not run, or an
    expected-SKIP surface whose leg unexpectedly ran it); or
  * any cross-language divergence among the languages that ran a scenario.

The per-language expectation is what keeps the matrix honest: storage
BidiReadObject is implemented for Go and Java but not exposed by the Python or
Node high-level clients, so those two legs declare SKIP rather than hiding the
gap. See README.md "Streaming-semantics tour".
"""

from __future__ import annotations

import json
import os
import sys

LANGS = ["go", "python", "java", "node"]

# scenario -> {lang: expected_status}. OK = must pass; SKIP = pre-declared
# unsupported surface (the leg must emit SKIP, not silently omit the row).
SCENARIOS: dict[str, dict[str, str]] = {
    "streaming.pubsub_ack_deadline": {"go": "OK", "python": "OK", "java": "OK", "node": "OK"},
    "streaming.pubsub_ack_extension": {"go": "OK", "python": "OK", "java": "OK", "node": "OK"},
    "streaming.pubsub_ordering_keys": {"go": "OK", "python": "OK", "java": "OK", "node": "OK"},
    "streaming.pubsub_exactly_once_ack": {"go": "OK", "python": "OK", "java": "OK", "node": "OK"},
    # All four high-level subscribers send max_outstanding_messages on the
    # initial StreamingPull and enforce the same cap locally, so no leg SKIPs.
    "streaming.pubsub_flow_control": {"go": "OK", "python": "OK", "java": "OK", "node": "OK"},
    "streaming.firestore_listen_resume_token": {"go": "OK", "python": "OK", "java": "OK", "node": "OK"},
    "streaming.firestore_snapshot_consistency": {"go": "OK", "python": "OK", "java": "OK", "node": "OK"},
    "streaming.logging_tail_reconnect": {"go": "OK", "python": "OK", "java": "OK", "node": "OK"},
    # Go (experimental.WithGRPCBidiReads) and Java (Storage#blobReadSession)
    # expose BidiReadObject; the Python and Node high-level clients do not.
    "streaming.storage_bidi_read": {"go": "OK", "python": "SKIP", "java": "OK", "node": "SKIP"},
}


def load(results_dir: str, lang: str) -> dict:
    path = os.path.join(results_dir, f"{lang}.jsonl")
    rows: dict[str, dict] = {}
    if not os.path.exists(path):
        return rows
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            rows[rec.get("scenario", "")] = rec
    return rows


def main() -> int:
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "results-streaming"
    data = {lang: load(results_dir, lang) for lang in LANGS}

    print("\nSDK TOUR — STREAMING MATRIX   (results: %s)\n" % results_dir)
    header = "scenario".ljust(44) + "".join(l.center(9) for l in LANGS) + "  observable"
    print(header)
    print("-" * len(header))

    failures: list[str] = []
    divergences: list[str] = []
    for scenario, expect in SCENARIOS.items():
        cells = []
        observables: dict[str, str] = {}
        for lang in LANGS:
            want = expect[lang]
            rec = data[lang].get(scenario)
            if rec is None:
                if want == "SKIP":
                    cells.append("-")
                    continue
                cells.append("MISSING")
                failures.append(f"{scenario}: {lang} did not run (expected OK)")
                continue
            status = rec.get("status", "FAIL")
            cells.append(status)
            if status == "FAIL":
                failures.append(
                    f"{scenario}: {lang} FAIL [{rec.get('classification', '')}] {rec.get('error', '')}"
                )
            elif status != want:
                failures.append(
                    f"{scenario}: {lang} status {status}, expected {want}"
                )
            elif status == "OK" and rec.get("observable"):
                observables[lang] = rec["observable"]
        distinct = sorted(set(observables.values()))
        obs_note = distinct[0] if len(distinct) == 1 else ""
        if len(distinct) > 1:
            divergences.append(f"{scenario}: observables diverge {observables}")
            obs_note = "DIVERGENCE " + ",".join(f"{k}={v}" for k, v in observables.items())
        print(scenario.ljust(44) + "".join(c.center(9) for c in cells) + "  " + obs_note)

    print()
    total_ok = 0
    total_skip = 0
    total_fail = 0
    for scenario in SCENARIOS:
        for lang in LANGS:
            cell = (data[lang].get(scenario) or {}).get("status", "")
            total_ok += cell == "OK"
            total_skip += cell == "SKIP"
            total_fail += cell == "FAIL"

    for f in failures:
        print("FAIL: " + f)
    for d in divergences:
        print("DIVERGENCE: " + d)

    if failures or divergences:
        print(
            f"\nsdk-tour-streaming: RED  ok={total_ok} skip={total_skip} "
            f"fail={total_fail} divergences={len(divergences)}"
        )
        return 1
    print(
        f"\nsdk-tour-streaming: GREEN  ok={total_ok} skip={total_skip} fail=0 "
        f"divergences=0  ({len(SCENARIOS)} scenarios x {len(LANGS)} languages)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
