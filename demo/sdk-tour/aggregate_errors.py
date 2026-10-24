#!/usr/bin/env python3
"""Aggregate the four SDK-tour error/retry legs into a cross-language matrix.

Reads results-errors/<lang>.jsonl (one JSON record per scenario per language),
prints a scenario x language matrix, asserts cross-language agreement on each
scenario's normalized observable, and exits non-zero on any FAIL, any expected
scenario a language did not run, or any cross-language divergence.

This is the error/retry/idempotency counterpart of aggregate.py: same protocol,
different scenario set (see README.md "Error / retry / idempotency tour").
"""

from __future__ import annotations

import json
import os
import sys

LANGS = ["go", "python", "java", "node"]

# Canonical scenario order. Every scenario is expected to pass; a language that
# cannot wire one is reported MISSING (red) rather than silently skipped.
SCENARIOS = [
    ("errors.storage_already_exists", "OK"),
    ("errors.storage_not_found", "OK"),
    ("errors.invalid_argument", "OK"),
    ("errors.iam_failed_precondition", "OK"),
    ("errors.no_retry_on_4xx", "OK"),
    ("errors.retry_429_storage_get", "OK"),
    ("errors.retry_503_storage_get", "OK"),
    ("errors.retry_info_shape", "OK"),
    ("errors.pagination_stability", "OK"),
    ("errors.idempotent_insertall", "OK"),
    ("errors.resumable_rewind", "OK"),
    ("errors.pubsub_topic_already_exists", "OK"),
]


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
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "results-errors"
    data = {lang: load(results_dir, lang) for lang in LANGS}

    print("\nSDK TOUR — ERROR / RETRY MATRIX   (results: %s)\n" % results_dir)
    header = "scenario".ljust(38) + "".join(l.center(9) for l in LANGS) + "  observable"
    print(header)
    print("-" * len(header))

    failures: list[str] = []
    divergences: list[str] = []
    for scenario, expect in SCENARIOS:
        cells = []
        observables = {}
        for lang in LANGS:
            rec = data[lang].get(scenario)
            if rec is None:
                cells.append("MISSING" if expect == "OK" else "-")
                if expect == "OK":
                    failures.append(f"{scenario}: {lang} did not run")
            else:
                status = rec.get("status", "FAIL")
                cells.append(status)
                if status == "FAIL":
                    detail = rec.get("error", "")
                    cls = rec.get("classification", "")
                    failures.append(f"{scenario}: {lang} FAIL [{cls}] {detail}")
                if status == "OK" and rec.get("observable"):
                    observables[lang] = rec["observable"]
        distinct = sorted(set(observables.values()))
        obs_note = distinct[0] if len(distinct) == 1 else ""
        if len(distinct) > 1:
            divergences.append(f"{scenario}: observables diverge {observables}")
            obs_note = "DIVERGENCE " + ",".join(f"{k}={v}" for k, v in observables.items())
        print(scenario.ljust(38) + "".join(c.center(9) for c in cells) + "  " + obs_note)

    print()
    total_ok = sum(
        1 for scenario, _ in SCENARIOS for lang in LANGS
        if (data[lang].get(scenario) or {}).get("status") == "OK")
    total_fail = sum(
        1 for scenario, _ in SCENARIOS for lang in LANGS
        if (data[lang].get(scenario) or {}).get("status") == "FAIL")

    for f in failures:
        print("FAIL: " + f)
    for d in divergences:
        print("DIVERGENCE: " + d)

    if failures or divergences:
        print(f"\nsdk-tour-errors: RED  ok={total_ok} fail={total_fail} "
              f"divergences={len(divergences)}")
        return 1
    print(f"\nsdk-tour-errors: GREEN  ok={total_ok} fail=0 divergences=0 "
          f"({len(SCENARIOS)} scenarios x {len(LANGS)} languages)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
