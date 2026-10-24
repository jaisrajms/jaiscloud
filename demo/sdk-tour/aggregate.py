#!/usr/bin/env python3
"""Aggregate the four SDK-tour legs into a cross-language PASS/FAIL matrix.

Reads results/<lang>.jsonl (one JSON record per scenario per language), prints a
scenario x language matrix, asserts cross-language agreement on each scenario's
normalized observable, and exits non-zero on any FAIL, any expected-OK scenario
that a language did not run, or any cross-language divergence.
"""

from __future__ import annotations

import json
import os
import sys

LANGS = ["go", "python", "java", "node"]

# Canonical scenario order and whether each must pass (OK) or is a documented
# unsupported surface (SKIP) — mirrors what each leg runs.
SCENARIOS = [
    ("storage.create_bucket", "OK"),
    ("storage.resumable_upload", "OK"),
    ("storage.stream_download_checksum", "OK"),
    ("storage.list_pagination", "OK"),
    ("pubsub.batch_publish", "OK"),
    ("pubsub.streaming_pull_ack", "OK"),
    ("firestore.listen_write", "OK"),
    ("firestore.transaction", "OK"),
    ("firestore.query_pagination", "OK"),
    ("logging.write_entry", "OK"),
    ("logging.tail", "OK"),
    ("secretmanager.add_access_list", "OK"),
    ("kms.encrypt_decrypt", "OK"),
    ("kms.asymmetric_sign", "OK"),
    ("bigquery.insertall_query", "OK"),
    ("bigquery.load_job", "OK"),
    ("lro.dataproc_cluster", "OK"),
    ("iam.policy_read_modify_write", "OK"),
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
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "results"
    data = {lang: load(results_dir, lang) for lang in LANGS}

    print("\nSDK TOUR MATRIX   (results: %s)\n" % results_dir)
    header = "scenario".ljust(34) + "".join(l.center(9) for l in LANGS) + "  observable"
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
        print(scenario.ljust(34) + "".join(c.center(9) for c in cells) + "  " + obs_note)

    print()
    total_ok = sum(
        1 for scenario, _ in SCENARIOS for lang in LANGS
        if (data[lang].get(scenario) or {}).get("status") == "OK")
    total_skips = sum(
        1 for scenario, _ in SCENARIOS for lang in LANGS
        if (data[lang].get(scenario) or {}).get("status") == "SKIP")
    total_fail = sum(
        1 for scenario, _ in SCENARIOS for lang in LANGS
        if (data[lang].get(scenario) or {}).get("status") == "FAIL")

    for f in failures:
        print("FAIL: " + f)
    for d in divergences:
        print("DIVERGENCE: " + d)

    if failures or divergences:
        print(f"\nsdk-tour: RED  ok={total_ok} skip={total_skips} fail={total_fail} "
              f"divergences={len(divergences)}")
        return 1
    print(f"\nsdk-tour: GREEN  ok={total_ok} skip={total_skips} fail=0 "
          f"divergences=0  ({len(SCENARIOS)} scenarios x {len(LANGS)} languages)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
