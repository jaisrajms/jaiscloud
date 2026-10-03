"""Report writer for the Python ``google-cloud-*`` client conformance suite.

Emits ``report/report.json`` (machine readable) and ``report/report.md``
(per-service rollup + full operation matrix), mirroring the Go conformance
reports in ``tests/gcpconformance/gcloud`` and ``tests/gcpconformance/grpc``.
"""

from __future__ import annotations

import dataclasses
import json
import os
import time
from pathlib import Path
from typing import Iterable

from harness import CONFIG, WIRING, Result


@dataclasses.dataclass
class Summary:
    total: int = 0
    passed: int = 0
    failed: int = 0
    unsupported: int = 0
    regressions: int = 0


def summarize(results: Iterable[Result]) -> tuple[Summary, dict[str, Summary]]:
    total = Summary()
    by_service: dict[str, Summary] = {}
    for r in results:
        svc = by_service.setdefault(r.service, Summary())
        for block in (total, svc):
            block.total += 1
            if r.regression:
                block.regressions += 1
            if r.status == "pass":
                block.passed += 1
            elif r.status == "unsupported":
                block.unsupported += 1
            else:
                block.failed += 1
    return total, by_service


def build_report(results: list[Result]) -> dict:
    total, by_service = summarize(results)
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "project": CONFIG.project,
        "rest_endpoint": CONFIG.rest,
        "grpc_endpoint": CONFIG.grpc,
        "wiring": WIRING,
        "summary": dataclasses.asdict(total),
        "by_service": {k: dataclasses.asdict(v) for k, v in sorted(by_service.items())},
        "results": [dataclasses.asdict(r) for r in results],
    }


def render_markdown(report: dict) -> str:
    lines: list[str] = []
    lines.append("# Python google-cloud client conformance report\n")
    lines.append(f"Generated: `{report['generated_at']}`  ")
    lines.append(f"Project: `{report['project']}`  ")
    lines.append(f"REST: `{report['rest_endpoint']}`  ")
    lines.append(f"gRPC: `{report['grpc_endpoint']}`\n")

    s = report["summary"]
    lines.append("## Summary\n")
    lines.append("| Total | Pass | Fail | Unsupported | Regressions |")
    lines.append("| ---: | ---: | ---: | ---: | ---: |")
    lines.append(
        f"| {s['total']} | {s['passed']} | {s['failed']} | "
        f"{s['unsupported']} | {s['regressions']} |\n"
    )

    lines.append("## Per-service matrix\n")
    lines.append("| Service | Total | Pass | Fail | Unsupported | Regressions |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
    for name, sm in report["by_service"].items():
        lines.append(
            f"| {name} | {sm['total']} | {sm['passed']} | {sm['failed']} | "
            f"{sm['unsupported']} | {sm['regressions']} |"
        )
    lines.append("")

    lines.append("## Client wiring recipe\n")
    for name, recipe in report["wiring"].items():
        lines.append(f"- **{name}**: {recipe}")
    lines.append("")

    lines.append("## Operations\n")
    lines.append("| Service | Operation | Status | Expected | Regression | Detail / error |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for r in report["results"]:
        note = _escape(r["error"] or r["detail"])
        lines.append(
            f"| {r['service']} | {r['operation']} | {r['status']} | "
            f"{r['expected']} | {'yes' if r['regression'] else 'no'} | {note} |"
        )
    lines.append("")
    return "\n".join(lines)


def _escape(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def write_report(report_dir: str | Path, results: list[Result]) -> Path:
    path = Path(report_dir)
    path.mkdir(parents=True, exist_ok=True)
    report = build_report(results)
    (path / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    (path / "report.md").write_text(render_markdown(report))
    return path


def report_dir() -> Path:
    override = os.environ.get("PY_CONFORMANCE_REPORT_DIR")
    if override:
        return Path(override)
    return Path(__file__).resolve().parent / "report"
