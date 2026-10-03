"""pytest configuration for the Python client conformance suite.

Responsibilities:
  * Point the official clients at the emulator by default (env vars read by the
    libraries) without clobbering values the caller already set.
  * Skip the whole suite cleanly when no emulator is listening.
  * Write the per-service report at session end and print a summary line.
"""

from __future__ import annotations

import os
import socket
import urllib.error
import urllib.request

from harness import CONFIG, strip_scheme

# Emulator-hook env vars honoured by the official clients. Set before test
# modules construct any client; an explicit caller value always wins.
os.environ.setdefault("PROJECT_ID", CONFIG.project)
os.environ.setdefault("STORAGE_EMULATOR_HOST", CONFIG.rest)
os.environ.setdefault("PUBSUB_EMULATOR_HOST", CONFIG.grpc)
os.environ.setdefault("FIRESTORE_EMULATOR_HOST", CONFIG.grpc)


def pytest_configure(config):
    config.addinivalue_line("markers", "rest: Cloud Storage REST checks")
    config.addinivalue_line(
        "markers", "grpc: gRPC checks (require the emulator's gRPC listener)"
    )


def _emulator_reachable() -> bool:
    url = CONFIG.rest + "/_jaiscloud/health"
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.URLError, OSError):
        return False


def _tcp_reachable(hostport: str) -> bool:
    """Fast TCP probe so a missing gRPC listener skips instead of hanging on
    the generated clients' long default gRPC retry deadlines."""
    host, _, port = hostport.rpartition(":")
    try:
        with socket.create_connection((host or "localhost", int(port)), timeout=2):
            return True
    except (OSError, ValueError):
        return False


def pytest_collection_modifyitems(config, items):  # noqa: ARG001
    import pytest

    rest_up = _emulator_reachable()
    grpc_up = rest_up and _tcp_reachable(strip_scheme(CONFIG.grpc))
    for item in items:
        if not rest_up:
            item.add_marker(pytest.mark.skip(reason=f"emulator not reachable at {CONFIG.rest}"))
        elif item.get_closest_marker("grpc") and not grpc_up:
            item.add_marker(
                pytest.mark.skip(reason=f"gRPC endpoint {CONFIG.grpc} not reachable")
            )


def pytest_sessionfinish(session, exitstatus):  # noqa: ARG001
    from harness import RESULTS
    from report import report_dir, write_report

    path = write_report(report_dir(), RESULTS)
    total = len(RESULTS)
    passed = sum(1 for r in RESULTS if r.status == "pass")
    failed = sum(1 for r in RESULTS if r.status == "fail")
    unsupported = sum(1 for r in RESULTS if r.status == "unsupported")
    regressions = sum(1 for r in RESULTS if r.regression)
    services = sorted({r.service for r in RESULTS})

    print()
    print("PYTHON SDK CONFORMANCE MATRIX")
    for svc in services:
        rows = [r for r in RESULTS if r.service == svc]
        p = sum(1 for r in rows if r.status == "pass")
        u = sum(1 for r in rows if r.status == "unsupported")
        f = sum(1 for r in rows if r.status == "fail")
        print(f"  {svc:<14} total={len(rows):<3} pass={p:<3} fail={f:<3} unsupported={u}")
    print(
        f"python-sdk conformance: total={total} pass={passed} fail={failed} "
        f"unsupported={unsupported} regressions={regressions} "
        f"services={','.join(services) if services else 'none'}"
    )
    print(f"report written to {path}/report.{{json,md}}")
