"""Shared harness for the Python ``google-cloud-*`` client conformance suite.

This suite drives the jaiscloud GCP emulator with the *official* Python client
libraries and records, per service/operation, whether the call passed, failed,
or returned an "unimplemented" error.  It is the Python analogue of the Go
suites in ``tests/gcpconformance/grpc`` and ``tests/gcpconformance/gcloud``.

Only a check that is expected to pass (``Expect.PASS``) and does not is treated
as a regression and fails the pytest run.  A check that the emulator does not
implement is recorded as ``unsupported`` and skipped, never faked as a pass.
"""

from __future__ import annotations

import dataclasses
import enum
import itertools
import os
import time
import uuid
from typing import Any, Callable, Optional

import grpc
import pytest

# ─── Environment ────────────────────────────────────────────────────────────
# Documented wiring knobs.  PROJECT_ID / EMULATOR_REST / EMULATOR_GRPC are the
# suite's public interface; defaults match the emulator's own defaults.
DEFAULT_PROJECT = "jaiscloud-project"
DEFAULT_REST = "http://localhost:8080"
DEFAULT_GRPC = "localhost:8081"


def strip_scheme(host: str) -> str:
    """Return host:port from a possibly-schemed endpoint."""
    if "://" in host:
        return host.split("://", 1)[1].rstrip("/")
    return host


@dataclasses.dataclass(frozen=True)
class Config:
    """Resolved per-run emulator connection + naming context."""

    project: str
    rest: str
    grpc: str
    suffix: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            project=os.environ.get("PROJECT_ID", DEFAULT_PROJECT),
            rest=os.environ.get("EMULATOR_REST", DEFAULT_REST).rstrip("/"),
            grpc=strip_scheme(os.environ.get("EMULATOR_GRPC", DEFAULT_GRPC)),
            # Per-run suffix keeps every resource name unique across repeated
            # runs against a long-lived emulator, so the suite is idempotent.
            suffix=uuid.uuid4().hex[:10],
        )

    def secret_path(self, secret_id: str) -> str:
        return f"projects/{self.project}/secrets/{secret_id}"

    def topic_path(self, topic_id: str) -> str:
        return f"projects/{self.project}/topics/{topic_id}"

    def subscription_path(self, sub_id: str) -> str:
        return f"projects/{self.project}/subscriptions/{sub_id}"

    def key_ring_path(self, ring_id: str) -> str:
        return f"projects/{self.project}/locations/global/keyRings/{ring_id}"


CONFIG = Config.from_env()

# Monotonic counter so every resource name is unique even when the same prefix
# is reused by a fixture across tests (KMS key rings, for example, cannot be
# deleted, so re-creating the same name would collide).
_resource_counter = itertools.count(1)


def unique(prefix: str) -> str:
    """A run-unique resource name (prefix + run suffix + call counter)."""
    return f"{prefix}-{CONFIG.suffix}-{next(_resource_counter)}"


# ─── Client wiring recipes ──────────────────────────────────────────────────
# These are the *exact* recipes that make the official Python clients talk to
# the emulator.  They are documented in README.md; keeping them here makes the
# suite the single source of truth for the wiring.

WIRING = {
    "storage": (
        "REST: STORAGE_EMULATOR_HOST=http://localhost:8080; "
        "storage.Client(project=...) auto-selects AnonymousCredentials"
    ),
    "pubsub": (
        "gRPC: PUBSUB_EMULATOR_HOST=localhost:8081; "
        "pubsub_v1.PublisherClient()/SubscriberClient() auto-select an "
        "insecure channel"
    ),
    "secretmanager": (
        "gRPC: no emulator env var. The generated client always builds a TLS "
        "channel, so client_options api_endpoint + AnonymousCredentials fails "
        "with an SSL handshake; inject an insecure generated transport instead: "
        "SecretManagerServiceGrpcTransport(channel=grpc.insecure_channel(...))"
    ),
    "kms": (
        "gRPC: same transport detail as Secret Manager; inject "
        "KeyManagementServiceGrpcTransport(channel=grpc.insecure_channel(...))"
    ),
    "firestore": (
        "gRPC: FIRESTORE_EMULATOR_HOST=localhost:8081; "
        "firestore.Client(project=...) auto-selects an insecure channel"
    ),
}


def storage_client():
    """REST Cloud Storage client wired through STORAGE_EMULATOR_HOST."""
    from google.cloud import storage

    return storage.Client(project=CONFIG.project)


def pubsub_publisher():
    """Pub/Sub v1 publisher wired through PUBSUB_EMULATOR_HOST."""
    from google.cloud import pubsub_v1

    return pubsub_v1.PublisherClient()


def pubsub_subscriber():
    """Pub/Sub v1 subscriber wired through PUBSUB_EMULATOR_HOST."""
    from google.cloud import pubsub_v1

    return pubsub_v1.SubscriberClient()


def secretmanager_client():
    """Secret Manager gRPC client over a plaintext (insecure) transport.

    The generated client's ``client_options={'api_endpoint': ...}`` path builds
    a *TLS* channel, which fails against the emulator's plaintext gRPC listener
    with ``SSL_ERROR_SSL ... WRONG_VERSION_NUMBER``.  Injecting the generated
    transport over ``grpc.insecure_channel`` is the working recipe.
    """
    from google.cloud import secretmanager
    from google.cloud.secretmanager_v1.services.secret_manager_service.transports import (
        SecretManagerServiceGrpcTransport,
    )

    transport = SecretManagerServiceGrpcTransport(
        channel=grpc.insecure_channel(CONFIG.grpc)
    )
    return secretmanager.SecretManagerServiceClient(transport=transport)


def kms_client():
    """Cloud KMS gRPC client over a plaintext (insecure) transport."""
    from google.cloud import kms_v1
    from google.cloud.kms_v1.services.key_management_service.transports import (
        KeyManagementServiceGrpcTransport,
    )

    transport = KeyManagementServiceGrpcTransport(
        channel=grpc.insecure_channel(CONFIG.grpc)
    )
    return kms_v1.KeyManagementServiceClient(transport=transport)


def firestore_client():
    """Firestore client wired through FIRESTORE_EMULATOR_HOST."""
    from google.cloud import firestore

    return firestore.Client(project=CONFIG.project)


# ─── Result model + check runner ────────────────────────────────────────────


class Expect(str, enum.Enum):
    """Whether an operation is expected to work or is a documented gap."""

    PASS = "pass"
    UNSUPPORTED = "unsupported"


class Status(str, enum.Enum):
    PASS = "pass"
    FAIL = "fail"
    UNSUPPORTED = "unsupported"


@dataclasses.dataclass
class Result:
    service: str
    operation: str
    status: str
    expected: str
    regression: bool
    detail: str = ""
    error: str = ""
    duration_ms: int = 0


# Populated by :func:`check`; conftest writes it to report/report.{json,md}.
RESULTS: "list[Result]" = []


def _grpc_code(exc: BaseException) -> Optional[str]:
    if isinstance(exc, grpc.RpcError):
        try:
            return exc.code().name
        except Exception:  # noqa: BLE001 - best effort diagnostics
            return None
    return None


def _is_unimplemented(exc: BaseException) -> bool:
    if isinstance(exc, NotImplementedError):
        return True
    if _grpc_code(exc) == "UNIMPLEMENTED":
        return True
    # google-api-core surfaces HTTP 501 as MethodNotImplemented (REST path).
    try:
        from google.api_core import exceptions as gexc

        return isinstance(exc, gexc.MethodNotImplemented)
    except Exception:  # noqa: BLE001 - google-api-core is always installed
        return False


def describe_error(exc: BaseException) -> str:
    code = _grpc_code(exc)
    if code:
        return f"{code}: {exc}"
    return f"{type(exc).__name__}: {exc}"


def check(
    service: str,
    operation: str,
    fn: Callable[[], Any],
    expected: Expect = Expect.PASS,
) -> Any:
    """Run one operation, classify it, record it, and reflect it in pytest.

    Returns the callable's return value (or detail string) on success.  A
    pass-expected operation that fails raises (a regression); an unimplemented
    or documented-gap operation is recorded as unsupported and skipped.
    """
    start = time.perf_counter()
    detail: str = ""
    error: str = ""
    status = Status.PASS
    try:
        value = fn()
        detail = "" if value is None else str(value)
    except Exception as exc:  # noqa: BLE001 - every failure is reported
        error = describe_error(exc)
        status = Status.UNSUPPORTED if _is_unimplemented(exc) else Status.FAIL

    duration_ms = int((time.perf_counter() - start) * 1000)
    regression = expected is Expect.PASS and status is not Status.PASS
    RESULTS.append(
        Result(
            service=service,
            operation=operation,
            status=status.value,
            expected=expected.value,
            regression=regression,
            detail=detail,
            error=error,
            duration_ms=duration_ms,
        )
    )

    if regression:
        pytest.fail(f"[{service}] {operation}: {error or 'returned no result'}")
    if status is Status.UNSUPPORTED:
        pytest.skip(f"[{service}] {operation} unsupported: {error}")
    if status is Status.FAIL:
        # Documented, non-fatal gap (expected=UNSUPPORTED but failed another
        # way): record it and move on rather than gating.
        pytest.skip(f"[{service}] {operation} recorded non-fatal failure: {error}")
    return detail
