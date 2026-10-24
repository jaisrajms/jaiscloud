"""jaiscloud-gcp SDK tour — Python errors leg (SDK_TOUR_MODE=errors).

Port of ../go/errors.go: the same 12 failure-surface scenarios, with the same
observable strings, driven by the official google-cloud-* clients. It reuses
the tour's Recorder/run helper and client wiring so its rows land in the
cross-language matrix beside the happy-path tour's.

Throttle injection goes through the emulator's runtime control plane
(POST /_jaiscloud/throttle); attempt counts are read back from the emulator's
Prometheus counters (emulator started with --metrics) so the retry scenarios
assert the observable attempt count rather than "it felt like it retried".

This is a demo/compliance artifact, not a conformance gate: failures are
recorded and classified, never papered over.
"""

from __future__ import annotations

import io
import json
import urllib.error
import urllib.request

from sdk_tour import (
    PROJECT,
    REST,
    Recorder,
    bigquery_client,
    pubsub_publisher,
    resumable_payload,
    rid,
    run,
    storage_client,
)

# ─── runtime throttle control ────────────────────────────────────────────────


def arm_throttle(body: dict) -> None:
    """Post a control document to the running injector."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        REST + "/_jaiscloud/throttle", data=data,
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req) as resp:  # raises HTTPError on non-2xx
        if resp.status != 200:
            raise RuntimeError(f"arm throttle: status {resp.status}")


def clear_throttle() -> None:
    arm_throttle({"mode": "off"})


# ─── metrics-backed attempt counting ─────────────────────────────────────────


def requests_total(service: str) -> int:
    """Sum jaiscloud_requests_total over series whose service label matches.

    The value counts the requests the emulator *received* for that surface, so
    the delta across an operation is its attempt count (refused and served
    alike).
    """
    with urllib.request.urlopen(REST + "/metrics") as resp:
        text = resp.read().decode()
    total = 0
    prefix = "jaiscloud_requests_total{"
    for raw in text.splitlines():
        line = raw.strip()
        if not line.startswith(prefix):
            continue
        end = line.find("}")
        if end < 0:
            continue
        labels = line[len(prefix):end]
        fields = line[end + 1:].split()
        if len(fields) != 1:
            continue
        if service == "":
            if 'service="unknown"' in labels:
                continue
        elif f'service="{service}"' not in labels:
            continue
        try:
            total += int(float(fields[0]))
        except ValueError:
            continue
    return total


def count_attempts(service: str, fn) -> int:
    """Run fn and return how many requests the emulator received for service."""
    before = requests_total(service)
    fn()
    after = requests_total(service)
    return after - before


# ─── error classification helpers ────────────────────────────────────────────


def http_status(exc: BaseException) -> int | None:
    """Extract the HTTP status an official client surfaced."""
    code = getattr(exc, "code", None)
    if isinstance(code, int):
        return code
    resp = getattr(exc, "response", None)
    if resp is not None:
        sc = getattr(resp, "status_code", None)
        if isinstance(sc, int):
            return sc
    return None


def require_status(exc: BaseException | None, want: int) -> None:
    """Raise unless exc is an HTTP status error with `want`."""
    if exc is None:
        raise AssertionError(f"expected HTTP {want}, got success")
    got = http_status(exc)
    if got is None:
        raise AssertionError(f"expected HTTP {want}, got non-HTTP error: {exc!r}")
    if got != want:
        raise AssertionError(f"expected HTTP {want}, got {got}: {exc}")


def is_conflict(exc: BaseException) -> bool:
    from google.api_core import exceptions as gexc

    return isinstance(exc, gexc.Conflict) or http_status(exc) == 409


# ─── storage error mapping ───────────────────────────────────────────────────


def storage_error_scenarios(rec: Recorder, bucket: str) -> None:
    client = storage_client()

    def ensure_bucket() -> None:
        try:
            client.create_bucket(bucket, location="US")
        except Exception as exc:  # noqa: BLE001 - idempotent across scenarios
            if not is_conflict(exc):
                raise

    def already_exists():
        ensure_bucket()
        try:
            client.create_bucket(bucket, location="US")
        except Exception as exc:  # noqa: BLE001
            require_status(exc, 409)
            return "already_exists=yes", "second bucket create surfaced 409 ALREADY_EXISTS"
        raise AssertionError("second bucket create did not raise Conflict")

    run(rec, "errors.storage_already_exists", "OK", already_exists)

    def not_found():
        ensure_bucket()
        try:
            client.bucket(bucket).blob("no/such/object").reload()
        except Exception as exc:  # noqa: BLE001
            require_status(exc, 404)
            return "not_found=yes", "missing object surfaced 404 NOT_FOUND"
        raise AssertionError("missing object did not raise NotFound")

    run(rec, "errors.storage_not_found", "OK", not_found)

    # A 404 must not be retried: the emulator must see exactly one request.
    def no_retry_on_4xx():
        ensure_bucket()

        def op():
            try:
                client.bucket(bucket).blob("no/such/object").reload()
            except Exception as exc:  # noqa: BLE001
                require_status(exc, 404)
                return
            raise AssertionError("missing object did not raise NotFound")

        n = count_attempts("storage", op)
        if n != 1:
            raise AssertionError(
                f"4xx must not be retried: emulator saw {n} attempts, want 1")
        return "no_retry_attempts=1", "404 failed after exactly one attempt"

    run(rec, "errors.no_retry_on_4xx", "OK", no_retry_on_4xx)

    # IAM optimistic concurrency: a stale policy etag must be rejected.
    def iam_failed_precondition():
        ensure_bucket()
        h = client.bucket(bucket)
        pol = h.get_iam_policy()
        pol.etag = "stale-etag"
        try:
            h.set_iam_policy(pol)
        except Exception as exc:  # noqa: BLE001
            require_status(exc, 409)
            return "failed_precondition=yes", "stale IAM etag surfaced 409"
        raise AssertionError("stale IAM etag was accepted (want 409 FAILED_PRECONDITION)")

    run(rec, "errors.iam_failed_precondition", "OK", iam_failed_precondition)


# ─── throttled retry scenarios ───────────────────────────────────────────────


def throttle_scenarios(rec: Recorder, bucket: str) -> None:
    client = storage_client()

    def ensure_bucket() -> None:
        try:
            client.create_bucket(bucket, location="US")
        except Exception as exc:  # noqa: BLE001
            if not is_conflict(exc):
                raise

    def make_retry_scenario(status: int, obs: str):
        def scenario():
            ensure_bucket()
            arm_throttle({"mode": "fault", "failFirst": 1, "services": ["storage"],
                          "status": status, "retryDelay": "1s"})
            try:
                n = count_attempts("storage", lambda: client.get_bucket(bucket))
            finally:
                clear_throttle()
            if n != 2:
                raise AssertionError(
                    f"client made {n} attempts, want 2 (one retry)")
            return obs, (f"injected {status} refused the first attempt; "
                         "the client retried once and succeeded")
        return scenario

    for name, status, obs in (
        ("errors.retry_429_storage_get", 429, "retry_429_attempts=2"),
        ("errors.retry_503_storage_get", 503, "retry_503_attempts=2"),
    ):
        run(rec, name, "OK", make_retry_scenario(status, obs))

    # Retry-info shape: the raw 429 envelope must carry the Retry-After header
    # and a google.rpc.RetryInfo detail, as real GCP does.
    def retry_info_shape():
        ensure_bucket()
        arm_throttle({"mode": "fault", "failFirst": 1, "services": ["storage"],
                      "status": 429, "retryDelay": "1s"})
        try:
            req = urllib.request.Request(REST + "/storage/v1/b/" + bucket)
            try:
                with urllib.request.urlopen(req) as resp:
                    raise AssertionError(f"want 429, got {resp.status}")
            except urllib.error.HTTPError as http_err:
                code = http_err.code
                headers = http_err.headers
                body = http_err.read().decode()
        finally:
            clear_throttle()
        if code != 429:
            raise AssertionError(f"want 429, got {code}: {body}")
        try:
            env = json.loads(body)
        except json.JSONDecodeError as exc:
            raise AssertionError(f"error body is not JSON: {exc} ({body})") from exc
        delay = None
        for detail in env.get("error", {}).get("details", []) or []:
            if detail.get("@type") == "type.googleapis.com/google.rpc.RetryInfo":
                delay = detail.get("retryDelay")
                break
        if delay is None:
            raise AssertionError(f"no google.rpc.RetryInfo detail in {body}")
        retry_after = headers.get("Retry-After")
        if not retry_after:
            raise AssertionError(f"no Retry-After header (headers={headers})")
        return (f"RetryInfo:{delay}:Retry-After={retry_after}",
                "injected 429 carried Retry-After + google.rpc.RetryInfo")

    run(rec, "errors.retry_info_shape", "OK", retry_info_shape)

    # Pagination stability: page tokens must survive a throttled retry.
    def pagination_stability():
        ensure_bucket()
        total = 5
        for i in range(total):
            client.bucket(bucket).blob(f"errors/page/{i:02d}.txt").upload_from_string(
                f"page-{i}")
        arm_throttle({"mode": "fault", "failFirst": 1, "services": ["storage"],
                      "status": 429, "retryDelay": "1s"})
        try:
            seen = 0
            for page in client.list_blobs(bucket, prefix="errors/page/",
                                          page_size=2).pages:
                seen += len(list(page))
        except Exception as exc:  # noqa: BLE001
            raise AssertionError(f"paginate across a throttled retry: {exc}") from exc
        finally:
            clear_throttle()
        if seen != total:
            raise AssertionError(f"listed {seen} objects across pages, want {total}")
        return (f"pagination_total={total}",
                "page tokens survived a throttled retry")

    run(rec, "errors.pagination_stability", "OK", pagination_stability)

    # Resumable rewind: a refused chunk must be re-sent and still checksum.
    def resumable_rewind():
        import hashlib

        ensure_bucket()
        # Scope the injected fault to the chunk PUT alone so the resumable
        # session start is unaffected: exactly a mid-upload failure.
        arm_throttle({"mode": "fault", "failFirst": 1,
                      "services": ["storage/objectsinsertresumable"],
                      "status": 429, "retryDelay": "1s"})
        try:
            payload = resumable_payload()
            blob = client.bucket(bucket).blob("errors/rewind.bin")
            blob.chunk_size = 256 * 1024
            blob.content_type = "application/octet-stream"
            # size=None forces the resumable protocol (a known small size would
            # take the single-request multipart path instead).
            blob.upload_from_file(io.BytesIO(payload), size=None, rewind=True)
        except Exception as exc:  # noqa: BLE001
            raise AssertionError(
                f"resumable write after mid-upload 429: {exc}") from exc
        finally:
            clear_throttle()
        got = client.bucket(bucket).blob("errors/rewind.bin").download_as_bytes()
        want = hashlib.sha256(payload).hexdigest()
        if hashlib.sha256(got).hexdigest() != want:
            raise AssertionError(
                f"rewound checksum {hashlib.sha256(got).hexdigest()}, want {want}")
        return (f"resumable_sha256={want}",
                "a refused chunk was re-sent and the object checksum matches")

    run(rec, "errors.resumable_rewind", "OK", resumable_rewind)


# ─── idempotency (BigQuery REST) ─────────────────────────────────────────────


def idempotency_scenarios(rec: Recorder) -> None:
    from google.api_core import exceptions as gexc
    from google.cloud import bigquery

    ds_id = rid("errors_ds").replace("-", "_")
    tbl_id = rid("errors_tbl").replace("-", "_")
    client = bigquery_client()

    def ensure_dataset() -> None:
        try:
            client.create_dataset(ds_id)
        except gexc.Conflict:
            pass

    def ensure_table() -> "bigquery.Table":
        table = bigquery.Table(f"{PROJECT}.{ds_id}.{tbl_id}", schema=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
        ])
        try:
            client.create_table(table)
        except gexc.Conflict:
            pass
        return client.get_table(f"{PROJECT}.{ds_id}.{tbl_id}")

    # A malformed request must surface INVALID_ARGUMENT synchronously.
    def invalid_argument():
        try:
            client.query("SELECT * FROM")
        except Exception as exc:  # noqa: BLE001
            require_status(exc, 400)
            return "invalid_argument=yes", "malformed SQL surfaced 400 INVALID_ARGUMENT"
        raise AssertionError("malformed SQL did not surface 400")

    run(rec, "errors.invalid_argument", "OK", invalid_argument)

    # insertId is the API's client-supplied idempotency key: replaying a row
    # with the same insertId must not create a second row.
    def idempotent_insertall():
        ensure_dataset()
        table = ensure_table()

        def ins():
            return client.insert_rows(
                table, [{"id": 1, "name": "a"}], row_ids=["idem-1"])

        first = ins()
        if first:
            raise AssertionError(f"first insertAll unexpectedly errored: {first}")
        second = ins()
        reasons = [e.get("reason") for err in second for e in err.get("errors", [])]
        # Real GCP's insertId dedup is best-effort: it may silently accept the
        # replayed row (empty errors) or report it as a duplicate. Either is
        # fine; what must hold is that no second row exists.
        unexpected = [r for r in reasons if r != "duplicate"]
        if unexpected:
            raise AssertionError(
                f"replayed insertAll returned unexpected errors {unexpected}: {second}")
        rows = list(client.list_rows(table))
        if len(rows) != 1:
            raise AssertionError(
                f"idempotent insert produced {len(rows)} rows, want 1")
        return ("idempotent_rows=1",
                "replayed insertId was de-duplicated (one stored row)")

    run(rec, "errors.idempotent_insertall", "OK", idempotent_insertall)


# ─── gRPC error mapping (Pub/Sub) ────────────────────────────────────────────


def pubsub_error_scenarios(rec: Recorder, topic: str) -> None:
    from google.api_core import exceptions as gexc

    def pubsub_topic_already_exists():
        publisher = pubsub_publisher()
        path = publisher.topic_path(PROJECT, topic)
        publisher.create_topic(request={"name": path})
        try:
            publisher.create_topic(request={"name": path})
        except Exception as exc:  # noqa: BLE001
            if isinstance(exc, gexc.AlreadyExists):
                return ("grpc_already_exists=yes",
                        "duplicate topic create surfaced gRPC ALREADY_EXISTS")
            raise AssertionError(
                f"duplicate topic create want AlreadyExists, got: {exc}") from exc
        raise AssertionError("duplicate topic create did not raise AlreadyExists")

    run(rec, "errors.pubsub_topic_already_exists", "OK",
        pubsub_topic_already_exists)


# ─── entry point ─────────────────────────────────────────────────────────────


def errors_scenarios(rec: Recorder) -> None:
    bucket = rid("errors-bucket")
    topic = rid("errors-topic")
    storage_error_scenarios(rec, bucket)
    throttle_scenarios(rec, bucket)
    idempotency_scenarios(rec)
    pubsub_error_scenarios(rec, topic)
