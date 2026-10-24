"""jaiscloud-gcp SDK tour — Python leg (official google-cloud-* clients).

Runs the same 18 scenarios as the Go/Java/Node legs against the emulator and
emits one JSONL record per scenario. It is the Python analogue of the Go
program in ../go, wired exactly like tests/clients/python/harness.py: the
clients with an emulator env hook (Storage/PubSub/Firestore) use it, the rest
get a plaintext generated transport / endpoint override.

This is a demo/compliance artifact, not a conformance gate: failures are
recorded and classified, never papered over.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
import time

import grpc

# ─── environment ─────────────────────────────────────────────────────────────

REST = os.environ.get("EMULATOR_REST", "http://localhost:8080").rstrip("/")
GRPC = os.environ.get("EMULATOR_GRPC", "localhost:8081")
PROJECT = os.environ.get("PROJECT_ID", "jaiscloud-project")
RUN_ID = os.environ.get("SDK_TOUR_RUN_ID", "local")
OUT = os.environ.get("SDK_TOUR_RESULTS", "")
# "tour" (default) runs the happy-path scenarios; "errors" runs the failure /
# retry / idempotency tour in errors_tour.py instead.
MODE = os.environ.get("SDK_TOUR_MODE", "tour")

# Emulator hooks the official Python clients honour. Set before any client is
# constructed; an explicit caller value always wins.
os.environ.setdefault("PROJECT_ID", PROJECT)
os.environ.setdefault("STORAGE_EMULATOR_HOST", REST)
os.environ.setdefault("PUBSUB_EMULATOR_HOST", GRPC)
os.environ.setdefault("FIRESTORE_EMULATOR_HOST", GRPC)

BATCH_COUNT = 32


def rid(leaf: str) -> str:
    return f"{leaf}-{RUN_ID}"


def resumable_payload() -> bytes:
    unit = b"jaiscloud-sdk-tour-resumable-payload-"
    target = 4 << 20
    return (unit * (target // len(unit) + 1))[:target]


# ─── result recording ────────────────────────────────────────────────────────

class Recorder:
    def __init__(self) -> None:
        self.fh = open(OUT, "w") if OUT else None

    def record(self, scenario: str, status: str, expect: str, observable: str = "",
               detail: str = "", error: str = "", classification: str = "") -> None:
        rec = {
            "lang": "python", "scenario": scenario, "status": status, "expect": expect,
            "observable": observable, "detail": detail, "error": error,
            "classification": classification,
        }
        line = f"PYTHON {scenario} {status}"
        if detail:
            line += f" {detail}"
        if error:
            line += f" err={error}"
        print(line, flush=True)
        if self.fh:
            self.fh.write(json.dumps(rec) + "\n")
            self.fh.flush()

    def close(self) -> None:
        if self.fh:
            self.fh.close()


def classify(exc: BaseException) -> str:
    if isinstance(exc, grpc.RpcError):
        try:
            code = exc.code()
        except Exception:  # noqa: BLE001
            code = None
        if code == grpc.StatusCode.UNIMPLEMENTED:
            return "unimplemented"
        if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.UNAUTHENTICATED,
                    grpc.StatusCode.PERMISSION_DENIED):
            return "wiring-gap"
    try:
        from google.api_core import exceptions as gexc

        if isinstance(exc, gexc.MethodNotImplemented):
            return "unimplemented"
        if isinstance(exc, (gexc.PermissionDenied, gexc.Unauthenticated, gexc.ServiceUnavailable)):
            return "wiring-gap"
    except Exception:  # noqa: BLE001
        pass
    msg = str(exc).lower()
    for needle in ("connection refused", "failed to connect", "tls", "handshake",
                   "no route to host", "name resolution", "statuscode.unavailable",
                   "statuscode.unauthenticated", "permissiondenied"):
        if needle in msg:
            return "wiring-gap"
    if "not implemented" in msg or "unimplemented" in msg:
        return "unimplemented"
    return "emulator-bug"


def run(rec: Recorder, scenario: str, expect: str, fn) -> None:
    """Run one scenario, classify a failure, and record OK|FAIL|SKIP.

    fn returns (observable, detail). expect="SKIP" marks a pre-declared
    unsupported surface: a failure is a documented gap (SKIP), not a hard FAIL.
    """
    start = time.monotonic()
    try:
        observable, detail = fn()
        rec.record(scenario, "OK", expect, observable=observable, detail=detail)
    except Exception as exc:  # noqa: BLE001 - every failure is reported
        cls = classify(exc)
        status = "SKIP" if expect == "SKIP" else "FAIL"
        rec.record(scenario, status, expect,
                   detail=f"{int((time.monotonic() - start) * 1000)}ms",
                   error=f"{type(exc).__name__}: {exc}", classification=cls)


# ─── client wiring ───────────────────────────────────────────────────────────

def storage_client():
    from google.cloud import storage

    return storage.Client(project=PROJECT)


def pubsub_publisher(**kwargs):
    from google.cloud import pubsub_v1

    return pubsub_v1.PublisherClient(**kwargs)


def pubsub_subscriber():
    from google.cloud import pubsub_v1

    return pubsub_v1.SubscriberClient()


def firestore_client():
    from google.cloud import firestore

    return firestore.Client(project=PROJECT)


def logging_client():
    from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
    from google.cloud.logging_v2.services.logging_service_v2.transports import (
        LoggingServiceV2GrpcTransport,
    )

    transport = LoggingServiceV2GrpcTransport(channel=grpc.insecure_channel(GRPC))
    return LoggingServiceV2Client(transport=transport)


def secretmanager_client():
    from google.cloud import secretmanager
    from google.cloud.secretmanager_v1.services.secret_manager_service.transports import (
        SecretManagerServiceGrpcTransport,
    )

    transport = SecretManagerServiceGrpcTransport(channel=grpc.insecure_channel(GRPC))
    return secretmanager.SecretManagerServiceClient(transport=transport)


def kms_client():
    from google.cloud import kms_v1
    from google.cloud.kms_v1.services.key_management_service.transports import (
        KeyManagementServiceGrpcTransport,
    )

    transport = KeyManagementServiceGrpcTransport(channel=grpc.insecure_channel(GRPC))
    return kms_v1.KeyManagementServiceClient(transport=transport)


def bigquery_client():
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery

    return bigquery.Client(project=PROJECT, credentials=AnonymousCredentials(),
                           client_options={"api_endpoint": REST})


def dataproc_client():
    from google.cloud import dataproc_v1
    from google.cloud.dataproc_v1.services.cluster_controller.transports import (
        ClusterControllerGrpcTransport,
    )

    transport = ClusterControllerGrpcTransport(channel=grpc.insecure_channel(GRPC))
    return dataproc_v1.ClusterControllerClient(transport=transport)


# ─── fixtures ────────────────────────────────────────────────────────────────

BUCKET = rid("sdk-tour-bucket")
TOPIC = rid("sdk-tour-topic")
SUB = rid("sdk-tour-sub")
BQ_BUCKET = rid("sdk-tour-bq")


def _ensure_topic(publisher):
    from google.api_core import exceptions as gexc

    path = publisher.topic_path(PROJECT, TOPIC)
    try:
        publisher.create_topic(request={"name": path})
    except gexc.AlreadyExists:
        pass
    return path


def _ensure_subscription(subscriber, topic_path):
    from google.api_core import exceptions as gexc

    path = subscriber.subscription_path(PROJECT, SUB)
    try:
        subscriber.create_subscription(request={"name": path, "topic": topic_path})
    except gexc.AlreadyExists:
        pass
    return path


# ─── scenarios ───────────────────────────────────────────────────────────────

def storage_scenarios(rec: Recorder) -> None:
    from google.cloud import storage

    client = storage_client()

    def create_bucket():
        bucket = client.bucket(BUCKET)
        bucket.location = "US"
        try:
            bucket.create()
        except Exception as exc:  # noqa: BLE001 - idempotent across runs
            if "409" not in str(exc) and "already" not in str(exc).lower():
                raise
        return "created", f"bucket={BUCKET}"

    run(rec, "storage.create_bucket", "OK", create_bucket)

    def resumable_upload():
        payload = resumable_payload()
        blob = client.bucket(BUCKET).blob("resumable/payload.bin")
        blob.chunk_size = 256 * 1024  # forces the resumable protocol
        blob.content_type = "application/octet-stream"
        blob.upload_from_string(payload)
        blob.reload()
        return str(blob.size), f"size={blob.size} chunk_size=262144 resumable=true"

    run(rec, "storage.resumable_upload", "OK", resumable_upload)

    def stream_download_checksum():
        blob = client.bucket(BUCKET).blob("resumable/payload.bin")
        h = hashlib.sha256()
        n = 0
        with blob.open("rb") as fh:
            while True:
                chunk = fh.read(64 * 1024)
                if not chunk:
                    break
                h.update(chunk)
                n += len(chunk)
        got, want = h.hexdigest(), hashlib.sha256(resumable_payload()).hexdigest()
        if got != want:
            raise AssertionError(f"checksum mismatch: got {got} want {want} bytes={n}")
        return got, f"sha256={got} bytes={n}"

    run(rec, "storage.stream_download_checksum", "OK", stream_download_checksum)

    def list_pagination():
        for i in range(7):
            client.bucket(BUCKET).blob(f"page/{i:02d}.txt").upload_from_string(f"page-{i}")
        pages = total = 0
        iterator = client.list_blobs(BUCKET, prefix="page/", page_size=2)
        for page in iterator.pages:
            pages += 1
            total += len(list(page))
        if total != 7:
            raise AssertionError(f"listed {total} objects, want 7")
        return f"pages={pages},total={total}", f"page_size=2 pages={pages} total={total}"

    run(rec, "storage.list_pagination", "OK", list_pagination)


def pubsub_scenarios(rec: Recorder) -> None:
    from google.cloud.pubsub_v1.types import BatchSettings

    publisher = pubsub_publisher(batch_settings=BatchSettings(
        max_messages=100, max_latency=0.05, max_bytes=1 << 20))
    topic_path = _ensure_topic(publisher)

    def batch_publish():
        futures = [publisher.publish(topic_path, data=f"batch-{i:02d}".encode())
                   for i in range(BATCH_COUNT)]
        for f in futures:
            f.result(timeout=30)
        return str(len(futures)), f"published={len(futures)} batching=true"

    run(rec, "pubsub.batch_publish", "OK", batch_publish)

    def streaming_pull_ack():
        subscriber = pubsub_subscriber()
        sub_path = _ensure_subscription(subscriber, topic_path)
        for i in range(BATCH_COUNT):
            publisher.publish(topic_path, data=f"stream-{i:02d}".encode()).result(timeout=30)
        received: list[bytes] = []
        done = threading.Event()

        def callback(message):
            received.append(message.data)
            message.ack()
            if len(received) >= BATCH_COUNT:
                done.set()

        future = subscriber.subscribe(sub_path, callback)
        try:
            if not done.wait(30):
                raise AssertionError(f"streaming pull received {len(received)}/{BATCH_COUNT}")
        finally:
            future.cancel()
            subscriber.close()
        n = len(received)
        return str(n), f"received={n} acked={n} streaming=true"

    run(rec, "pubsub.streaming_pull_ack", "OK", streaming_pull_ack)

    def policy_read_modify_write():
        role, member = "roles/pubsub.publisher", "allUsers"
        policy = publisher.get_iam_policy(request={"resource": topic_path})
        policy.bindings.add(role=role, members=[member])
        publisher.set_iam_policy(request={"resource": topic_path, "policy": policy})
        policy2 = publisher.get_iam_policy(request={"resource": topic_path})
        found = any(b.role == role and member in b.members for b in policy2.bindings)
        if not found:
            raise AssertionError(f"role {role} members {[list(b.members) for b in policy2.bindings]}")
        return "true", f"role={role} member={member}"

    run(rec, "iam.policy_read_modify_write", "OK", policy_read_modify_write)


def firestore_scenarios(rec: Recorder) -> None:
    from google.cloud import firestore

    client = firestore_client()

    def listen_write():
        col = client.collection(rid("fs-listen"))
        seen = threading.Event()

        def on_snapshot(col_snapshot, changes, read_time):  # noqa: ARG001
            if any(doc.id == "live-doc" for doc in col_snapshot):
                seen.set()

        watcher = col.on_snapshot(on_snapshot)
        try:
            col.document("live-doc").set({"v": 1})
            if not seen.wait(20):
                raise AssertionError("Listen did not deliver live-doc")
        finally:
            watcher.unsubscribe()
        return "true", "Listen delivered ADD for live-doc"

    run(rec, "firestore.listen_write", "OK", listen_write)

    def transaction():
        col = client.collection(rid("fs-tx"))
        ref = col.document("counter")
        ref.set({"n": 0})

        @firestore.transactional
        def bump(tx):
            snap = ref.get(transaction=tx)
            tx.set(ref, {"n": snap.get("n") + 1})

        for _ in range(3):
            bump(client.transaction())
        n = ref.get().get("n")
        if n != 3:
            raise AssertionError(f"counter = {n}, want 3")
        return str(n), "3 read-modify-write transactions"

    run(rec, "firestore.transaction", "OK", transaction)

    def query_pagination():
        col = client.collection(rid("fs-page"))
        for i in range(5):
            col.document(f"d{i}").set({"i": i})
        query = col.order_by("i").limit(2)
        cursor_i = None
        pages = total = 0
        while True:
            q = query.start_after([cursor_i]) if cursor_i is not None else query
            docs = list(q.stream())
            if not docs:
                break
            cursor_i = docs[-1].get("i")
            pages += 1
            total += len(docs)
        if total != 5:
            raise AssertionError(f"paged {total} docs, want 5")
        return f"pages={pages},total={total}", f"limit=2 pages={pages} total={total}"

    run(rec, "firestore.query_pagination", "OK", query_pagination)


def logging_scenarios(rec: Recorder) -> None:
    from google.cloud.logging_v2 import types as lpb

    parent = f"projects/{PROJECT}"
    log_name = f"{parent}/logs/{rid('sdk-tour-log')}"
    client = logging_client()

    def write_entry():
        client.write_log_entries(request={
            "log_name": log_name,
            "resource": {"type": "global", "labels": {"project_id": PROJECT}},
            "entries": [{"log_name": log_name, "severity": "INFO", "text_payload": "sdk-tour-entry"}],
        })
        pager = client.list_log_entries(request={
            "resource_names": [parent], "filter": f'logName="{log_name}"'})
        n = sum(1 for _ in pager)
        if n != 1:
            raise AssertionError(f"ListLogEntries returned {n} entries, want 1")
        return str(n), "WriteLogEntries + ListLogEntries round-trip"

    run(rec, "logging.write_entry", "OK", write_entry)

    def tail():
        sent = threading.Event()

        def request_iter():
            yield lpb.TailLogEntriesRequest(
                resource_names=[parent], filter=f'logName="{log_name}"',
                buffer_window={"seconds": 0, "nanos": 200_000_000})
            sent.set()
            time.sleep(30)  # keep the bidi stream open; no further sends

        def writer():
            sent.wait(5)
            time.sleep(0.4)
            for i in range(40):
                client.write_log_entries(request={
                    "log_name": log_name,
                    "resource": {"type": "global", "labels": {"project_id": PROJECT}},
                    "entries": [{"log_name": log_name, "severity": "INFO",
                                 "text_payload": f"sdk-tour-tail-{i}"}],
                })
                time.sleep(0.5)

        threading.Thread(target=writer, daemon=True).start()
        for resp in client.tail_log_entries(requests=request_iter()):
            for entry in resp.entries:
                if entry.text_payload.startswith("sdk-tour-tail"):
                    return "true", "TailLogEntries delivered the entry"
        raise AssertionError("TailLogEntries stream ended without the entry")

    run(rec, "logging.tail", "OK", tail)


def secret_scenarios(rec: Recorder) -> None:
    parent = f"projects/{PROJECT}"
    secret_id = rid("sdk-tour-secret")
    client = secretmanager_client()

    def add_access_list():
        secret = client.create_secret(request={
            "parent": parent, "secret_id": secret_id,
            "secret": {"replication": {"automatic": {}}}})
        client.add_secret_version(request={
            "parent": secret.name, "payload": {"data": b"sdk-tour"}})
        acc = client.access_secret_version(request={"name": secret.name + "/versions/1"})
        if acc.payload.data != b"sdk-tour":
            raise AssertionError(f"accessed payload {acc.payload.data!r}")
        versions = sum(1 for _ in client.list_secret_versions(request={"parent": secret.name}))
        if versions < 1:
            raise AssertionError(f"ListSecretVersions returned {versions}")
        return str(versions), f"access=ok versions={versions}"

    run(rec, "secretmanager.add_access_list", "OK", add_access_list)


def kms_scenarios(rec: Recorder) -> None:
    import base64

    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding, utils

    parent = f"projects/{PROJECT}/locations/global"
    client = kms_client()

    def encrypt_decrypt():
        ring = client.create_key_ring(request={
            "parent": parent, "key_ring_id": rid("sdk-tour-ring"), "key_ring": {}})
        key = client.create_crypto_key(request={
            "parent": ring.name, "crypto_key_id": rid("sdk-tour-key"),
            "crypto_key": {"purpose": "ENCRYPT_DECRYPT"}})
        enc = client.encrypt(request={"name": key.name, "plaintext": b"sdk-tour"})
        dec = client.decrypt(request={"name": key.name, "ciphertext": enc.ciphertext})
        if dec.plaintext != b"sdk-tour":
            raise AssertionError(f"decrypted {dec.plaintext!r}")
        return "true", "encrypt/decrypt round-trip"

    run(rec, "kms.encrypt_decrypt", "OK", encrypt_decrypt)

    def asymmetric_sign():
        ring = client.create_key_ring(request={
            "parent": parent, "key_ring_id": rid("sdk-tour-sign-ring"), "key_ring": {}})
        key = client.create_crypto_key(request={
            "parent": ring.name, "crypto_key_id": rid("sdk-tour-sign-key"),
            "crypto_key": {
                "purpose": "ASYMMETRIC_SIGN",
                "version_template": {"algorithm": "RSA_SIGN_PKCS1_2048_SHA256"}}})
        version = key.name + "/cryptoKeyVersions/1"
        digest = hashlib.sha256(b"sdk-tour-sign").digest()
        sig = client.asymmetric_sign(request={
            "name": version, "digest": {"sha256": digest}})
        pk = client.get_public_key(request={"name": version})
        pub = serialization.load_pem_public_key(pk.pem.encode())
        try:
            # KMS signs a pre-computed digest, so verify it as prehashed rather
            # than letting cryptography hash the 32 digest bytes again.
            pub.verify(sig.signature, digest, padding.PKCS1v15(),
                       utils.Prehashed(hashes.SHA256()))
        except InvalidSignature as exc:
            raise AssertionError(f"signature did not verify: {exc}") from exc
        return "true", "AsymmetricSign verified with GetPublicKey"

    run(rec, "kms.asymmetric_sign", "OK", asymmetric_sign)


def bigquery_scenarios(rec: Recorder) -> None:
    from google.cloud import bigquery

    ds_id = rid("sdk_tour_ds").replace("-", "_")
    tbl_id = rid("sdk_tour_tbl").replace("-", "_")
    load_tbl_id = rid("sdk_tour_load").replace("-", "_")
    client = bigquery_client()

    def ensure_dataset():
        from google.api_core import exceptions as gexc

        try:
            client.create_dataset(ds_id)
        except gexc.Conflict:
            pass

    def ensure_table(tbl):
        from google.api_core import exceptions as gexc

        table = bigquery.Table(f"{PROJECT}.{ds_id}.{tbl}", schema=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
        ])
        try:
            client.create_table(table)
        except gexc.Conflict:
            pass

    def insertall_query():
        ensure_dataset()
        ensure_table(tbl_id)
        errors = client.insert_rows_json(f"{PROJECT}.{ds_id}.{tbl_id}",
                                         [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])
        if errors:
            raise AssertionError(f"insertAll errors: {errors}")
        rows = list(client.query(
            f"SELECT id, name FROM `{PROJECT}.{ds_id}.{tbl_id}` ORDER BY id"))
        if len(rows) != 2:
            raise AssertionError(f"query returned {len(rows)} rows, want 2")
        return str(len(rows)), "insertAll + query returned 2 rows"

    run(rec, "bigquery.insertall_query", "OK", insertall_query)

    def load_job():
        sc = storage_client()
        bucket = sc.bucket(BQ_BUCKET)
        bucket.location = "US"
        from google.api_core import exceptions as gexc

        try:
            bucket.create()
        except gexc.Conflict:
            pass
        bucket.blob("rows.json").upload_from_string(
            '{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n', content_type="application/x-ndjson")

        ensure_dataset()
        ensure_table(load_tbl_id)
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        job = client.load_table_from_uri(
            f"gs://{BQ_BUCKET}/rows.json", f"{PROJECT}.{ds_id}.{load_tbl_id}", job_config=cfg)
        job.result(timeout=60)
        if job.state != "DONE":
            raise AssertionError(f"load job state {job.state}")
        rows = list(client.list_rows(f"{PROJECT}.{ds_id}.{load_tbl_id}"))
        if len(rows) != 2:
            raise AssertionError(f"load job produced {len(rows)} rows, want 2")
        return str(len(rows)), "gs:// load job produced 2 rows"

    run(rec, "bigquery.load_job", "OK", load_job)


def dataproc_scenarios(rec: Recorder) -> None:
    from google.cloud import dataproc_v1

    region = "us-central1"
    client = dataproc_client()

    def cluster_lro():
        name = rid("sdk-tour-cluster")
        cluster = dataproc_v1.Cluster(
            project_id=PROJECT, cluster_name=name,
            config=dataproc_v1.ClusterConfig(
                gce_cluster_config=dataproc_v1.GceClusterConfig(zone_uri="us-central1-a"),
                software_config=dataproc_v1.SoftwareConfig(image_version="2.2")))
        op = client.create_cluster(request=dataproc_v1.CreateClusterRequest(
            project_id=PROJECT, region=region, cluster=cluster))
        result = op.result(timeout=60)  # gax operation poller
        state = dataproc_v1.ClusterStatus.State(result.status.state).name
        if state != "RUNNING":
            raise AssertionError(f"cluster state = {state}, want RUNNING")
        try:
            client.delete_cluster(request=dataproc_v1.DeleteClusterRequest(
                project_id=PROJECT, region=region, cluster_name=name)).result(timeout=60)
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass
        return state, f"op.result() settled the LRO to {state}"

    run(rec, "lro.dataproc_cluster", "OK", cluster_lro)


def main() -> int:
    rec = Recorder()
    try:
        if MODE == "errors":
            import errors_tour

            errors_tour.errors_scenarios(rec)
            return 0
        if MODE == "streaming":
            import streaming_tour

            streaming_tour.streaming_scenarios(rec)
            return 0
        storage_scenarios(rec)
        pubsub_scenarios(rec)
        firestore_scenarios(rec)
        logging_scenarios(rec)
        secret_scenarios(rec)
        kms_scenarios(rec)
        bigquery_scenarios(rec)
        dataproc_scenarios(rec)
    finally:
        rec.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
