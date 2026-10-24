"""BigQuery REST conformance via the official ``google-cloud-bigquery``.

Wiring: no emulator env hook exists for BigQuery, so the client is pointed at
the emulator with ``client_options.api_endpoint`` and ``AnonymousCredentials``
(see ``harness.bigquery_client``).  A GCS source object is staged with the
storage client (``STORAGE_EMULATOR_HOST``) and loaded from ``gs://`` with
``Client.load_table_from_uri``, exercising the load-job path (BQL1).
"""

from __future__ import annotations

import io
import os
import tempfile

import pytest
from google.cloud import bigquery

from harness import CONFIG, bigquery_client, check, storage_client, unique

SERVICE = "bigquery"

pytestmark = pytest.mark.rest


@pytest.fixture
def dataset(client):
    ds_id = unique("pyc_bq_ds").replace("-", "_")
    ds = client.create_dataset(ds_id)
    yield ds
    try:
        client.delete_dataset(ds, delete_contents=True)
    except Exception:  # noqa: BLE001 - best-effort teardown
        pass


@pytest.fixture
def client():
    return bigquery_client()


def test_load_ndjson_from_uri(client, dataset):
    """The canonical ingestion path: load_table_from_uri of a gs:// NDJSON object."""
    table_id = unique("pyc_bq_tbl").replace("-", "_")
    table = client.create_table(
        bigquery.Table(
            f"{CONFIG.project}.{dataset.dataset_id}.{table_id}",
            schema=[
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING"),
            ],
        )
    )

    # Stage the NDJSON source in the emulated GCS (same emulator origin).
    bucket = storage_client().create_bucket(unique("pyc-bq-load"))
    blob = bucket.blob("rows.json")
    blob.upload_from_string(
        '{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n',
        content_type="application/x-ndjson",
    )

    def op():
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = client.load_table_from_uri(
            f"gs://{bucket.name}/{blob.name}", table, job_config=cfg
        )
        job.result()  # polls jobs.get; the emulator completes synchronously
        assert job.state == "DONE", f"job state {job.state!r}"
        rows = sorted((r["id"], r["name"]) for r in client.list_rows(table))
        assert rows == [(1, "a"), (2, "b")], rows
        return f"{len(rows)} rows"

    try:
        check(SERVICE, "LoadTableFromUri", op)
    finally:
        try:
            blob.delete()
            bucket.delete()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass


def _make_upload_table(client, dataset):
    table_id = unique("pyc_bq_up").replace("-", "_")
    return client.create_table(
        bigquery.Table(
            f"{CONFIG.project}.{dataset.dataset_id}.{table_id}",
            schema=[
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING"),
            ],
        )
    )


def test_load_ndjson_from_file_resumable(client, dataset):
    """load_table_from_file with an unknown size uses the resumable session path."""
    table = _make_upload_table(client, dataset)
    payload = b'{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n'

    def op():
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        # size is intentionally omitted: the client then drives a resumable
        # upload session (initiate -> chunk PUT -> finalize).
        job = client.load_table_from_file(io.BytesIO(payload), table, job_config=cfg)
        job.result()
        assert job.state == "DONE", f"job state {job.state!r}"
        rows = sorted((r["id"], r["name"]) for r in client.list_rows(table))
        assert rows == [(1, "a"), (2, "b")], rows
        return f"{len(rows)} rows"

    check(SERVICE, "LoadTableFromFileResumable", op)


def test_load_ndjson_from_file_multipart(client, dataset):
    """load_table_from_file with an explicit size uses the multipart path."""
    table = _make_upload_table(client, dataset)
    payload = b'{"id":3,"name":"c"}\n{"id":4,"name":"d"}\n'

    fd, path = tempfile.mkstemp(suffix=".ndjson")
    with os.fdopen(fd, "wb") as fh:
        fh.write(payload)

    def op():
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        with open(path, "rb") as file_obj:
            job = client.load_table_from_file(
                file_obj, table, job_config=cfg, size=len(payload)
            )
            job.result()
            assert job.state == "DONE", f"job state {job.state!r}"
        rows = sorted((r["id"], r["name"]) for r in client.list_rows(table))
        assert rows == [(3, "c"), (4, "d")], rows
        return f"{len(rows)} rows"

    try:
        check(SERVICE, "LoadTableFromFileMultipart", op)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def _stage_bytes(prefix: str, name: str, data: bytes, content_type: str):
    """Upload bytes to the emulated GCS, returning (bucket, blob) for teardown."""
    bucket = storage_client().create_bucket(unique(prefix))
    blob = bucket.blob(name)
    blob.upload_from_string(data, content_type=content_type)
    return bucket, blob


def test_load_parquet_from_uri(client, dataset):
    """The self-describing Parquet path (BQL3): the file's schema drives the table."""
    table = _make_upload_table(client, dataset)
    fixture = os.path.join(os.path.dirname(__file__), "testdata", "load.parquet")
    with open(fixture, "rb") as fh:
        payload = fh.read()
    bucket, blob = _stage_bytes(
        "pyc-bq-parquet", "rows.parquet", payload, "application/octet-stream"
    )

    def op():
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = client.load_table_from_uri(
            f"gs://{bucket.name}/{blob.name}", table, job_config=cfg
        )
        job.result()
        assert job.state == "DONE", f"job state {job.state!r}"
        rows = sorted((r["id"], r["name"]) for r in client.list_rows(table))
        assert rows == [(101, "alice"), (102, "bob")], rows
        return f"{len(rows)} rows"

    try:
        check(SERVICE, "LoadTableFromUriParquet", op)
    finally:
        try:
            blob.delete()
            bucket.delete()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass


def test_load_parquet_autodetect_table(client, dataset):
    """A PARQUET load into a table created with no schema derives it from the file."""
    table_id = unique("pyc_bq_pqauto").replace("-", "_")
    table = client.create_table(bigquery.Table(f"{CONFIG.project}.{dataset.dataset_id}.{table_id}"))
    fixture = os.path.join(os.path.dirname(__file__), "testdata", "load.parquet")
    with open(fixture, "rb") as fh:
        payload = fh.read()
    bucket, blob = _stage_bytes(
        "pyc-bq-pqauto", "rows.parquet", payload, "application/octet-stream"
    )

    def op():
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = client.load_table_from_uri(
            f"gs://{bucket.name}/{blob.name}", table, job_config=cfg
        )
        job.result()
        assert job.state == "DONE", f"job state {job.state!r}"
        rows = sorted((r["id"], r["name"]) for r in client.list_rows(table))
        assert rows == [(101, "alice"), (102, "bob")], rows
        return f"{len(rows)} rows"

    try:
        check(SERVICE, "LoadTableFromUriParquetDerivedSchema", op)
    finally:
        try:
            blob.delete()
            bucket.delete()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass
