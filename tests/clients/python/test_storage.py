"""Cloud Storage REST conformance via the official ``google-cloud-storage``.

Wiring: ``STORAGE_EMULATOR_HOST`` points the client at the emulator and makes
it select ``AnonymousCredentials`` automatically.  ``google.cloud.storage.Client``
speaks REST (JSON API + resumable/media uploads).
"""

from __future__ import annotations

import pytest
from google.api_core import exceptions as gexc

from harness import check, storage_client, unique

SERVICE = "storage"

pytestmark = pytest.mark.rest


@pytest.fixture
def client():
    return storage_client()


@pytest.fixture
def bucket(client):
    b = client.create_bucket(unique("pyc-bucket"))
    yield b
    try:
        for blob in b.list_blobs():
            blob.delete()
        b.delete()
    except Exception:  # noqa: BLE001 - best-effort teardown
        pass


def test_create_bucket(client):
    name = unique("pyc-create")

    def op():
        created = client.create_bucket(name).name
        assert created == name, f"CreateBucket returned {created!r}, want {name!r}"
        return created

    check(SERVICE, "CreateBucket", op)


def test_get_bucket(client, bucket):
    def op():
        got = client.get_bucket(bucket.name).name
        assert got == bucket.name, f"GetBucket returned {got!r}, want {bucket.name!r}"
        return got

    check(SERVICE, "GetBucket", op)


def test_list_buckets(client, bucket):
    def op():
        names = [b.name for b in client.list_buckets()]
        assert bucket.name in names, f"{bucket.name} not in list"
        return f"{len(names)} buckets"

    check(SERVICE, "ListBuckets", op)


def test_upload_object(client, bucket):
    blob = bucket.blob("objects/hello.txt")
    check(
        SERVICE,
        "UploadObject",
        lambda: (
            blob.upload_from_string(b"hello-bytes", content_type="text/plain"),
            blob.name,
        )[1],
    )


def test_download_object(client, bucket):
    payload = b"the quick brown fox"

    def op():
        bucket.blob("objects/download.bin").upload_from_string(
            payload, content_type="application/octet-stream"
        )
        data = bucket.blob("objects/download.bin").download_as_bytes()
        assert data == payload, f"downloaded {data!r}, want {payload!r}"
        return f"{len(data)} bytes"

    check(SERVICE, "DownloadObject", op)


def test_get_object_metadata(client, bucket):
    def op():
        blob = bucket.blob("objects/meta.txt")
        blob.upload_from_string(b"content", content_type="text/plain")
        blob.reload()
        assert blob.size == len(b"content"), f"size={blob.size}"
        assert blob.content_type == "text/plain", f"content_type={blob.content_type}"
        return f"size={blob.size} content_type={blob.content_type}"

    check(SERVICE, "GetObjectMetadata", op)


def test_list_objects(client, bucket):
    def op():
        for name in ("objects/a.txt", "objects/b.txt", "dir/c.txt"):
            bucket.blob(name).upload_from_string(b"x")
        names = {b.name for b in client.list_blobs(bucket)}
        for want in ("objects/a.txt", "objects/b.txt", "dir/c.txt"):
            assert want in names, f"{want} not listed"
        return f"{len(names)} objects"

    check(SERVICE, "ListObjects", op)


def test_copy_object(client, bucket):
    def op():
        src = bucket.blob("objects/source.txt")
        src.upload_from_string(b"copy me")
        copied = bucket.copy_blob(src, bucket, "objects/copied.txt")
        data = copied.download_as_bytes()
        assert data == b"copy me", f"copied content {data!r}"
        return copied.name

    check(SERVICE, "CopyObject", op)


def test_delete_object(client, bucket):
    blob = bucket.blob("objects/gone.txt")
    blob.upload_from_string(b"bye")

    def op():
        blob.delete()
        assert not blob.exists(), "object still exists after DeleteObject"
        return "deleted"

    check(SERVICE, "DeleteObject", op)


def test_get_missing_object_not_found(client, bucket):
    def op():
        with pytest.raises(gexc.NotFound):
            bucket.blob("objects/does-not-exist.txt").download_as_bytes()
        return "raised NotFound"

    check(SERVICE, "GetObject (missing -> NotFound)", op)


def test_bucket_iam_policy(client, bucket):
    check(
        SERVICE,
        "GetBucketIamPolicy",
        lambda: f"{len(bucket.get_iam_policy().bindings)} bindings",
    )


def test_delete_bucket(client):
    name = unique("pyc-delete")
    client.create_bucket(name)

    def op():
        client.get_bucket(name).delete()
        with pytest.raises(gexc.NotFound):
            client.get_bucket(name).reload()
        return "deleted"

    check(SERVICE, "DeleteBucket (then Gone)", op)
