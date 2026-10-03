"""Cloud Firestore gRPC conformance via ``google-cloud-firestore``.

Wiring: ``FIRESTORE_EMULATOR_HOST`` makes ``firestore.Client`` connect over an
insecure channel with anonymous credentials.  ``DocumentRef.set``/``delete``
exercise the Commit RPC; ``get`` exercises BatchGetDocuments.
"""

from __future__ import annotations

import pytest
from google.cloud.firestore_v1.base_query import FieldFilter

from harness import check, firestore_client, unique

SERVICE = "firestore"

pytestmark = pytest.mark.grpc


@pytest.fixture
def client():
    c = firestore_client()
    yield c
    c.close()


def _doc(client, collection, doc_id="doc1"):
    return client.collection(collection).document(doc_id)


def test_set_document(client):
    coll = unique("pyc_set")
    ref = _doc(client, coll)

    def op():
        ref.set({"name": "doc-value", "n": 1, "ok": True})
        return f"{coll}/doc1"

    check(SERVICE, "Doc.Set (Commit)", op)
    ref.delete()


def test_get_document(client):
    coll = unique("pyc_get")
    ref = _doc(client, coll)
    ref.set({"name": "doc-value", "n": 1})

    def op():
        snap = ref.get()
        assert snap.exists, "Doc.Get reported the document does not exist"
        data = snap.to_dict()
        assert data.get("name") == "doc-value", f"name={data.get('name')!r}"
        assert data.get("n") == 1, f"n={data.get('n')!r}"
        return f"name={data['name']} n={data['n']}"

    check(SERVICE, "Doc.Get (BatchGetDocuments)", op)
    ref.delete()


def test_query_collection(client):
    coll = unique("pyc_query")
    ref = _doc(client, coll)
    ref.set({"name": "doc-value"})

    def op():
        found = list(
            client.collection(coll)
            .where(filter=FieldFilter("name", "==", "doc-value"))
            .stream()
        )
        assert len(found) == 1, f"query matched {len(found)} documents"
        return f"{len(found)} match"

    check(SERVICE, "Collection.Query", op)
    ref.delete()


def test_delete_document(client):
    coll = unique("pyc_delete")
    ref = _doc(client, coll)
    ref.set({"name": "doc-value"})

    def op():
        ref.delete()
        assert not ref.get().exists, "document still exists after Doc.Delete"
        return "deleted"

    check(SERVICE, "Doc.Delete (Commit)", op)
