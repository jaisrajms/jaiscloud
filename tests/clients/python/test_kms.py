"""Cloud KMS gRPC conformance via ``google-cloud-kms``.

Wiring detail: like Secret Manager, KMS has no emulator env hook and the
generated client builds a TLS channel from ``client_options``.  The suite
injects ``KeyManagementServiceGrpcTransport`` over an insecure channel
(see ``harness.kms_client``).
"""

from __future__ import annotations

import pytest
from google.api_core import exceptions as gexc

from harness import CONFIG, check, kms_client, unique

SERVICE = "kms"

pytestmark = pytest.mark.grpc


@pytest.fixture
def client():
    c = kms_client()
    yield c
    c.transport.close()


@pytest.fixture
def key_ring(client):
    ring_id = unique("pyc-ring")
    client.create_key_ring(
        request={
            "parent": f"projects/{CONFIG.project}/locations/global",
            "key_ring_id": ring_id,
            "key_ring": {},
        }
    )
    return CONFIG.key_ring_path(ring_id)


@pytest.fixture
def crypto_key(client, key_ring):
    key_id = unique("pyc-key")
    client.create_crypto_key(
        request={
            "parent": key_ring,
            "crypto_key_id": key_id,
            "crypto_key": {"purpose": "ENCRYPT_DECRYPT"},
        }
    )
    return f"{key_ring}/cryptoKeys/{key_id}"


def test_create_key_ring(client):
    ring_id = unique("pyc-create-ring")
    parent = f"projects/{CONFIG.project}/locations/global"

    def op():
        name = client.create_key_ring(
            request={"parent": parent, "key_ring_id": ring_id, "key_ring": {}}
        ).name
        want = CONFIG.key_ring_path(ring_id)
        assert name == want, f"CreateKeyRing returned {name!r}, want {want!r}"
        return name

    check(SERVICE, "CreateKeyRing", op)


def test_get_key_ring(client, key_ring):
    def op():
        got = client.get_key_ring(request={"name": key_ring}).name
        assert got == key_ring, f"GetKeyRing returned {got!r}, want {key_ring!r}"
        return got

    check(SERVICE, "GetKeyRing", op)


def test_list_key_rings(client, key_ring):
    def op():
        names = [
            r.name
            for r in client.list_key_rings(
                request={"parent": f"projects/{CONFIG.project}/locations/global"}
            )
        ]
        assert key_ring in names, f"{key_ring} not listed"
        return f"{len(names)} key rings"

    check(SERVICE, "ListKeyRings", op)


def test_create_crypto_key(client, key_ring):
    key_id = unique("pyc-create-key")

    def op():
        name = client.create_crypto_key(
            request={
                "parent": key_ring,
                "crypto_key_id": key_id,
                "crypto_key": {"purpose": "ENCRYPT_DECRYPT"},
            }
        ).name
        want = f"{key_ring}/cryptoKeys/{key_id}"
        assert name == want, f"CreateCryptoKey returned {name!r}, want {want!r}"
        return name

    check(SERVICE, "CreateCryptoKey", op)


def test_get_crypto_key(client, crypto_key):
    def op():
        key = client.get_crypto_key(request={"name": crypto_key})
        assert key.name == crypto_key, f"GetCryptoKey returned {key.name!r}"
        assert key.primary.name, "GetCryptoKey returned no primary version"
        return key.primary.name

    check(SERVICE, "GetCryptoKey", op)


def test_list_crypto_keys(client, key_ring, crypto_key):
    def op():
        names = [k.name for k in client.list_crypto_keys(request={"parent": key_ring})]
        assert crypto_key in names, f"{crypto_key} not listed"
        return f"{len(names)} crypto keys"

    check(SERVICE, "ListCryptoKeys", op)


def test_encrypt_and_decrypt(client, crypto_key):
    plaintext = b"kms-conformance-plaintext"

    def op():
        encrypted = client.encrypt(
            request={"name": crypto_key, "plaintext": plaintext}
        )
        assert encrypted.ciphertext, "encrypt returned no ciphertext"
        decrypted = client.decrypt(
            request={"name": crypto_key, "ciphertext": encrypted.ciphertext}
        )
        assert decrypted.plaintext == plaintext, f"got {decrypted.plaintext!r}"
        return f"{len(encrypted.ciphertext)} ciphertext bytes"

    check(SERVICE, "Encrypt+Decrypt", op)


def test_destroy_crypto_key_version(client, crypto_key):
    def op():
        state = client.destroy_crypto_key_version(
            request={"name": crypto_key + "/cryptoKeyVersions/1"}
        ).state.name
        assert state in ("DESTROY_SCHEDULED", "DESTROYED"), f"state={state}"
        return state

    check(SERVICE, "DestroyCryptoKeyVersion", op)


def test_get_missing_key_ring_not_found(client):
    def op():
        with pytest.raises(gexc.NotFound):
            client.get_key_ring(
                request={"name": CONFIG.key_ring_path(unique("pyc-missing"))}
            )
        return "raised NotFound"

    check(SERVICE, "GetKeyRing (missing -> NotFound)", op)
