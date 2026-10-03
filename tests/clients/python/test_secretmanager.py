"""Secret Manager gRPC conformance via ``google-cloud-secret-manager``.

Wiring detail (the crux): Secret Manager has no ``*_EMULATOR_HOST`` hook, and
the generated client's ``client_options={'api_endpoint': ...}`` path always
builds a TLS channel.  Pointing that at the emulator's plaintext gRPC listener
fails with an SSL handshake.  The working recipe injects the generated gRPC
transport over ``grpc.insecure_channel`` (see ``harness.secretmanager_client``).
"""

from __future__ import annotations

import pytest
from google.api_core import exceptions as gexc

from harness import CONFIG, check, secretmanager_client, unique

SERVICE = "secretmanager"

pytestmark = pytest.mark.grpc


@pytest.fixture
def client():
    c = secretmanager_client()
    yield c
    c.transport.close()


@pytest.fixture
def secret(client):
    secret_id = unique("pyc-secret")
    client.create_secret(
        request={
            "parent": f"projects/{CONFIG.project}",
            "secret_id": secret_id,
            "secret": {"replication": {"automatic": {}}},
        }
    )
    name = CONFIG.secret_path(secret_id)
    yield name
    try:
        client.delete_secret(request={"name": name})
    except Exception:  # noqa: BLE001 - best-effort teardown
        pass


def test_create_secret(client):
    secret_id = unique("pyc-create-secret")

    def op():
        name = client.create_secret(
            request={
                "parent": f"projects/{CONFIG.project}",
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}},
            }
        ).name
        want = CONFIG.secret_path(secret_id)
        assert name == want, f"CreateSecret returned {name!r}, want {want!r}"
        return name

    check(SERVICE, "CreateSecret", op)


def test_get_secret(client, secret):
    def op():
        got = client.get_secret(request={"name": secret}).name
        assert got == secret, f"GetSecret returned {got!r}, want {secret!r}"
        return got

    check(SERVICE, "GetSecret", op)


def test_list_secrets(client, secret):
    def op():
        names = [
            s.name
            for s in client.list_secrets(request={"parent": f"projects/{CONFIG.project}"})
        ]
        assert secret in names, f"{secret} not listed"
        return f"{len(names)} secrets"

    check(SERVICE, "ListSecrets", op)


def test_add_secret_version(client, secret):
    def op():
        got = client.add_secret_version(
            request={"parent": secret, "payload": {"data": b"payload-1"}}
        ).name
        want = secret + "/versions/1"
        assert got == want, f"AddSecretVersion returned {got!r}, want {want!r}"
        return got

    check(SERVICE, "AddSecretVersion", op)


def test_access_secret_version(client, secret):
    payload = b"access-payload"

    def op():
        client.add_secret_version(
            request={"parent": secret, "payload": {"data": payload}}
        )
        got = client.access_secret_version(
            request={"name": secret + "/versions/1"}
        ).payload.data
        assert got == payload, f"payload {got!r}, want {payload!r}"
        return f"{len(got)} bytes"

    check(SERVICE, "AccessSecretVersion", op)


def test_get_secret_version(client, secret):
    client.add_secret_version(request={"parent": secret, "payload": {"data": b"v1"}})

    def op():
        state = client.get_secret_version(
            request={"name": secret + "/versions/1"}
        ).state.name
        assert state == "ENABLED", f"GetSecretVersion state={state}, want ENABLED"
        return state

    check(SERVICE, "GetSecretVersion", op)


def test_list_secret_versions(client, secret):
    def op():
        for i in range(3):
            client.add_secret_version(
                request={"parent": secret, "payload": {"data": f"v{i}".encode()}}
            )
        versions = [
            v.name for v in client.list_secret_versions(request={"parent": secret})
        ]
        assert len(versions) == 3, f"expected 3 versions, got {len(versions)}"
        return f"{len(versions)} versions"

    check(SERVICE, "ListSecretVersions", op)


def test_disable_and_enable_secret_version(client, secret):
    client.add_secret_version(request={"parent": secret, "payload": {"data": b"toggle"}})
    version = secret + "/versions/1"

    def op():
        disabled = client.disable_secret_version(request={"name": version}).state.name
        assert disabled == "DISABLED", disabled
        enabled = client.enable_secret_version(request={"name": version}).state.name
        assert enabled == "ENABLED", enabled
        return "DISABLED -> ENABLED"

    check(SERVICE, "DisableSecretVersion+EnableSecretVersion", op)


def test_destroy_secret_version(client, secret):
    client.add_secret_version(
        request={"parent": secret, "payload": {"data": b"destroy"}}
    )

    def op():
        state = client.destroy_secret_version(
            request={"name": secret + "/versions/1"}
        ).state.name
        assert state == "DESTROYED", f"DestroySecretVersion state={state}"
        return state

    check(SERVICE, "DestroySecretVersion", op)


def test_get_missing_secret_not_found(client):
    def op():
        with pytest.raises(gexc.NotFound):
            client.get_secret(request={"name": CONFIG.secret_path(unique("pyc-missing"))})
        return "raised NotFound"

    check(SERVICE, "GetSecret (missing -> NotFound)", op)


def test_delete_secret(client):
    secret_id = unique("pyc-del-secret")
    parent = f"projects/{CONFIG.project}"
    client.create_secret(
        request={
            "parent": parent,
            "secret_id": secret_id,
            "secret": {"replication": {"automatic": {}}},
        }
    )
    name = CONFIG.secret_path(secret_id)

    def op():
        client.delete_secret(request={"name": name})
        with pytest.raises(gexc.NotFound):
            client.get_secret(request={"name": name})
        return "deleted"

    check(SERVICE, "DeleteSecret", op)
