"""Pub/Sub gRPC conformance via the official ``google-cloud-pubsub`` v1 clients.

Wiring: ``PUBSUB_EMULATOR_HOST`` makes ``pubsub_v1.PublisherClient`` and
``pubsub_v1.SubscriberClient`` connect over an insecure channel with emulator
credentials; no API endpoint or auth options are needed.
"""

from __future__ import annotations

import pytest
from google.api_core import exceptions as gexc

from harness import (
    CONFIG,
    check,
    pubsub_publisher,
    pubsub_subscriber,
    unique,
)

SERVICE = "pubsub"

pytestmark = pytest.mark.grpc


@pytest.fixture
def publisher():
    return pubsub_publisher()


@pytest.fixture
def subscriber():
    return pubsub_subscriber()


@pytest.fixture
def topic(publisher):
    path = publisher.topic_path(CONFIG.project, unique("pyc-topic"))
    publisher.create_topic(request={"name": path})
    yield path
    try:
        publisher.delete_topic(request={"topic": path})
    except Exception:  # noqa: BLE001 - best-effort teardown
        pass


@pytest.fixture
def subscription(subscriber, topic):
    path = subscriber.subscription_path(CONFIG.project, unique("pyc-sub"))
    subscriber.create_subscription(request={"name": path, "topic": topic})
    yield path
    try:
        subscriber.delete_subscription(request={"subscription": path})
    except Exception:  # noqa: BLE001 - best-effort teardown
        pass


def test_create_topic(publisher):
    path = publisher.topic_path(CONFIG.project, unique("pyc-create-topic"))

    def op():
        got = publisher.create_topic(request={"name": path}).name
        assert got == path, f"CreateTopic returned {got!r}, want {path!r}"
        return got

    check(SERVICE, "CreateTopic", op)


def test_get_topic(publisher, topic):
    def op():
        got = publisher.get_topic(request={"topic": topic}).name
        assert got == topic, f"GetTopic returned {got!r}, want {topic!r}"
        return got

    check(SERVICE, "GetTopic", op)


def test_list_topics(publisher, topic):
    def op():
        names = [
            t.name
            for t in publisher.list_topics(
                request={"project": f"projects/{CONFIG.project}"}
            )
        ]
        assert topic in names, f"{topic} not listed"
        return f"{len(names)} topics"

    check(SERVICE, "ListTopics", op)


def test_create_subscription(subscriber, topic):
    path = subscriber.subscription_path(CONFIG.project, unique("pyc-create-sub"))

    def op():
        got = subscriber.create_subscription(
            request={"name": path, "topic": topic}
        ).name
        assert got == path, f"CreateSubscription returned {got!r}, want {path!r}"
        return got

    check(SERVICE, "CreateSubscription", op)


def test_get_subscription(subscriber, subscription):
    def op():
        got = subscriber.get_subscription(request={"subscription": subscription}).name
        assert got == subscription, f"GetSubscription returned {got!r}"
        return got

    check(SERVICE, "GetSubscription", op)


def test_list_subscriptions(subscriber, subscription):
    def op():
        names = [
            s.name
            for s in subscriber.list_subscriptions(
                request={"project": f"projects/{CONFIG.project}"}
            )
        ]
        assert subscription in names, f"{subscription} not listed"
        return f"{len(names)} subscriptions"

    check(SERVICE, "ListSubscriptions", op)


def test_publish_and_pull(publisher, subscriber, subscription):
    payload = b"python-conformance-payload"

    def op():
        sub = subscriber.get_subscription(request={"subscription": subscription})
        msg_id = publisher.publish(sub.topic, payload, source="python").result(
            timeout=10
        )
        assert msg_id, "publish returned an empty message id"
        resp = subscriber.pull(
            request={"subscription": subscription, "max_messages": 10}
        )
        msgs = [m.message for m in resp.received_messages]
        assert len(msgs) == 1, f"expected 1 message, got {len(msgs)}"
        assert msgs[0].data == payload, f"payload {msgs[0].data!r}"
        assert msgs[0].attributes.get("source") == "python", "attributes lost"
        return f"message_id={msg_id} payload={msgs[0].data!r}"

    check(SERVICE, "Publish+Pull", op)


def test_acknowledge(publisher, subscriber, subscription):
    def op():
        sub = subscriber.get_subscription(request={"subscription": subscription})
        publisher.publish(sub.topic, b"to-ack").result(timeout=10)
        resp = subscriber.pull(
            request={"subscription": subscription, "max_messages": 10}
        )
        assert len(resp.received_messages) == 1, "expected one deliverable message"
        subscriber.acknowledge(
            request={
                "subscription": subscription,
                "ack_ids": [m.ack_id for m in resp.received_messages],
            }
        )
        again = subscriber.pull(
            request={"subscription": subscription, "max_messages": 10}
        )
        assert len(again.received_messages) == 0, "acked message was redelivered"
        return "acked"

    check(SERVICE, "Acknowledge", op)


def test_modify_ack_deadline(publisher, subscriber, subscription):
    def op():
        sub = subscriber.get_subscription(request={"subscription": subscription})
        publisher.publish(sub.topic, b"nack-me").result(timeout=10)
        resp = subscriber.pull(
            request={"subscription": subscription, "max_messages": 10}
        )
        assert len(resp.received_messages) == 1, "expected one message"
        subscriber.modify_ack_deadline(
            request={
                "subscription": subscription,
                "ack_ids": [m.ack_id for m in resp.received_messages],
                "ack_deadline_seconds": 0,
            }
        )
        again = subscriber.pull(
            request={"subscription": subscription, "max_messages": 10}
        )
        assert len(again.received_messages) == 1, "message not redelivered after nack"
        return "redelivered"

    check(SERVICE, "ModifyAckDeadline", op)


def test_get_missing_topic_not_found(publisher):
    def op():
        missing = publisher.topic_path(CONFIG.project, unique("pyc-missing"))
        with pytest.raises(gexc.NotFound):
            publisher.get_topic(request={"topic": missing})
        return "raised NotFound"

    check(SERVICE, "GetTopic (missing -> NotFound)", op)


def test_delete_topic(publisher):
    path = publisher.topic_path(CONFIG.project, unique("pyc-del-topic"))
    publisher.create_topic(request={"name": path})

    def op():
        publisher.delete_topic(request={"topic": path})
        with pytest.raises(gexc.NotFound):
            publisher.get_topic(request={"topic": path})
        return "deleted"

    check(SERVICE, "DeleteTopic", op)


def test_delete_subscription(subscriber, topic):
    path = subscriber.subscription_path(CONFIG.project, unique("pyc-del-sub"))
    subscriber.create_subscription(request={"name": path, "topic": topic})

    def op():
        subscriber.delete_subscription(request={"subscription": path})
        with pytest.raises(gexc.NotFound):
            subscriber.get_subscription(request={"subscription": path})
        return "deleted"

    check(SERVICE, "DeleteSubscription", op)
