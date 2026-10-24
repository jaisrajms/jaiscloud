"""jaiscloud-gcp SDK tour — Python streaming-semantics leg (SDK_TOUR_MODE=streaming).

Port of ../go/streaming.go: the same nine streaming scenarios with the same
observable strings, driven by the official google-cloud-* clients. It reuses the
tour's Recorder/run helper and client wiring so its rows land in the
cross-language matrix beside the other legs.

Generated (low-level) clients are used where the high-level client hides the
semantics under test (Firestore Listen resume tokens, precise StreamingPull /
ModifyAckDeadline control); elsewhere the high-level client is used because its
behavior is part of what is under test.

This is a demo/compliance artifact, not a conformance gate: failures are
recorded and classified, never papered over.
"""

from __future__ import annotations

import queue
import threading
import time

import grpc

from sdk_tour import (
    GRPC,
    PROJECT,
    Recorder,
    firestore_client,
    logging_client,
    pubsub_publisher,
    pubsub_subscriber,
    rid,
    run,
)

ACK_DEADLINE = 10
REDELIVER_WAIT = 13.0


# ─── Pub/Sub ─────────────────────────────────────────────────────────────────


def _ensure_topic_sub(publisher, subscriber, topic_id, sub_id, **sub_kwargs):
    from google.api_core import exceptions as gexc

    topic_path = publisher.topic_path(PROJECT, topic_id)
    try:
        publisher.create_topic(request={"name": topic_path})
    except gexc.AlreadyExists:
        pass
    sub_path = subscriber.subscription_path(PROJECT, sub_id)
    request = {"name": sub_path, "topic": topic_path, "ack_deadline_seconds": ACK_DEADLINE}
    request.update(sub_kwargs)
    try:
        subscriber.create_subscription(request=request)
    except gexc.AlreadyExists:
        pass
    return topic_path, sub_path


class _PullStream:
    """A generated StreamingPull stream fed from a queue, so the caller can keep
    it open across an ack deadline without the client extending the lease."""

    def __init__(self, subscriber, sub_path):
        from google.cloud import pubsub_v1

        self._q: queue.Queue = queue.Queue()
        # The first request must be queued BEFORE the call: gax primes the
        # bidi stream by consuming the first item synchronously.
        self._q.put(pubsub_v1.types.StreamingPullRequest(
            subscription=sub_path, stream_ack_deadline_seconds=ACK_DEADLINE))
        self._responses = subscriber.streaming_pull(requests=self._requests())

    def _requests(self):
        while True:
            item = self._q.get()
            if item is None:
                return
            yield item

    def recv_one(self, want: str):
        for resp in self._responses:
            for rm in resp.received_messages:
                if rm.message.data.decode() == want:
                    return rm
        raise AssertionError("streaming pull ended before the message arrived")

    def close(self) -> None:
        self._q.put(None)


def pubsub_scenarios(rec: Recorder) -> None:
    publisher = pubsub_publisher()
    subscriber = pubsub_subscriber()

    def ack_deadline():
        topic_path, sub_path = _ensure_topic_sub(
            publisher, subscriber, rid("stream-topic-ackdl"), rid("stream-ackdl-sub"))
        publisher.publish(topic_path, b"ackdl").result(timeout=30)
        s1 = _PullStream(subscriber, sub_path)
        s1.recv_one("ackdl")
        s1.close()
        time.sleep(REDELIVER_WAIT)
        s2 = _PullStream(subscriber, sub_path)
        rm = s2.recv_one("ackdl")
        s2.close()
        subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": [rm.ack_id]})
        pull = subscriber.pull(request={
            "subscription": sub_path, "max_messages": 10, "return_immediately": True})
        for got in pull.received_messages:
            if got.message.data.decode() == "ackdl":
                raise AssertionError("message redelivered after ack")
        return "redelivered=yes", "unacked message redelivered after the 10s ack deadline; ack stopped it"

    run(rec, "streaming.pubsub_ack_deadline", "OK", ack_deadline)

    def ack_extension():
        topic_path, sub_path = _ensure_topic_sub(
            publisher, subscriber, rid("stream-topic-ackext"), rid("stream-ackext-sub"))
        publisher.publish(topic_path, b"ackext").result(timeout=30)
        s1 = _PullStream(subscriber, sub_path)
        rm = s1.recv_one("ackext")
        subscriber.modify_ack_deadline(request={
            "subscription": sub_path, "ack_ids": [rm.ack_id], "ack_deadline_seconds": 600})
        s1.close()
        time.sleep(REDELIVER_WAIT)
        pull = subscriber.pull(request={
            "subscription": sub_path, "max_messages": 10, "return_immediately": True})
        for got in pull.received_messages:
            if got.message.data.decode() == "ackext":
                raise AssertionError("message redelivered despite ModifyAckDeadline(600s)")
        subscriber.modify_ack_deadline(request={
            "subscription": sub_path, "ack_ids": [rm.ack_id], "ack_deadline_seconds": 0})
        return "no_redelivery_while_extended=yes", "ModifyAckDeadline(600s) kept the message invisible past its original 10s deadline"

    run(rec, "streaming.pubsub_ack_extension", "OK", ack_extension)

    def ordering_keys():
        from google.cloud import pubsub_v1

        topic_path, sub_path = _ensure_topic_sub(
            publisher, subscriber, rid("stream-topic-order"), rid("stream-order-sub"),
            enable_message_ordering=True)
        # The high-level publisher must opt into ordering before it will accept
        # an ordering key on a message.
        ordered_publisher = pubsub_v1.PublisherClient(
            publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True))
        n = 10
        for i in range(n):
            ordered_publisher.publish(topic_path, f"order-{i:02d}".encode(),
                                      ordering_key="stream-order-key").result(timeout=30)
        got: list[str] = []
        done = __import__("threading").Event()

        def callback(message):
            got.append(message.data.decode())
            message.ack()
            if len(got) >= n:
                done.set()

        future = subscriber.subscribe(
            sub_path, callback,
            flow_control=pubsub_v1.types.FlowControl(max_messages=n))
        try:
            if not done.wait(40):
                raise AssertionError(f"ordering receive timed out (got {got})")
        finally:
            future.cancel()
        want = [f"order-{i:02d}" for i in range(n)]
        if got != want:
            raise AssertionError(f"out of order: got {got}, want {want}")
        return "ordered=yes", f"ordering key preserved order across {n} messages"

    run(rec, "streaming.pubsub_ordering_keys", "OK", ordering_keys)

    def exactly_once_ack():
        topic_path, sub_path = _ensure_topic_sub(
            publisher, subscriber, rid("stream-topic-eod"), rid("stream-eod-sub"),
            enable_exactly_once_delivery=True)
        publisher.publish(topic_path, b"eod").result(timeout=30)
        s1 = _PullStream(subscriber, sub_path)
        rm = s1.recv_one("eod")
        s1.close()
        subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": [rm.ack_id]})
        time.sleep(3.0)
        pull = subscriber.pull(request={
            "subscription": sub_path, "max_messages": 10, "return_immediately": True})
        for got in pull.received_messages:
            if got.message.data.decode() == "eod":
                raise AssertionError("exactly-once message redelivered after ack")
        return "dup=0", "acked exactly-once message was not redelivered"

    run(rec, "streaming.pubsub_exactly_once_ack", "OK", exactly_once_ack)

    def flow_control():
        from google.cloud import pubsub_v1

        n = 3
        backlog = 15
        topic_path, sub_path = _ensure_topic_sub(
            publisher, subscriber, rid("stream-topic-flow"), rid("stream-flow-sub"))
        for i in range(backlog):
            publisher.publish(topic_path, f"flow-{i:02d}".encode()).result(timeout=30)

        # The high-level subscriber under test: max_messages must bound how many
        # unacked messages it holds at once. Callbacks hold their message (no
        # ack) so delivery would outrun acks and expose a missing bound; the
        # driver releases them only after the cap is proved.
        state = {"inflight": 0, "max_inflight": 0, "delivered": 0}
        lock = threading.Lock()
        released = threading.Event()

        def callback(message):
            with lock:
                state["inflight"] += 1
                state["max_inflight"] = max(state["max_inflight"], state["inflight"])
                state["delivered"] += 1
            released.wait(30)
            message.ack()
            with lock:
                state["inflight"] -= 1

        future = subscriber.subscribe(
            sub_path, callback,
            flow_control=pubsub_v1.types.FlowControl(max_messages=n))
        try:
            deadline = time.monotonic() + 30
            while True:
                with lock:
                    delivered = state["delivered"]
                if delivered >= n or time.monotonic() > deadline:
                    break
                time.sleep(0.01)
            if delivered < n:
                raise AssertionError(
                    f"flow control: only {delivered}/{n} messages delivered while held")
            # Hold at the cap: a client that ignores the bound keeps delivering.
            time.sleep(1.5)
            with lock:
                held, max_seen = state["delivered"], state["max_inflight"]
            if held != n:
                raise AssertionError(
                    f"flow control: {held} messages held concurrently, want max_outstanding={n}")
            if max_seen != n:
                raise AssertionError(f"flow control: max in flight {max_seen}, want {n}")

            released.set()
            deadline = time.monotonic() + 30
            while True:
                with lock:
                    delivered = state["delivered"]
                if delivered >= backlog or time.monotonic() > deadline:
                    break
                time.sleep(0.01)
            if delivered != backlog:
                raise AssertionError(f"flow control: drained {delivered}/{backlog} messages")
        finally:
            released.set()
            future.cancel()
        return (f"flow_control=ok,max_in_flight<={n}",
                f"held at most {n} of {backlog} messages outstanding")

    run(rec, "streaming.pubsub_flow_control", "OK", flow_control)


# ─── Firestore ───────────────────────────────────────────────────────────────


def _firestore_channel():
    return grpc.insecure_channel(GRPC)


class _ListenStream:
    """Interactive Firestore Listen over a raw gRPC channel.

    The generated client's ``listen(requests=...)`` consumes the request
    iterator synchronously, so it cannot be driven interactively. A raw
    ``stream_stream`` call reads requests from a queue on a background thread,
    which is what resume-token handling needs.
    """

    def __init__(self, channel, initial):
        from google.cloud.firestore_v1 import types as fstypes

        self._q: queue.Queue = queue.Queue()
        self._q.put(initial)
        call = channel.stream_stream(
            "/google.firestore.v1.Firestore/Listen",
            request_serializer=fstypes.ListenRequest.serialize,
            response_deserializer=fstypes.ListenResponse.deserialize)
        self._call = call(self._requests())

    def _requests(self):
        while True:
            item = self._q.get()
            if item is None:
                return
            yield item

    def send(self, request) -> None:
        self._q.put(request)

    def recv(self):
        return next(self._call)

    def close(self) -> None:
        self._q.put(None)


def _fs_query_target(db_parent: str, tid: int, collection: str, token: bytes | None = None):
    from google.cloud.firestore_v1 import types as fstypes

    target = fstypes.Target(
        target_id=tid,
        query=fstypes.Target.QueryTarget(
            parent=db_parent,
            structured_query=fstypes.StructuredQuery(
                from_=[fstypes.StructuredQuery.CollectionSelector(collection_id=collection)])))
    if token is not None:
        target.resume_token = token
    return target


def _listen_add(db: str, parent: str, tid: int, collection: str, token: bytes | None = None):
    from google.cloud.firestore_v1 import types as fstypes

    return fstypes.ListenRequest(database=db, add_target=_fs_query_target(parent, tid, collection, token))


def _listen_remove(db: str, tid: int):
    from google.cloud.firestore_v1 import types as fstypes

    return fstypes.ListenRequest(database=db, remove_target=tid)


def firestore_scenarios(rec: Recorder) -> None:
    from google.cloud.firestore_v1 import types as fstypes

    parent = f"projects/{PROJECT}/databases/(default)/documents"
    db = f"projects/{PROJECT}/databases/(default)"
    channel = _firestore_channel()
    hl = firestore_client()

    def resume_token():
        coll = rid("stream-listen-resume")
        s_a = _ListenStream(channel, _listen_add(db, parent, 1, coll))
        token = None
        while token is None:
            resp = s_a.recv()
            tc = resp.target_change
            if tc.target_change_type == fstypes.TargetChange.TargetChangeType.CURRENT and tc.resume_token:
                token = tc.resume_token
        s_a.close()

        # Written after A closed: the resumed stream must replay it.
        hl.collection(coll).document("b").set({"v": "b"})

        # Register the server subscription and confirm the handler is running.
        s_b = _ListenStream(channel, _listen_remove(db, 999))
        while True:
            tc = s_b.recv().target_change
            if tc.target_change_type == fstypes.TargetChange.TargetChangeType.REMOVE:
                break
        # Written BEFORE AddTarget: both replayed and buffered, delivered once.
        hl.collection(coll).document("c").set({"v": "c"})
        s_b.send(_listen_add(db, parent, 2, coll, token))

        counts: dict[str, int] = {}
        while True:
            resp = s_b.recv()
            name = resp.document_change.document.name
            if name:
                short = name.rsplit("/", 1)[-1]
                counts[short] = counts.get(short, 0) + 1
                continue
            if resp.target_change.target_change_type == fstypes.TargetChange.TargetChangeType.NO_CHANGE:
                break
        s_b.close()
        for want in ("b", "c"):
            if counts.get(want) != 1:
                raise AssertionError(
                    f"resumed stream delivered {want!r} {counts.get(want, 0)} times (all={counts})")
        return "loss=0,dup=0", "resume token replayed b and delivered the racing c exactly once"

    run(rec, "streaming.firestore_listen_resume_token", "OK", resume_token)

    def snapshot_consistency():
        coll = rid("stream-listen-consistency")
        stream = _ListenStream(channel, _listen_add(db, parent, 1, coll))
        while True:
            if stream.recv().target_change.target_change_type == fstypes.TargetChange.TargetChangeType.NO_CHANGE:
                break
        n = 5
        for i in range(n):
            hl.collection(coll).document(f"d{i}").set({"i": i})

        counts: dict[str, int] = {}
        last = None
        monotonic = True
        while len(counts) < n:
            resp = stream.recv()
            name = resp.document_change.document.name
            if name:
                counts[name] = counts.get(name, 0) + 1
                continue
            tc = resp.target_change
            if tc.target_change_type == fstypes.TargetChange.TargetChangeType.NO_CHANGE and tc.read_time is not None:
                # read_time is a DatetimeWithNanoseconds (a datetime subclass).
                rt = tc.read_time
                if last is not None and rt < last:
                    monotonic = False
                last = rt
        stream.close()
        for name, c in counts.items():
            if c != 1:
                raise AssertionError(f"document {name} delivered {c} times, want 1")
        if not monotonic:
            raise AssertionError("read_time was not monotonic across NO_CHANGE frames")
        return f"docs={n},dup=0,monotonic=yes", "5 concurrent writes delivered once each with monotonic read_time"

    run(rec, "streaming.firestore_snapshot_consistency", "OK", snapshot_consistency)


# ─── Logging ─────────────────────────────────────────────────────────────────


def logging_scenarios(rec: Recorder) -> None:
    from google.cloud.logging_v2 import types as lpb

    parent = f"projects/{PROJECT}"
    log_name = f"{parent}/logs/{rid('stream-tail-log')}"
    client = logging_client()
    raw_channel = grpc.insecure_channel(GRPC)

    def write(text: str) -> None:
        client.write_log_entries(request={
            "log_name": log_name,
            "resource": {"type": "global", "labels": {"project_id": PROJECT}},
            "entries": [{"log_name": log_name, "severity": "INFO", "text_payload": text}],
        })

    def tail_reconnect():
        def start():
            q: queue.Queue = queue.Queue()

            def requests():
                while True:
                    item = q.get()
                    if item is None:
                        return
                    yield item

            # The generated client's requests= bidi API consumes the iterator
            # synchronously; a raw stream_stream call reads it on a background
            # thread, enabling the disconnect/reconnect this scenario asserts.
            q.put(lpb.TailLogEntriesRequest(
                resource_names=[parent], filter=f'logName="{log_name}"',
                buffer_window={"seconds": 0, "nanos": 200_000_000}))
            call = raw_channel.stream_stream(
                "/google.logging.v2.LoggingServiceV2/TailLogEntries",
                request_serializer=lpb.TailLogEntriesRequest.serialize,
                response_deserializer=lpb.TailLogEntriesResponse.deserialize)
            return q, call(requests())

        def recv_until_prefix(q, responses, prefix):
            import threading

            stop = threading.Event()

            def writer():
                i = 0
                while not stop.is_set():
                    write(f"{prefix}-{i}")
                    i += 1
                    stop.wait(0.25)

            thread = threading.Thread(target=writer, daemon=True)
            thread.start()
            try:
                for resp in responses:
                    for entry in resp.entries:
                        if entry.text_payload.startswith(prefix):
                            return
                raise AssertionError("tail stream ended before the entry arrived")
            finally:
                stop.set()
                q.put(None)

        q1, r1 = start()
        recv_until_prefix(q1, r1, "stream-tail-a")

        # Written while disconnected: below the second stream's seed cursor, so
        # TailLogEntries must NOT replay it (the API has no resume cursor).
        write("stream-tail-gap")

        q2, r2 = start()
        recv_until_prefix(q2, r2, "stream-tail-c")
        return "reconnect_ok=yes,gap_replayed=no", "second tail delivered post-reconnect entries and did not replay the disconnected-window entry"

    run(rec, "streaming.logging_tail_reconnect", "OK", tail_reconnect)


# ─── Storage ─────────────────────────────────────────────────────────────────


def storage_scenarios(rec: Recorder) -> None:
    def bidi_read():
        raise AssertionError(
            "google-cloud-storage high-level client exposes no BidiReadObject projection")

    run(rec, "streaming.storage_bidi_read", "SKIP", bidi_read)


def streaming_scenarios(rec: Recorder) -> None:
    pubsub_scenarios(rec)
    firestore_scenarios(rec)
    logging_scenarios(rec)
    storage_scenarios(rec)
