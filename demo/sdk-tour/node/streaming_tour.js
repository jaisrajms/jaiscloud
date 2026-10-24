// jaiscloud-gcp SDK tour — Node streaming-semantics leg (SDK_TOUR_MODE=streaming).
//
// The happy-path tour proves the official clients can drive every surface; this
// leg goes deeper on the *streaming* surfaces and asserts their semantics, not
// just that a message arrived: ack-deadline redelivery and extension, ordering
// keys, exactly-once ack, subscriber flow control, Firestore Listen resume
// tokens and snapshot consistency, Logging tail reconnects, and Storage
// BidiReadObject.
//
// It ports the Go reference (demo/sdk-tour/go/streaming.go) scenario-for-scenario
// and reuses the parent tour's run()/record() helpers and official clients, so
// its rows land in the cross-language matrix beside the other legs.
//
// Where the high-level client hides the semantics under test (Firestore Listen
// resume tokens, precise StreamingPull / ModifyAckDeadline control) the generated
// low-level client is used; elsewhere the high-level client is used because its
// behavior is part of what is under test.
//
// This is a demo/compliance artifact, not a conformance gate: failures are
// recorded and classified, never papered over.
"use strict";

// The generated low-level clients live in the companion `@google-cloud/*-api`
// packages in this pinned dependency set; the high-level packages re-export them
// under `v1`/`v2`. Prefer the documented `build/src/v{1,2}` subpath and fall back
// to the package that actually ships it.
function loadGenerated(primary, fallback) {
  try {
    return require(primary);
  } catch (err) {
    return require(fallback);
  }
}

const { PubSub } = require("@google-cloud/pubsub");
const { Firestore } = require("@google-cloud/firestore");

const pubsubV1 = loadGenerated("@google-cloud/pubsub/build/src/v1", "@google-cloud/pubsub-api/build/src/v1");
const firestoreV1 = loadGenerated("@google-cloud/firestore/build/src/v1", "@google-cloud/firestore-api/build/src/v1");
const loggingV2 = loadGenerated("@google-cloud/logging/build/src/v2", "@google-cloud/logging-api/build/src/v2");

// The protocol minimum: it makes an ignored redelivery observable within a
// short, bounded wait.
const ACK_DEADLINE = 10;
// Ack deadline + margin. Redelivery is a property of the emulator's own clock,
// so a small margin is not a timing race.
const REDELIVER_WAIT_MS = 13000;
const READ_TIMEOUT_MS = 30000;

// ─── small helpers ───────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// toArray normalizes a repeated protobuf field, which protobufjs may decode as
// an array (the common case) or an object keyed by index.
function toArray(v) {
  if (v == null) return [];
  if (Array.isArray(v)) return v;
  if (typeof v !== "string" && typeof v[Symbol.iterator] === "function") return [...v];
  return Object.values(v);
}

// bytesToText decodes a protobuf `bytes` field (Buffer/Uint8Array/base64 string)
// to its UTF-8 text.
function bytesToText(v) {
  if (v == null) return "";
  if (typeof v === "string") return v;
  if (Buffer.isBuffer(v)) return v.toString("utf8");
  if (v instanceof Uint8Array) return Buffer.from(v).toString("utf8");
  if (Array.isArray(v)) return Buffer.from(v).toString("utf8");
  if (typeof v === "object" && v.type === "Buffer" && Array.isArray(v.data)) {
    return Buffer.from(v.data).toString("utf8");
  }
  return String(v);
}

// bytesValue round-trips a protobuf `bytes` field so a decoded resume token can
// be sent back verbatim.
function bytesValue(v) {
  if (v == null) return v;
  if (Buffer.isBuffer(v) || v instanceof Uint8Array) return Buffer.from(v);
  if (Array.isArray(v)) return Buffer.from(v);
  if (typeof v === "object" && v.type === "Buffer" && Array.isArray(v.data)) {
    return Buffer.from(v.data);
  }
  return v; // a base64 string is serialized as bytes directly
}

// tsMillis converts a google.protobuf.Timestamp (string, {seconds,nanos}, or
// protobufjs Long) to epoch milliseconds for ordering comparisons.
function tsMillis(ts) {
  if (ts == null) return null;
  if (typeof ts === "string") return Date.parse(ts);
  if (typeof ts === "number") return ts;
  let sec = ts.seconds;
  if (sec && typeof sec === "object") {
    sec = typeof sec.toNumber === "function"
      ? sec.toNumber()
      : Number(sec.low >>> 0) + Number(sec.high || 0) * 4294967296;
  }
  const nanos = Number(ts.nanos || 0);
  return Number(sec || 0) * 1000 + Math.floor(nanos / 1e6);
}

// makeReader turns a gax bidirectional stream into an awaitable queue, so the
// caller can interleave writes and reads without event-handler gymnastics.
function makeReader(stream) {
  const queue = [];
  const waiters = [];
  let ended = false;
  let failure = null;

  function pump() {
    while (waiters.length > 0) {
      const w = waiters[0];
      if (queue.length > 0) {
        waiters.shift();
        w.resolve({ value: queue.shift(), done: false });
      } else if (failure) {
        waiters.shift();
        w.reject(failure);
      } else if (ended) {
        waiters.shift();
        w.resolve({ value: undefined, done: true });
      } else {
        break;
      }
    }
  }

  stream.on("data", (d) => { queue.push(d); pump(); });
  stream.on("error", (e) => { failure = e; pump(); });
  stream.on("end", () => { ended = true; pump(); });
  stream.on("close", () => { ended = true; pump(); });

  return {
    next() {
      if (queue.length > 0) return Promise.resolve({ value: queue.shift(), done: false });
      if (failure) return Promise.reject(failure);
      if (ended) return Promise.resolve({ value: undefined, done: true });
      return new Promise((resolve, reject) => { waiters.push({ resolve, reject }); });
    },
  };
}

function nextWithTimeout(reader, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([reader.next(), timeout]).finally(() => clearTimeout(timer));
}

// endStream closes a gax bidi stream: half-close, then cancel the underlying
// RPC (equivalent to the Go reference's context cancel).
function endStream(stream) {
  try { stream.end(); } catch (_) { /* already ending */ }
  try { if (typeof stream.cancel === "function") stream.cancel(); } catch (_) { /* already gone */ }
}

// ─── Firestore Listen request builders ───────────────────────────────────────

function firestoreDb(project) {
  return `projects/${project}/databases/(default)`;
}

function listenAddRequest(db, parent, targetId, collection, token) {
  const target = {
    targetId,
    query: {
      parent,
      structuredQuery: { from: [{ collectionId: collection }] },
    },
  };
  if (token != null) target.resumeToken = token;
  return { database: db, addTarget: target };
}

function listenRemoveRequest(db, targetId) {
  return { database: db, removeTarget: targetId };
}

// targetChangeType is decoded as its enum name ("NO_CHANGE", "ADD", ...) by the
// generated client; normalize a numeric decode too.
function targetChangeType(tc) {
  const v = tc && tc.targetChangeType;
  if (typeof v === "string") return v;
  const names = ["NO_CHANGE", "ADD", "REMOVE", "CURRENT", "RESET"];
  return names[v] != null ? names[v] : String(v);
}

// ─── scenarios ───────────────────────────────────────────────────────────────

async function streamingScenarios(ctx) {
  const { run, PROJECT, rid, grpcOpts } = ctx;

  const { SubscriberClient } = pubsubV1;
  const { FirestoreClient } = firestoreV1;
  const { LoggingServiceV2Client } = loggingV2;

  const pubsub = new PubSub({ projectId: PROJECT });
  const firestore = new Firestore({ projectId: PROJECT });

  const topicId = rid("stream-topic");
  const subName = (id) => `projects/${PROJECT}/subscriptions/${id}`;

  async function ensureTopic(topicOptions) {
    try {
      await pubsub.createTopic(topicId);
    } catch (err) {
      if (err.code !== 6) throw new Error(`create topic: ${err.message}`);
    }
    return pubsub.topic(topicId, topicOptions || {});
  }

  async function ensureSubscription(subId, options) {
    await ensureTopic();
    try {
      await pubsub.topic(topicId).createSubscription(subId, options);
    } catch (err) {
      if (err.code !== 6) throw new Error(`create subscription ${subId}: ${err.message}`);
    }
    return subName(subId);
  }

  // streamPullOne opens the generated StreamingPull, sends the initial request,
  // and returns the ack id of the first message whose body is want. The stream is
  // left open so the caller controls redelivery.
  async function streamPullOne(subClient, name, want) {
    const stream = subClient.streamingPull();
    const reader = makeReader(stream);
    stream.write({ subscription: name, streamAckDeadlineSeconds: ACK_DEADLINE });
    for (;;) {
      const { value, done } = await nextWithTimeout(reader, READ_TIMEOUT_MS, `streaming pull for ${want}`);
      if (done) throw new Error(`streaming pull ended before ${want} arrived`);
      for (const rm of toArray(value.receivedMessages)) {
        if (bytesToText(rm.message && rm.message.data) === want) {
          return { ackId: rm.ackId, stream };
        }
      }
    }
  }

  // 1. An unacked message must be redelivered after the 10s ack deadline, and an
  // ack must stop it.
  await run("streaming.pubsub_ack_deadline", "OK", async () => {
    const name = await ensureSubscription(rid("stream-ackdl-sub"), { ackDeadlineSeconds: ACK_DEADLINE });
    const topic = await ensureTopic();
    await topic.publishMessage({ data: Buffer.from("ackdl") });
    const subClient = new SubscriberClient(grpcOpts());
    try {
      // First delivery: receive and deliberately do not ack or extend.
      const first = await streamPullOne(subClient, name, "ackdl");
      endStream(first.stream);
      await sleep(REDELIVER_WAIT_MS);

      // After the ack deadline the message must be redelivered.
      const second = await streamPullOne(subClient, name, "ackdl");
      endStream(second.stream);

      await subClient.acknowledge({ subscription: name, ackIds: [second.ackId] });
      const [pull] = await subClient.pull({ subscription: name, maxMessages: 10, returnImmediately: true });
      for (const rm of toArray(pull.receivedMessages)) {
        if (bytesToText(rm.message && rm.message.data) === "ackdl") {
          throw new Error("message redelivered after ack");
        }
      }
      return {
        observable: "redelivered=yes",
        detail: "unacked message redelivered after the 10s ack deadline; ack stopped it",
      };
    } finally {
      await subClient.close().catch(() => {});
    }
  });

  // 2. ModifyAckDeadline must keep an already-delivered message invisible past
  // its original deadline.
  await run("streaming.pubsub_ack_extension", "OK", async () => {
    const name = await ensureSubscription(rid("stream-ackext-sub"), { ackDeadlineSeconds: ACK_DEADLINE });
    const topic = await ensureTopic();
    await topic.publishMessage({ data: Buffer.from("ackext") });
    const subClient = new SubscriberClient(grpcOpts());
    try {
      const recv = await streamPullOne(subClient, name, "ackext");
      // Extend well past the original 10s, then hold across the old deadline.
      await subClient.modifyAckDeadline({
        subscription: name, ackIds: [recv.ackId], ackDeadlineSeconds: 600,
      });
      endStream(recv.stream);
      await sleep(REDELIVER_WAIT_MS);

      const [pull] = await subClient.pull({ subscription: name, maxMessages: 10, returnImmediately: true });
      for (const rm of toArray(pull.receivedMessages)) {
        if (bytesToText(rm.message && rm.message.data) === "ackext") {
          throw new Error("message redelivered despite ModifyAckDeadline(600s)");
        }
      }
      // Clean up: nack so the subscription is not left holding an invisible
      // message for the emulator's lifetime.
      await subClient.modifyAckDeadline({
        subscription: name, ackIds: [recv.ackId], ackDeadlineSeconds: 0,
      });
      return {
        observable: "no_redelivery_while_extended=yes",
        detail: "ModifyAckDeadline(600s) kept the message invisible past its original 10s deadline",
      };
    } finally {
      await subClient.close().catch(() => {});
    }
  });

  // 3. An ordering key must preserve publish order (high-level subscriber under
  // test).
  await run("streaming.pubsub_ordering_keys", "OK", async () => {
    const n = 10;
    const subId = rid("stream-order-sub");
    const topic = await ensureTopic({ enableMessageOrdering: true });
    await ensureSubscription(subId, { enableMessageOrdering: true });

    for (let i = 0; i < n; i++) {
      await topic.publishMessage({
        data: Buffer.from(`order-${String(i).padStart(2, "0")}`),
        orderingKey: "stream-order-key",
      });
    }

    const sub = pubsub.subscription(subId);
    const got = [];
    await new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`ordering receive timed out (got ${got.length})`)), 40000);
      const onMessage = (msg) => {
        got.push(bytesToText(msg.data));
        msg.ack();
        if (got.length >= n) {
          clearTimeout(timer);
          sub.removeListener("message", onMessage);
          resolve();
        }
      };
      sub.on("message", onMessage);
      sub.on("error", reject);
    });
    await sub.close();

    const want = [];
    for (let i = 0; i < n; i++) want.push(`order-${String(i).padStart(2, "0")}`);
    for (let i = 0; i < n; i++) {
      if (got[i] !== want[i]) {
        throw new Error(`out of order at ${i}: got ${JSON.stringify(got)} want ${JSON.stringify(want)}`);
      }
    }
    return { observable: "ordered=yes", detail: `ordering key preserved order across ${n} messages` };
  });

  // 4. An acked exactly-once message must not be redelivered.
  await run("streaming.pubsub_exactly_once_ack", "OK", async () => {
    const name = await ensureSubscription(rid("stream-eod-sub"), {
      ackDeadlineSeconds: ACK_DEADLINE, enableExactlyOnceDelivery: true,
    });
    const topic = await ensureTopic();
    await topic.publishMessage({ data: Buffer.from("eod") });
    const subClient = new SubscriberClient(grpcOpts());
    try {
      const recv = await streamPullOne(subClient, name, "eod");
      await subClient.acknowledge({ subscription: name, ackIds: [recv.ackId] });
      endStream(recv.stream);

      // Settle: an acked message on an exactly-once subscription must not be
      // redelivered.
      await sleep(3000);
      const [pull] = await subClient.pull({ subscription: name, maxMessages: 10, returnImmediately: true });
      for (const rm of toArray(pull.receivedMessages)) {
        if (bytesToText(rm.message && rm.message.data) === "eod") {
          throw new Error("exactly-once message redelivered after ack");
        }
      }
      return { observable: "dup=0", detail: "acked exactly-once message was not redelivered" };
    } finally {
      await subClient.close().catch(() => {});
    }
  });

  // 5. The high-level subscriber must honor its configured flow-control cap:
  // with maxMessages below the backlog it must never hold more than maxMessages
  // unacked messages, and it must drain the whole backlog.
  await run("streaming.pubsub_flow_control", "OK", async () => {
    const cap = 3;
    const backlog = 15;
    const subId = rid("stream-flow-sub");
    await ensureSubscription(subId, { ackDeadlineSeconds: ACK_DEADLINE });
    const topic = await ensureTopic();
    for (let i = 0; i < backlog; i++) {
      await topic.publishMessage({ data: Buffer.from(`flow-${String(i).padStart(2, "0")}`) });
    }

    // The subscriber under test holds each message (no ack) until released, so
    // delivery would outrun acks and expose a missing bound. The driver releases
    // only after the cap is proved.
    let inflight = 0;
    let maxInflight = 0;
    let delivered = 0;
    let releaseGate;
    const gate = new Promise((resolve) => { releaseGate = resolve; });

    const sub = pubsub.subscription(subId, { flowControl: { maxMessages: cap } });
    try {
      const drained = new Promise((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error(`flow control: drain timed out (delivered=${delivered})`)), 40000);
        sub.on("message", (msg) => {
          inflight += 1;
          maxInflight = Math.max(maxInflight, inflight);
          delivered += 1;
          gate.then(() => {
            msg.ack();
            inflight -= 1;
            if (delivered >= backlog) {
              clearTimeout(timer);
              resolve();
            }
          });
        });
        sub.on("error", reject);
      });

      const deadline = Date.now() + 30000;
      while (delivered < cap && Date.now() < deadline) {
        await sleep(10);
      }
      if (delivered < cap) {
        throw new Error(`flow control: only ${delivered}/${cap} messages delivered while held`);
      }
      // Hold at the cap: a client that ignores the bound keeps delivering.
      await sleep(1500);
      if (delivered !== cap) {
        throw new Error(`flow control: ${delivered} messages held concurrently, want max_outstanding=${cap}`);
      }
      if (maxInflight !== cap) {
        throw new Error(`flow control: max in flight ${maxInflight}, want ${cap}`);
      }

      releaseGate();
      await drained;
    } finally {
      releaseGate();
      await sub.close().catch(() => {});
    }
    return {
      observable: `flow_control=ok,max_in_flight<=${cap}`,
      detail: `held at most ${cap} of ${backlog} messages outstanding`,
    };
  });

  // 6. A Listen resume token must replay the post-token write and de-duplicate
  // the racing write opened before AddTarget.
  await run("streaming.firestore_listen_resume_token", "OK", async () => {
    const coll = rid("stream-listen-resume");
    const db = firestoreDb(PROJECT);
    const parent = `${db}/documents`;
    const client = new FirestoreClient(grpcOpts());
    try {
      // Stream A: snapshot, then read the token from the CURRENT frame.
      const streamA = client.listen();
      const readerA = makeReader(streamA);
      streamA.write(listenAddRequest(db, parent, 1, coll, null));
      let token = null;
      while (token == null) {
        const { value, done } = await nextWithTimeout(readerA, READ_TIMEOUT_MS, "stream A snapshot");
        if (done) throw new Error("stream A ended before CURRENT");
        const tc = value.targetChange;
        if (tc && targetChangeType(tc) === "CURRENT" && tc.resumeToken && tc.resumeToken.length > 0) {
          token = bytesValue(tc.resumeToken);
        }
      }
      endStream(streamA);

      // Written after A closed: it must be replayed to the resumed stream.
      await firestore.collection(coll).doc("b").set({ v: "b" });

      // Stream B: register the subscription (a harmless remove proves the
      // handler is running), then write "c" BEFORE AddTarget. "c" is therefore
      // both replayed history and a buffered live delta — it must be delivered
      // exactly once.
      const streamB = client.listen();
      const readerB = makeReader(streamB);
      streamB.write(listenRemoveRequest(db, 999));
      for (;;) {
        const { value, done } = await nextWithTimeout(readerB, READ_TIMEOUT_MS, "stream B barrier");
        if (done) throw new Error("stream B ended before REMOVE");
        const tc = value.targetChange;
        if (tc && targetChangeType(tc) === "REMOVE") break;
      }
      await firestore.collection(coll).doc("c").set({ v: "c" });
      streamB.write(listenAddRequest(db, parent, 2, coll, token));

      const counts = {};
      for (;;) {
        const { value, done } = await nextWithTimeout(readerB, READ_TIMEOUT_MS, "stream B resumed");
        if (done) throw new Error("stream B ended before NO_CHANGE");
        const dc = value.documentChange;
        if (dc && dc.document && dc.document.name) {
          const short = dc.document.name.split("/").pop();
          counts[short] = (counts[short] || 0) + 1;
          continue;
        }
        const tc = value.targetChange;
        if (tc && targetChangeType(tc) === "NO_CHANGE") break;
      }
      endStream(streamB);

      for (const want of ["b", "c"]) {
        if (counts[want] !== 1) {
          throw new Error(`resumed stream delivered "${want}" ${counts[want] || 0} times, want 1 (all=${JSON.stringify(counts)})`);
        }
      }
      return {
        observable: "loss=0,dup=0",
        detail: "resume token replayed b and delivered the racing c exactly once",
      };
    } finally {
      await client.close().catch(() => {});
    }
  });

  // 7. A live Listen snapshot must deliver each document exactly once with a
  // non-decreasing read_time.
  await run("streaming.firestore_snapshot_consistency", "OK", async () => {
    const coll = rid("stream-listen-consistency");
    const db = firestoreDb(PROJECT);
    const parent = `${db}/documents`;
    const client = new FirestoreClient(grpcOpts());
    try {
      const stream = client.listen();
      const reader = makeReader(stream);
      stream.write(listenAddRequest(db, parent, 1, coll, null));
      for (;;) {
        const { value, done } = await nextWithTimeout(reader, READ_TIMEOUT_MS, "listen initial snapshot");
        if (done) throw new Error("listen ended before the initial NO_CHANGE");
        const tc = value.targetChange;
        if (tc && targetChangeType(tc) === "NO_CHANGE") break;
      }

      const n = 5;
      for (let i = 0; i < n; i++) {
        await firestore.collection(coll).doc(`d${i}`).set({ i });
      }

      const counts = {};
      let distinct = 0;
      let lastRead = null;
      let monotonic = true;
      while (distinct < n) {
        const { value, done } = await nextWithTimeout(reader, READ_TIMEOUT_MS, "listen live changes");
        if (done) throw new Error("listen ended before all documents arrived");
        const dc = value.documentChange;
        if (dc && dc.document && dc.document.name) {
          counts[dc.document.name] = (counts[dc.document.name] || 0) + 1;
          distinct = Object.keys(counts).length;
          continue;
        }
        const tc = value.targetChange;
        if (tc && targetChangeType(tc) === "NO_CHANGE") {
          const rt = tsMillis(tc.readTime);
          if (rt != null) {
            if (lastRead != null && rt < lastRead) monotonic = false;
            lastRead = rt;
          }
        }
      }
      endStream(stream);

      for (const [name, c] of Object.entries(counts)) {
        if (c !== 1) throw new Error(`document ${name} delivered ${c} times, want 1`);
      }
      if (!monotonic) throw new Error("read_time was not monotonic across NO_CHANGE frames");
      return {
        observable: `docs=${n},dup=0,monotonic=yes`,
        detail: "5 concurrent writes delivered once each with monotonic read_time",
      };
    } finally {
      await client.close().catch(() => {});
    }
  });

  // 8. A reconnected tail must deliver post-reconnect entries and must not
  // replay the disconnected-window entry (TailLogEntries has no resume cursor).
  await run("streaming.logging_tail_reconnect", "OK", async () => {
    const parent = `projects/${PROJECT}`;
    const logName = `${parent}/logs/${rid("stream-tail-log")}`;
    const client = new LoggingServiceV2Client(grpcOpts());

    const writeEntry = (text) => client.writeLogEntries({
      logName,
      resource: { type: "global", labels: { project_id: PROJECT } },
      entries: [{ logName, severity: "INFO", textPayload: text }],
    });

    const startTail = () => {
      const stream = client.tailLogEntries();
      const reader = makeReader(stream);
      stream.write({
        resourceNames: [parent],
        filter: `logName="${logName}"`,
        bufferWindow: { seconds: 0, nanos: 200000000 },
      });
      return { stream, reader };
    };

    const seenGap = [];
    // recvUntilPrefix keeps writing unique entries with prefix until one is
    // delivered, tolerating the emulator's "new since stream start" seed race.
    const recvUntilPrefix = async (reader, prefix) => {
      let i = 0;
      let writing = true;
      const writer = (async () => {
        while (writing) {
          try { await writeEntry(`${prefix}-${i}`); } catch (_) { /* keep trying */ }
          i += 1;
          await sleep(250);
        }
      })();
      try {
        for (;;) {
          const { value, done } = await nextWithTimeout(reader, READ_TIMEOUT_MS, `tail ${prefix}`);
          if (done) throw new Error("tail stream ended before the entry arrived");
          for (const e of toArray(value.entries)) {
            const text = String(e.textPayload || "");
            if (text === "stream-tail-gap") seenGap.push(text);
            if (text.startsWith(prefix)) return;
          }
        }
      } finally {
        writing = false;
        await writer.catch(() => {});
      }
    };

    try {
      const s1 = startTail();
      await recvUntilPrefix(s1.reader, "stream-tail-a");
      endStream(s1.stream);

      // Written while disconnected: below the second stream's seed cursor, so
      // it must NOT be replayed.
      await writeEntry("stream-tail-gap");

      const s2 = startTail();
      await recvUntilPrefix(s2.reader, "stream-tail-c");
      endStream(s2.stream);

      if (seenGap.length > 0) throw new Error("tail replayed the disconnected-window entry");
      return {
        observable: "reconnect_ok=yes,gap_replayed=no",
        detail: "second tail delivered post-reconnect entries and did not replay the disconnected-window entry",
      };
    } finally {
      await client.close().catch(() => {});
    }
  });

  // 9. Node's high-level Storage client has no BidiReadObject projection.
  await run("streaming.storage_bidi_read", "SKIP", async () => {
    throw new Error("@google-cloud/storage has no high-level BidiReadObject projection; use the Go/Java legs");
  });

  // The parent exits the process as soon as this leg returns, before its
  // results write stream has necessarily flushed the final row. Yield briefly so
  // the SKIP row lands in the JSONL alongside the others.
  await sleep(200);
}

module.exports = { streamingScenarios };
