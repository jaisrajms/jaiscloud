// jaiscloud-gcp SDK tour — Node leg (official @google-cloud/* clients).
//
// Runs the same 18 scenarios as the Go/Python/Java legs against the emulator
// and emits one JSONL record per scenario. Wiring: Storage/PubSub/Firestore use
// their emulator env hooks; Logging/Secret Manager/KMS/Dataproc get an explicit
// plaintext channel (apiEndpoint host + port + insecure sslCreds); BigQuery
// uses BIGQUERY_EMULATOR_HOST.
//
// This is a demo/compliance artifact, not a conformance gate: failures are
// recorded and classified, never papered over.
"use strict";

const crypto = require("crypto");
const fs = require("fs");
const grpc = require("@grpc/grpc-js");

const REST = (process.env.EMULATOR_REST || "http://localhost:8080").replace(/\/+$/, "");
const GRPC = process.env.EMULATOR_GRPC || "localhost:8081";
const PROJECT = process.env.PROJECT_ID || "jaiscloud-project";
const RUN_ID = process.env.SDK_TOUR_RUN_ID || "local";
const OUT = process.env.SDK_TOUR_RESULTS || "";
const MODE = process.env.SDK_TOUR_MODE || "tour";
const BATCH_COUNT = 32;

const [GRPC_HOST, GRPC_PORT] = (() => {
  const i = GRPC.lastIndexOf(":");
  if (i < 0) return [GRPC, 443];
  return [GRPC.slice(0, i), Number(GRPC.slice(i + 1))];
})();

// Emulator hooks the official clients honour. The Node Storage client uses
// STORAGE_EMULATOR_HOST as the *whole* base URL for JSON calls (baseUrl =
// EMULATOR_HOST) but appends /upload/storage/v1 to apiEndpoint for resumable
// uploads, so no single env value satisfies both. Passing apiEndpoint instead
// makes baseUrl = apiEndpoint + "/storage/v1" (JSON) and the resumable URL =
// apiEndpoint + "/upload/storage/v1" — both of which jaiscloud serves.
delete process.env.STORAGE_EMULATOR_HOST;
process.env.PUBSUB_EMULATOR_HOST = process.env.PUBSUB_EMULATOR_HOST || GRPC;
process.env.FIRESTORE_EMULATOR_HOST = process.env.FIRESTORE_EMULATOR_HOST || GRPC;
process.env.BIGQUERY_EMULATOR_HOST = process.env.BIGQUERY_EMULATOR_HOST || REST;

const rid = (leaf) => `${leaf}-${RUN_ID}`;

function resumablePayload() {
  const unit = Buffer.from("jaiscloud-sdk-tour-resumable-payload-");
  const target = 4 << 20;
  const n = Math.ceil(target / unit.length);
  return Buffer.concat(Array(n).fill(unit)).subarray(0, target);
}

function sha256Hex(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

// ─── result recording ────────────────────────────────────────────────────────

const outStream = OUT ? fs.createWriteStream(OUT) : null;

function record(scenario, status, expect, { observable = "", detail = "", error = "", classification = "" } = {}) {
  let line = `NODE ${scenario} ${status}`;
  if (detail) line += ` ${detail}`;
  if (error) line += ` err=${error}`;
  console.log(line);
  if (outStream) {
    outStream.write(JSON.stringify({
      lang: "node", scenario, status, expect, observable, detail, error, classification,
    }) + "\n");
  }
}

function classify(err) {
  const code = err && (err.code ?? (err.status && err.status.code));
  if (code === 12 || code === "UNIMPLEMENTED") return "unimplemented";
  if (code === 14 || code === 16 || code === 7 || code === "UNAVAILABLE" ||
      code === "UNAUTHENTICATED" || code === "PERMISSION_DENIED") return "wiring-gap";
  const msg = String((err && err.message) || err).toLowerCase();
  for (const needle of ["unimplemented", "not implemented"]) if (msg.includes(needle)) return "unimplemented";
  for (const needle of ["unavailable", "connection refused", "econnrefused", "tls", "handshake",
                        "name resolution", "permission denied", "unauthenticated", "enotfound"]) {
    if (msg.includes(needle)) return "wiring-gap";
  }
  return "emulator-bug";
}

async function run(scenario, expect, fn) {
  const start = Date.now();
  try {
    const { observable = "", detail = "" } = (await fn()) || {};
    record(scenario, "OK", expect, { observable, detail });
  } catch (err) {
    const status = expect === "SKIP" ? "SKIP" : "FAIL";
    record(scenario, status, expect, {
      detail: `${Date.now() - start}ms`,
      error: `${err.name || "Error"}: ${err.message || err}`,
      classification: classify(err),
    });
  }
}

// ─── client wiring ───────────────────────────────────────────────────────────

function grpcOpts() {
  return {
    apiEndpoint: GRPC_HOST,
    port: GRPC_PORT,
    sslCreds: grpc.credentials.createInsecure(),
  };
}

function Ctor(mod, name) {
  return require(mod)[name];
}

// ─── fixtures ────────────────────────────────────────────────────────────────

const BUCKET = rid("sdk-tour-bucket");
const TOPIC = rid("sdk-tour-topic");
const SUB = rid("sdk-tour-sub");
const BQ_BUCKET = rid("sdk-tour-bq");

const { Storage } = require("@google-cloud/storage");
const { PubSub } = require("@google-cloud/pubsub");
const { Firestore } = require("@google-cloud/firestore");
const { BigQuery } = require("@google-cloud/bigquery");
const { v2: loggingV2 } = require("@google-cloud/logging");

const storage = new Storage({ projectId: PROJECT, apiEndpoint: REST });
const pubsub = new PubSub({ projectId: PROJECT });
const firestore = new Firestore({ projectId: PROJECT });
const bigquery = new BigQuery({ projectId: PROJECT });

async function ensureTopic() {
  const topic = pubsub.topic(TOPIC);
  const [exists] = await topic.exists();
  if (!exists) await pubsub.createTopic(TOPIC);
  return topic;
}

async function ensureSubscription(topic) {
  const sub = topic.subscription(SUB);
  const [exists] = await sub.exists();
  if (!exists) await topic.createSubscription(SUB);
  return sub;
}

// ─── scenarios ───────────────────────────────────────────────────────────────

async function storageScenarios() {
  await run("storage.create_bucket", "OK", async () => {
    const bucket = storage.bucket(BUCKET);
    const [exists] = await bucket.exists();
    if (!exists) await bucket.create({ location: "US" });
    return { observable: "created", detail: `bucket=${BUCKET}` };
  });

  await run("storage.resumable_upload", "OK", async () => {
    const payload = resumablePayload();
    const file = storage.bucket(BUCKET).file("resumable/payload.bin");
    await file.save(payload, {
      resumable: true,           // force the resumable protocol
      chunkSize: 256 * 1024,
      contentType: "application/octet-stream",
      validation: false,
    });
    const [meta] = await file.getMetadata();
    return { observable: String(meta.size), detail: `size=${meta.size} chunkSize=262144 resumable=true` };
  });

  await run("storage.stream_download_checksum", "OK", async () => {
    const file = storage.bucket(BUCKET).file("resumable/payload.bin");
    const hash = crypto.createHash("sha256");
    let n = 0;
    await new Promise((resolve, reject) => {
      file.createReadStream()
        .on("data", (c) => { hash.update(c); n += c.length; })
        .on("end", resolve)
        .on("error", reject);
    });
    const got = hash.digest("hex");
    const want = sha256Hex(resumablePayload());
    if (got !== want) throw new Error(`checksum mismatch: got ${got} want ${want} bytes=${n}`);
    return { observable: got, detail: `sha256=${got} bytes=${n}` };
  });

  await run("storage.list_pagination", "OK", async () => {
    const bucket = storage.bucket(BUCKET);
    for (let i = 0; i < 7; i++) {
      await bucket.file(`page/${String(i).padStart(2, "0")}.txt`).save(`page-${i}`, { resumable: false });
    }
    let query = { prefix: "page/", maxResults: 2 };
    let pages = 0, total = 0;
    for (;;) {
      const [files, next] = await bucket.getFiles(query);
      pages += 1;
      total += files.length;
      if (!next) break;
      query = next;
    }
    if (total !== 7) throw new Error(`listed ${total} objects, want 7`);
    return { observable: `pages=${pages},total=${total}`, detail: `maxResults=2 pages=${pages} total=${total}` };
  });
}

async function pubsubScenarios() {
  await run("pubsub.batch_publish", "OK", async () => {
    const topic = await ensureTopic();
    const ids = [];
    for (let i = 0; i < BATCH_COUNT; i++) {
      ids.push(await topic.publishMessage({ data: Buffer.from(`batch-${String(i).padStart(2, "0")}`) }));
    }
    return { observable: String(ids.length), detail: `published=${ids.length} batching=true` };
  });

  await run("pubsub.streaming_pull_ack", "OK", async () => {
    const topic = await ensureTopic();
    const sub = await ensureSubscription(topic);
    for (let i = 0; i < BATCH_COUNT; i++) {
      await topic.publishMessage({ data: Buffer.from(`stream-${String(i).padStart(2, "0")}`) });
    }
    let received = 0;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`streaming pull received ${received}/${BATCH_COUNT}`)), 30000);
      const handler = (msg) => {
        msg.ack();
        received += 1;
        if (received >= BATCH_COUNT) { clearTimeout(timer); resolve(); }
      };
      sub.on("message", handler);
      sub.on("error", reject);
    });
    await sub.close();
    return { observable: String(received), detail: `received=${received} acked=${received} streaming=true` };
  });

  await run("iam.policy_read_modify_write", "OK", async () => {
    const topic = await ensureTopic();
    const role = "roles/pubsub.publisher";
    const member = "allUsers";
    const [policy] = await topic.iam.getPolicy();
    policy.bindings.push({ role, members: [member] });
    await topic.iam.setPolicy(policy);
    const [policy2] = await topic.iam.getPolicy();
    const found = (policy2.bindings || []).some(
      (b) => b.role === role && (b.members || []).includes(member));
    if (!found) throw new Error(`role ${role} not present after set`);
    return { observable: "true", detail: `role=${role} member=${member}` };
  });
}

async function firestoreScenarios() {
  await run("firestore.listen_write", "OK", async () => {
    const col = firestore.collection(rid("fs-listen"));
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Listen did not deliver live-doc")), 20000);
      const unsub = col.onSnapshot((snap) => {
        if (snap.docs.some((d) => d.id === "live-doc")) {
          clearTimeout(timer);
          unsub();
          resolve();
        }
      }, reject);
      col.doc("live-doc").set({ v: 1 });
    });
    return { observable: "true", detail: "Listen delivered ADD for live-doc" };
  });

  await run("firestore.transaction", "OK", async () => {
    const col = firestore.collection(rid("fs-tx"));
    const ref = col.doc("counter");
    await ref.set({ n: 0 });
    for (let i = 0; i < 3; i++) {
      await firestore.runTransaction(async (tx) => {
        const snap = await tx.get(ref);
        const n = snap.get("n") || 0;
        tx.set(ref, { n: n + 1 });
      });
    }
    const snap = await ref.get();
    const n = snap.get("n");
    if (n !== 3) throw new Error(`counter = ${n}, want 3`);
    return { observable: String(n), detail: "3 read-modify-write transactions" };
  });

  await run("firestore.query_pagination", "OK", async () => {
    const col = firestore.collection(rid("fs-page"));
    for (let i = 0; i < 5; i++) await col.doc(`d${i}`).set({ i });
    let last = null, pages = 0, total = 0;
    for (;;) {
      let q = col.orderBy("i").limit(2);
      if (last !== null) q = q.startAfter(last);
      const snap = await q.get();
      if (snap.empty) break;
      last = snap.docs[snap.docs.length - 1].get("i");
      pages += 1;
      total += snap.docs.length;
    }
    if (total !== 5) throw new Error(`paged ${total} docs, want 5`);
    return { observable: `pages=${pages},total=${total}`, detail: `limit=2 pages=${pages} total=${total}` };
  });
}

function writeLogEntry(client, logName, text) {
  return client.writeLogEntries({
    logName,
    resource: { type: "global", labels: { project_id: PROJECT } },
    entries: [{ logName, severity: "INFO", textPayload: text }],
  });
}

async function loggingScenarios() {
  const parent = `projects/${PROJECT}`;
  const logName = `${parent}/logs/${rid("sdk-tour-log")}`;
  const client = new loggingV2.LoggingServiceV2Client(grpcOpts());

  await run("logging.write_entry", "OK", async () => {
    await writeLogEntry(client, logName, "sdk-tour-entry");
    const [entries] = await client.listLogEntries({
      resourceNames: [parent], filter: `logName="${logName}"`,
    });
    if (entries.length !== 1) throw new Error(`ListLogEntries returned ${entries.length} entries, want 1`);
    return { observable: String(entries.length), detail: "WriteLogEntries + ListLogEntries round-trip" };
  });

  await run("logging.tail", "OK", async () => {
    await new Promise((resolve, reject) => {
      const stream = client.tailLogEntries();
      const timer = setTimeout(() => { stream.end(); reject(new Error("TailLogEntries did not deliver the entry")); }, 30000);
      stream.on("data", (resp) => {
        for (const entry of resp.entries || []) {
          if ((entry.textPayload || "").startsWith("sdk-tour-tail")) {
            clearTimeout(timer);
            stream.end();
            resolve();
          }
        }
      });
      stream.on("error", reject);
      stream.write({
        resourceNames: [parent], filter: `logName="${logName}"`,
        bufferWindow: { seconds: 0, nanos: 200000000 },
      });
      // The emulator seeds its cursor when it reads the opening request, so the
      // entry must be written AFTER the stream is open. Retry on a timer.
      setTimeout(async () => {
        for (let i = 0; i < 40; i++) {
          try { await writeLogEntry(client, logName, `sdk-tour-tail-${i}`); } catch { /* keep trying */ }
          await new Promise((r) => setTimeout(r, 500));
        }
      }, 400);
    });
    return { observable: "true", detail: "TailLogEntries delivered the entry" };
  });
}

async function secretScenarios() {
  const parent = `projects/${PROJECT}`;
  const secretId = rid("sdk-tour-secret");
  const C = Ctor("@google-cloud/secret-manager", "SecretManagerServiceClient");
  const client = new C(grpcOpts());

  await run("secretmanager.add_access_list", "OK", async () => {
    const [secret] = await client.createSecret({
      parent, secretId, secret: { replication: { automatic: {} } },
    });
    await client.addSecretVersion({ parent: secret.name, payload: { data: Buffer.from("sdk-tour") } });
    const [acc] = await client.accessSecretVersion({ name: `${secret.name}/versions/1` });
    if (acc.payload.data.toString() !== "sdk-tour") {
      throw new Error(`accessed payload ${acc.payload.data.toString()}`);
    }
    const [versions] = await client.listSecretVersions({ parent: secret.name });
    if (versions.length < 1) throw new Error(`ListSecretVersions returned ${versions.length}`);
    return { observable: String(versions.length), detail: `access=ok versions=${versions.length}` };
  });
}

async function kmsScenarios() {
  const parent = `projects/${PROJECT}/locations/global`;
  const C = Ctor("@google-cloud/kms", "KeyManagementServiceClient");
  const client = new C(grpcOpts());

  await run("kms.encrypt_decrypt", "OK", async () => {
    const [ring] = await client.createKeyRing({
      parent, keyRingId: rid("sdk-tour-ring"), keyRing: {},
    });
    const [key] = await client.createCryptoKey({
      parent: ring.name, cryptoKeyId: rid("sdk-tour-key"),
      cryptoKey: { purpose: "ENCRYPT_DECRYPT" },
    });
    const [enc] = await client.encrypt({ name: key.name, plaintext: Buffer.from("sdk-tour") });
    const [dec] = await client.decrypt({ name: key.name, ciphertext: enc.ciphertext });
    if (dec.plaintext.toString() !== "sdk-tour") throw new Error(`decrypted ${dec.plaintext}`);
    return { observable: "true", detail: "encrypt/decrypt round-trip" };
  });

  await run("kms.asymmetric_sign", "OK", async () => {
    const [ring] = await client.createKeyRing({
      parent, keyRingId: rid("sdk-tour-sign-ring"), keyRing: {},
    });
    const [key] = await client.createCryptoKey({
      parent: ring.name, cryptoKeyId: rid("sdk-tour-sign-key"),
      cryptoKey: {
        purpose: "ASYMMETRIC_SIGN",
        versionTemplate: { algorithm: "RSA_SIGN_PKCS1_2048_SHA256" },
      },
    });
    const version = `${key.name}/cryptoKeyVersions/1`;
    const digest = crypto.createHash("sha256").update("sdk-tour-sign").digest();
    const [sig] = await client.asymmetricSign({ name: version, digest: { sha256: digest } });
    const [pk] = await client.getPublicKey({ name: version });
    // KMS signs the SHA-256 digest; Node's crypto.verify has no prehashed mode,
    // so verify the original message it hashes to (sha256(message) == digest).
    const ok = crypto.verify("sha256", Buffer.from("sdk-tour-sign"), pk.pem, sig.signature);
    if (!ok) throw new Error("signature did not verify with GetPublicKey");
    return { observable: "true", detail: "AsymmetricSign verified with GetPublicKey" };
  });
}

async function bigqueryScenarios() {
  const dsId = rid("sdk_tour_ds").replace(/-/g, "_");
  const tblId = rid("sdk_tour_tbl").replace(/-/g, "_");
  const loadTblId = rid("sdk_tour_load").replace(/-/g, "_");
  const schema = [
    { name: "id", type: "INTEGER" },
    { name: "name", type: "STRING" },
  ];

  async function ensureDataset() {
    const ds = bigquery.dataset(dsId);
    const [exists] = await ds.exists();
    if (!exists) await ds.create({ location: "US" });
  }
  async function ensureTable(tbl) {
    const t = bigquery.dataset(dsId).table(tbl);
    const [exists] = await t.exists();
    if (!exists) await t.create({ schema });
  }

  await run("bigquery.insertall_query", "OK", async () => {
    await ensureDataset();
    await ensureTable(tblId);
    const table = bigquery.dataset(dsId).table(tblId);
    await table.insert([{ id: 1, name: "alice" }, { id: 2, name: "bob" }]);
    const [rows] = await bigquery.query(
      `SELECT id, name FROM \`${PROJECT}.${dsId}.${tblId}\` ORDER BY id`);
    if (rows.length !== 2) throw new Error(`query returned ${rows.length} rows, want 2`);
    return { observable: String(rows.length), detail: "insertAll + query returned 2 rows" };
  });

  await run("bigquery.load_job", "OK", async () => {
    const bucket = storage.bucket(BQ_BUCKET);
    const [exists] = await bucket.exists();
    if (!exists) await bucket.create({ location: "US" });
    const gcsFile = bucket.file("rows.json");
    await gcsFile.save('{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n', {
      resumable: false, contentType: "application/x-ndjson",
    });
    await ensureDataset();
    await ensureTable(loadTblId);
    const table = bigquery.dataset(dsId).table(loadTblId);
    await table.load(gcsFile, {
      sourceFormat: "NEWLINE_DELIMITED_JSON",
      writeDisposition: "WRITE_APPEND",
    });
    const [rows] = await bigquery.query(
      `SELECT id, name FROM \`${PROJECT}.${dsId}.${loadTblId}\` ORDER BY id`);
    if (rows.length !== 2) throw new Error(`load job produced ${rows.length} rows, want 2`);
    return { observable: String(rows.length), detail: "gs:// load job produced 2 rows" };
  });
}

async function dataprocScenarios() {
  const C = Ctor("@google-cloud/dataproc", "ClusterControllerClient");
  const client = new C(grpcOpts());
  const region = "us-central1";

  await run("lro.dataproc_cluster", "OK", async () => {
    const name = rid("sdk-tour-cluster");
    const [op] = await client.createCluster({
      projectId: PROJECT,
      region,
      cluster: {
        projectId: PROJECT,
        clusterName: name,
        config: {
          gceClusterConfig: { zoneUri: "us-central1-a" },
          softwareConfig: { imageVersion: "2.2" },
        },
      },
    });
    const [result] = await op.promise(); // gax operation poller
    const rawState = result.status.state;
    // The generated client may decode the enum as a number or a string.
    const STATE_NAMES = ["UNKNOWN", "CREATING", "RUNNING", "ERROR", "DELETING",
                         "UPDATING", "STOPPING", "STOPPED", "STARTING"];
    const state = typeof rawState === "number" ? (STATE_NAMES[rawState] || String(rawState)) : rawState;
    if (state !== "RUNNING") throw new Error(`cluster state = ${state}, want RUNNING`);
    try {
      const [delOp] = await client.deleteCluster({ projectId: PROJECT, region, clusterName: name });
      await delOp.promise();
    } catch { /* best-effort teardown */ }
    return { observable: state, detail: `op.promise() settled the LRO to ${state}` };
  });
}

async function main() {
  if (MODE === "errors") {
    const { errorsScenarios } = require("./errors_tour");
    await errorsScenarios({
      run, REST, GRPC, PROJECT,
      storage, pubsub, bigquery,
      rid, resumablePayload, sha256Hex,
    });
    if (outStream) outStream.end();
    return;
  }

  if (MODE === "streaming") {
    const { streamingScenarios } = require("./streaming_tour");
    await streamingScenarios({
      run, REST, GRPC, GRPC_HOST, GRPC_PORT, PROJECT, rid, grpc, grpcOpts,
    });
    if (outStream) outStream.end();
    return;
  }
  await storageScenarios();
  await pubsubScenarios();
  await firestoreScenarios();
  await loggingScenarios();
  await secretScenarios();
  await kmsScenarios();
  await bigqueryScenarios();
  await dataprocScenarios();
  if (outStream) outStream.end();
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("fatal:", err);
  process.exit(1);
});
