// jaiscloud-gcp SDK tour — Node error / retry / idempotency leg
// (SDK_TOUR_MODE=errors).
//
// The happy-path tour asserts the official clients can drive every surface.
// This leg asserts the *failure* surfaces a green tour cannot see:
// error-code mapping, retry classification, backoff / Retry-After honoring,
// idempotency and resumable-upload rewind. It ports the Go reference
// (demo/sdk-tour/go/errors.go) scenario-for-scenario and reuses the parent
// tour's record()/run() helpers and official clients, so its rows land in the
// cross-language matrix beside the tour's.
//
// Throttle injection is armed through the emulator's runtime control plane
// (POST /_jaiscloud/throttle) per scenario, and attempt counts are read back
// from the emulator's Prometheus counters (emulator started with --metrics), so
// the retry scenarios assert the *observable* attempt count rather than "it
// felt like it retried".
"use strict";

// ─── runtime throttle control ────────────────────────────────────────────────

// armThrottle posts a control document to the running injector.
async function armThrottle(rest, body) {
  const resp = await fetch(rest + "/_jaiscloud/throttle", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    throw new Error(`arm throttle: status ${resp.status}: ${await resp.text()}`);
  }
}

function clearThrottle(rest) {
  return armThrottle(rest, { mode: "off" });
}

// ─── metrics-backed attempt counting ─────────────────────────────────────────

// requestsTotal sums the emulator's jaiscloud_requests_total counter over all
// series whose service label matches want (empty = every cloud service,
// excluding the unnamed admin/metrics series). The value counts the requests
// the emulator *received* for that surface, so the delta across an operation is
// its attempt count.
async function requestsTotal(rest, want) {
  const resp = await fetch(rest + "/metrics");
  if (!resp.ok) throw new Error(`scrape metrics: status ${resp.status}`);
  const text = await resp.text();
  let total = 0;
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (!line.startsWith("jaiscloud_requests_total{")) continue;
    const open = line.indexOf("}");
    if (open < 0) continue;
    const labels = line.slice("jaiscloud_requests_total{".length, open);
    const fields = line.slice(open + 1).trim().split(/\s+/);
    if (fields.length !== 1) continue;
    if (want === "") {
      if (labels.includes('service="unknown"')) continue;
    } else if (!labels.includes(`service="${want}"`)) {
      continue;
    }
    const v = Number(fields[0]);
    if (!Number.isFinite(v)) continue;
    total += v;
  }
  return total;
}

// countAttempts runs fn and returns how many requests the emulator received for
// service during it.
async function countAttempts(rest, service, fn) {
  const before = await requestsTotal(rest, service);
  await fn();
  const after = await requestsTotal(rest, service);
  return after - before;
}

// ─── error classification helpers ────────────────────────────────────────────

// httpCode extracts the HTTP status an official client surfaced: the JSON
// apiary clients set a numeric err.code, while gRPC clients set the gRPC code.
function httpCode(err) {
  if (!err) return 0;
  if (typeof err.code === "number" && err.code >= 100 && err.code < 600) return err.code;
  if (err.status && typeof err.status.code === "number") return err.status.code;
  return 0;
}

function requireStatus(err, want) {
  if (!err) throw new Error(`expected HTTP ${want}, got success`);
  const got = httpCode(err);
  if (got !== want) {
    throw new Error(`expected HTTP ${want}, got ${got || err.code}: ${err.message}`);
  }
}

// retryInfoDelay finds the google.rpc.RetryInfo detail in a Google JSON error
// envelope and returns its retryDelay.
function retryInfoDelay(env) {
  const errObj = env && env.error;
  if (!errObj || !Array.isArray(errObj.details)) return null;
  for (const d of errObj.details) {
    if (d && d["@type"] === "type.googleapis.com/google.rpc.RetryInfo") {
      return typeof d.retryDelay === "string" && d.retryDelay ? d.retryDelay : "0s";
    }
  }
  return null;
}

// ─── scenarios ───────────────────────────────────────────────────────────────

async function errorsScenarios(ctx) {
  const {
    run, REST, storage, pubsub, bigquery, PROJECT,
    rid, resumablePayload, sha256Hex,
  } = ctx;

  const BUCKET = rid("errors-bucket");
  const TOPIC = rid("errors-topic");

  const bucket = () => storage.bucket(BUCKET);

  // The Node Storage client disables client-wide auto-retry as a side effect of
  // some operations (notably a non-resumable save) and never restores it; the
  // retry scenarios re-arm it explicitly so their attempt counts are observable.
  const ensureRetry = () => {
    storage.retryOptions.autoRetry = true;
    storage.retryOptions.maxRetries = 3;
  };

  async function ensureBucket() {
    const [exists] = await bucket().exists();
    if (!exists) {
      const err = await bucket().create({ location: "US" }).then(() => null, (e) => e);
      if (err && httpCode(err) !== 409) throw new Error(`create bucket: ${err.message}`);
    }
  }

  // 1. A second bucket create surfaces 409 ALREADY_EXISTS.
  await run("errors.storage_already_exists", "OK", async () => {
    await ensureBucket();
    const err = await bucket().create({ location: "US" }).then(() => null, (e) => e);
    if (!err) throw new Error("second bucket create unexpectedly succeeded");
    requireStatus(err, 409);
    return { observable: "already_exists=yes", detail: "second bucket create surfaced 409" };
  });

  // 2. A missing object surfaces 404 NOT_FOUND.
  await run("errors.storage_not_found", "OK", async () => {
    await ensureBucket();
    const err = await bucket().file("no/such/object").getMetadata().then(() => null, (e) => e);
    if (!err) throw new Error("missing object get unexpectedly succeeded");
    requireStatus(err, 404);
    return { observable: "not_found=yes", detail: "missing object surfaced 404" };
  });

  // 3. A 404 must not be retried: the emulator must see exactly one request.
  await run("errors.no_retry_on_4xx", "OK", async () => {
    await ensureBucket();
    ensureRetry();
    const n = await countAttempts(REST, "storage", async () => {
      const err = await bucket().file("no/such/object").getMetadata().then(() => null, (e) => e);
      if (!err) throw new Error("missing object get unexpectedly succeeded");
      requireStatus(err, 404);
    });
    if (n !== 1) throw new Error(`4xx must not be retried: emulator saw ${n} attempts, want 1`);
    return { observable: "no_retry_attempts=1", detail: "404 failed after exactly one attempt" };
  });

  // 4. IAM optimistic concurrency: a stale policy etag must be rejected with 409.
  await run("errors.iam_failed_precondition", "OK", async () => {
    await ensureBucket();
    const h = bucket().iam;
    const [policy] = await h.getPolicy();
    policy.etag = Buffer.from("stale-etag").toString("base64");
    const err = await h.setPolicy(policy).then(() => null, (e) => e);
    if (!err) throw new Error("stale IAM etag was accepted (want 409 FAILED_PRECONDITION)");
    requireStatus(err, 409);
    return { observable: "failed_precondition=yes", detail: "stale IAM etag surfaced 409" };
  });

  // 5./6. A refused attempt is retried once (429 and 503), then succeeds.
  for (const [status, name, obs] of [
    [429, "errors.retry_429_storage_get", "retry_429_attempts=2"],
    [503, "errors.retry_503_storage_get", "retry_503_attempts=2"],
  ]) {
    await run(name, "OK", async () => {
      await ensureBucket();
      ensureRetry();
      await armThrottle(REST, {
        mode: "fault", failFirst: 1, services: ["storage"], status, retryDelay: "1s",
      });
      let n;
      try {
        n = await countAttempts(REST, "storage", async () => {
          const err = await bucket().getMetadata().then(() => null, (e) => e);
          if (err) throw new Error(`get bucket attrs after injected ${status}: ${err.message}`);
        });
      } finally {
        await clearThrottle(REST);
      }
      if (n !== 2) throw new Error(`client made ${n} attempts, want 2 (one retry)`);
      return {
        observable: obs,
        detail: `injected ${status} refused the first attempt; the client retried once and succeeded`,
      };
    });
  }

  // 7. Retry-info shape: the raw 429 envelope must carry the Retry-After header
  // and a google.rpc.RetryInfo detail, as real GCP does.
  await run("errors.retry_info_shape", "OK", async () => {
    await ensureBucket();
    await armThrottle(REST, {
      mode: "fault", failFirst: 1, services: ["storage"], status: 429, retryDelay: "1s",
    });
    try {
      const resp = await fetch(`${REST}/storage/v1/b/${BUCKET}`);
      const body = await resp.text();
      if (resp.status !== 429) throw new Error(`want 429, got ${resp.status}: ${body}`);
      let env;
      try { env = JSON.parse(body); } catch (e) {
        throw new Error(`error body is not JSON: ${e.message} (${body})`);
      }
      const delay = retryInfoDelay(env);
      if (delay === null) throw new Error(`no google.rpc.RetryInfo detail in ${body}`);
      const retryAfter = resp.headers.get("retry-after");
      if (!retryAfter) throw new Error(`no Retry-After header (headers=${[...resp.headers]})`);
      return {
        observable: `RetryInfo:${delay}:Retry-After=${retryAfter}`,
        detail: "injected 429 carried Retry-After + google.rpc.RetryInfo",
      };
    } finally {
      await clearThrottle(REST);
    }
  });

  // 8. Pagination stability: page tokens must survive a throttled retry.
  await run("errors.pagination_stability", "OK", async () => {
    await ensureBucket();
    const total = 5;
    for (let i = 0; i < total; i++) {
      await bucket().file(`errors/page/${String(i).padStart(2, "0")}.txt`)
        .save(`page-${i}`, { resumable: false });
    }
    ensureRetry();
    await armThrottle(REST, {
      mode: "fault", failFirst: 1, services: ["storage"], status: 429, retryDelay: "1s",
    });
    let seen = 0;
    try {
      let query = { prefix: "errors/page/", maxResults: 2, autoPaginate: false };
      for (;;) {
        const [files, next] = await bucket().getFiles(query);
        seen += files.length;
        if (!next) break;
        query = next;
      }
    } finally {
      await clearThrottle(REST);
    }
    if (seen !== total) throw new Error(`listed ${seen} objects across pages, want ${total}`);
    return { observable: `pagination_total=${total}`, detail: "page tokens survived a throttled retry" };
  });

  // 9. Resumable rewind: a refused chunk must be re-sent and still checksum.
  await run("errors.resumable_rewind", "OK", async () => {
    await ensureBucket();
    ensureRetry();
    const payload = resumablePayload();
    const file = bucket().file("errors/rewind.bin");
    // Scope the injected fault to the chunk PUT alone so the resumable session
    // start is unaffected. ifGenerationMatch makes the upload idempotent, which
    // is what lets the Node SDK retry the refused chunk instead of aborting.
    await armThrottle(REST, {
      mode: "fault", failFirst: 1,
      services: ["storage/objectsinsertresumable"], status: 429, retryDelay: "1s",
    });
    let got;
    try {
      await file.save(payload, {
        resumable: true,
        chunkSize: 256 * 1024,
        contentType: "application/octet-stream",
        validation: false,
        preconditionOpts: { ifGenerationMatch: 0 },
      });
      const [buf] = await file.download();
      if (buf.length !== payload.length) {
        throw new Error(`rewound object size ${buf.length}, want ${payload.length}`);
      }
      got = sha256Hex(buf);
      const want = sha256Hex(payload);
      if (got !== want) throw new Error(`rewound checksum ${got}, want ${want}`);
    } finally {
      await clearThrottle(REST);
    }
    return { observable: `resumable_sha256=${got}`, detail: "a refused chunk was re-sent and the object checksum matches" };
  });

  // 10. A malformed request must surface INVALID_ARGUMENT synchronously.
  await run("errors.invalid_argument", "OK", async () => {
    const err = await bigquery.query({ query: "SELECT * FROM" }).then(() => null, (e) => e);
    if (!err) throw new Error("malformed SQL unexpectedly succeeded");
    requireStatus(err, 400);
    return { observable: "invalid_argument=yes", detail: "malformed SQL surfaced 400 INVALID_ARGUMENT" };
  });

  // 11. insertId is the API's client-supplied idempotency key: replaying a row
  // with the same insertId must not create a second row.
  await run("errors.idempotent_insertall", "OK", async () => {
    const dsId = rid("errors_ds").replace(/-/g, "_");
    const tblId = rid("errors_tbl").replace(/-/g, "_");
    const schema = [
      { name: "id", type: "INTEGER" },
      { name: "name", type: "STRING" },
    ];
    const ds = bigquery.dataset(dsId);
    const [dsExists] = await ds.exists();
    if (!dsExists) await ds.create({ location: "US" });
    const table = ds.table(tblId);
    const [tblExists] = await table.exists();
    if (!tblExists) await table.create({ schema });

    // raw:true is the high-level form that carries an explicit insertId; the
    // auto-generated insertId of raw:false cannot express a replay.
    const rows = [{ insertId: "idem-1", json: { id: 1, name: "a" } }];
    const first = await table.insert(rows, { raw: true, partialRetries: 0 }).then(() => null, (e) => e);
    if (first) {
      throw new Error(`first insertAll unexpectedly errored: ${first.message}`);
    }
    const replay = await table.insert(rows, { raw: true, partialRetries: 0 }).then(() => null, (e) => e);
    // Real GCP's insertId dedup is best-effort: the replay may be silently
    // accepted (no error) or reported as a duplicate. Either is fine; what must
    // hold is that no second row exists.
    if (replay) {
      const reasons = [];
      for (const ie of (replay.errors || [])) {
        for (const e of (ie.errors || [])) reasons.push(e.reason);
      }
      const unexpected = reasons.filter((r) => r !== "duplicate");
      if (unexpected.length > 0) {
        throw new Error(`replayed insertAll returned unexpected errors ${JSON.stringify(reasons)}`);
      }
    }
    const [data] = await table.getRows();
    if (data.length !== 1) throw new Error(`idempotent insert produced ${data.length} rows, want 1`);
    return { observable: "idempotent_rows=1", detail: "replayed insertId was de-duplicated (one stored row)" };
  });

  // 12. A duplicate topic create surfaces gRPC ALREADY_EXISTS.
  await run("errors.pubsub_topic_already_exists", "OK", async () => {
    const firstErr = await pubsub.createTopic(TOPIC).then(() => null, (e) => e);
    if (firstErr && firstErr.code !== 6) {
      throw new Error(`create topic: ${firstErr.message}`);
    }
    const err = await pubsub.createTopic(TOPIC).then(() => null, (e) => e);
    if (!err) throw new Error("duplicate topic create unexpectedly succeeded");
    if (err.code !== 6) {
      throw new Error(`expected gRPC ALREADY_EXISTS (6), got ${err.code}: ${err.message}`);
    }
    return { observable: "grpc_already_exists=yes", detail: "duplicate topic create surfaced gRPC ALREADY_EXISTS" };
  });
}

module.exports = { errorsScenarios };
