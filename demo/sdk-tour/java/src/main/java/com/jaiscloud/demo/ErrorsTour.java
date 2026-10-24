package com.jaiscloud.demo;

import com.google.api.gax.paging.Page;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.NoCredentials;
import com.google.cloud.Policy;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageRetryStrategy;
import com.google.cloud.WriteChannel;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.Topic;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * jaiscloud-gcp SDK tour — Java "errors" leg (SDK_TOUR_MODE=errors).
 *
 * Ports the twelve failure/retry/idempotency scenarios of the Go reference
 * ({@code demo/sdk-tour/go/errors.go}) onto the official Java clients, emitting
 * the same observable strings through the shared {@link SdkTour} recorder. A
 * green happy-path tour cannot see error-code mapping, retry classification,
 * backoff / Retry-After honoring, idempotency or resumable-upload rewind; this
 * leg asserts those surfaces. Failures are recorded and classified, never
 * papered over.
 */
final class ErrorsTour {

  static final String BUCKET = SdkTour.rid("errors-bucket");
  static final String TOPIC = SdkTour.rid("errors-topic");
  static final String DS = SdkTour.rid("errors_ds").replace("-", "_");
  static final String TBL = SdkTour.rid("errors_tbl").replace("-", "_");

  private ErrorsTour() {}

  static void runErrors() {
    storageErrorScenarios();
    throttleScenarios();
    idempotencyScenarios();
    pubsubErrorScenarios();
  }

  // ─── runtime throttle control (POST /_jaiscloud/throttle) ─────────────────

  static void armThrottle(String body) throws Exception {
    HttpResponse<String> resp = http().send(
        HttpRequest.newBuilder(URI.create(SdkTour.REST + "/_jaiscloud/throttle"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() != 200) {
      throw new IllegalStateException("arm throttle: status " + resp.statusCode() + ": " + resp.body());
    }
  }

  static void clearThrottle() throws Exception {
    armThrottle("{\"mode\":\"off\"}");
  }

  // ─── metrics-backed attempt counting ──────────────────────────────────────

  /**
   * Sums the emulator's {@code jaiscloud_requests_total} counter over the series
   * whose {@code service} label matches {@code want} (empty = every cloud
   * service, excluding the unnamed admin/metrics series). The value counts the
   * requests the emulator received for that surface, so the delta across an
   * operation is its attempt count.
   */
  static int requestsTotal(String want) throws Exception {
    HttpResponse<String> resp = http().send(
        HttpRequest.newBuilder(URI.create(SdkTour.REST + "/metrics")).GET().build(),
        HttpResponse.BodyHandlers.ofString());
    int total = 0;
    for (String raw : resp.body().split("\n")) {
      String line = raw.trim();
      if (!line.startsWith("jaiscloud_requests_total{")) {
        continue;
      }
      int open = line.indexOf('}');
      if (open < 0) {
        continue;
      }
      String labels = line.substring("jaiscloud_requests_total{".length(), open);
      String[] fields = line.substring(open + 1).trim().split("\\s+");
      if (fields.length != 1) {
        continue;
      }
      if (want == null || want.isEmpty()) {
        if (labels.contains("service=\"unknown\"")) {
          continue;
        }
      } else if (!labels.contains("service=\"" + want + "\"")) {
        continue;
      }
      try {
        total += (int) Double.parseDouble(fields[0]);
      } catch (NumberFormatException ignored) {
        // malformed sample
      }
    }
    return total;
  }

  interface AttemptFn {
    void run() throws Exception;
  }

  static int countAttempts(String service, AttemptFn fn) throws Exception {
    int before = requestsTotal(service);
    fn.run();
    int after = requestsTotal(service);
    return after - before;
  }

  // ─── HTTP plumbing ────────────────────────────────────────────────────────

  static HttpClient http() {
    return HttpClient.newHttpClient();
  }

  // ─── storage client ───────────────────────────────────────────────────────

  /**
   * The storage RetrySettings the error leg uses. Retrying RESOURCE_EXHAUSTED
   * (429) and UNAVAILABLE (503) is what the throttle scenarios observe; the
   * uniform {@link StorageRetryStrategy} also lets the resumable chunk put (a
   * non-idempotent-looking RPC) be re-sent after a refused chunk, which the
   * default idempotent-only handler would not do. The Retry-After/RetryInfo
   * shape is still honored by the transport; these delays just bound the wait.
   */
  static Storage storage() {
    RetrySettings retrySettings = RetrySettings.newBuilder()
        .setInitialRetryDelayDuration(Duration.ofMillis(200))
        .setRetryDelayMultiplier(1.5)
        .setMaxRetryDelayDuration(Duration.ofSeconds(2))
        .setTotalTimeoutDuration(Duration.ofSeconds(60))
        .setInitialRpcTimeoutDuration(Duration.ofSeconds(60))
        .setMaxRpcTimeoutDuration(Duration.ofSeconds(60))
        .setMaxAttempts(6)
        .build();
    return StorageOptions.newBuilder()
        .setProjectId(SdkTour.PROJECT)
        .setHost(SdkTour.REST)
        .setCredentials(NoCredentials.getInstance())
        .setRetrySettings(retrySettings)
        .setStorageRetryStrategy(StorageRetryStrategy.getUniformStorageRetryStrategy())
        .build()
        .getService();
  }

  static void ensureBucket(Storage s) {
    if (s.get(BUCKET) == null) {
      s.create(BucketInfo.newBuilder(BUCKET).setLocation("US").build());
    }
  }

  // ─── error classification helpers ─────────────────────────────────────────

  static int httpStatus(Throwable t) {
    if (t instanceof com.google.cloud.BaseServiceException) {
      return ((com.google.cloud.BaseServiceException) t).getCode();
    }
    return 0;
  }

  static void requireStatus(Throwable err, int want) {
    if (err == null) {
      throw new IllegalStateException("expected HTTP " + want + ", got success");
    }
    int got = httpStatus(err);
    if (got != want) {
      throw new IllegalStateException("expected HTTP " + want + ", got " + got + ": " + err);
    }
  }

  // ─── storage error mapping ────────────────────────────────────────────────

  static void storageErrorScenarios() {
    Storage storage = storage();

    SdkTour.run("errors.storage_already_exists", "OK", () -> {
      ensureBucket(storage);
      Throwable err;
      try {
        storage.create(BucketInfo.newBuilder(BUCKET).setLocation("US").build());
        err = null;
      } catch (Throwable t) {
        err = t;
      }
      requireStatus(err, 409);
      return new String[]{"already_exists=yes", "second bucket create surfaced 409 ALREADY_EXISTS"};
    });

    SdkTour.run("errors.storage_not_found", "OK", () -> {
      ensureBucket(storage);
      Blob missing = storage.get(BlobId.of(BUCKET, "no/such/object"));
      if (missing != null) {
        throw new IllegalStateException("missing object get returned a Blob");
      }
      return new String[]{"not_found=yes", "missing object surfaced 404 NOT_FOUND (client maps it to null)"};
    });

    // A 404 must not be retried: the emulator must see exactly one request.
    SdkTour.run("errors.no_retry_on_4xx", "OK", () -> {
      ensureBucket(storage);
      int n = countAttempts("storage", () -> {
        Blob missing = storage.get(BlobId.of(BUCKET, "no/such/object"));
        if (missing != null) {
          throw new IllegalStateException("missing object get returned a Blob");
        }
      });
      if (n != 1) {
        throw new IllegalStateException("4xx must not be retried: emulator saw " + n + " attempts, want 1");
      }
      return new String[]{"no_retry_attempts=1", "404 failed after exactly one attempt"};
    });

    // IAM optimistic concurrency: a stale policy etag must be rejected.
    SdkTour.run("errors.iam_failed_precondition", "OK", () -> {
      ensureBucket(storage);
      Policy policy = storage.getIamPolicy(BUCKET);
      // The wire field is the etag string; a value that differs from the stored
      // policy's etag ("stale-etag" here) is what real GCP and the emulator
      // reject with 409. ByteString.copyFromUtf8("stale-etag") is the same
      // logical value in the Go/gRPC shape.
      Policy stale = policy.toBuilder().setEtag("stale-etag").build();
      Throwable err;
      try {
        storage.setIamPolicy(BUCKET, stale);
        err = null;
      } catch (Throwable t) {
        err = t;
      }
      if (err == null) {
        throw new IllegalStateException("stale IAM etag was accepted (want 409 FAILED_PRECONDITION)");
      }
      requireStatus(err, 409);
      return new String[]{"failed_precondition=yes", "stale IAM etag surfaced 409"};
    });
  }

  // ─── throttled retry scenarios ────────────────────────────────────────────

  static void throttleScenarios() {
    Storage storage = storage();

    for (int[] tc : new int[][]{{429, 0}, {503, 1}}) {
      int status = tc[0];
      String name = status == 429 ? "errors.retry_429_storage_get" : "errors.retry_503_storage_get";
      String obs = status == 429 ? "retry_429_attempts=2" : "retry_503_attempts=2";
      SdkTour.run(name, "OK", () -> {
        ensureBucket(storage);
        armThrottle("{\"mode\":\"fault\",\"failFirst\":1,\"services\":[\"storage\"],\"status\":" + status
            + ",\"retryDelay\":\"1s\"}");
        try {
          int n = countAttempts("storage", () -> {
            if (storage.get(BUCKET) == null) {
              throw new IllegalStateException("get bucket attrs after injected " + status + " returned null");
            }
          });
          if (n != 2) {
            throw new IllegalStateException("client made " + n + " attempts, want 2 (one retry)");
          }
        } finally {
          clearThrottle();
        }
        return new String[]{obs,
            "injected " + status + " refused the first attempt; the client retried once and succeeded"};
      });
    }

    // Retry-info shape: the raw 429 envelope must carry the Retry-After header
    // and a google.rpc.RetryInfo detail, as real GCP does.
    SdkTour.run("errors.retry_info_shape", "OK", () -> {
      ensureBucket(storage);
      armThrottle("{\"mode\":\"fault\",\"failFirst\":1,\"services\":[\"storage\"],\"status\":429,\"retryDelay\":\"1s\"}");
      int code;
      String retryAfter;
      String body;
      try {
        HttpResponse<String> resp = http().send(
            HttpRequest.newBuilder(URI.create(SdkTour.REST + "/storage/v1/b/" + BUCKET)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
        code = resp.statusCode();
        retryAfter = resp.headers().firstValue("Retry-After").orElse("");
        body = resp.body();
      } finally {
        clearThrottle();
      }
      if (code != 429) {
        throw new IllegalStateException("want 429, got " + code + ": " + body);
      }
      String delay;
      try {
        delay = retryInfoDelay(body);
      } catch (IllegalStateException e) {
        throw new IllegalStateException(e.getMessage() + " in " + body);
      }
      if (retryAfter.isEmpty()) {
        throw new IllegalStateException("no Retry-After header");
      }
      return new String[]{"RetryInfo:" + delay + ":Retry-After=" + retryAfter,
          "injected 429 carried Retry-After + google.rpc.RetryInfo"};
    });

    // Pagination stability: page tokens must survive a throttled retry.
    SdkTour.run("errors.pagination_stability", "OK", () -> {
      ensureBucket(storage);
      final int total = 5;
      for (int i = 0; i < total; i++) {
        storage.create(
            BlobInfo.newBuilder(BlobId.of(BUCKET, String.format("errors/page/%02d.txt", i))).build(),
            ("page-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
      }
      armThrottle("{\"mode\":\"fault\",\"failFirst\":1,\"services\":[\"storage\"],\"status\":429,\"retryDelay\":\"1s\"}");
      int seen = 0;
      try {
        Page<Blob> page = storage.list(BUCKET,
            Storage.BlobListOption.prefix("errors/page/"), Storage.BlobListOption.pageSize(2));
        while (page != null) {
          for (Blob ignored : page.getValues()) {
            seen++;
          }
          page = page.hasNextPage() ? page.getNextPage() : null;
        }
      } finally {
        clearThrottle();
      }
      if (seen != total) {
        throw new IllegalStateException("listed " + seen + " objects across pages, want " + total);
      }
      return new String[]{"pagination_total=" + total, "page tokens survived a throttled retry"};
    });

    // Resumable rewind: a refused chunk must be re-sent and still checksum.
    SdkTour.run("errors.resumable_rewind", "OK", () -> {
      ensureBucket(storage);
      // Scope the injected fault to the chunk PU T alone so the resumable
      // session start is unaffected: exactly a mid-upload failure.
      armThrottle("{\"mode\":\"fault\",\"failFirst\":1,\"services\":[\"storage/objectsinsertresumable\"],"
          + "\"status\":429,\"retryDelay\":\"1s\"}");
      String got;
      byte[] payload = SdkTour.resumablePayload();
      try {
        BlobId id = BlobId.of(BUCKET, "errors/rewind.bin");
        BlobInfo info = BlobInfo.newBuilder(id).setContentType("application/octet-stream").build();
        try (WriteChannel ch = storage.writer(info)) {
          ch.setChunkSize(256 * 1024);
          ByteBuffer buf = ByteBuffer.wrap(payload);
          while (buf.hasRemaining()) {
            ch.write(buf);
          }
        }
        Blob attrs = storage.get(id);
        if (attrs == null || attrs.getSize() != payload.length) {
          throw new IllegalStateException("rewound object size " + (attrs == null ? "null" : attrs.getSize())
              + ", want " + payload.length);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (com.google.cloud.ReadChannel rc = storage.reader(id)) {
          ByteBuffer dst = ByteBuffer.allocate(64 * 1024);
          while (rc.read(dst) >= 0) {
            dst.flip();
            out.write(dst.array(), 0, dst.limit());
            dst.clear();
          }
        }
        byte[] body = out.toByteArray();
        got = SdkTour.sha256Hex(payload);
        String sum = SdkTour.sha256Hex(body);
        if (!sum.equals(got)) {
          throw new IllegalStateException("rewound checksum " + sum + ", want " + got);
        }
      } finally {
        clearThrottle();
      }
      return new String[]{"resumable_sha256=" + got,
          "a refused chunk was re-sent and the object checksum matches"};
    });
  }

  /**
   * Finds the {@code google.rpc.RetryInfo} detail in a Google JSON error
   * envelope and returns its {@code retryDelay}. Throws when absent.
   */
  static String retryInfoDelay(String body) {
    JsonObject env = JsonParser.parseString(body).getAsJsonObject();
    JsonObject err = env.has("error") ? env.getAsJsonObject("error") : null;
    if (err != null && err.has("details")) {
      JsonArray details = err.getAsJsonArray("details");
      for (JsonElement d : details) {
        JsonObject dm = d.getAsJsonObject();
        if (dm.has("@type")
            && "type.googleapis.com/google.rpc.RetryInfo".equals(dm.get("@type").getAsString())) {
          if (dm.has("retryDelay") && !dm.get("retryDelay").isJsonNull()) {
            return dm.get("retryDelay").getAsString();
          }
          return "0s";
        }
      }
    }
    throw new IllegalStateException("no google.rpc.RetryInfo detail");
  }

  // ─── idempotency ──────────────────────────────────────────────────────────

  static void idempotencyScenarios() {
    // A malformed request must surface INVALID_ARGUMENT synchronously.
    SdkTour.run("errors.invalid_argument", "OK", () -> {
      BigQuery bq = SdkTour.bigquery();
      Throwable err;
      try {
        bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM").build());
        err = null;
      } catch (BigQueryException e) {
        err = e;
      }
      requireStatus(err, 400);
      return new String[]{"invalid_argument=yes", "malformed SQL surfaced 400 INVALID_ARGUMENT"};
    });

    // insertId is the API's client-supplied idempotency key: replaying a row
    // with the same insertId must not create a second row.
    SdkTour.run("errors.idempotent_insertall", "OK", () -> {
      BigQuery bq = SdkTour.bigquery();
      DatasetId ds = DatasetId.of(SdkTour.PROJECT, DS);
      if (bq.getDataset(ds) == null) {
        bq.create(DatasetInfo.newBuilder(ds).build());
      }
      TableId tbl = TableId.of(SdkTour.PROJECT, DS, TBL);
      Schema schema = Schema.of(
          Field.newBuilder("id", StandardSQLTypeName.INT64).build(),
          Field.newBuilder("name", StandardSQLTypeName.STRING).build());
      if (bq.getTable(tbl) == null) {
        bq.create(TableInfo.of(tbl, StandardTableDefinition.of(schema)));
      }
      List<String> first = insertAll(bq, tbl, "idem-1", 1, "a");
      if (!first.isEmpty()) {
        throw new IllegalStateException("first insertAll unexpectedly errored: " + first);
      }
      List<String> replayed = insertAll(bq, tbl, "idem-1", 1, "a");
      if (!(replayed.size() == 1 && "duplicate".equals(replayed.get(0)))) {
        throw new IllegalStateException("replayed insertAll want one duplicate error, got " + replayed);
      }
      TableResult data = bq.listTableData(tbl);
      int rows = 0;
      for (FieldValueList ignored : data.iterateAll()) {
        rows++;
      }
      if (rows != 1) {
        throw new IllegalStateException("idempotent insert produced " + rows + " rows, want 1");
      }
      return new String[]{"idempotent_rows=1", "replayed insertId was de-duplicated (one stored row)"};
    });
  }

  static List<String> insertAll(BigQuery bq, TableId tbl, String insertId, long id, String name) {
    InsertAllResponse resp = bq.insertAll(InsertAllRequest.newBuilder(tbl)
        .addRow(InsertAllRequest.RowToInsert.of(insertId, Map.of("id", id, "name", name)))
        .build());
    List<String> reasons = new ArrayList<>();
    for (Map.Entry<Long, List<com.google.cloud.bigquery.BigQueryError>> e
        : resp.getInsertErrors().entrySet()) {
      for (com.google.cloud.bigquery.BigQueryError be : e.getValue()) {
        reasons.add(be.getReason());
      }
    }
    return reasons;
  }

  // ─── gRPC error mapping ───────────────────────────────────────────────────

  static void pubsubErrorScenarios() {
    SdkTour.run("errors.pubsub_topic_already_exists", "OK", () -> {
      ProjectTopicName topicName = ProjectTopicName.of(SdkTour.PROJECT, TOPIC);
      TopicAdminSettings settings = TopicAdminSettings.newBuilder()
          .setTransportChannelProvider(SdkTour.plaintext(SdkTour.GRPC))
          .setCredentialsProvider(com.google.api.gax.core.NoCredentialsProvider.create())
          .build();
      try (TopicAdminClient admin = TopicAdminClient.create(settings)) {
        admin.createTopic(Topic.newBuilder().setName(topicName.toString()).build());
        Throwable err;
        try {
          admin.createTopic(Topic.newBuilder().setName(topicName.toString()).build());
          err = null;
        } catch (Throwable t) {
          err = t;
        }
        if (err == null) {
          throw new IllegalStateException("second CreateTopic succeeded (want gRPC ALREADY_EXISTS)");
        }
        if (!isAlreadyExists(err)) {
          throw new IllegalStateException("second CreateTopic want gRPC ALREADY_EXISTS, got: " + err);
        }
      }
      return new String[]{"grpc_already_exists=yes", "duplicate topic create surfaced gRPC ALREADY_EXISTS"};
    });
  }

  static boolean isAlreadyExists(Throwable t) {
    if (t instanceof AlreadyExistsException) {
      return true;
    }
    if (t instanceof io.grpc.StatusRuntimeException) {
      return ((io.grpc.StatusRuntimeException) t).getStatus().getCode()
          == io.grpc.Status.Code.ALREADY_EXISTS;
    }
    return false;
  }
}
