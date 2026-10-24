package com.jaiscloud.demo;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.dataproc.v1.Cluster;
import com.google.cloud.dataproc.v1.ClusterConfig;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import com.google.cloud.dataproc.v1.CreateClusterRequest;
import com.google.cloud.dataproc.v1.DeleteClusterRequest;
import com.google.cloud.dataproc.v1.GceClusterConfig;
import com.google.cloud.dataproc.v1.SoftwareConfig;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.ListenerRegistration;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKeyVersion;
import com.google.cloud.kms.v1.CryptoKeyVersionTemplate;
import com.google.cloud.kms.v1.Digest;
import com.google.cloud.kms.v1.AsymmetricSignRequest;
import com.google.cloud.kms.v1.AsymmetricSignResponse;
import com.google.cloud.kms.v1.DecryptRequest;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.EncryptRequest;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.GetPublicKeyRequest;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyManagementServiceSettings;
import com.google.cloud.kms.v1.KeyRing;
import com.google.cloud.kms.v1.CreateCryptoKeyRequest;
import com.google.cloud.kms.v1.CreateKeyRingRequest;
import com.google.api.MonitoredResource;
import com.google.api.gax.rpc.ApiStreamObserver;
import com.google.cloud.logging.v2.LoggingClient;
import com.google.cloud.logging.v2.LoggingSettings;
import com.google.cloud.secretmanager.v1.AddSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.CreateSecretRequest;
import com.google.cloud.secretmanager.v1.ListSecretVersionsRequest;
import com.google.cloud.secretmanager.v1.Replication;
import com.google.cloud.secretmanager.v1.Secret;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretManagerServiceSettings;
import com.google.cloud.secretmanager.v1.SecretPayload;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.iam.v1.Binding;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.google.cloud.bigquery.JobStatus;
import com.google.logging.v2.TailLogEntriesRequest;
import com.google.logging.v2.TailLogEntriesResponse;
import com.google.logging.v2.ListLogEntriesRequest;
import com.google.logging.v2.LogEntry;
import com.google.logging.v2.WriteLogEntriesRequest;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * jaiscloud-gcp SDK tour — Java leg (official com.google.cloud clients).
 *
 * Runs the same 18 scenarios as the Go/Python/Node legs against the emulator and
 * emits one JSONL record per scenario. Generated gRPC clients are wired with an
 * explicit plaintext channel provider; Storage/BigQuery are REST with a host
 * override; Firestore uses FIRESTORE_EMULATOR_HOST.
 *
 * This is a demo/compliance artifact, not a conformance gate: failures are
 * recorded and classified, never papered over.
 */
public final class SdkTour {

  static final String REST = trimSlash(env("EMULATOR_REST", "http://localhost:8080"));
  static final String GRPC = env("EMULATOR_GRPC", "localhost:8081");
  static final String PROJECT = env("PROJECT_ID", "jaiscloud-project");
  static final String RUN_ID = env("SDK_TOUR_RUN_ID", "local");
  static final String OUT = env("SDK_TOUR_RESULTS", "");
  static final int BATCH_COUNT = 32;

  static PrintWriter out;

  public static void main(String[] args) throws Exception {
    if (!OUT.isEmpty()) {
      out = new PrintWriter(new FileWriter(OUT));
    }

    // SDK_TOUR_MODE=errors runs the failure/retry/idempotency leg only; the
    // happy-path tour is the default.
    String mode = env("SDK_TOUR_MODE", "tour");
    if ("errors".equals(mode)) {
      ErrorsTour.runErrors();
      if (out != null) {
        out.close();
      }
      return;
    }
    if ("streaming".equals(mode)) {
      StreamingTour.runStreaming();
      if (out != null) {
        out.close();
      }
      return;
    }

    storageScenarios();
    pubsubScenarios();
    firestoreScenarios();
    loggingScenarios();
    secretScenarios();
    kmsScenarios();
    bigqueryScenarios();
    dataprocScenarios();

    if (out != null) {
      out.close();
    }
  }

  // ─── helpers ───────────────────────────────────────────────────────────────

  interface Scenario {
    String[] run() throws Exception; // {observable, detail}
  }

  static void run(String scenario, String expect, Scenario fn) {
    long start = System.currentTimeMillis();
    String observable = "";
    String detail = "";
    try {
      String[] r = fn.run();
      observable = r.length > 0 ? r[0] : "";
      detail = r.length > 1 ? r[1] : "";
      record(scenario, "OK", expect, observable, detail, "", "");
    } catch (Throwable t) {
      String cls = classify(t);
      String status = expect.equals("SKIP") ? "SKIP" : "FAIL";
      record(scenario, status, expect, "", (System.currentTimeMillis() - start) + "ms",
          t.getClass().getSimpleName() + ": " + String.valueOf(t.getMessage()), cls);
    }
  }

  static void record(String scenario, String status, String expect, String observable,
                     String detail, String error, String classification) {
    String line = "JAVA " + scenario + " " + status;
    if (!detail.isEmpty()) {
      line += " " + detail;
    }
    if (!error.isEmpty()) {
      line += " err=" + error;
    }
    System.out.println(line);
    if (out != null) {
      out.println("{"
          + "\"lang\":\"java\","
          + "\"scenario\":\"" + esc(scenario) + "\","
          + "\"status\":\"" + esc(status) + "\","
          + "\"expect\":\"" + esc(expect) + "\","
          + "\"observable\":\"" + esc(observable) + "\","
          + "\"detail\":\"" + esc(detail) + "\","
          + "\"error\":\"" + esc(error) + "\","
          + "\"classification\":\"" + esc(classification) + "\"}");
      out.flush();
    }
  }

  static String classify(Throwable t) {
    String msg = String.valueOf(t.getMessage()).toLowerCase();
    for (String n : new String[]{"unimplemented", "not implemented"}) {
      if (msg.contains(n)) {
        return "unimplemented";
      }
    }
    for (String n : new String[]{"unavailable", "connection refused", "ssl", "handshake",
        "getaddrinfo", "unknownhost", "permission denied", "unauthenticated", "failed to connect"}) {
      if (msg.contains(n)) {
        return "wiring-gap";
      }
    }
    if (t instanceof io.grpc.StatusRuntimeException) {
      io.grpc.Status.Code code = ((io.grpc.StatusRuntimeException) t).getStatus().getCode();
      if (code == io.grpc.Status.Code.UNIMPLEMENTED) {
        return "unimplemented";
      }
      if (code == io.grpc.Status.Code.UNAVAILABLE || code == io.grpc.Status.Code.UNAUTHENTICATED
          || code == io.grpc.Status.Code.PERMISSION_DENIED) {
        return "wiring-gap";
      }
    }
    return "emulator-bug";
  }

  static TransportChannelProvider plaintext(String endpoint) {
    return InstantiatingGrpcChannelProvider.newBuilder()
        .setEndpoint(endpoint)
        .setCredentials(NoCredentials.getInstance())
        .setChannelConfigurator(b -> b.usePlaintext())
        .build();
  }

  static String rid(String leaf) {
    return leaf + "-" + RUN_ID;
  }

  static byte[] resumablePayload() {
    byte[] unit = "jaiscloud-sdk-tour-resumable-payload-".getBytes(StandardCharsets.UTF_8);
    int target = 4 << 20;
    ByteArrayOutputStream buf = new ByteArrayOutputStream(target + unit.length);
    while (buf.size() < target) {
      buf.write(unit, 0, unit.length);
    }
    byte[] all = buf.toByteArray();
    byte[] out = new byte[target];
    System.arraycopy(all, 0, out, 0, target);
    return out;
  }

  static String sha256Hex(byte[] data) throws Exception {
    java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
    byte[] h = md.digest(data);
    StringBuilder sb = new StringBuilder();
    for (byte b : h) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  static String host(String hostPort) {
    int i = hostPort.lastIndexOf(':');
    return i < 0 ? hostPort : hostPort.substring(0, i);
  }

  static int port(String hostPort) {
    int i = hostPort.lastIndexOf(':');
    return i < 0 ? 443 : Integer.parseInt(hostPort.substring(i + 1));
  }

  static String env(String key, String def) {
    String v = System.getenv(key);
    return v == null || v.isBlank() ? def : v;
  }

  static String trimSlash(String s) {
    return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }

  static String esc(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("\\", "\\\\").replace("\"", "\\\"")
        .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
  }

  // ─── Storage ───────────────────────────────────────────────────────────────

  static final String BUCKET = rid("sdk-tour-bucket");
  static final String BQ_BUCKET = rid("sdk-tour-bq");

  static Storage storage() {
    return StorageOptions.newBuilder()
        .setProjectId(PROJECT)
        .setHost(REST)
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
  }

  static void storageScenarios() {
    run("storage.create_bucket", "OK", () -> {
      Storage s = storage();
      if (s.get(BUCKET) == null) {
        s.create(BucketInfo.newBuilder(BUCKET).setLocation("US").build());
      }
      return new String[]{"created", "bucket=" + BUCKET};
    });

    run("storage.resumable_upload", "OK", () -> {
      Storage s = storage();
      byte[] payload = resumablePayload();
      BlobId id = BlobId.of(BUCKET, "resumable/payload.bin");
      BlobInfo info = BlobInfo.newBuilder(id).setContentType("application/octet-stream").build();
      try (WriteChannel ch = s.writer(info)) {
        ch.setChunkSize(256 * 1024); // forces chunked resumable upload
        ch.write(ByteBuffer.wrap(payload));
      }
      long size = s.get(id).getSize();
      return new String[]{String.valueOf(size), "size=" + size + " chunkSize=262144 resumable=true"};
    });

    run("storage.stream_download_checksum", "OK", () -> {
      Storage s = storage();
      BlobId id = BlobId.of(BUCKET, "resumable/payload.bin");
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      try (ReadChannel ch = s.reader(id)) {
        ByteBuffer dst = ByteBuffer.allocate(64 * 1024);
        while (ch.read(dst) >= 0) {
          dst.flip();
          buf.write(dst.array(), 0, dst.limit());
          dst.clear();
        }
      }
      byte[] got = buf.toByteArray();
      String sum = sha256Hex(got);
      String want = sha256Hex(resumablePayload());
      if (!sum.equals(want)) {
        throw new IllegalStateException("checksum mismatch: " + sum + " want " + want);
      }
      return new String[]{sum, "sha256=" + sum + " bytes=" + got.length};
    });

    run("storage.list_pagination", "OK", () -> {
      Storage s = storage();
      for (int i = 0; i < 7; i++) {
        s.create(BlobInfo.newBuilder(BlobId.of(BUCKET, String.format("page/%02d.txt", i))).build(),
            ("page-" + i).getBytes(StandardCharsets.UTF_8));
      }
      int pages = 0;
      int total = 0;
      Page<com.google.cloud.storage.Blob> page =
          s.list(BUCKET, Storage.BlobListOption.prefix("page/"), Storage.BlobListOption.pageSize(2));
      while (page != null) {
        pages++;
        for (com.google.cloud.storage.Blob ignored : page.getValues()) {
          total++;
        }
        page = page.hasNextPage() ? page.getNextPage() : null;
      }
      if (total != 7) {
        throw new IllegalStateException("listed " + total + " objects, want 7");
      }
      return new String[]{"pages=" + pages + ",total=" + total,
          "pageSize=2 pages=" + pages + " total=" + total};
    });
  }

  // ─── Pub/Sub ───────────────────────────────────────────────────────────────

  static final String TOPIC = rid("sdk-tour-topic");
  static final String SUB = rid("sdk-tour-sub");

  static ProjectTopicName topicName() {
    return ProjectTopicName.of(PROJECT, TOPIC);
  }

  static void ensureTopic(TopicAdminClient admin) {
    try {
      admin.createTopic(Topic.newBuilder().setName(topicName().toString()).build());
    } catch (com.google.api.gax.rpc.AlreadyExistsException ignored) {
      // idempotent
    }
  }

  static void pubsubScenarios() {
    run("pubsub.batch_publish", "OK", () -> {
      TopicAdminSettings adminSettings = TopicAdminSettings.newBuilder()
          .setTransportChannelProvider(plaintext(GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .build();
      try (TopicAdminClient admin = TopicAdminClient.create(adminSettings)) {
        ensureTopic(admin);
      }
      Publisher publisher = Publisher.newBuilder(topicName())
          .setChannelProvider(plaintext(GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .setBatchingSettings(com.google.api.gax.batching.BatchingSettings.newBuilder()
              .setElementCountThreshold(100L)
              .setDelayThreshold(org.threeten.bp.Duration.ofMillis(50))
              .build())
          .build();
      try {
        List<com.google.api.core.ApiFuture<String>> futures = new ArrayList<>();
        for (int i = 0; i < BATCH_COUNT; i++) {
          futures.add(publisher.publish(PubsubMessage.newBuilder()
              .setData(ByteString.copyFromUtf8(String.format("batch-%02d", i))).build()));
        }
        for (com.google.api.core.ApiFuture<String> f : futures) {
          f.get(30, TimeUnit.SECONDS);
        }
        return new String[]{String.valueOf(futures.size()),
            "published=" + futures.size() + " batching=true"};
      } finally {
        publisher.shutdown();
        publisher.awaitTermination(30, TimeUnit.SECONDS);
      }
    });

    run("pubsub.streaming_pull_ack", "OK", () -> {
      ProjectSubscriptionName subName = ProjectSubscriptionName.of(PROJECT, SUB);
      SubscriptionAdminSettings subSettings = SubscriptionAdminSettings.newBuilder()
          .setTransportChannelProvider(plaintext(GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .build();
      try (TopicAdminClient admin = TopicAdminClient.create(TopicAdminSettings.newBuilder()
          .setTransportChannelProvider(plaintext(GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create()).build())) {
        ensureTopic(admin);
      }
      try (SubscriptionAdminClient subAdmin = SubscriptionAdminClient.create(subSettings)) {
        try {
          subAdmin.createSubscription(Subscription.newBuilder()
              .setName(subName.toString())
              .setTopic(topicName().toString())
              .setAckDeadlineSeconds(10)
              .setPushConfig(PushConfig.getDefaultInstance())
              .build());
        } catch (com.google.api.gax.rpc.AlreadyExistsException ignored) {
          // idempotent
        }
      }
      Publisher publisher = Publisher.newBuilder(topicName())
          .setChannelProvider(plaintext(GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .build();
      try {
        List<com.google.api.core.ApiFuture<String>> futures = new ArrayList<>();
        for (int i = 0; i < BATCH_COUNT; i++) {
          futures.add(publisher.publish(PubsubMessage.newBuilder()
              .setData(ByteString.copyFromUtf8(String.format("stream-%02d", i))).build()));
        }
        for (com.google.api.core.ApiFuture<String> f : futures) {
          f.get(30, TimeUnit.SECONDS);
        }
      } finally {
        publisher.shutdown();
        publisher.awaitTermination(30, TimeUnit.SECONDS);
      }

      AtomicInteger received = new AtomicInteger();
      CountDownLatch done = new CountDownLatch(1);
      MessageReceiver receiver = (message, consumer) -> {
        consumer.ack();
        if (received.incrementAndGet() >= BATCH_COUNT) {
          done.countDown();
        }
      };
      Subscriber subscriber = Subscriber.newBuilder(subName, receiver)
          .setChannelProvider(plaintext(GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .build();
      subscriber.startAsync().awaitRunning();
      try {
        if (!done.await(30, TimeUnit.SECONDS)) {
          throw new IllegalStateException("streaming pull received " + received.get() + "/" + BATCH_COUNT);
        }
      } finally {
        subscriber.stopAsync().awaitTerminated(30, TimeUnit.SECONDS);
      }
      int n = received.get();
      return new String[]{String.valueOf(n), "received=" + n + " acked=" + n + " streaming=true"};
    });

    run("iam.policy_read_modify_write", "OK", () -> {
      try (TopicAdminClient admin = TopicAdminClient.create(TopicAdminSettings.newBuilder()
          .setTransportChannelProvider(plaintext(GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create()).build())) {
        ensureTopic(admin);
        String topic = topicName().toString();
        String role = "roles/pubsub.publisher";
        String member = "allUsers";
        Policy policy = admin.getIamPolicy(GetIamPolicyRequest.newBuilder().setResource(topic).build());
        Policy updated = policy.toBuilder().addBindings(
            Binding.newBuilder().setRole(role).addMembers(member).build()).build();
        admin.setIamPolicy(SetIamPolicyRequest.newBuilder().setResource(topic).setPolicy(updated).build());
        Policy again = admin.getIamPolicy(GetIamPolicyRequest.newBuilder().setResource(topic).build());
        boolean found = again.getBindingsList().stream()
            .anyMatch(b -> b.getRole().equals(role) && b.getMembersList().contains(member));
        if (!found) {
          throw new IllegalStateException("role " + role + " not present after set");
        }
        return new String[]{"true", "role=" + role + " member=" + member};
      }
    });
  }

  // ─── Firestore ─────────────────────────────────────────────────────────────

  static Firestore firestore() {
    // The Java Firestore client honours FIRESTORE_EMULATOR_HOST (plaintext + no
    // credentials); the env var is set by run.sh / the launcher.
    return FirestoreOptions.getDefaultInstance().toBuilder()
        .setProjectId(PROJECT)
        .setHost(GRPC)
        .setChannelProvider(plaintext(GRPC))
        .setCredentialsProvider(NoCredentialsProvider.create())
        .build()
        .getService();
  }

  static void firestoreScenarios() {
    run("firestore.listen_write", "OK", () -> {
      Firestore db = firestore();
      CountDownLatch latch = new CountDownLatch(1);
      ListenerRegistration reg = db.collection(rid("fs-listen"))
          .addSnapshotListener((snapshot, e) -> {
            if (e != null) {
              return;
            }
            if (snapshot != null && snapshot.getDocuments().stream()
                .anyMatch(d -> d.getId().equals("live-doc"))) {
              latch.countDown();
            }
          });
      try {
        db.collection(rid("fs-listen")).document("live-doc").set(java.util.Map.of("v", 1)).get();
        if (!latch.await(20, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Listen did not deliver live-doc");
        }
      } finally {
        reg.remove();
      }
      return new String[]{"true", "Listen delivered ADD for live-doc"};
    });

    run("firestore.transaction", "OK", () -> {
      Firestore db = firestore();
      DocumentReference ref = db.collection(rid("fs-tx")).document("counter");
      ref.set(java.util.Map.of("n", 0)).get();
      for (int i = 0; i < 3; i++) {
        db.runTransaction(transaction -> {
          DocumentSnapshot snap = transaction.get(ref).get();
          Long n = snap.getLong("n");
          transaction.set(ref, java.util.Map.of("n", (n == null ? 0 : n) + 1));
          return null;
        }).get();
      }
      Long n = ref.get().get().getLong("n");
      if (n == null || n != 3) {
        throw new IllegalStateException("counter = " + n + ", want 3");
      }
      return new String[]{String.valueOf(n), "3 read-modify-write transactions"};
    });

    run("firestore.query_pagination", "OK", () -> {
      Firestore db = firestore();
      com.google.cloud.firestore.CollectionReference col = db.collection(rid("fs-page"));
      for (int i = 0; i < 5; i++) {
        col.document("d" + i).set(java.util.Map.of("i", i)).get();
      }
      Object cursor = null;
      int pages = 0;
      int total = 0;
      while (true) {
        Query q = col.orderBy("i").limit(2);
        if (cursor != null) {
          q = q.startAfter(cursor);
        }
        QuerySnapshot snap = q.get().get();
        if (snap.isEmpty()) {
          break;
        }
        cursor = snap.getDocuments().get(snap.getDocuments().size() - 1).get("i");
        pages++;
        total += snap.getDocuments().size();
      }
      if (total != 5) {
        throw new IllegalStateException("paged " + total + " docs, want 5");
      }
      return new String[]{"pages=" + pages + ",total=" + total,
          "limit=2 pages=" + pages + " total=" + total};
    });
  }

  // ─── Logging ───────────────────────────────────────────────────────────────

  static LoggingClient loggingClient() throws Exception {
    LoggingSettings settings = LoggingSettings.newBuilder()
        .setTransportChannelProvider(plaintext(GRPC))
        .setCredentialsProvider(NoCredentialsProvider.create())
        .build();
    return LoggingClient.create(settings);
  }

  static com.google.logging.v2.LogEntry logEntry(String logName, String text) {
    return com.google.logging.v2.LogEntry.newBuilder()
        .setLogName(logName)
        .setSeverity(com.google.logging.type.LogSeverity.INFO)
        .setTextPayload(text)
        .build();
  }

  static WriteLogEntriesRequest writeRequest(String logName, String text) {
    return WriteLogEntriesRequest.newBuilder()
        .setLogName(logName)
        .setResource(MonitoredResource.newBuilder().setType("global")
            .putLabels("project_id", PROJECT).build())
        .addEntries(logEntry(logName, text))
        .build();
  }

  static void loggingScenarios() {
    String parent = "projects/" + PROJECT;
    String logName = parent + "/logs/" + rid("sdk-tour-log");

    run("logging.write_entry", "OK", () -> {
      try (LoggingClient logging = loggingClient()) {
        logging.writeLogEntries(writeRequest(logName, "sdk-tour-entry"));
        int n = 0;
        for (LogEntry e : logging.listLogEntries(ListLogEntriesRequest.newBuilder()
            .addResourceNames(parent).setFilter("logName=\"" + logName + "\"").build()).iterateAll()) {
          n++;
        }
        if (n != 1) {
          throw new IllegalStateException("ListLogEntries returned " + n + " entries, want 1");
        }
        return new String[]{String.valueOf(n), "WriteLogEntries + ListLogEntries round-trip"};
      }
    });

    run("logging.tail", "OK", () -> {
      try (LoggingClient logging = loggingClient()) {
        CountDownLatch latch = new CountDownLatch(1);
        ApiStreamObserver<TailLogEntriesResponse> respObserver = new ApiStreamObserver<>() {
          @Override public void onNext(TailLogEntriesResponse resp) {
            for (LogEntry e : resp.getEntriesList()) {
              if (e.getTextPayload().startsWith("sdk-tour-tail")) {
                latch.countDown();
              }
            }
          }

          @Override public void onError(Throwable t) { }

          @Override public void onCompleted() { }
        };
        ApiStreamObserver<TailLogEntriesRequest> reqObserver =
            logging.tailLogEntriesCallable().bidiStreamingCall(respObserver);
        reqObserver.onNext(TailLogEntriesRequest.newBuilder()
            .addResourceNames(parent)
            .setFilter("logName=\"" + logName + "\"")
            .setBufferWindow(Duration.newBuilder().setNanos(200_000_000).build())
            .build());

        // The emulator seeds its cursor when it reads the opening request, so
        // write AFTER the stream is open; retry on a timer.
        Thread writer = new Thread(() -> {
          try {
            Thread.sleep(400);
            for (int i = 0; i < 40; i++) {
              logging.writeLogEntries(writeRequest(logName, "sdk-tour-tail-" + i));
              Thread.sleep(500);
            }
          } catch (Exception ignored) {
            // best effort
          }
        });
        writer.setDaemon(true);
        writer.start();
        if (!latch.await(30, TimeUnit.SECONDS)) {
          throw new IllegalStateException("TailLogEntries did not deliver the entry");
        }
        reqObserver.onCompleted();
        return new String[]{"true", "TailLogEntries delivered the entry"};
      }
    });
  }

  // ─── Secret Manager ────────────────────────────────────────────────────────

  static void secretScenarios() {
    run("secretmanager.add_access_list", "OK", () -> {
      SecretManagerServiceSettings settings = SecretManagerServiceSettings.newBuilder()
          .setTransportChannelProvider(plaintext(GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .build();
      try (SecretManagerServiceClient client = SecretManagerServiceClient.create(settings)) {
        String parent = "projects/" + PROJECT;
        String secretId = rid("sdk-tour-secret");
        Secret secret = client.createSecret(CreateSecretRequest.newBuilder()
            .setParent(parent).setSecretId(secretId)
            .setSecret(Secret.newBuilder().setReplication(Replication.newBuilder()
                .setAutomatic(Replication.Automatic.newBuilder().build()).build()).build())
            .build());
        client.addSecretVersion(AddSecretVersionRequest.newBuilder()
            .setParent(secret.getName())
            .setPayload(SecretPayload.newBuilder().setData(ByteString.copyFromUtf8("sdk-tour")).build())
            .build());
        AccessSecretVersionResponse acc = client.accessSecretVersion(AccessSecretVersionRequest.newBuilder()
            .setName(secret.getName() + "/versions/1").build());
        if (!acc.getPayload().getData().toStringUtf8().equals("sdk-tour")) {
          throw new IllegalStateException("accessed payload " + acc.getPayload().getData().toStringUtf8());
        }
        int versions = 0;
        for (com.google.cloud.secretmanager.v1.SecretVersion v : client.listSecretVersions(
            ListSecretVersionsRequest.newBuilder().setParent(secret.getName()).build()).iterateAll()) {
          versions++;
        }
        if (versions < 1) {
          throw new IllegalStateException("ListSecretVersions returned " + versions);
        }
        return new String[]{String.valueOf(versions), "access=ok versions=" + versions};
      }
    });
  }

  // ─── KMS ───────────────────────────────────────────────────────────────────

  static void kmsScenarios() {
    String parent = "projects/" + PROJECT + "/locations/global";

    run("kms.encrypt_decrypt", "OK", () -> {
      KeyManagementServiceSettings settings = KeyManagementServiceSettings.newBuilder()
          .setTransportChannelProvider(plaintext(GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .build();
      try (KeyManagementServiceClient client = KeyManagementServiceClient.create(settings)) {
        KeyRing ring = client.createKeyRing(CreateKeyRingRequest.newBuilder()
            .setParent(parent).setKeyRingId(rid("sdk-tour-ring"))
            .setKeyRing(KeyRing.newBuilder().build()).build());
        CryptoKey key = client.createCryptoKey(CreateCryptoKeyRequest.newBuilder()
            .setParent(ring.getName()).setCryptoKeyId(rid("sdk-tour-key"))
            .setCryptoKey(CryptoKey.newBuilder()
                .setPurpose(CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT).build())
            .build());
        EncryptResponse enc = client.encrypt(EncryptRequest.newBuilder()
            .setName(key.getName())
            .setPlaintext(ByteString.copyFromUtf8("sdk-tour")).build());
        DecryptResponse dec = client.decrypt(DecryptRequest.newBuilder()
            .setName(key.getName()).setCiphertext(enc.getCiphertext()).build());
        if (!dec.getPlaintext().toStringUtf8().equals("sdk-tour")) {
          throw new IllegalStateException("decrypted " + dec.getPlaintext().toStringUtf8());
        }
        return new String[]{"true", "encrypt/decrypt round-trip"};
      }
    });

    run("kms.asymmetric_sign", "OK", () -> {
      KeyManagementServiceSettings settings = KeyManagementServiceSettings.newBuilder()
          .setTransportChannelProvider(plaintext(GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .build();
      try (KeyManagementServiceClient client = KeyManagementServiceClient.create(settings)) {
        KeyRing ring = client.createKeyRing(CreateKeyRingRequest.newBuilder()
            .setParent(parent).setKeyRingId(rid("sdk-tour-sign-ring"))
            .setKeyRing(KeyRing.newBuilder().build()).build());
        CryptoKey key = client.createCryptoKey(CreateCryptoKeyRequest.newBuilder()
            .setParent(ring.getName()).setCryptoKeyId(rid("sdk-tour-sign-key"))
            .setCryptoKey(CryptoKey.newBuilder()
                .setPurpose(CryptoKey.CryptoKeyPurpose.ASYMMETRIC_SIGN)
                .setVersionTemplate(CryptoKeyVersionTemplate.newBuilder()
                    .setAlgorithm(CryptoKeyVersion.CryptoKeyVersionAlgorithm.RSA_SIGN_PKCS1_2048_SHA256))
                .build())
            .build());
        String version = key.getName() + "/cryptoKeyVersions/1";
        byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
            .digest("sdk-tour-sign".getBytes(StandardCharsets.UTF_8));
        AsymmetricSignResponse sig = client.asymmetricSign(AsymmetricSignRequest.newBuilder()
            .setName(version)
            .setDigest(Digest.newBuilder().setSha256(ByteString.copyFrom(digest)).build())
            .build());
        com.google.cloud.kms.v1.PublicKey kmsPk = client.getPublicKey(
            GetPublicKeyRequest.newBuilder().setName(version).build());
        String b64 = kmsPk.getPem().replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", "");
        PublicKey pub = KeyFactory.getInstance("RSA")
            .generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(b64)));
        Signature verifier = Signature.getInstance("SHA256withRSA");
        verifier.initVerify(pub);
        verifier.update("sdk-tour-sign".getBytes(StandardCharsets.UTF_8));
        if (!verifier.verify(sig.getSignature().toByteArray())) {
          throw new IllegalStateException("signature did not verify with GetPublicKey");
        }
        return new String[]{"true", "AsymmetricSign verified with GetPublicKey"};
      }
    });
  }

  // ─── BigQuery ──────────────────────────────────────────────────────────────

  static BigQuery bigquery() {
    return BigQueryOptions.newBuilder()
        .setProjectId(PROJECT)
        .setHost(REST)
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
  }

  static void bigqueryScenarios() {
    String dsId = rid("sdk_tour_ds").replace("-", "_");
    String tblId = rid("sdk_tour_tbl").replace("-", "_");
    String loadTblId = rid("sdk_tour_load").replace("-", "_");
    Schema schema = Schema.of(
        Field.newBuilder("id", StandardSQLTypeName.INT64).build(),
        Field.newBuilder("name", StandardSQLTypeName.STRING).build());

    run("bigquery.insertall_query", "OK", () -> {
      BigQuery bq = bigquery();
      DatasetId ds = DatasetId.of(PROJECT, dsId);
      if (bq.getDataset(dsId) == null) {
        bq.create(DatasetInfo.newBuilder(ds).build());
      }
      TableId tbl = TableId.of(PROJECT, dsId, tblId);
      if (bq.getTable(tbl) == null) {
        bq.create(com.google.cloud.bigquery.TableInfo.of(tbl, StandardTableDefinition.of(schema)));
      }
      InsertAllResponse resp = bq.insertAll(InsertAllRequest.newBuilder(tbl)
          .addRow(java.util.Map.of("id", 1, "name", "alice"))
          .addRow(java.util.Map.of("id", 2, "name", "bob"))
          .build());
      if (resp.hasErrors()) {
        throw new IllegalStateException("insertAll errors: " + resp.getInsertErrors());
      }
      TableResult result = bq.query(QueryJobConfiguration.newBuilder(
          String.format("SELECT id, name FROM `%s.%s.%s` ORDER BY id", PROJECT, dsId, tblId)).build());
      int n = result.getValues().iterator().hasNext() ? (int) result.getTotalRows() : 0;
      if (n != 2) {
        throw new IllegalStateException("query returned " + n + " rows, want 2");
      }
      return new String[]{String.valueOf(n), "insertAll + query returned " + n + " rows"};
    });

    run("bigquery.load_job", "OK", () -> {
      Storage s = storage();
      if (s.get(BQ_BUCKET) == null) {
        s.create(BucketInfo.newBuilder(BQ_BUCKET).setLocation("US").build());
      }
      s.create(BlobInfo.newBuilder(BlobId.of(BQ_BUCKET, "rows.json"))
              .setContentType("application/x-ndjson").build(),
          "{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n".getBytes(StandardCharsets.UTF_8));

      BigQuery bq = bigquery();
      DatasetId ds = DatasetId.of(PROJECT, dsId);
      if (bq.getDataset(dsId) == null) {
        bq.create(DatasetInfo.newBuilder(ds).build());
      }
      TableId tbl = TableId.of(PROJECT, dsId, loadTblId);
      if (bq.getTable(tbl) == null) {
        bq.create(com.google.cloud.bigquery.TableInfo.of(tbl, StandardTableDefinition.of(schema)));
      }
      LoadJobConfiguration cfg = LoadJobConfiguration.newBuilder(tbl,
              String.format("gs://%s/rows.json", BQ_BUCKET))
          .setFormatOptions(FormatOptions.json())
          .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
          .build();
      Job job = bq.create(JobInfo.newBuilder(cfg)
          .setJobId(JobId.of(PROJECT, rid("sdk_tour_loadjob").replace("-", "_"))).build());
      Job done = job.waitFor();
      if (done == null || done.getStatus().getState() != JobStatus.State.DONE) {
        throw new IllegalStateException("load job did not reach DONE");
      }
      TableResult result = bq.query(QueryJobConfiguration.newBuilder(
          String.format("SELECT id, name FROM `%s.%s.%s` ORDER BY id", PROJECT, dsId, loadTblId)).build());
      int n = (int) result.getTotalRows();
      if (n != 2) {
        throw new IllegalStateException("load job produced " + n + " rows, want 2");
      }
      return new String[]{String.valueOf(n), "gs:// load job produced " + n + " rows"};
    });
  }

  // ─── Dataproc LRO ──────────────────────────────────────────────────────────

  static void dataprocScenarios() {
    run("lro.dataproc_cluster", "OK", () -> {
      ClusterControllerSettings settings = ClusterControllerSettings.newBuilder()
          .setTransportChannelProvider(plaintext(GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .build();
      try (ClusterControllerClient client = ClusterControllerClient.create(settings)) {
        String region = "us-central1";
        String name = rid("sdk-tour-cluster");
        Cluster cluster = Cluster.newBuilder()
            .setProjectId(PROJECT).setClusterName(name)
            .setConfig(ClusterConfig.newBuilder()
                .setGceClusterConfig(GceClusterConfig.newBuilder().setZoneUri("us-central1-a").build())
                .setSoftwareConfig(SoftwareConfig.newBuilder().setImageVersion("2.2").build())
                .build())
            .build();
        // The SDK's gax OperationFuture.get() polls operations.get to completion.
        Cluster created = client.createClusterAsync(CreateClusterRequest.newBuilder()
            .setProjectId(PROJECT).setRegion(region).setCluster(cluster).build()).get(60, TimeUnit.SECONDS);
        String state = created.getStatus().getState().name();
        if (!state.equals("RUNNING")) {
          throw new IllegalStateException("cluster state = " + state + ", want RUNNING");
        }
        try {
          client.deleteClusterAsync(DeleteClusterRequest.newBuilder()
              .setProjectId(PROJECT).setRegion(region).setClusterName(name).build())
              .get(60, TimeUnit.SECONDS);
        } catch (Exception ignored) {
          // best-effort teardown
        }
        return new String[]{state, "OperationFuture.get() settled the LRO to " + state};
      }
    });
  }
}
