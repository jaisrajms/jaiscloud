package com.jaiscloud.demo;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiStreamObserver;
import com.google.cloud.NoCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.cloud.logging.v2.LoggingClient;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.GrpcStorageOptions;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.firestore.v1.ListenRequest;
import com.google.firestore.v1.ListenResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.Target;
import com.google.firestore.v1.TargetChange;
import com.google.logging.v2.LogEntry;
import com.google.logging.v2.TailLogEntriesRequest;
import com.google.logging.v2.TailLogEntriesResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.StreamingPullRequest;
import com.google.pubsub.v1.StreamingPullResponse;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * jaiscloud-gcp SDK tour — Java "streaming" leg (SDK_TOUR_MODE=streaming).
 *
 * Ports the nine streaming-semantics scenarios of the Go reference
 * ({@code demo/sdk-tour/go/streaming.go}) onto the official Java clients,
 * emitting the same observable strings through the shared {@link SdkTour}
 * recorder. A happy-path tour proves a stream can carry one message; this leg
 * asserts the stream's *semantics*: ack-deadline redelivery and extension,
 * ordering keys, exactly-once ack, subscriber flow control, Firestore Listen
 * resume tokens, Logging tail reconnects, and Storage BidiReadObject.
 *
 * Where the high-level client hides the semantics being asserted, the generated
 * low-level client is used (the gax SubscriberStub's StreamingPull for precise
 * ack control, the generated Firestore Listen, the generated Logging tail).
 * The high-level Publisher/Subscriber is used for ordering keys because its
 * delivery behavior is itself part of what is under test. Failures are recorded
 * and classified, never papered over.
 */
final class StreamingTour {

  // Protocol minimum, chosen so an ignored redelivery is observable quickly.
  private static final int STREAM_ACK_DEADLINE = 10;
  // Ack deadline + a margin; redelivery is a property of the emulator's own
  // clock, so a small margin is not a timing race.
  private static final long REDELIVER_WAIT_MS = 13_000;

  private StreamingTour() {}

  static void runStreaming() {
    pubsubScenarios();
    firestoreScenarios();
    loggingScenarios();
    storageScenarios();
  }

  // ─── Pub/Sub ─────────────────────────────────────────────────────────────────

  private static String subResource(String subId) {
    return ProjectSubscriptionName.of(SdkTour.PROJECT, subId).toString();
  }

  /**
   * Creates the topic and subscription idempotently (AlreadyExists is the
   * normal outcome on a second run against the same emulator).
   */
  private static void ensurePubsub(String topicId, String subId, int ackDeadlineSeconds,
                                   boolean ordering, boolean exactlyOnce) throws Exception {
    try (TopicAdminClient topicAdmin = TopicAdminClient.create(TopicAdminSettings.newBuilder()
        .setTransportChannelProvider(SdkTour.plaintext(SdkTour.GRPC))
        .setCredentialsProvider(NoCredentialsProvider.create())
        .build())) {
      try {
        topicAdmin.createTopic(Topic.newBuilder()
            .setName(ProjectTopicName.of(SdkTour.PROJECT, topicId).toString())
            .build());
      } catch (AlreadyExistsException ignored) {
        // idempotent
      }
    }
    try (SubscriptionAdminClient subAdmin = SubscriptionAdminClient.create(
        SubscriptionAdminSettings.newBuilder()
            .setTransportChannelProvider(SdkTour.plaintext(SdkTour.GRPC))
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build())) {
      try {
        subAdmin.createSubscription(Subscription.newBuilder()
            .setName(subResource(subId))
            .setTopic(ProjectTopicName.of(SdkTour.PROJECT, topicId).toString())
            .setAckDeadlineSeconds(ackDeadlineSeconds)
            .setPushConfig(PushConfig.getDefaultInstance())
            .setEnableMessageOrdering(ordering)
            .setEnableExactlyOnceDelivery(exactlyOnce)
            .build());
      } catch (AlreadyExistsException ignored) {
        // idempotent
      }
    }
  }

  private static void publishOne(String topicId, String body, boolean ordering) throws Exception {
    Publisher publisher = Publisher.newBuilder(ProjectTopicName.of(SdkTour.PROJECT, topicId))
        .setChannelProvider(SdkTour.plaintext(SdkTour.GRPC))
        .setCredentialsProvider(NoCredentialsProvider.create())
        .setEnableMessageOrdering(ordering)
        .build();
    try {
      publisher.publish(PubsubMessage.newBuilder()
          .setData(ByteString.copyFromUtf8(body)).build()).get(30, TimeUnit.SECONDS);
    } finally {
      publisher.shutdown();
      publisher.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  private static SubscriberStub subscriberStub() throws Exception {
    return GrpcSubscriberStub.create(SubscriberStubSettings.newBuilder()
        .setTransportChannelProvider(SdkTour.plaintext(SdkTour.GRPC))
        .setCredentialsProvider(NoCredentialsProvider.create())
        .build());
  }

  private static PullResponse pull(SubscriberStub stub, String subName) {
    return stub.pullCallable().call(PullRequest.newBuilder()
        .setSubscription(subName)
        .setMaxMessages(10)
        .setReturnImmediately(true)
        .build());
  }

  private static void ack(SubscriberStub stub, String subName, String ackId) {
    stub.acknowledgeCallable().call(AcknowledgeRequest.newBuilder()
        .setSubscription(subName).addAckIds(ackId).build());
  }

  private static void modifyAckDeadline(SubscriberStub stub, String subName, String ackId, int seconds) {
    stub.modifyAckDeadlineCallable().call(ModifyAckDeadlineRequest.newBuilder()
        .setSubscription(subName).addAckIds(ackId).setAckDeadlineSeconds(seconds).build());
  }

  /**
   * A single generated StreamingPull stream. Responses are handed to a queue
   * from the gax callback thread; {@link #pull} blocks until a message with the
   * wanted body arrives and returns its ack id. Closing half-closes the stream
   * (onCompleted) so the caller controls redelivery by an unacked close.
   */
  private static final class StreamPull implements AutoCloseable {
    private static final StreamingPullResponse SENTINEL = StreamingPullResponse.getDefaultInstance();

    private final ApiStreamObserver<StreamingPullRequest> requests;
    private final BlockingQueue<StreamingPullResponse> responses = new LinkedBlockingQueue<>();
    private volatile Throwable error;

    StreamPull(SubscriberStub stub, String subName, int ackDeadlineSeconds) {
      this.requests = stub.streamingPullCallable().bidiStreamingCall(new ApiStreamObserver<>() {
        @Override public void onNext(StreamingPullResponse value) {
          responses.offer(value);
        }

        @Override public void onError(Throwable t) {
          error = t;
          responses.offer(SENTINEL);
        }

        @Override public void onCompleted() {
          responses.offer(SENTINEL);
        }
      });
      requests.onNext(StreamingPullRequest.newBuilder()
          .setSubscription(subName)
          .setStreamAckDeadlineSeconds(ackDeadlineSeconds)
          .build());
    }

    String pull(String want, long timeoutMs) throws Exception {
      long deadline = System.currentTimeMillis() + timeoutMs;
      while (true) {
        long remain = deadline - System.currentTimeMillis();
        if (remain <= 0) {
          throw new IllegalStateException("timed out waiting for " + want + " on StreamingPull");
        }
        StreamingPullResponse resp = responses.poll(remain, TimeUnit.MILLISECONDS);
        if (resp == SENTINEL) {
          throw new IllegalStateException("StreamingPull ended: " + error);
        }
        if (resp == null) {
          throw new IllegalStateException("timed out waiting for " + want + " on StreamingPull");
        }
        for (ReceivedMessage rm : resp.getReceivedMessagesList()) {
          if (rm.getMessage().getData().toStringUtf8().equals(want)) {
            return rm.getAckId();
          }
        }
      }
    }

    @Override public void close() {
      try {
        requests.onCompleted();
      } catch (Throwable ignored) {
        // best effort
      }
    }
  }

  private static void pubsubScenarios() {
    String topicId = SdkTour.rid("stream-topic");

    SdkTour.run("streaming.pubsub_ack_deadline", "OK", () -> {
      String subId = SdkTour.rid("stream-ackdl-sub");
      ensurePubsub(topicId, subId, STREAM_ACK_DEADLINE, false, false);
      publishOne(topicId, "ackdl", false);
      String name = subResource(subId);
      try (SubscriberStub stub = subscriberStub()) {
        // First delivery: receive and deliberately do not ack or extend.
        try (StreamPull first = new StreamPull(stub, name, STREAM_ACK_DEADLINE)) {
          first.pull("ackdl", 20_000);
        }
        Thread.sleep(REDELIVER_WAIT_MS);

        // After the ack deadline the message must be redelivered.
        String ackId;
        try (StreamPull second = new StreamPull(stub, name, STREAM_ACK_DEADLINE)) {
          ackId = second.pull("ackdl", 20_000);
        }

        // Ack it; an acked message must not be delivered again.
        ack(stub, name, ackId);
        PullResponse after = pull(stub, name);
        for (ReceivedMessage rm : after.getReceivedMessagesList()) {
          if (rm.getMessage().getData().toStringUtf8().equals("ackdl")) {
            throw new IllegalStateException("message redelivered after ack");
          }
        }
      }
      return new String[]{"redelivered=yes",
          "unacked message redelivered after the 10s ack deadline; ack stopped it"};
    });

    SdkTour.run("streaming.pubsub_ack_extension", "OK", () -> {
      String subId = SdkTour.rid("stream-ackext-sub");
      ensurePubsub(topicId, subId, STREAM_ACK_DEADLINE, false, false);
      publishOne(topicId, "ackext", false);
      String name = subResource(subId);
      try (SubscriberStub stub = subscriberStub()) {
        String ackId;
        try (StreamPull first = new StreamPull(stub, name, STREAM_ACK_DEADLINE)) {
          ackId = first.pull("ackext", 20_000);
          // Extend well past the original 10s while the stream is still open.
          modifyAckDeadline(stub, name, ackId, 600);
        }
        Thread.sleep(REDELIVER_WAIT_MS);

        PullResponse after = pull(stub, name);
        for (ReceivedMessage rm : after.getReceivedMessagesList()) {
          if (rm.getMessage().getData().toStringUtf8().equals("ackext")) {
            throw new IllegalStateException("message redelivered despite ModifyAckDeadline(600s)");
          }
        }
        // Clean up: nack so the subscription is not left with an invisible
        // message for the emulator's lifetime.
        modifyAckDeadline(stub, name, ackId, 0);
      }
      return new String[]{"no_redelivery_while_extended=yes",
          "ModifyAckDeadline(600s) kept the message invisible past its original 10s deadline"};
    });

    SdkTour.run("streaming.pubsub_ordering_keys", "OK", () -> {
      String subId = SdkTour.rid("stream-order-sub");
      ensurePubsub(topicId, subId, STREAM_ACK_DEADLINE, true, false);

      Publisher publisher = Publisher.newBuilder(ProjectTopicName.of(SdkTour.PROJECT, topicId))
          .setChannelProvider(SdkTour.plaintext(SdkTour.GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .setEnableMessageOrdering(true)
          .build();
      final int n = 10;
      try {
        for (int i = 0; i < n; i++) {
          publisher.publish(PubsubMessage.newBuilder()
              .setData(ByteString.copyFromUtf8(String.format("order-%02d", i)))
              .setOrderingKey("stream-order-key")
              .build()).get(30, TimeUnit.SECONDS);
        }
      } finally {
        publisher.shutdown();
        publisher.awaitTermination(30, TimeUnit.SECONDS);
      }

      List<String> got = Collections.synchronizedList(new ArrayList<>());
      CountDownLatch done = new CountDownLatch(1);
      MessageReceiver receiver = (message, consumer) -> {
        got.add(message.getData().toStringUtf8());
        consumer.ack();
        if (got.size() >= n) {
          done.countDown();
        }
      };
      Subscriber subscriber = Subscriber.newBuilder(
              ProjectSubscriptionName.of(SdkTour.PROJECT, subId), receiver)
          .setChannelProvider(SdkTour.plaintext(SdkTour.GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .build();
      subscriber.startAsync().awaitRunning();
      try {
        if (!done.await(30, TimeUnit.SECONDS)) {
          throw new IllegalStateException("ordering receive timed out (got " + got.size() + "/" + n + ")");
        }
      } finally {
        subscriber.stopAsync().awaitTerminated(30, TimeUnit.SECONDS);
      }
      List<String> snapshot = new ArrayList<>(got);
      if (snapshot.size() != n) {
        throw new IllegalStateException("received " + snapshot.size() + " messages, want " + n);
      }
      for (int i = 0; i < n; i++) {
        String want = String.format("order-%02d", i);
        if (!snapshot.get(i).equals(want)) {
          throw new IllegalStateException("out of order at " + i + ": got " + snapshot.get(i)
              + " want " + want + " (all=" + snapshot + ")");
        }
      }
      return new String[]{"ordered=yes", "ordering key preserved order across " + n + " messages"};
    });

    SdkTour.run("streaming.pubsub_exactly_once_ack", "OK", () -> {
      String subId = SdkTour.rid("stream-eod-sub");
      ensurePubsub(topicId, subId, STREAM_ACK_DEADLINE, false, true);
      publishOne(topicId, "eod", false);
      String name = subResource(subId);
      try (SubscriberStub stub = subscriberStub()) {
        String ackId;
        try (StreamPull first = new StreamPull(stub, name, STREAM_ACK_DEADLINE)) {
          ackId = first.pull("eod", 20_000);
        }
        ack(stub, name, ackId);

        // Settle: an acked message on an exactly-once subscription must not be
        // redelivered.
        Thread.sleep(3_000);
        PullResponse after = pull(stub, name);
        for (ReceivedMessage rm : after.getReceivedMessagesList()) {
          if (rm.getMessage().getData().toStringUtf8().equals("eod")) {
            throw new IllegalStateException("exactly-once message redelivered after ack");
          }
        }
      }
      return new String[]{"dup=0", "acked exactly-once message was not redelivered"};
    });

    SdkTour.run("streaming.pubsub_flow_control", "OK", () -> {
      final int cap = 3;
      final int backlog = 15;
      String subId = SdkTour.rid("stream-flow-sub");
      ensurePubsub(topicId, subId, STREAM_ACK_DEADLINE, false, false);

      Publisher publisher = Publisher.newBuilder(ProjectTopicName.of(SdkTour.PROJECT, topicId))
          .setChannelProvider(SdkTour.plaintext(SdkTour.GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .build();
      try {
        for (int i = 0; i < backlog; i++) {
          publisher.publish(PubsubMessage.newBuilder()
              .setData(ByteString.copyFromUtf8(String.format("flow-%02d", i))).build())
              .get(30, TimeUnit.SECONDS);
        }
      } finally {
        publisher.shutdown();
        publisher.awaitTermination(30, TimeUnit.SECONDS);
      }

      // The high-level subscriber under test: the configured cap must bound how
      // many unacked messages it holds at once. Receivers hold their message (no
      // ack) so delivery would outrun acks and expose a missing bound; the driver
      // releases them only after the cap is proved.
      AtomicInteger inflight = new AtomicInteger();
      AtomicInteger maxInflight = new AtomicInteger();
      AtomicInteger delivered = new AtomicInteger();
      AtomicBoolean released = new AtomicBoolean(false);
      MessageReceiver receiver = (message, consumer) -> {
        int cur = inflight.incrementAndGet();
        maxInflight.accumulateAndGet(cur, Math::max);
        delivered.incrementAndGet();
        while (!released.get()) {
          try {
            Thread.sleep(5);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }
        consumer.ack();
        inflight.decrementAndGet();
      };
      Subscriber subscriber = Subscriber.newBuilder(
              ProjectSubscriptionName.of(SdkTour.PROJECT, subId), receiver)
          .setChannelProvider(SdkTour.plaintext(SdkTour.GRPC))
          .setCredentialsProvider(NoCredentialsProvider.create())
          .setFlowControlSettings(FlowControlSettings.newBuilder()
              .setMaxOutstandingElementCount((long) cap)
              .build())
          .build();
      subscriber.startAsync().awaitRunning();
      try {
        long deadline = System.currentTimeMillis() + 30_000;
        while (delivered.get() < cap && System.currentTimeMillis() < deadline) {
          Thread.sleep(10);
        }
        if (delivered.get() < cap) {
          throw new IllegalStateException("flow control: only " + delivered.get() + "/" + cap
              + " messages delivered while held");
        }
        // Hold at the cap: a client that ignores the bound keeps delivering.
        Thread.sleep(1_500);
        int held = delivered.get();
        int maxSeen = maxInflight.get();
        if (held != cap) {
          throw new IllegalStateException("flow control: " + held
              + " messages held concurrently, want max_outstanding=" + cap);
        }
        if (maxSeen != cap) {
          throw new IllegalStateException("flow control: max in flight " + maxSeen + ", want " + cap);
        }
        released.set(true);
        deadline = System.currentTimeMillis() + 30_000;
        while (delivered.get() < backlog && System.currentTimeMillis() < deadline) {
          Thread.sleep(10);
        }
        if (delivered.get() != backlog) {
          throw new IllegalStateException("flow control: drained " + delivered.get() + "/" + backlog
              + " messages");
        }
      } finally {
        released.set(true);
        subscriber.stopAsync().awaitTerminated(30, TimeUnit.SECONDS);
      }
      return new String[]{"flow_control=ok,max_in_flight<=" + cap,
          "held at most " + cap + " of " + backlog + " messages outstanding"};
    });
  }

  // ─── Firestore Listen (generated) ────────────────────────────────────────────

  private static String firestoreParent() {
    return "projects/" + SdkTour.PROJECT + "/databases/(default)/documents";
  }

  private static String firestoreDB() {
    return "projects/" + SdkTour.PROJECT + "/databases/(default)";
  }

  private static FirestoreClient firestoreListenClient() throws Exception {
    return FirestoreClient.create(FirestoreSettings.newBuilder()
        .setTransportChannelProvider(SdkTour.plaintext(SdkTour.GRPC))
        .setCredentialsProvider(NoCredentialsProvider.create())
        .build());
  }

  private static ListenRequest addTarget(int targetId, String collection, ByteString resumeToken) {
    Target.Builder target = Target.newBuilder()
        .setTargetId(targetId)
        .setQuery(Target.QueryTarget.newBuilder()
            .setParent(firestoreParent())
            .setStructuredQuery(StructuredQuery.newBuilder()
                .addFrom(StructuredQuery.CollectionSelector.newBuilder()
                    .setCollectionId(collection))));
    if (resumeToken != null) {
      target.setResumeToken(resumeToken);
    }
    return ListenRequest.newBuilder()
        .setDatabase(firestoreDB())
        .setAddTarget(target)
        .build();
  }

  private static ListenRequest removeTarget(int targetId) {
    return ListenRequest.newBuilder()
        .setDatabase(firestoreDB())
        .setRemoveTarget(targetId)
        .build();
  }

  /** A generated Firestore Listen bidi stream with a response queue. */
  private static final class ListenStream implements AutoCloseable {
    private static final ListenResponse SENTINEL = ListenResponse.getDefaultInstance();

    private final ApiStreamObserver<ListenRequest> requests;
    private final BlockingQueue<ListenResponse> responses = new LinkedBlockingQueue<>();
    private volatile Throwable error;

    ListenStream(FirestoreClient client) {
      this.requests = client.listenCallable().bidiStreamingCall(new ApiStreamObserver<>() {
        @Override public void onNext(ListenResponse value) {
          responses.offer(value);
        }

        @Override public void onError(Throwable t) {
          error = t;
          responses.offer(SENTINEL);
        }

        @Override public void onCompleted() {
          responses.offer(SENTINEL);
        }
      });
    }

    void send(ListenRequest request) {
      requests.onNext(request);
    }

    ListenResponse recv(long timeoutMs) throws Exception {
      ListenResponse resp = responses.poll(timeoutMs, TimeUnit.MILLISECONDS);
      if (resp == SENTINEL) {
        throw new IllegalStateException("Listen stream ended: " + error);
      }
      if (resp == null) {
        throw new IllegalStateException("Listen recv timed out");
      }
      return resp;
    }

    @Override public void close() {
      try {
        requests.onCompleted();
      } catch (Throwable ignored) {
        // best effort
      }
    }
  }

  /** Reads frames until the NO_CHANGE that concludes the snapshot/epoch. */
  private static List<String> listenRecvUntilNoChange(ListenStream stream) throws Exception {
    List<String> names = new ArrayList<>();
    while (true) {
      ListenResponse resp = stream.recv(20_000);
      if (resp.hasDocumentChange()) {
        names.add(resp.getDocumentChange().getDocument().getName());
        continue;
      }
      if (resp.hasTargetChange()
          && resp.getTargetChange().getTargetChangeType() == TargetChange.TargetChangeType.NO_CHANGE) {
        return names;
      }
    }
  }

  /** Blocks until a TargetChange of the given type is read. */
  private static void listenRecvUntil(ListenStream stream, TargetChange.TargetChangeType want)
      throws Exception {
    while (true) {
      ListenResponse resp = stream.recv(20_000);
      if (resp.hasTargetChange() && resp.getTargetChange().getTargetChangeType() == want) {
        return;
      }
    }
  }

  private static String shortName(String documentName) {
    int slash = documentName.lastIndexOf('/');
    return slash < 0 ? documentName : documentName.substring(slash + 1);
  }

  private static void firestoreScenarios() {
    SdkTour.run("streaming.firestore_listen_resume_token", "OK", () -> {
      Firestore high = SdkTour.firestore();
      try (FirestoreClient listen = firestoreListenClient()) {
        String coll = SdkTour.rid("stream-listen-resume");

        // Stream A: snapshot, then read the resume token from the CURRENT frame.
        ByteString token = null;
        try (ListenStream a = new ListenStream(listen)) {
          a.send(addTarget(1, coll, null));
          while (token == null) {
            ListenResponse resp = a.recv(20_000);
            if (resp.hasTargetChange()) {
              TargetChange tc = resp.getTargetChange();
              if (tc.getTargetChangeType() == TargetChange.TargetChangeType.CURRENT
                  && tc.getResumeToken().size() > 0) {
                token = tc.getResumeToken();
              }
            }
          }
        }

        // Write "b" after A is closed: it must be replayed to the resumed stream.
        high.collection(coll).document("b").set(Map.of("v", "b")).get();

        // Stream B: register the subscription (a harmless remove proves the
        // handler is running), then write "c" BEFORE AddTarget. "c" is therefore
        // both in the replayed history and buffered as a live delta — it must be
        // delivered exactly once.
        try (ListenStream b = new ListenStream(listen)) {
          b.send(removeTarget(999));
          listenRecvUntil(b, TargetChange.TargetChangeType.REMOVE);
          high.collection(coll).document("c").set(Map.of("v", "c")).get();
          b.send(addTarget(2, coll, token));

          List<String> names = listenRecvUntilNoChange(b);
          Map<String, Integer> counts = new HashMap<>();
          for (String name : names) {
            counts.merge(shortName(name), 1, Integer::sum);
          }
          for (String want : new String[]{"b", "c"}) {
            int c = counts.getOrDefault(want, 0);
            if (c != 1) {
              throw new IllegalStateException("resumed stream delivered " + want + " " + c
                  + " times, want 1 (all=" + counts + ")");
            }
          }
        }
      } finally {
        high.close();
      }
      return new String[]{"loss=0,dup=0",
          "resume token replayed b and delivered the racing c exactly once"};
    });

    SdkTour.run("streaming.firestore_snapshot_consistency", "OK", () -> {
      Firestore high = SdkTour.firestore();
      try (FirestoreClient listen = firestoreListenClient()) {
        String coll = SdkTour.rid("stream-listen-consistency");
        final int n = 5;
        try (ListenStream stream = new ListenStream(listen)) {
          stream.send(addTarget(1, coll, null));
          listenRecvUntilNoChange(stream);

          for (int i = 0; i < n; i++) {
            high.collection(coll).document("d" + i).set(Map.of("i", i)).get();
          }

          Map<String, Integer> counts = new HashMap<>();
          long lastRead = Long.MIN_VALUE;
          boolean monotonic = true;
          while (counts.size() < n) {
            ListenResponse resp = stream.recv(20_000);
            if (resp.hasDocumentChange()) {
              counts.merge(shortName(resp.getDocumentChange().getDocument().getName()), 1,
                  Integer::sum);
              continue;
            }
            if (resp.hasTargetChange()
                && resp.getTargetChange().getTargetChangeType() == TargetChange.TargetChangeType.NO_CHANGE) {
              TargetChange tc = resp.getTargetChange();
              if (tc.hasReadTime()) {
                long rt = timestampNanos(tc.getReadTime());
                if (lastRead != Long.MIN_VALUE && rt < lastRead) {
                  monotonic = false;
                }
                lastRead = rt;
              }
            }
          }
          for (Map.Entry<String, Integer> e : counts.entrySet()) {
            if (e.getValue() != 1) {
              throw new IllegalStateException("document " + e.getKey() + " delivered "
                  + e.getValue() + " times, want 1");
            }
          }
          if (!monotonic) {
            throw new IllegalStateException("read_time was not monotonic across NO_CHANGE frames");
          }
        }
      } finally {
        high.close();
      }
      return new String[]{"docs=5,dup=0,monotonic=yes",
          "5 concurrent writes delivered once each with monotonic read_time"};
    });
  }

  private static long timestampNanos(Timestamp t) {
    return t.getSeconds() * 1_000_000_000L + t.getNanos();
  }

  // ─── Logging tail ────────────────────────────────────────────────────────────

  private static final class TailStream implements AutoCloseable {
    private static final List<LogEntry> SENTINEL = Collections.emptyList();

    private final ApiStreamObserver<TailLogEntriesRequest> requests;
    private final BlockingQueue<List<LogEntry>> batches = new LinkedBlockingQueue<>();
    final List<String> seen = Collections.synchronizedList(new ArrayList<>());
    volatile boolean closed;
    private volatile Throwable error;

    TailStream(LoggingClient client) {
      this.requests = client.tailLogEntriesCallable().bidiStreamingCall(
          new ApiStreamObserver<>() {
            @Override public void onNext(TailLogEntriesResponse value) {
              List<LogEntry> entries = value.getEntriesList();
              for (LogEntry e : entries) {
                seen.add(e.getTextPayload());
              }
              batches.offer(entries);
            }

            @Override public void onError(Throwable t) {
              error = t;
              batches.offer(SENTINEL);
            }

            @Override public void onCompleted() {
              batches.offer(SENTINEL);
            }
          });
    }

    void send(TailLogEntriesRequest request) {
      requests.onNext(request);
    }

    List<LogEntry> recv(long timeoutMs) throws Exception {
      List<LogEntry> batch = batches.poll(timeoutMs, TimeUnit.MILLISECONDS);
      if (batch == SENTINEL) {
        throw new IllegalStateException("TailLogEntries stream ended: " + error);
      }
      if (batch == null) {
        throw new IllegalStateException("TailLogEntries recv timed out");
      }
      return batch;
    }

    @Override public void close() {
      closed = true;
      try {
        requests.onCompleted();
      } catch (Throwable ignored) {
        // best effort
      }
    }
  }

  private static TailStream startTail(LoggingClient logging, String parent, String logName) {
    TailStream stream = new TailStream(logging);
    stream.send(TailLogEntriesRequest.newBuilder()
        .addResourceNames(parent)
        .setFilter("logName=\"" + logName + "\"")
        .setBufferWindow(Duration.newBuilder().setNanos(200_000_000).build())
        .build());
    return stream;
  }

  /**
   * Keeps writing unique entries with {@code prefix} until one is delivered,
   * tolerating the emulator's "new since stream start" seed race.
   */
  private static void recvUntilPrefix(LoggingClient logging, TailStream stream, String logName,
                                      String prefix) throws Exception {
    Thread writer = new Thread(() -> {
      int i = 0;
      try {
        while (!stream.closed) {
          logging.writeLogEntries(SdkTour.writeRequest(logName, prefix + "-" + i++));
          Thread.sleep(250);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception ignored) {
        // best effort
      }
    });
    writer.setDaemon(true);
    writer.start();
    try {
      while (true) {
        List<LogEntry> batch = stream.recv(30_000);
        for (LogEntry e : batch) {
          if (e.getTextPayload().startsWith(prefix)) {
            return;
          }
        }
      }
    } finally {
      stream.closed = true;
      writer.interrupt();
    }
  }

  private static void loggingScenarios() {
    String parent = "projects/" + SdkTour.PROJECT;
    String logName = parent + "/logs/" + SdkTour.rid("stream-tail-log");

    SdkTour.run("streaming.logging_tail_reconnect", "OK", () -> {
      try (LoggingClient logging = SdkTour.loggingClient()) {
        TailStream first = startTail(logging, parent, logName);
        recvUntilPrefix(logging, first, logName, "stream-tail-a");
        first.close();

        // Written while disconnected: below the second stream's seed cursor, so
        // it must NOT be replayed (TailLogEntries has no resume cursor).
        logging.writeLogEntries(SdkTour.writeRequest(logName, "stream-tail-gap"));

        TailStream second = startTail(logging, parent, logName);
        recvUntilPrefix(logging, second, logName, "stream-tail-c");
        for (String text : new ArrayList<>(second.seen)) {
          if (text.startsWith("stream-tail-gap")) {
            second.close();
            throw new IllegalStateException("second tail replayed the disconnected-window entry: " + text);
          }
        }
        second.close();
      }
      return new String[]{"reconnect_ok=yes,gap_replayed=no",
          "second tail delivered post-reconnect entries and did not replay the disconnected-window entry"};
    });
  }

  // ─── Storage BidiReadObject ───────────────────────────────────────────────────

  private static void storageScenarios() {
    SdkTour.run("streaming.storage_bidi_read", "OK", () -> {
      // GrpcStorageOptions reads the scheme from setHost(): an http:// host
      // makes it install a plaintext channel configurator, matching the Go
      // reference's insecure gRPC dial (a bare host:port would be dialled TLS).
      Storage storage = GrpcStorageOptions.newBuilder()
          .setHost("http://" + SdkTour.GRPC)
          .setProjectId(SdkTour.PROJECT)
          .setCredentials(NoCredentials.getInstance())
          .build()
          .getService();

      String bucket = SdkTour.rid("stream-bidi-bucket");
      if (storage.get(bucket) == null) {
        storage.create(BucketInfo.newBuilder(bucket).setLocation("US").build());
      }
      byte[] payload = "bidi-stream-payload-0123456789".getBytes(StandardCharsets.UTF_8);
      BlobId id = BlobId.of(bucket, "bidi.bin");
      storage.create(BlobInfo.newBuilder(id).build(), payload);

      byte[] full;
      try (BlobReadSession session = storage.blobReadSession(id).get(30, TimeUnit.SECONDS)) {
        full = session.readAs(ReadProjectionConfigs.asFutureBytes()).get(30, TimeUnit.SECONDS);
      }
      if (!Arrays.equals(full, payload)) {
        throw new IllegalStateException("full read = " + new String(full, StandardCharsets.UTF_8)
            + ", want " + new String(payload, StandardCharsets.UTF_8));
      }

      byte[] sub;
      try (BlobReadSession session = storage.blobReadSession(id).get(30, TimeUnit.SECONDS)) {
        sub = session.readAs(ReadProjectionConfigs.asFutureBytes()
            .withRangeSpec(RangeSpec.beginAt(5).withMaxLength(4))).get(30, TimeUnit.SECONDS);
      }
      byte[] want = Arrays.copyOfRange(payload, 5, 9);
      if (!Arrays.equals(sub, want)) {
        throw new IllegalStateException("range read = " + new String(sub, StandardCharsets.UTF_8)
            + ", want " + new String(want, StandardCharsets.UTF_8));
      }
      return new String[]{"full=ok,range=ok", "BidiReadObject served a full read and a subrange"};
    });
  }
}
