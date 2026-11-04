package com.jaiscloud.demo.dpgke;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.NoCredentials;
import com.google.cloud.container.v1.ClusterManagerClient;
import com.google.cloud.container.v1.ClusterManagerSettings;
import com.google.cloud.dataproc.v1.Cluster;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import com.google.cloud.dataproc.v1.ClusterOperationMetadata;
import com.google.cloud.dataproc.v1.ClusterStatus;
import com.google.cloud.dataproc.v1.CreateClusterRequest;
import com.google.cloud.dataproc.v1.DeleteClusterRequest;
import com.google.cloud.dataproc.v1.GkeClusterConfig;
import com.google.cloud.dataproc.v1.GkeNodePoolConfig;
import com.google.cloud.dataproc.v1.GkeNodePoolTarget;
import com.google.cloud.dataproc.v1.GetClusterRequest;
import com.google.cloud.dataproc.v1.GetJobRequest;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobPlacement;
import com.google.cloud.dataproc.v1.JobStatus;
import com.google.cloud.dataproc.v1.KubernetesClusterConfig;
import com.google.cloud.dataproc.v1.KubernetesSoftwareConfig;
import com.google.cloud.dataproc.v1.ListJobsRequest;
import com.google.cloud.dataproc.v1.PySparkJob;
import com.google.cloud.dataproc.v1.SubmitJobRequest;
import com.google.cloud.dataproc.v1.VirtualClusterConfig;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.container.v1.GetOperationRequest;
import com.google.container.v1.ListNodePoolsRequest;
import com.google.container.v1.NodePool;
import com.google.container.v1.WorkloadIdentityConfig;
import com.google.protobuf.Empty;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Dataproc-on-GKE lifecycle through the official Google Java SDKs.
 *
 * <p>This is DGD3, the leg that proves the Java clients' <b>default transports</b>
 * against the emulator:
 *
 * <ul>
 *   <li>{@code ClusterManagerClient} (GKE, gRPC by default) — verify or create
 *       the zonal cluster with Workload Identity, then list the node pools
 *       Dataproc reconciled into it;
 *   <li>{@code ClusterControllerClient} (Dataproc, gRPC by default) — create the
 *       virtual cluster, wait on the gax {@code OperationFuture};
 *   <li>{@code JobControllerClient} (Dataproc, gRPC by default) — submit the
 *       PySpark job, wait for DONE, read {@code driverOutputResourceUri}.
 * </ul>
 *
 * <p>Only {@code Storage} is REST: the Java Cloud Storage client speaks the GCS
 * JSON API, and that is how the staged driver output object is read. That is a
 * property of the client, not a fallback around an emulator gap.
 *
 * <p><b>Honesty:</b> the emulator's default Dataproc executor is the mock Spark
 * executor. A job reaches DONE with a driver-output URI <i>without starting
 * Spark</i>; the driver object is a small synthetic terminal log and the output
 * bucket stays empty. This app asserts exactly that (job DONE, driver object
 * readable) and does not claim Spark ran or that it wrote records. Real Spark
 * bytes are DGD6's k3d gate.
 *
 * <p>DGD5 adds the credential half: two Credential Access Boundaries (input
 * read, output write) are exchanged at the emulator's STS endpoint and used as
 * the credential on the wire, with the <em>denials</em> asserted — see
 * {@link DownscopedAccess}. A boundary that is never refused proves nothing.
 *
 * <p>Run it twice to prove idempotent create: with {@code DGD3_TEARDOWN=false}
 * it leaves the clusters in place; a second run adopts them, submits another
 * job, and tears everything down. {@code run.sh} does exactly that.
 */
public final class DpgkeLifecycle {

  private static final String WORKLOAD_IDENTITY_SUFFIX = ".svc.id.goog";

  public static void main(String[] args) {
    DemoConfig cfg = DemoConfig.fromEnv();
    logConfig(cfg);

    Storage storage = storage(cfg);
    String jobId = null;
    Throwable failure = null;

    try (ClusterControllerClient dataproc = dataproc(cfg);
         ClusterManagerClient gke = gke(cfg)) {

      // The body and the teardown are separate try blocks: a failure anywhere
      // in the lifecycle must still reach cleanup (and, for the gate, still
      // produce a non-zero exit after it).
      Throwable bodyFailure = null;
      try {
        if (!cfg.cleanupOnly) {
          ensureBucketsAndObjects(cfg, storage);
          // DGD5: the credential path a customer's workload runs with. It only
          // needs the buckets above, so it stays before the cluster work: a
          // boundary regression fails the run in seconds, not after a create.
          DownscopedAccess.assertBuckets(cfg, storage);
          ensureGkeCluster(gke, cfg);
          ensureVirtualCluster(dataproc, cfg);
          assertVirtualCluster(dataproc, cfg);
          assertNodePools(gke, cfg);

          jobId = submitJob(cfg);
          Job done = waitForJob(cfg, jobId);
          readDriverOutput(storage, cfg, done);
          reportOutputBucket(storage, cfg);
        }
      } catch (Throwable t) {
        bodyFailure = t;
      }

      if (cfg.teardown || cfg.cleanupOnly) {
        try {
          cleanup(dataproc, gke, storage, cfg, jobId);
          if (!cfg.cleanupOnly) {
            assertClean(dataproc, gke, storage, cfg, jobId);
          }
        } catch (Throwable teardownFailure) {
          if (bodyFailure == null) {
            bodyFailure = teardownFailure;
          } else {
            bodyFailure.addSuppressed(teardownFailure);
          }
        }
      }
      failure = bodyFailure;
    } catch (Throwable clientFailure) {
      failure = clientFailure;
    }

    if (failure != null) {
      System.err.println("FAILED: " + failure);
      failure.printStackTrace(System.err);
      System.exit(1);
    }
    System.out.println("== DGD3 Java lifecycle complete (transport: Dataproc+GKE gRPC, Storage REST)");
  }

  // ─── Clients ───────────────────────────────────────────────────────────────

  /**
   * Plaintext + no credentials on a fixed endpoint: exactly what the official
   * gRPC clients do against the emulator, and the same helper the sdk-tour uses.
   * Credentials are never invented; the emulator does not authenticate.
   */
  private static TransportChannelProvider plaintext(String target) {
    return InstantiatingGrpcChannelProvider.newBuilder()
        .setEndpoint(target)
        .setCredentials(NoCredentials.getInstance())
        .setChannelConfigurator(builder -> builder.usePlaintext())
        .build();
  }

  private static ClusterControllerClient dataproc(DemoConfig cfg) throws Exception {
    return ClusterControllerClient.create(ClusterControllerSettings.newBuilder()
        .setTransportChannelProvider(plaintext(cfg.grpcTarget()))
        .setCredentialsProvider(NoCredentialsProvider.create())
        .build());
  }

  private static JobControllerClient jobs(DemoConfig cfg) throws Exception {
    return JobControllerClient.create(JobControllerSettings.newBuilder()
        .setTransportChannelProvider(plaintext(cfg.grpcTarget()))
        .setCredentialsProvider(NoCredentialsProvider.create())
        .build());
  }

  private static ClusterManagerClient gke(DemoConfig cfg) throws Exception {
    return ClusterManagerClient.create(ClusterManagerSettings.newBuilder()
        .setTransportChannelProvider(plaintext(cfg.grpcTarget()))
        .setCredentialsProvider(NoCredentialsProvider.create())
        .build());
  }

  private static Storage storage(DemoConfig cfg) {
    return StorageOptions.newBuilder()
        .setProjectId(cfg.storageProject)
        .setHost(cfg.restEndpoint)
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
  }

  // ─── GCS: buckets and the objects Terraform would have uploaded ────────────

  /**
   * The Terraform module owns these on a full demo run. This leg creates them
   * when it runs standalone (the gate does), with the same names, the same job
   * file and the same seed document, so the job points at the object Terraform
   * would have uploaded rather than an invented one.
   */
  private static void ensureBucketsAndObjects(DemoConfig cfg, Storage storage) throws Exception {
    ensureBucket(cfg.inputBucket, storage);
    ensureBucket(cfg.outputBucket, storage);
    ensureBucket(cfg.stagingBucket, storage);

    Path script = cfg.sparkScript == null ? null : Path.of(cfg.sparkScript);
    if (storage.get(BlobId.of(cfg.inputBucket, cfg.jobCodeObject)) == null) {
      if (script == null || !Files.isReadable(script)) {
        throw new IllegalStateException(
            "gs://" + cfg.inputBucket + "/" + cfg.jobCodeObject + " is missing and no local "
                + "spark/wordcount.py was found; set DGD3_SPARK_SCRIPT");
      }
      byte[] code = Files.readAllBytes(script);
      ensureObject(storage, cfg.inputBucket, cfg.jobCodeObject, "text/x-python", code);
      log("uploaded " + script + " -> " + cfg.jobCodeUri());
    } else {
      log("job code object already present: " + cfg.jobCodeUri());
    }

    if (storage.get(BlobId.of(cfg.inputBucket, cfg.jobInputObject)) == null) {
      ensureObject(storage, cfg.inputBucket, cfg.jobInputObject, "text/plain; charset=utf-8",
          SEED_INPUT.getBytes(StandardCharsets.UTF_8));
      log("seeded " + cfg.jobInputUri());
    }
  }

  private static void ensureBucket(String bucket, Storage storage) {
    if (storage.get(bucket) != null) {
      log("bucket already exists: gs://" + bucket);
      return;
    }
    storage.create(BucketInfo.newBuilder(bucket).setLocation("US").build());
    log("created bucket gs://" + bucket);
  }

  private static void ensureObject(
      Storage storage, String bucket, String object, String contentType, byte[] data) {
    if (storage.get(BlobId.of(bucket, object)) != null) {
      return;
    }
    storage.create(BlobInfo.newBuilder(BlobId.of(bucket, object))
        .setContentType(contentType)
        .build(), data);
  }

  /** The same three lines terraform/main.tf seeds into input/words.txt. */
  static final String SEED_INPUT =
      "dataproc on gke reads this object and writes a word count\n"
          + "the quick brown fox jumps over the lazy dog\n"
          + "dataproc on gke is the job the demo proves\n";

  // ─── GKE ───────────────────────────────────────────────────────────────────

  private static void ensureGkeCluster(ClusterManagerClient gke, DemoConfig cfg) {
    String name = cfg.gkeClusterTarget();
    try {
      com.google.container.v1.Cluster existing = gke.getCluster(
          com.google.container.v1.GetClusterRequest.newBuilder().setName(name).build());
      log("adopting existing GKE cluster " + name);
      assertWorkloadIdentity(existing, cfg);
      return;
    } catch (NotFoundException expected) {
      // Create it below.
    }

    com.google.container.v1.Cluster requested = com.google.container.v1.Cluster.newBuilder()
        .setName(cfg.gkeCluster)
        .setInitialNodeCount(1)
        .setWorkloadIdentityConfig(WorkloadIdentityConfig.newBuilder()
            .setWorkloadPool(cfg.project + WORKLOAD_IDENTITY_SUFFIX)
            .build())
        .build();

    try {
      com.google.container.v1.Operation operation = gke.createCluster(
          com.google.container.v1.CreateClusterRequest.newBuilder()
              .setParent("projects/" + cfg.project + "/locations/" + cfg.gkeLocation)
              .setCluster(requested)
              .build());
      log("creating GKE cluster " + name + " (operation " + operation.getName() + ")");
      awaitGkeOperation(gke, operation, "create GKE cluster", cfg);
    } catch (AlreadyExistsException raced) {
      log("GKE cluster appeared while creating; adopting it");
    }

    com.google.container.v1.Cluster created = gke.getCluster(
        com.google.container.v1.GetClusterRequest.newBuilder().setName(name).build());
    check("GKE cluster " + name + " is RUNNING",
        created.getStatus() == com.google.container.v1.Cluster.Status.RUNNING,
        "status=" + created.getStatus());
    assertWorkloadIdentity(created, cfg);
  }

  private static void assertWorkloadIdentity(
      com.google.container.v1.Cluster cluster, DemoConfig cfg) {
    String want = cfg.project + WORKLOAD_IDENTITY_SUFFIX;
    check("GKE cluster has workloadIdentityConfig.workloadPool = " + want,
        cluster.hasWorkloadIdentityConfig()
            && want.equals(cluster.getWorkloadIdentityConfig().getWorkloadPool()),
        "got " + (cluster.hasWorkloadIdentityConfig()
            ? cluster.getWorkloadIdentityConfig().getWorkloadPool() : "<unset>"));
  }

  private static void awaitGkeOperation(
      ClusterManagerClient gke,
      com.google.container.v1.Operation operation,
      String what,
      DemoConfig cfg) {
    // Operation.name is the server-assigned ID (Discovery: "The server-assigned
    // ID for the operation"), while GetOperation.name is the canonical
    // projects/{p}/locations/{l}/operations/{id} path. Normalise once so the
    // read works whether the ID arrives bare (real GKE, the emulator) or
    // already qualified.
    String name = gkeOperationPath(cfg, operation.getName());
    for (int attempt = 1; attempt <= cfg.pollAttempts; attempt++) {
      com.google.container.v1.Operation current = gke.getOperation(
          GetOperationRequest.newBuilder().setName(name).build());
      if (current.getStatus() == com.google.container.v1.Operation.Status.DONE) {
        log(what + " operation " + name + " is DONE");
        return;
      }
      if (current.getStatus() == com.google.container.v1.Operation.Status.ABORTING) {
        throw new IllegalStateException(what + " operation " + name + " is ABORTING: "
            + current.getStatusMessage());
      }
      sleep(cfg, what + " is " + current.getStatus());
    }
    throw new IllegalStateException(what + " operation " + name + " did not finish after "
        + cfg.pollAttempts + " x " + cfg.pollSeconds + "s");
  }

  private static String gkeOperationPath(DemoConfig cfg, String operationName) {
    if (operationName != null && operationName.startsWith("projects/")) {
      return operationName;
    }
    return "projects/" + cfg.project + "/locations/" + cfg.gkeLocation
        + "/operations/" + operationName;
  }

  // ─── Dataproc virtual cluster ──────────────────────────────────────────────

  private static void ensureVirtualCluster(ClusterControllerClient dataproc, DemoConfig cfg)
      throws Exception {
    try {
      Cluster existing = dataproc.getCluster(GetClusterRequest.newBuilder()
          .setProjectId(cfg.project)
          .setRegion(cfg.region)
          .setClusterName(cfg.dataprocCluster)
          .build());
      log("adopting existing virtual cluster " + cfg.dataprocCluster + " (state "
          + existing.getStatus().getState() + ")");
      waitForClusterRunning(dataproc, cfg);
      return;
    } catch (NotFoundException expected) {
      // Create it below.
    }

    Cluster requested = Cluster.newBuilder()
        .setProjectId(cfg.project)
        .setClusterName(cfg.dataprocCluster)
        .setVirtualClusterConfig(VirtualClusterConfig.newBuilder()
            .setStagingBucket(cfg.stagingBucket)
            .setKubernetesClusterConfig(KubernetesClusterConfig.newBuilder()
                .setKubernetesNamespace(cfg.namespace)
                .setGkeClusterConfig(GkeClusterConfig.newBuilder()
                    .setGkeClusterTarget(cfg.gkeClusterTarget())
                    // The official client does not expand short ids: Discovery
                    // requires the full node-pool resource name, which is why
                    // these are built from cfg rather than passed as suffixes.
                    .addNodePoolTarget(nodePoolTarget(cfg, "controller",
                        GkeNodePoolTarget.Role.DEFAULT, GkeNodePoolTarget.Role.CONTROLLER))
                    .addNodePoolTarget(nodePoolTarget(cfg, "spark",
                        GkeNodePoolTarget.Role.SPARK_DRIVER, GkeNodePoolTarget.Role.SPARK_EXECUTOR))
                    .build())
                .setKubernetesSoftwareConfig(KubernetesSoftwareConfig.newBuilder()
                    // A fully-qualified release, not an alias: aliases are
                    // dereferenced at create time and would hide which release
                    // the demo ran.
                    .putComponentVersion("SPARK", cfg.sparkRelease)
                    .build())
                .build())
            .build())
        .build();

    OperationFuture<Cluster, ClusterOperationMetadata> future = dataproc.createClusterAsync(
        CreateClusterRequest.newBuilder()
            .setProjectId(cfg.project)
            .setRegion(cfg.region)
            .setCluster(requested)
            .build());
    log("creating virtual cluster " + cfg.dataprocCluster + " (operation " + future.getName() + ")");
    try {
      awaitDataprocOperation(future, "create virtual cluster", cfg);
    } catch (AlreadyExistsException raced) {
      log("virtual cluster appeared while creating; adopting it");
    }
    waitForClusterRunning(dataproc, cfg);
  }

  private static GkeNodePoolTarget nodePoolTarget(
      DemoConfig cfg, String suffix, GkeNodePoolTarget.Role... roles) {
    GkeNodePoolTarget.Builder target = GkeNodePoolTarget.newBuilder()
        .setNodePool(cfg.nodePoolName(suffix))
        .setNodePoolConfig(GkeNodePoolConfig.newBuilder()
            .setConfig(GkeNodePoolConfig.GkeNodeConfig.newBuilder()
                .setMachineType(cfg.machineType)
                .build())
            .setAutoscaling(GkeNodePoolConfig.GkeNodePoolAutoscalingConfig.newBuilder()
                .setMinNodeCount(cfg.minNodes)
                .setMaxNodeCount(cfg.maxNodes)
                .build())
            .addLocations(cfg.gkeLocation));
    for (GkeNodePoolTarget.Role role : roles) {
      target.addRoles(role);
    }
    return target.build();
  }

  /**
   * Poll the gax {@code OperationFuture} the way the medallion script polled
   * the REST operation: {@code DGD3_POLL_SECONDS} (2s) by
   * {@code DGD3_POLL_ATTEMPTS} (60). The future is driven by gax's LRO polling
   * against {@code google.longrunning.Operations}; this loop only bounds it.
   */
  private static <T> T awaitDataprocOperation(
      OperationFuture<T, ?> future, String what, DemoConfig cfg) throws Exception {
    for (int attempt = 1; attempt <= cfg.pollAttempts; attempt++) {
      if (future.isDone()) {
        return future.get();
      }
      sleep(cfg, what + " operation " + future.getName() + " is still in flight");
    }
    if (future.isDone()) {
      return future.get();
    }
    future.cancel(true);
    throw new IllegalStateException(what + " operation " + future.getName()
        + " did not finish after " + cfg.pollAttempts + " x " + cfg.pollSeconds + "s");
  }

  private static Cluster waitForClusterRunning(ClusterControllerClient dataproc, DemoConfig cfg) {
    for (int attempt = 1; attempt <= cfg.pollAttempts; attempt++) {
      Cluster cluster = dataproc.getCluster(GetClusterRequest.newBuilder()
          .setProjectId(cfg.project)
          .setRegion(cfg.region)
          .setClusterName(cfg.dataprocCluster)
          .build());
      ClusterStatus.State state = cluster.getStatus().getState();
      if (state == ClusterStatus.State.RUNNING) {
        check("virtual cluster " + cfg.dataprocCluster + " is RUNNING", true, null);
        return cluster;
      }
      if (state == ClusterStatus.State.ERROR
          || state == ClusterStatus.State.ERROR_DUE_TO_UPDATE
          || state == ClusterStatus.State.DELETING) {
        throw new IllegalStateException("virtual cluster " + cfg.dataprocCluster + " is " + state
            + ": " + cluster.getStatus().getDetail());
      }
      sleep(cfg, "virtual cluster is " + state);
    }
    throw new IllegalStateException("virtual cluster " + cfg.dataprocCluster
        + " did not reach RUNNING after " + cfg.pollAttempts + " x " + cfg.pollSeconds + "s");
  }

  private static void assertVirtualCluster(ClusterControllerClient dataproc, DemoConfig cfg) {
    Cluster cluster = dataproc.getCluster(GetClusterRequest.newBuilder()
        .setProjectId(cfg.project)
        .setRegion(cfg.region)
        .setClusterName(cfg.dataprocCluster)
        .build());
    VirtualClusterConfig vcc = cluster.getVirtualClusterConfig();
    check("virtual cluster stages into gs://" + cfg.stagingBucket,
        cfg.stagingBucket.equals(vcc.getStagingBucket()), "got " + vcc.getStagingBucket());
    check("virtual cluster targets the zonal GKE cluster " + cfg.gkeClusterTarget(),
        vcc.getKubernetesClusterConfig().getGkeClusterConfig().getGkeClusterTarget()
            .equals(cfg.gkeClusterTarget()),
        "got " + vcc.getKubernetesClusterConfig().getGkeClusterConfig().getGkeClusterTarget());
    check("virtual cluster uses namespace " + cfg.namespace,
        cfg.namespace.equals(vcc.getKubernetesClusterConfig().getKubernetesNamespace()),
        "got " + vcc.getKubernetesClusterConfig().getKubernetesNamespace());
    check("virtual cluster pins SPARK " + cfg.sparkRelease,
        cfg.sparkRelease.equals(
            vcc.getKubernetesClusterConfig().getKubernetesSoftwareConfig()
                .getComponentVersionMap().get("SPARK")),
        "got " + vcc.getKubernetesClusterConfig().getKubernetesSoftwareConfig()
            .getComponentVersionMap());
  }

  // ─── GKE node pools (reconciled by Dataproc) ───────────────────────────────

  private static List<NodePool> listPools(ClusterManagerClient gke, DemoConfig cfg) {
    return gke.listNodePools(ListNodePoolsRequest.newBuilder()
        .setParent(cfg.gkeNodePoolsParent())
        .build()).getNodePoolsList();
  }

  private static void assertNodePools(ClusterManagerClient gke, DemoConfig cfg) {
    List<NodePool> pools = listPools(gke, cfg);
    String names = pools.stream().map(NodePool::getName).collect(Collectors.joining(", "));
    log("GKE node pools: [" + names + "]");

    for (String suffix : List.of("controller", "spark")) {
      String expected = cfg.dataprocCluster + "-" + suffix;
      String full = cfg.nodePoolName(suffix);
      NodePool pool = pools.stream()
          .filter(p -> expected.equals(p.getName()) || full.equals(p.getSelfLink()))
          .findFirst()
          .orElse(null);
      check("Dataproc reconciled node pool " + expected + " into GKE", pool != null, "pools=" + names);
      if (pool == null) {
        continue;
      }
      check("node pool " + expected + " uses machine type " + cfg.machineType,
          cfg.machineType.equals(pool.getConfig().getMachineType()),
          "got " + pool.getConfig().getMachineType());
      check("node pool " + expected + " autoscaling maxNodeCount is " + cfg.maxNodes,
          pool.getAutoscaling().getMaxNodeCount() == cfg.maxNodes,
          "got " + pool.getAutoscaling().getMaxNodeCount());
      check("node pool " + expected + " carries goog-dataproc-cluster-name=" + cfg.dataprocCluster,
          cfg.dataprocCluster.equals(
              pool.getConfig().getLabelsMap().get("goog-dataproc-cluster-name")),
          "labels=" + pool.getConfig().getLabelsMap());
    }
  }

  // ─── The job ───────────────────────────────────────────────────────────────

  private static String submitJob(DemoConfig cfg) throws Exception {
    Job job = Job.newBuilder()
        .setPlacement(JobPlacement.newBuilder().setClusterName(cfg.dataprocCluster).build())
        .setPysparkJob(PySparkJob.newBuilder()
            .setMainPythonFileUri(cfg.jobCodeUri())
            .addArgs(cfg.jobInputUri())
            .addArgs(cfg.jobOutputUri())
            .build())
        .build();

    // JobControllerClient is its own generated client; Dataproc serves it on the
    // same gRPC endpoint as ClusterController.
    try (JobControllerClient jobs = jobs(cfg)) {
      Job submitted = jobs.submitJob(SubmitJobRequest.newBuilder()
          .setProjectId(cfg.project)
          .setRegion(cfg.region)
          .setJob(job)
          .build());
      String jobId = submitted.getReference().getJobId();
      check("Dataproc accepted the PySpark job and assigned an id", !jobId.isEmpty(),
          "reference=" + submitted.getReference());
      log("submitted job " + jobId + ": " + cfg.jobCodeUri() + " " + cfg.jobInputUri()
          + " -> " + cfg.jobOutputUri());
      return jobId;
    }
  }

  private static Job waitForJob(DemoConfig cfg, String jobId) throws Exception {
    try (JobControllerClient jobs = jobs(cfg)) {
      JobStatus.State last = null;
      for (int attempt = 1; attempt <= cfg.pollAttempts; attempt++) {
        Job job = jobs.getJob(GetJobRequest.newBuilder()
            .setProjectId(cfg.project)
            .setRegion(cfg.region)
            .setJobId(jobId)
            .build());
        JobStatus.State state = job.getStatus().getState();
        if (state != last) {
          log("job " + jobId + " state: " + state);
          last = state;
        }
        switch (state) {
          case DONE:
            check("job " + jobId + " reached DONE", true, null);
            return job;
          case ERROR:
          case CANCELLED:
            throw new IllegalStateException("job " + jobId + " ended " + state + ": "
                + job.getStatus().getDetails());
          default:
            sleep(cfg, "job is " + state);
        }
      }
      throw new IllegalStateException("job " + jobId + " did not finish after "
          + cfg.pollAttempts + " x " + cfg.pollSeconds + "s");
    }
  }

  /**
   * Read the staged driver output through the Storage client (HTTP/JSON). The
   * advertised URI is a prefix; the official samples and gcloud append
   * {@code .000000000}. In the emulator's default mock executor the object is a
   * synthetic terminal log written at DONE — never Spark's stdout.
   */
  private static void readDriverOutput(Storage storage, DemoConfig cfg, Job job) {
    String uri = job.getDriverOutputResourceUri();
    check("job advertises a driverOutputResourceUri", uri != null && uri.startsWith("gs://"),
        "got " + uri);
    String[] parts = splitGs(uri);
    check("driver output URI is in the virtual cluster's staging bucket (" + cfg.stagingBucket + ")",
        cfg.stagingBucket.equals(parts[0]), "got " + uri);

    String object = parts[1] + ".000000000";
    Blob blob = storage.get(BlobId.of(parts[0], object));
    if (blob == null) {
      blob = storage.get(BlobId.of(parts[0], parts[1]));
    }
    if (blob == null) {
      log("driver output object gs://" + parts[0] + "/" + object
          + " was not materialized; nothing to read (acceptable without a Spark driver)");
      return;
    }

    byte[] bytes = blob.getContent();
    check("driver output object gs://" + parts[0] + "/" + blob.getName() + " is non-empty",
        bytes.length > 0, bytes.length + " bytes");
    log("read gs://" + parts[0] + "/" + blob.getName() + " through Storage ("
        + bytes.length + " bytes):");
    for (String line : new String(bytes, StandardCharsets.UTF_8).split("\\R")) {
      if (!line.isEmpty()) {
        System.out.println("    | " + line);
      }
    }
    log("note: with the emulator's default mock executor this is a synthetic terminal log, "
        + "not Spark stdout; real job bytes are DGD6's k3d gate");
  }

  /** Report the output prefix without pretending Spark wrote anything. */
  private static void reportOutputBucket(Storage storage, DemoConfig cfg) {
    int objects = 0;
    for (Blob ignored : storage.list(cfg.outputBucket,
        Storage.BlobListOption.prefix(cfg.jobOutputPrefix)).iterateAll()) {
      objects++;
    }
    log("output prefix " + cfg.jobOutputUri() + " holds " + objects
        + " object(s) (DGD5's downscope probe is one of them, when this run wrote it)");
    log("this leg does not assert Spark output: the default executor is mock and writes no "
        + "records. DGD6's k3d gate asserts the real word count.");
  }

  // ─── Teardown: job -> virtual cluster -> GKE -> (buckets) ──────────────────

  private static void cleanup(
      ClusterControllerClient dataproc,
      ClusterManagerClient gke,
      Storage storage,
      DemoConfig cfg,
      String jobId) throws Exception {
    try {
      deleteJobs(cfg, jobId);
    } catch (NotFoundException noCluster) {
      log("no virtual cluster to list jobs from; nothing to delete");
    }

    boolean deletedVirtualCluster = deleteVirtualCluster(dataproc, cfg);
    if (deletedVirtualCluster && !cfg.cleanupOnly) {
      // Real Dataproc does not own the pools it reconciles; the GKE cluster
      // does. Prove that middle step here rather than only in prose.
      try {
        List<NodePool> survivors = listPools(gke, cfg);
        String names = survivors.stream().map(NodePool::getName).collect(Collectors.joining(", "));
        check("deleting the virtual cluster leaves the reconciled node pools in GKE",
            survivors.stream().anyMatch(p -> p.getName().equals(cfg.dataprocCluster + "-controller"))
                && survivors.stream().anyMatch(p -> p.getName().equals(cfg.dataprocCluster + "-spark")),
            "pools=" + names);
      } catch (NotFoundException gone) {
        log("GKE cluster is already gone, so its reconciled pools went with it");
      }
    }

    deleteGkeCluster(gke, cfg);

    if (cfg.deleteBuckets) {
      for (String bucket : List.of(cfg.inputBucket, cfg.outputBucket, cfg.stagingBucket)) {
        deleteBucket(storage, bucket);
      }
    }
  }

  private static void deleteJobs(DemoConfig cfg, String jobId) throws Exception {
    // List first: the first gate phase leaves a DONE job behind so the second
    // phase can prove it adopted existing infrastructure. Cleanup must remove
    // every job on the virtual cluster, not only the one this run submitted.
    List<String> ids = new ArrayList<>();
    try (JobControllerClient jobs = jobs(cfg)) {
      for (Job job : jobs.listJobs(ListJobsRequest.newBuilder()
          .setProjectId(cfg.project)
          .setRegion(cfg.region)
          .setClusterName(cfg.dataprocCluster)
          .build()).iterateAll()) {
        ids.add(job.getReference().getJobId());
      }
      if (jobId != null && !ids.contains(jobId)) {
        ids.add(jobId);
      }
      for (String id : ids) {
        try {
          jobs.deleteJob(cfg.project, cfg.region, id);
          log("deleted job " + id);
        } catch (NotFoundException gone) {
          // Already gone; cleanup is idempotent.
        } catch (FailedPreconditionException stillActive) {
          jobs.cancelJob(cfg.project, cfg.region, id);
          jobs.deleteJob(cfg.project, cfg.region, id);
          log("cancelled then deleted job " + id);
        }
      }
    }
  }

  private static boolean deleteVirtualCluster(ClusterControllerClient dataproc, DemoConfig cfg)
      throws Exception {
    try {
      dataproc.getCluster(GetClusterRequest.newBuilder()
          .setProjectId(cfg.project)
          .setRegion(cfg.region)
          .setClusterName(cfg.dataprocCluster)
          .build());
    } catch (NotFoundException gone) {
      log("virtual cluster already gone");
      return false;
    }

    OperationFuture<Empty, ClusterOperationMetadata> future = dataproc.deleteClusterAsync(
        DeleteClusterRequest.newBuilder()
            .setProjectId(cfg.project)
            .setRegion(cfg.region)
            .setClusterName(cfg.dataprocCluster)
            .build());
    log("deleting virtual cluster (operation " + future.getName() + ")");
    awaitDataprocOperation(future, "delete virtual cluster", cfg);

    for (int attempt = 1; attempt <= cfg.pollAttempts; attempt++) {
      try {
        ClusterStatus.State state = dataproc.getCluster(GetClusterRequest.newBuilder()
            .setProjectId(cfg.project)
            .setRegion(cfg.region)
            .setClusterName(cfg.dataprocCluster)
            .build()).getStatus().getState();
        sleep(cfg, "virtual cluster is " + state);
      } catch (NotFoundException gone) {
        check("virtual cluster " + cfg.dataprocCluster + " is deleted", true, null);
        return true;
      }
    }
    throw new IllegalStateException("virtual cluster " + cfg.dataprocCluster + " did not disappear");
  }

  private static void deleteGkeCluster(ClusterManagerClient gke, DemoConfig cfg) {
    String name = cfg.gkeClusterTarget();
    try {
      gke.getCluster(com.google.container.v1.GetClusterRequest.newBuilder().setName(name).build());
    } catch (NotFoundException gone) {
      log("GKE cluster already gone");
      return;
    }
    com.google.container.v1.Operation operation = gke.deleteCluster(
        com.google.container.v1.DeleteClusterRequest.newBuilder().setName(name).build());
    log("deleting GKE cluster (operation " + operation.getName() + ")");
    awaitGkeOperation(gke, operation, "delete GKE cluster", cfg);

    for (int attempt = 1; attempt <= cfg.pollAttempts; attempt++) {
      try {
        gke.getCluster(com.google.container.v1.GetClusterRequest.newBuilder().setName(name).build());
        sleep(cfg, "GKE cluster still visible");
      } catch (NotFoundException gone) {
        check("GKE cluster " + name + " is deleted", true, null);
        return;
      }
    }
    throw new IllegalStateException("GKE cluster " + name + " did not disappear");
  }

  private static void deleteBucket(Storage storage, String bucket) {
    if (storage.get(bucket) == null) {
      return;
    }
    for (Blob blob : storage.list(bucket).iterateAll()) {
      storage.delete(blob.getBlobId());
    }
    storage.delete(bucket);
    log("deleted bucket gs://" + bucket);
  }

  // ─── Nothing is left ───────────────────────────────────────────────────────

  private static void assertClean(
      ClusterControllerClient dataproc,
      ClusterManagerClient gke,
      Storage storage,
      DemoConfig cfg,
      String jobId) throws Exception {
    try {
      dataproc.getCluster(GetClusterRequest.newBuilder()
          .setProjectId(cfg.project)
          .setRegion(cfg.region)
          .setClusterName(cfg.dataprocCluster)
          .build());
      check("virtual cluster is gone", false, "getCluster still returned it");
    } catch (NotFoundException gone) {
      check("virtual cluster is gone", true, null);
    }

    try {
      gke.getCluster(com.google.container.v1.GetClusterRequest.newBuilder().setName(cfg.gkeClusterTarget()).build());
      check("GKE cluster is gone", false, "getCluster still returned it");
    } catch (NotFoundException gone) {
      check("GKE cluster is gone", true, null);
    }

    if (jobId != null) {
      try (JobControllerClient jobs = jobs(cfg)) {
        jobs.getJob(GetJobRequest.newBuilder()
            .setProjectId(cfg.project)
            .setRegion(cfg.region)
            .setJobId(jobId)
            .build());
        check("job " + jobId + " is gone", false, "getJob still returned it");
      } catch (NotFoundException gone) {
        check("job " + jobId + " is gone", true, null);
      } catch (Exception other) {
        check("job " + jobId + " is gone", false, other.toString());
      }
    }

    int remainingJobs = 0;
    try (JobControllerClient jobs = jobs(cfg)) {
      for (Job ignored : jobs.listJobs(ListJobsRequest.newBuilder()
          .setProjectId(cfg.project)
          .setRegion(cfg.region)
          .setClusterName(cfg.dataprocCluster)
          .build()).iterateAll()) {
        remainingJobs++;
      }
    }
    check("no jobs remain on the virtual cluster", remainingJobs == 0,
        remainingJobs + " job(s)");

    try {
      List<NodePool> pools = listPools(gke, cfg);
      check("no reconciled node pools remain", pools.isEmpty(), "pools=" + pools.size());
    } catch (NotFoundException gone) {
      check("no reconciled node pools remain", true, null);
    }

    if (cfg.deleteBuckets) {
      for (String bucket : List.of(cfg.inputBucket, cfg.outputBucket, cfg.stagingBucket)) {
        check("bucket gs://" + bucket + " is gone", storage.get(bucket) == null, "still present");
      }
    }
  }

  // ─── Helpers ───────────────────────────────────────────────────────────────

  private static void logConfig(DemoConfig cfg) {
    log("project=" + cfg.project + " region=" + cfg.region + " gke_location=" + cfg.gkeLocation);
    log("gke_cluster=" + cfg.gkeCluster + " dataproc_cluster=" + cfg.dataprocCluster
        + " namespace=" + cfg.namespace);
    log("buckets: input=gs://" + cfg.inputBucket + " output=gs://" + cfg.outputBucket
        + " staging=gs://" + cfg.stagingBucket + " (owned by " + cfg.storageProject + ")");
    log("downscoped tokens: input=" + (cfg.inputToken == null ? "mint at STS" : "injected")
        + " output=" + (cfg.outputToken == null ? "mint at STS" : "injected"));
    log("dataproc+gke transport=gRPC plaintext " + cfg.grpcTarget()
        + "; storage transport=REST " + cfg.restEndpoint);
    log("teardown=" + cfg.teardown + " delete_buckets=" + cfg.deleteBuckets
        + " cleanup_only=" + cfg.cleanupOnly);
  }

  // Package-private so DGD5's boundary phase (DownscopedAccess) reports its
  // assertions in exactly the same PASS/FAIL shape as the lifecycle's.
  static void check(String name, boolean ok, String detail) {
    if (ok) {
      System.out.println("  [PASS] " + name);
      return;
    }
    System.out.println("  [FAIL] " + name + (detail == null ? "" : " (" + detail + ")"));
    throw new IllegalStateException("assertion failed: " + name);
  }

  private static void sleep(DemoConfig cfg, String reason) {
    System.out.println("  ... " + reason + " (retry in " + cfg.pollSeconds + "s)");
    try {
      Thread.sleep(cfg.pollSeconds * 1000L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while waiting", e);
    }
  }

  private static String[] splitGs(String uri) {
    if (uri == null || !uri.startsWith("gs://")) {
      throw new IllegalArgumentException("not a gs:// URI: " + uri);
    }
    String rest = uri.substring("gs://".length());
    int slash = rest.indexOf('/');
    if (slash <= 0 || slash == rest.length() - 1) {
      throw new IllegalArgumentException("malformed gs:// URI: " + uri);
    }
    return new String[]{rest.substring(0, slash), rest.substring(slash + 1)};
  }

  static void log(String message) {
    System.out.println("== " + message);
  }

  private DpgkeLifecycle() {
  }
}
