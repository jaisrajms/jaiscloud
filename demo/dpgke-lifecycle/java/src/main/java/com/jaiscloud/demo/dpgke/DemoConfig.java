package com.jaiscloud.demo.dpgke;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The demo's configuration surface, read from the environment so the Helm leg
 * (DGD4) can inject it without editing code.
 *
 * <p>Every default mirrors {@code demo/dpgke-lifecycle/terraform/variables.tf}
 * and its outputs, so a Java run against infrastructure Terraform created needs
 * no environment at all beyond the endpoints. The names are documented in
 * {@code README.md}; {@link #fromEnv()} is the only place that reads the
 * process environment, which is what the unit test pins.
 */
final class DemoConfig {

  static final String DEFAULT_PROJECT = "dpgke-demo";
  static final String DEFAULT_REGION = "us-central1";
  static final String DEFAULT_GKE_LOCATION = "us-central1-a";
  static final String DEFAULT_GKE_CLUSTER = "dpgke-gke";
  static final String DEFAULT_DATAPROC_CLUSTER = "dpgke-dataproc";
  static final String DEFAULT_NAMESPACE = "dpgke-demo";
  static final String DEFAULT_SPARK_RELEASE = "3.5-dataproc-28";
  static final String DEFAULT_MACHINE_TYPE = "e2-small";
  static final String DEFAULT_JOB_CODE_OBJECT = "code/wordcount.py";
  static final String DEFAULT_JOB_INPUT_OBJECT = "input/words.txt";
  static final String DEFAULT_JOB_OUTPUT_PREFIX = "wordcount";
  static final int DEFAULT_POLL_SECONDS = 2;
  static final int DEFAULT_POLL_ATTEMPTS = 60;

  final String project;
  final String region;
  final String gkeLocation;
  final String gkeCluster;
  final String dataprocCluster;
  final String namespace;

  /** Project that owns the three buckets; equals {@link #project} by default. */
  final String storageProject;
  final String inputBucket;
  final String outputBucket;
  final String stagingBucket;

  /** REST base for the Storage client (the Java Storage client speaks HTTP/JSON). */
  final String restEndpoint;
  /** gRPC host for the Dataproc and GKE clients (they default to gRPC). */
  final String grpcEndpoint;
  final int grpcPort;

  final String sparkRelease;
  final String machineType;
  final int minNodes;
  final int maxNodes;

  final String jobCodeObject;
  final String jobInputObject;
  final String jobOutputPrefix;
  /** Local copy of the PySpark file; null when the object is already in GCS. */
  final String sparkScript;

  /**
   * DGD5: the two downscoped tokens, when a minting stage injected them. Null
   * means this run mints the pair itself through the emulator's STS exchange at
   * {@code /v1/token}; the chart's matching keys are optional and absent when
   * it does not inject them.
   */
  final String inputToken;
  final String outputToken;

  final int pollSeconds;
  final int pollAttempts;

  /** Delete the clusters when the lifecycle finishes (phase 2 of the gate). */
  final boolean teardown;
  /**
   * Delete the three buckets on teardown. Default false because Terraform owns
   * them in the full demo; the standalone gate sets it because nothing else does.
   */
  final boolean deleteBuckets;
  /** Only delete what is there, do not create or submit (the failure-path safety net). */
  final boolean cleanupOnly;

  private DemoConfig(
      String project,
      String region,
      String gkeLocation,
      String gkeCluster,
      String dataprocCluster,
      String namespace,
      String storageProject,
      String inputBucket,
      String outputBucket,
      String stagingBucket,
      String restEndpoint,
      String grpcEndpoint,
      int grpcPort,
      String sparkRelease,
      String machineType,
      int minNodes,
      int maxNodes,
      String jobCodeObject,
      String jobInputObject,
      String jobOutputPrefix,
      String sparkScript,
      String inputToken,
      String outputToken,
      int pollSeconds,
      int pollAttempts,
      boolean teardown,
      boolean deleteBuckets,
      boolean cleanupOnly) {
    this.project = project;
    this.region = region;
    this.gkeLocation = gkeLocation;
    this.gkeCluster = gkeCluster;
    this.dataprocCluster = dataprocCluster;
    this.namespace = namespace;
    this.storageProject = storageProject;
    this.inputBucket = inputBucket;
    this.outputBucket = outputBucket;
    this.stagingBucket = stagingBucket;
    this.restEndpoint = restEndpoint;
    this.grpcEndpoint = grpcEndpoint;
    this.grpcPort = grpcPort;
    this.sparkRelease = sparkRelease;
    this.machineType = machineType;
    this.minNodes = minNodes;
    this.maxNodes = maxNodes;
    this.jobCodeObject = jobCodeObject;
    this.jobInputObject = jobInputObject;
    this.jobOutputPrefix = jobOutputPrefix;
    this.sparkScript = sparkScript;
    this.inputToken = inputToken;
    this.outputToken = outputToken;
    this.pollSeconds = pollSeconds;
    this.pollAttempts = pollAttempts;
    this.teardown = teardown;
    this.deleteBuckets = deleteBuckets;
    this.cleanupOnly = cleanupOnly;
  }

  static DemoConfig fromEnv() {
    String project = env("DGD3_PROJECT", DEFAULT_PROJECT);
    String region = env("DGD3_REGION", DEFAULT_REGION);
    String storageProject = env("DGD3_STORAGE_PROJECT", project);

    return new DemoConfig(
        project,
        region,
        env("DGD3_GKE_LOCATION", DEFAULT_GKE_LOCATION),
        env("DGD3_GKE_CLUSTER", DEFAULT_GKE_CLUSTER),
        env("DGD3_DATAPROC_CLUSTER", DEFAULT_DATAPROC_CLUSTER),
        env("DGD3_NAMESPACE", DEFAULT_NAMESPACE),
        storageProject,
        // These are exactly the names terraform/main.tf derives from
        // bucket_prefix (= project), so a Terraform-provisioned run needs no
        // overrides. Helm injects whatever the module actually created.
        env("DGD3_INPUT_BUCKET", project + "-input"),
        env("DGD3_OUTPUT_BUCKET", project + "-output"),
        env("DGD3_STAGING_BUCKET", project + "-staging"),
        trimSlash(env("DGD3_REST_ENDPOINT", "http://localhost:8080")),
        env("DGD3_GRPC_ENDPOINT", "localhost"),
        envInt("DGD3_GRPC_PORT", 8081),
        env("DGD3_SPARK_RELEASE", DEFAULT_SPARK_RELEASE),
        env("DGD3_MACHINE_TYPE", DEFAULT_MACHINE_TYPE),
        envInt("DGD3_MIN_NODES", 1),
        envInt("DGD3_MAX_NODES", 3),
        env("DGD3_JOB_CODE_OBJECT", DEFAULT_JOB_CODE_OBJECT),
        env("DGD3_JOB_INPUT_OBJECT", DEFAULT_JOB_INPUT_OBJECT),
        env("DGD3_JOB_OUTPUT_PREFIX", DEFAULT_JOB_OUTPUT_PREFIX),
        resolveSparkScript(),
        // A blank token is not a token: the app falls back to the STS exchange
        // rather than sending "Bearer " to GCS and failing on a 401/403 that
        // looks like a boundary denial.
        envOrNull("DGD5_INPUT_TOKEN"),
        envOrNull("DGD5_OUTPUT_TOKEN"),
        envInt("DGD3_POLL_SECONDS", DEFAULT_POLL_SECONDS),
        envInt("DGD3_POLL_ATTEMPTS", DEFAULT_POLL_ATTEMPTS),
        envBool("DGD3_TEARDOWN", true),
        envBool("DGD3_DELETE_BUCKETS", false),
        envBool("DGD3_CLEANUP_ONLY", false));
  }

  /** {@code gs://} target Dataproc takes: the zonal cluster, not the region. */
  String gkeClusterTarget() {
    return "projects/" + project + "/locations/" + gkeLocation + "/clusters/" + gkeCluster;
  }

  /**
   * Parent of a {@code ListNodePools} call. Discovery defines it as the GKE
   * cluster ({@code projects/{p}/locations/{l}/clusters/{c}}), not the
   * {@code .../nodePools} collection.
   */
  String gkeNodePoolsParent() {
    return gkeClusterTarget();
  }

  /**
   * Full resource name of one reconciled node pool. Dataproc requires the full
   * form in {@code nodePoolTarget.nodePool}; Terraform expands its short ids,
   * the official client does not.
   */
  String nodePoolName(String suffix) {
    return gkeClusterTarget() + "/nodePools/" + dataprocCluster + "-" + suffix;
  }

  /** {@code host:port} the gRPC clients connect to. */
  String grpcTarget() {
    return grpcEndpoint + ":" + grpcPort;
  }

  String jobCodeUri() {
    return "gs://" + inputBucket + "/" + jobCodeObject;
  }

  String jobInputUri() {
    return "gs://" + inputBucket + "/" + jobInputObject;
  }

  String jobOutputUri() {
    return "gs://" + outputBucket + "/" + jobOutputPrefix;
  }

  // ─── env helpers ───────────────────────────────────────────────────────────

  private static String env(String key, String def) {
    String value = System.getenv(key);
    return value == null || value.isBlank() ? def : value.trim();
  }

  private static String envOrNull(String key) {
    String value = System.getenv(key);
    return value == null || value.isBlank() ? null : value.trim();
  }

  private static int envInt(String key, int def) {
    String value = System.getenv(key);
    if (value == null || value.isBlank()) {
      return def;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be an integer, got: " + value, e);
    }
  }

  private static boolean envBool(String key, boolean def) {
    String value = System.getenv(key);
    if (value == null || value.isBlank()) {
      return def;
    }
    switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
      case "1":
      case "true":
      case "yes":
      case "on":
        return true;
      case "0":
      case "false":
      case "no":
      case "off":
        return false;
      default:
        throw new IllegalArgumentException(key + " must be a boolean, got: " + value);
    }
  }

  private static String trimSlash(String s) {
    return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
  }

  /**
   * Locate {@code ../spark/wordcount.py} whether the app runs from the Maven
   * module (exec:java) or from the repository root (a packaged jar). Returns
   * null when neither exists: a run against Terraform-provisioned buckets never
   * needs the upload, so the missing file is only an error if the object is.
   */
  private static String resolveSparkScript() {
    String configured = env("DGD3_SPARK_SCRIPT", "");
    if (!configured.isEmpty()) {
      return configured;
    }
    Path[] candidates = {
      Path.of("..", "spark", "wordcount.py"),
      Path.of("demo", "dpgke-lifecycle", "spark", "wordcount.py"),
    };
    for (Path candidate : candidates) {
      if (Files.isReadable(candidate)) {
        return candidate.toString();
      }
    }
    return null;
  }
}
