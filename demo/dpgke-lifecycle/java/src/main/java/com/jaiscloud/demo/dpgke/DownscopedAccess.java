package com.jaiscloud.demo.dpgke;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * DGD5: a workload's short-lived credentials, and the proof that their boundary
 * actually denies.
 *
 * <p>The demo's buckets are otherwise reachable with any plain token, which
 * proves nothing about the path a customer uses to hand a workload scoped
 * access. Here the app does what a customer's job does: it exchanges a source
 * access token at the emulator's STS endpoint ({@code POST /v1/token}, RFC 8693
 * token-exchange) for <b>two</b> downscoped tokens built from Credential Access
 * Boundaries — one that may read the input bucket, one that may write the output
 * bucket — and then uses each token as the credential on the wire through a
 * separate {@code Storage} client.
 *
 * <p>The boundary is the thing under test, so the positive alone is not enough.
 * Cloud Storage does not verify the emulator's token signatures (it trusts the
 * {@code access_boundary} claim in the JWT payload, see
 * {@code internal/gcp/downscope}), so a demo that only shows 200s proves
 * nothing: a boundary that is never enforced looks identical to a working one.
 * Every denial below is therefore asserted, and the last assertion reads the
 * same object with a plain (non-downscoped) token, so a 403 cannot be an
 * artefact of a wrong object name or a broken client.
 *
 * <p><b>What this cannot prove:</b> the PySpark job this app submits never
 * fetches a credential or touches GCS — the job reads and writes with whatever
 * the executor gives it, while this class is the submitter's credential path,
 * the same code shape a driver would run. DGD6's k3d gate moves the whole app
 * into a pod in the cluster, so the exchange and the denials happen over the
 * in-cluster Service; it still does not make the Spark driver the token holder.
 */
final class DownscopedAccess {

  /** The documented CAB permission for object bytes/metadata reads. */
  static final String INPUT_PERMISSION = "inRole:roles/storage.legacyObjectReader";
  /**
   * The documented CAB permission for object create/overwrite. It also covers
   * list and delete, which is why the reverse assertion below is about
   * <em>read</em>, not about being write-only in every direction.
   */
  static final String OUTPUT_PERMISSION = "inRole:roles/storage.legacyBucketWriter";

  private static final String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange";
  private static final String TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token";
  /** Path the emulator's OAuth2/STS service is mounted on (internal/gcp/auth). */
  private static final String TOKEN_PATH = "/v1/token";

  /** Bytes the write boundary writes, so the denials below target a real object. */
  static final String PROBE =
      "written with a downscoped credential scoped to the output prefix\n";

  private DownscopedAccess() {
  }

  /**
   * The whole phase: mint the pair, do the work with each credential, then
   * assert that each boundary denies what it must.
   *
   * @param cfg the demo configuration
   * @param admin the unrestricted client this run already uses for setup — it
   *     is only used to observe what the downscoped clients did
   */
  static void assertBuckets(DemoConfig cfg, Storage admin) throws Exception {
    String inputScope = directoryPrefix(cfg.jobInputObject);
    String outputScope = normalizePrefix(cfg.jobOutputPrefix);
    String inputOptions = boundaryOptions(cfg.inputBucket, INPUT_PERMISSION, inputScope);
    String outputOptions = boundaryOptions(cfg.outputBucket, OUTPUT_PERMISSION, outputScope);

    log("DGD5: two Credential Access Boundaries, exchanged at "
        + cfg.restEndpoint + TOKEN_PATH);
    log("input boundary: gs://" + cfg.inputBucket + "/" + inputScope + " as "
        + INPUT_PERMISSION);
    log("output boundary: gs://" + cfg.outputBucket + "/" + outputScope + " as "
        + OUTPUT_PERMISSION);

    // The source credential a real workload gets from the metadata server /
    // Workload Identity. Both downscoped tokens are minted from it, which is
    // what makes them short-lived and identity-preserving rather than invented.
    String sourceToken = sourceToken(cfg);
    String inputToken = boundaryToken(cfg, cfg.inputToken, sourceToken, inputOptions, "input");
    String outputToken = boundaryToken(cfg, cfg.outputToken, sourceToken, outputOptions, "output");

    Storage reader = storageWith(cfg, inputToken);
    Storage writer = storageWith(cfg, outputToken);
    Storage plain = storageWith(cfg, sourceToken);

    String probe = outputScope + "downscope-probe.txt";
    String denied = outputScope + "denied-write.txt";

    // 1. The work: read the input object with the read boundary...
    int read = status(() -> reader.get(BlobId.of(cfg.inputBucket, cfg.jobInputObject)));
    check("input read under the input boundary is 200", read == 200, "HTTP " + read);

    // 2. ...and write the job's output with the write boundary.
    int wrote = status(() -> writer.create(blob(cfg.outputBucket, probe), PROBE.getBytes(
        StandardCharsets.UTF_8)));
    check("output write under the output boundary is 200", wrote == 200, "HTTP " + wrote);

    // 3. The witness: the same object, same client shape, plain token. If this
    //    were not 200, every 403 below could be a typo instead of a boundary.
    int plainRead = status(() -> plain.get(BlobId.of(cfg.outputBucket, probe)));
    check("output read with the plain token is 200", plainRead == 200, "HTTP " + plainRead);

    // 4. The reverse denial: the write boundary grants create/overwrite, not
    //    read, so the writer cannot read back what it just wrote.
    int reverseRead = status(() -> writer.get(BlobId.of(cfg.outputBucket, probe)));
    check("output read under the output boundary is 403", reverseRead == 403, "HTTP " + reverseRead);

    // 5. The bucket denial: the input boundary names the input bucket, so an
    //    object in the output bucket is outside it — 403, not 404.
    int crossRead = status(() -> reader.get(BlobId.of(cfg.outputBucket, probe)));
    check("output read under the input boundary is 403", crossRead == 403, "HTTP " + crossRead);

    int crossWrite = status(() -> reader.create(blob(cfg.outputBucket, denied), PROBE.getBytes(
        StandardCharsets.UTF_8)));
    check("output write under the input boundary is 403", crossWrite == 403, "HTTP " + crossWrite);

    // The refusal has to be a refusal, not a half-applied write.
    check("the denied write created no object in the output bucket",
        admin.get(BlobId.of(cfg.outputBucket, denied)) == null, denied + " exists");

    // 6. The mirror: the write boundary does not reach the input bucket either.
    int inputWrite = status(() -> writer.create(blob(cfg.inputBucket, inputScope + "denied-write.txt"),
        PROBE.getBytes(StandardCharsets.UTF_8)));
    check("input write under the output boundary is 403", inputWrite == 403, "HTTP " + inputWrite);

    // The probe lives inside the job's output prefix on purpose: the boundary a
    // workload is handed is scoped to the prefix it may write. Real Spark's
    // output committer refuses an output prefix that already exists, so the
    // temporary probe must not outlive the credential phase. This only showed
    // up once DGD6 ran a real driver: the mock executor never writes, so the
    // leftover object was invisible to DGD5's own gate.
    admin.delete(BlobId.of(cfg.outputBucket, probe));
    check("the downscope probe is cleared so the job's output prefix starts empty",
        admin.get(BlobId.of(cfg.outputBucket, probe)) == null, probe + " still exists");

    log("DGD5: the boundary is enforced, not just advertised — "
        + "the same object reads 200 for the plain token and 403 for the two boundaries");
  }

  /**
   * The Credential Access Boundary document {@code CredentialAccessBoundary
   * .toJson()} would put in the {@code options} form field. Building it here
   * keeps the demo honest about the documented shape: an access boundary
   * applies to a bucket's full resource name, and the object restriction lives
   * in the availability condition, never in {@code availableResource}
   * (https://cloud.google.com/iam/docs/downscoping-short-lived-credentials).
   *
   * @param objectPrefix object-name prefix the rule admits; empty/null means the
   *     whole bucket and renders no availability condition
   */
  static String boundaryOptions(String bucket, String permission, String objectPrefix) {
    String resource = "//storage.googleapis.com/projects/_/buckets/" + bucket;
    JsonArray permissions = new JsonArray();
    permissions.add(permission);

    JsonObject rule = new JsonObject();
    rule.addProperty("availableResource", resource);
    rule.add("availablePermissions", permissions);
    String prefix = normalizePrefix(objectPrefix);
    if (!prefix.isEmpty()) {
      // The documented expression names the object prefix in the resource-name
      // form the GCS provider enforces; the emulator's parser accepts exactly
      // this shape and rejects a prefix that does not match availableResource.
      JsonObject condition = new JsonObject();
      condition.addProperty("expression",
          "resource.name.startsWith('projects/_/buckets/" + bucket + "/objects/" + prefix + "')");
      rule.add("availabilityCondition", condition);
    }

    JsonArray rules = new JsonArray();
    rules.add(rule);
    JsonObject boundary = new JsonObject();
    boundary.add("accessBoundaryRules", rules);
    JsonObject document = new JsonObject();
    document.add("accessBoundary", boundary);
    // A JsonObject keeps insertion order, so two renders of the same boundary
    // are byte-identical — the unit test pins the exact document.
    return document.toString();
  }

  /**
   * Mint one downscoped token through the emulator's STS exchange. The response
   * is an opaque bearer token; it is never logged, only its lifetime is.
   */
  static String exchange(DemoConfig cfg, String subjectToken, String options) throws Exception {
    Map<String, String> form = new LinkedHashMap<>();
    form.put("grant_type", GRANT_TYPE);
    form.put("subject_token_type", TOKEN_TYPE);
    form.put("requested_token_type", TOKEN_TYPE);
    form.put("subject_token", subjectToken);
    form.put("options", options);

    HttpResponse<String> response = postForm(cfg, form);
    if (response.statusCode() != 200) {
      throw new IllegalStateException("STS token exchange failed with HTTP "
          + response.statusCode() + ": " + response.body());
    }
    JsonObject body = JsonParser.parseString(response.body()).getAsJsonObject();
    String token = body.has("access_token") ? body.get("access_token").getAsString() : "";
    if (token.isEmpty()) {
      throw new IllegalStateException("STS token exchange returned no access_token: " + response.body());
    }
    log("STS exchange issued a boundary token (issued_token_type="
        + (body.has("issued_token_type") ? body.get("issued_token_type").getAsString() : "?")
        + ", expires_in=" + (body.has("expires_in") ? body.get("expires_in").getAsString() : "?")
        + "s)");
    return token;
  }

  /**
   * The source access token: the one the chart injects as
   * {@code GOOGLE_OAUTH_ACCESS_TOKEN} when it exists, otherwise one minted at
   * the emulator's token endpoint. A real workload gets this from the metadata
   * server or Workload Identity; the emulator has no token store, so its refresh
   * grant mints an ordinary access token for any non-empty refresh token.
   */
  static String sourceToken(DemoConfig cfg) throws Exception {
    String injected = System.getenv("GOOGLE_OAUTH_ACCESS_TOKEN");
    if (injected != null && !injected.isBlank()) {
      log("source token: the credential provided by the environment");
      return injected.trim();
    }
    Map<String, String> form = new LinkedHashMap<>();
    form.put("grant_type", "refresh_token");
    form.put("refresh_token", "dpgke-demo");
    HttpResponse<String> response = postForm(cfg, form);
    if (response.statusCode() != 200) {
      throw new IllegalStateException("could not mint a source access token: HTTP "
          + response.statusCode() + ": " + response.body());
    }
    JsonObject body = JsonParser.parseString(response.body()).getAsJsonObject();
    String token = body.has("access_token") ? body.get("access_token").getAsString() : "";
    if (token.isEmpty()) {
      throw new IllegalStateException("token endpoint returned no access_token");
    }
    log("source token: an ordinary (non-downscoped) access token minted at "
        + cfg.restEndpoint + TOKEN_PATH);
    return token;
  }

  /**
   * A Storage client whose credential is exactly {@code token}. The Storage
   * client is the REST/JSON one, and its options take a {@code Credentials}
   * rather than gax's {@code CredentialsProvider}; a {@link GoogleCredentials}
   * built around one {@link AccessToken} is that fixed credential — no refresh,
   * no ambient ADC, so the token really is what travels on the wire.
   */
  static Storage storageWith(DemoConfig cfg, String token) {
    GoogleCredentials credentials = GoogleCredentials.create(
        new AccessToken(token, Date.from(Instant.now().plusSeconds(3600))));
    return StorageOptions.newBuilder()
        .setProjectId(cfg.storageProject)
        .setHost(cfg.restEndpoint)
        .setCredentials(credentials)
        .build()
        .getService();
  }

  /** {@code "input/words.txt"} → {@code "input/"}; a bare name has no prefix. */
  static String directoryPrefix(String object) {
    int slash = object.lastIndexOf('/');
    return slash < 0 ? "" : object.substring(0, slash + 1);
  }

  private static String normalizePrefix(String prefix) {
    if (prefix == null || prefix.isBlank()) {
      return "";
    }
    String trimmed = prefix.trim();
    return trimmed.endsWith("/") ? trimmed : trimmed + "/";
  }

  /** Use the injected token, or exchange for one, and say which happened. */
  private static String boundaryToken(
      DemoConfig cfg, String injected, String source, String options, String label)
      throws Exception {
    if (injected != null && !injected.isBlank()) {
      log(label + " boundary token: injected by the chart ("
          + injected.length() + " chars, not logged)");
      return injected.trim();
    }
    log(label + " boundary token: minting through the STS exchange");
    return exchange(cfg, source, options);
  }

  private static HttpResponse<String> postForm(DemoConfig cfg, Map<String, String> form)
      throws Exception {
    StringBuilder body = new StringBuilder();
    for (Map.Entry<String, String> entry : form.entrySet()) {
      if (body.length() > 0) {
        body.append('&');
      }
      body.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8))
          .append('=')
          .append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
    }
    HttpRequest request = HttpRequest.newBuilder(URI.create(cfg.restEndpoint + TOKEN_PATH))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8))
        .build();
    // A JDK HttpClient rather than the Storage client: this is the OAuth2/STS
    // surface, not a GCS operation, and the demo should not pretend a storage
    // client can exchange a token.
    return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
        .send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static BlobInfo blob(String bucket, String object) {
    return BlobInfo.newBuilder(BlobId.of(bucket, object))
        .setContentType("text/plain; charset=utf-8")
        .build();
  }

  /**
   * The HTTP status one Storage call produced. 200 when it succeeded, the
   * server's code when it did not — a 403 is the boundary refusing, anything
   * else (a transport error) is a bug in the demo and must fail loudly.
   */
  private static int status(GcsCall call) {
    try {
      call.run();
      return 200;
    } catch (StorageException e) {
      return e.getCode();
    } catch (Exception e) {
      throw new IllegalStateException("GCS request failed before an HTTP status", e);
    }
  }

  @FunctionalInterface
  private interface GcsCall {
    void run() throws Exception;
  }

  private static void check(String name, boolean ok, String detail) {
    DpgkeLifecycle.check(name, ok, detail);
  }

  private static void log(String message) {
    DpgkeLifecycle.log(message);
  }
}
