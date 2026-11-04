package com.jaiscloud.demo.dpgke;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Pins the config surface that Helm (DGD4) will inject and the resource names
 * the lifecycle builds from it. No emulator is needed, so this runs in the
 * normal Maven build; the emulator run is {@code run.sh}.
 */
class DemoConfigTest {

  @Test
  void derivedNamesFollowTheTerraformModuleShape() {
    DemoConfig cfg = DemoConfig.fromEnv();

    assertEquals(
        "projects/" + cfg.project + "/locations/" + cfg.gkeLocation + "/clusters/" + cfg.gkeCluster,
        cfg.gkeClusterTarget());
    assertEquals(cfg.gkeClusterTarget(), cfg.gkeNodePoolsParent());
    assertEquals(
        cfg.gkeClusterTarget() + "/nodePools/" + cfg.dataprocCluster + "-controller",
        cfg.nodePoolName("controller"));
    assertEquals(
        cfg.gkeClusterTarget() + "/nodePools/" + cfg.dataprocCluster + "-spark",
        cfg.nodePoolName("spark"));
  }

  @Test
  void grpcTargetJoinsHostAndPort() {
    DemoConfig cfg = DemoConfig.fromEnv();
    assertEquals(cfg.grpcEndpoint + ":" + cfg.grpcPort, cfg.grpcTarget());
  }

  @Test
  void jobUrisUseTheConfiguredBuckets() {
    DemoConfig cfg = DemoConfig.fromEnv();
    assertEquals("gs://" + cfg.inputBucket + "/" + cfg.jobCodeObject, cfg.jobCodeUri());
    assertEquals("gs://" + cfg.inputBucket + "/" + cfg.jobInputObject, cfg.jobInputUri());
    assertEquals("gs://" + cfg.outputBucket + "/" + cfg.jobOutputPrefix, cfg.jobOutputUri());
  }

  @Test
  void downscopedTokensAreAbsentUntilInjected() {
    // DGD5: absent means "mint them here"; a blank value from a chart-rendered
    // Secret must mean the same, never an empty bearer token.
    Assumptions.assumeTrue(System.getenv("DGD5_INPUT_TOKEN") == null, "DGD5_INPUT_TOKEN is set");
    Assumptions.assumeTrue(System.getenv("DGD5_OUTPUT_TOKEN") == null, "DGD5_OUTPUT_TOKEN is set");

    DemoConfig cfg = DemoConfig.fromEnv();
    assertNull(cfg.inputToken);
    assertNull(cfg.outputToken);
  }

  @Test
  void defaultsMatchTheTerraformModule() {
    // run.sh exports the same variables, so only assert the defaults when the
    // process environment has not overridden them.
    Assumptions.assumeTrue(System.getenv("DGD3_PROJECT") == null, "DGD3_PROJECT is set");
    Assumptions.assumeTrue(System.getenv("DGD3_GKE_LOCATION") == null, "DGD3_GKE_LOCATION is set");

    DemoConfig cfg = DemoConfig.fromEnv();
    assertEquals(DemoConfig.DEFAULT_PROJECT, cfg.project);
    assertEquals(DemoConfig.DEFAULT_REGION, cfg.region);
    assertEquals(DemoConfig.DEFAULT_GKE_LOCATION, cfg.gkeLocation);
    assertEquals(DemoConfig.DEFAULT_GKE_CLUSTER, cfg.gkeCluster);
    assertEquals(DemoConfig.DEFAULT_DATAPROC_CLUSTER, cfg.dataprocCluster);
    assertEquals(DemoConfig.DEFAULT_NAMESPACE, cfg.namespace);
    assertEquals(cfg.project + "-input", cfg.inputBucket);
    assertEquals(cfg.project + "-output", cfg.outputBucket);
    assertEquals(cfg.project + "-staging", cfg.stagingBucket);
    assertEquals(DemoConfig.DEFAULT_SPARK_RELEASE, cfg.sparkRelease);
    assertEquals(8081, cfg.grpcPort);
    assertTrue(cfg.teardown, "teardown defaults to true");
  }
}
