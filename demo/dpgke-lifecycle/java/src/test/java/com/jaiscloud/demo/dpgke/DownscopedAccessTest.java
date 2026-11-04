package com.jaiscloud.demo.dpgke;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Pins the two Credential Access Boundary documents DGD5 sends as the STS
 * {@code options} field, and the object prefixes it derives them from. No
 * emulator is needed: the boundary is a JSON contract, and a typo in a
 * permission or an expression would otherwise only show up as a 400 from the
 * exchange in {@code run.sh}.
 */
class DownscopedAccessTest {

  @Test
  void inputBoundaryIsReadOnlyOnTheInputObjectPrefix() {
    String options = DownscopedAccess.boundaryOptions(
        "dpgke-demo-input", DownscopedAccess.INPUT_PERMISSION, "input/");

    assertEquals(
        "{\"accessBoundary\":{\"accessBoundaryRules\":[{"
            + "\"availableResource\":\"//storage.googleapis.com/projects/_/buckets/dpgke-demo-input\","
            + "\"availablePermissions\":[\"inRole:roles/storage.legacyObjectReader\"],"
            + "\"availabilityCondition\":{\"expression\":"
            + "\"resource.name.startsWith('projects/_/buckets/dpgke-demo-input/objects/input/')\""
            + "}}]}}",
        options);
    // The documented CAB surface has no wildcard: a read grant is a role name,
    // and object restrictions belong in the condition, not the resource.
    assertFalse(options.contains("\"availableResource\":\"//storage.googleapis.com/projects/_/buckets/\""));
  }

  @Test
  void outputBoundaryWritesTheOutputObjectPrefixAndCannotRead() {
    String options = DownscopedAccess.boundaryOptions(
        "dpgke-demo-output", DownscopedAccess.OUTPUT_PERMISSION, "wordcount/");

    assertEquals(
        "{\"accessBoundary\":{\"accessBoundaryRules\":[{"
            + "\"availableResource\":\"//storage.googleapis.com/projects/_/buckets/dpgke-demo-output\","
            + "\"availablePermissions\":[\"inRole:roles/storage.legacyBucketWriter\"],"
            + "\"availabilityCondition\":{\"expression\":"
            + "\"resource.name.startsWith('projects/_/buckets/dpgke-demo-output/objects/wordcount/')\""
            + "}}]}}",
        options);
    // The reverse denial in the run is about read: this permission maps to
    // create/overwrite (and list/delete), never to object reads.
    assertFalse(options.contains("objectViewer"));
    assertFalse(options.contains("legacyObjectReader"));
  }

  @Test
  void conditionIsOmittedForAWholeBucketBoundary() {
    String options = DownscopedAccess.boundaryOptions(
        "bkt", DownscopedAccess.INPUT_PERMISSION, "");
    assertFalse(options.contains("availabilityCondition"),
        "a whole-bucket rule must not invent an empty prefix");
  }

  @Test
  void prefixIsNormalisedAndDerivedFromTheJobObjects() {
    assertTrue(DownscopedAccess.boundaryOptions("bkt", DownscopedAccess.INPUT_PERMISSION, "in")
        .contains("/objects/in/'"), "a missing trailing slash must be added");

    assertEquals("input/", DownscopedAccess.directoryPrefix("input/words.txt"));
    assertEquals("wordcount/", DownscopedAccess.directoryPrefix("wordcount/"));
    assertEquals("", DownscopedAccess.directoryPrefix("words.txt"));
  }
}
