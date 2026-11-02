package discoverydoc

import (
	"bytes"
	"os"
	"testing"
)

// snapshotPath is the vendored discovery snapshot the conformance harness uses,
// relative to this package directory.
const snapshotPath = "../../../../../tests/gcpconformance/discovery/bigquery.json.gz"

// TestEmbeddedDocumentMatchesSnapshot pins the embedded copy to the vendored
// snapshot, so a refreshed discovery snapshot fails here until it is copied into
// this package in the same change. (Go embed patterns cannot reach ../tests/...,
// which is why the copy exists at all.)
func TestEmbeddedDocumentMatchesSnapshot(t *testing.T) {
	want, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read vendored snapshot: %v", err)
	}
	if !bytes.Equal(compressed, want) {
		t.Fatalf("embedded %s (%d bytes) differs from the vendored snapshot (%d bytes); copy tests/gcpconformance/discovery/bigquery.json.gz over it",
			"bigquery_v2.json.gz", len(compressed), len(want))
	}
}

// TestDocumentRewritesOrigin covers the rewrite each request gets: the origin the
// caller used, with the real servicePath retained.
func TestDocumentRewritesOrigin(t *testing.T) {
	doc, err := Document("http://bq.local:8080/")
	if err != nil {
		t.Fatalf("Document: %v", err)
	}
	if got := doc["rootUrl"]; got != "http://bq.local:8080/" {
		t.Errorf("rootUrl = %v", got)
	}
	if got := doc["baseUrl"]; got != "http://bq.local:8080/bigquery/v2/" {
		t.Errorf("baseUrl = %v", got)
	}
	if got := doc["servicePath"]; got != ServicePath {
		t.Errorf("servicePath = %v, want %s", got, ServicePath)
	}

	// Two calls must not share the decoded tree.
	other, err := Document("https://other.local")
	if err != nil {
		t.Fatalf("Document: %v", err)
	}
	doc["rootUrl"] = "http://mutated/"
	if got := other["rootUrl"]; got != "https://other.local/" {
		t.Errorf("rootUrl = %v, want an independent copy", got)
	}
}
