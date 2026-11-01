package dataproc

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"jaiscloud/internal/model"
)

// TestCreateClusterAggregatesValidationErrors pins DPG45: a create with several
// independent problems answers one 400 whose message lists every one of them in
// the fixed check order, with real GCP's "Multiple validation errors" envelope
// (docs/gcp/dataproc-aggregate-validation-errors.md).
func TestCreateClusterAggregatesValidationErrors(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	raw, err := json.Marshal(map[string]any{
		"stagingBucket": "gs://not-a-bare-name",
		"kubernetesClusterConfig": map[string]any{
			"gkeClusterConfig": map[string]any{
				"gkeClusterTarget": "projects/other/locations/europe-west1/clusters/gke-9",
				"nodePoolTarget": []any{
					poolTarget("projects/other/locations/europe-west1/clusters/gke-9/nodePools/a", []string{"DEFAULT"}, nil),
					poolTarget("projects/other/locations/europe-west1/clusters/gke-9/nodePools/b", []string{"DEFAULT"}, nil),
				},
			},
			"kubernetesSoftwareConfig": map[string]any{
				"componentVersion": map[string]any{"SPARK": "dataproc-2.3"},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, _, err = p.CreateCluster(ctx, "proj", "us-central1", "dp-bad", ClusterInput{VirtualClusterConfig: raw})
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
		t.Fatalf("want 400 InvalidArgument, got %v", err)
	}
	if !strings.HasPrefix(pe.Message, "Multiple validation errors:\n - ") {
		t.Fatalf("message has no aggregation envelope: %q", pe.Message)
	}
	lines := strings.Split(strings.TrimPrefix(pe.Message, "Multiple validation errors:\n - "), "\n - ")
	if len(lines) != 5 {
		t.Fatalf("want 5 collected errors, got %d in %q", len(lines), pe.Message)
	}
	// The order is the order the checks run: software config, staging bucket,
	// cross-pool roles, then placement (project, region).
	for i, want := range []string{
		"unknown Spark engine version",
		"must be a bare Cloud Storage bucket name",
		"role DEFAULT is assigned to both a and b",
		"must be in the same project",
		"must be in the same region",
	} {
		if !strings.Contains(lines[i], want) {
			t.Fatalf("line %d = %q, want it to mention %q (whole message: %q)", i, lines[i], want, pe.Message)
		}
	}
	// Nothing was persisted.
	if _, err := p.GetCluster(ctx, "proj", "us-central1", "dp-bad"); err == nil {
		t.Fatal("rejected create persisted a cluster")
	}
}

// TestCreateClusterMalformedPoolKeepsSingleError pins that a target which fails
// to parse is reported on its own: its absence from the parsed set must not add
// a spurious "missing DEFAULT role" line.
func TestCreateClusterMalformedPoolKeepsSingleError(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	raw, err := json.Marshal(vccWith(map[string]any{
		"gkeClusterTarget": gkeClusterPath,
		"nodePoolTarget": []any{map[string]any{
			"nodePool": "bare-pool",
			"roles":    []any{"DEFAULT"},
		}},
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, _, err = p.CreateCluster(ctx, "proj", "us-central1", "dp-bare", ClusterInput{VirtualClusterConfig: raw})
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("want *model.ProviderError, got %T (%v)", err, err)
	}
	if strings.Contains(pe.Message, "Multiple validation errors") || strings.Contains(pe.Message, "DEFAULT role") {
		t.Fatalf("malformed pool target produced extra errors: %q", pe.Message)
	}
	if !strings.Contains(pe.Message, "must conform to pattern") {
		t.Fatalf("message = %q, want the pool-name error", pe.Message)
	}
}

// TestCreateClusterSingleValidationErrorUnchanged pins the other half of DPG45:
// a request with exactly one problem keeps its original message byte for byte
// (no "Multiple validation errors" envelope).
func TestCreateClusterSingleValidationErrorUnchanged(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	raw, err := json.Marshal(vccWith(map[string]any{
		"gkeClusterTarget": gkeClusterPath,
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// A software config with one unknown version is the single failure.
	var vcc map[string]any
	if err := json.Unmarshal(raw, &vcc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	vcc["kubernetesClusterConfig"].(map[string]any)["kubernetesSoftwareConfig"] = map[string]any{
		"componentVersion": map[string]any{"SPARK": "dataproc-2.3"},
	}
	raw, err = json.Marshal(vcc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, _, err = p.CreateCluster(ctx, "proj", "us-central1", "dp-one", ClusterInput{VirtualClusterConfig: raw})
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Code != "InvalidArgument" {
		t.Fatalf("want 400 InvalidArgument, got %v", err)
	}
	if want := `unknown Spark engine version "dataproc-2.3"`; !strings.HasPrefix(pe.Message, want) || strings.Contains(pe.Message, "Multiple validation errors") {
		t.Fatalf("single error message = %q, want the bare %q", pe.Message, want)
	}
}

// TestAggregatedValidationOrderIsDeterministic guards the joined message against
// map-iteration order: repeated renders of the same multi-error request must be
// identical.
func TestAggregatedValidationOrderIsDeterministic(t *testing.T) {
	software := map[string]any{
		"componentVersion": map[string]any{"SPARK": "3.5", "BOGUS": "1"},
		"properties": map[string]any{
			"spark:spark.executor.memory": "4g",
			"yarn:yarn.memory":            "4096",
			"presto:presto.query.max":     "1",
		},
	}
	var first string
	for i := 0; i < 20; i++ {
		_, errs := collectKubernetesSoftwareConfig(software)
		got := errs.err()
		if got == nil {
			t.Fatal("expected validation errors")
		}
		msg := got.Error()
		if i == 0 {
			first = msg
			continue
		}
		if msg != first {
			t.Fatalf("non-deterministic message:\nfirst: %q\nnow:   %q", first, msg)
		}
	}
}
