package dataproc

import (
	"context"
	"encoding/json"
	"testing"
)

// TestUpdateClusterBareConfigMaskMerges covers DPG68: a bare `config` update
// mask must apply the supplied config keys instead of merging nothing while
// answering 200 (the caller's config was silently discarded).
func TestUpdateClusterBareConfigMaskMerges(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()

	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{
		Config: json.RawMessage(`{"workerConfig":{"numInstances":2},"softwareConfig":{"imageVersion":"2.1"}}`),
	}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, _, err := p.UpdateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{
		Config: json.RawMessage(`{"workerConfig":{"numInstances":5}}`),
	}, []string{"config"}); err != nil {
		t.Fatalf("UpdateCluster(mask=config): %v", err)
	}

	got, err := p.GetCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	var config map[string]any
	if err := json.Unmarshal(got.Config, &config); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	worker, _ := config["workerConfig"].(map[string]any)
	if worker["numInstances"] != float64(5) {
		t.Errorf("workerConfig.numInstances = %v, want 5 (bare config mask applied nothing)", worker["numInstances"])
	}
	software, _ := config["softwareConfig"].(map[string]any)
	if software["imageVersion"] != "2.1" {
		t.Errorf("unrelated config field dropped: softwareConfig = %v", config["softwareConfig"])
	}
}
