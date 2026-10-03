package main

import "testing"

// TestLambdaCodeURL locks the K8s code-mount URL derivation: the admin base is
// built from a cluster-reachable emulator endpoint (with /_jaiscloud appended),
// and stays empty when none is configured so no unreachable localhost URL is
// guessed.
func TestLambdaCodeURL(t *testing.T) {
	t.Setenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT", "")
	if got := lambdaCodeURL(); got != "" {
		t.Errorf("unset endpoint: got %q, want empty (code mount disabled)", got)
	}

	t.Setenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT", "jaiscloud-gcp.jaiscloud.svc.cluster.local:8080")
	want := "http://jaiscloud-gcp.jaiscloud.svc.cluster.local:8080/_jaiscloud"
	if got := lambdaCodeURL(); got != want {
		t.Errorf("scheme-less endpoint: got %q, want %q", got, want)
	}

	t.Setenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT", "https://emulator.example.com/")
	if got := lambdaCodeURL(); got != "https://emulator.example.com/_jaiscloud" {
		t.Errorf("trailing slash: got %q, want https://emulator.example.com/_jaiscloud", got)
	}
}
