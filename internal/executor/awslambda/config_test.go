package awslambda_test

import (
	"testing"

	lambdaexec "jaiscloud/internal/executor/awslambda"
)

func TestLambdaConfigFrom_Defaults(t *testing.T) {
	// Clear any env overrides so we get pure defaults.
	t.Setenv("JAISCLOUD_LAMBDA_CONCURRENCY_LIMIT", "")
	t.Setenv("JAISCLOUD_LAMBDA_SYNC_PAYLOAD_MAX_BYTES", "")
	t.Setenv("JAISCLOUD_LAMBDA_ASYNC_PAYLOAD_MAX_BYTES", "")
	t.Setenv("JAISCLOUD_LAMBDA_RESPONSE_PAYLOAD_MAX_BYTES", "")

	got := lambdaexec.LambdaConfigFrom(lambdaexec.LambdaConfig{})

	if got.ConcurrencyLimit != 1000 {
		t.Errorf("ConcurrencyLimit: got %d, want 1000", got.ConcurrencyLimit)
	}
	if got.SyncPayloadMax != 6*1024*1024 {
		t.Errorf("SyncPayloadMax: got %d, want %d", got.SyncPayloadMax, 6*1024*1024)
	}
	if got.AsyncPayloadMax != 256*1024 {
		t.Errorf("AsyncPayloadMax: got %d, want %d", got.AsyncPayloadMax, 256*1024)
	}
	if got.ResponsePayloadMax != 6*1024*1024 {
		t.Errorf("ResponsePayloadMax: got %d, want %d", got.ResponsePayloadMax, 6*1024*1024)
	}
}

func TestLambdaConfigFrom_EnvOverrides(t *testing.T) {
	t.Setenv("JAISCLOUD_LAMBDA_CONCURRENCY_LIMIT", "50")
	t.Setenv("JAISCLOUD_LAMBDA_SYNC_PAYLOAD_MAX_BYTES", "1048576")
	t.Setenv("JAISCLOUD_LAMBDA_ASYNC_PAYLOAD_MAX_BYTES", "65536")
	t.Setenv("JAISCLOUD_LAMBDA_RESPONSE_PAYLOAD_MAX_BYTES", "2097152")

	got := lambdaexec.LambdaConfigFrom(lambdaexec.LambdaConfig{})

	if got.ConcurrencyLimit != 50 {
		t.Errorf("ConcurrencyLimit: got %d, want 50", got.ConcurrencyLimit)
	}
	if got.SyncPayloadMax != 1048576 {
		t.Errorf("SyncPayloadMax: got %d, want 1048576", got.SyncPayloadMax)
	}
	if got.AsyncPayloadMax != 65536 {
		t.Errorf("AsyncPayloadMax: got %d, want 65536", got.AsyncPayloadMax)
	}
	if got.ResponsePayloadMax != 2097152 {
		t.Errorf("ResponsePayloadMax: got %d, want 2097152", got.ResponsePayloadMax)
	}
}

func TestLambdaConfigFrom_InvalidEnvFallsToDefault(t *testing.T) {
	t.Setenv("JAISCLOUD_LAMBDA_CONCURRENCY_LIMIT", "not-a-number")

	got := lambdaexec.LambdaConfigFrom(lambdaexec.LambdaConfig{})

	if got.ConcurrencyLimit != 1000 {
		t.Errorf("ConcurrencyLimit: got %d, want 1000 (default on parse failure)", got.ConcurrencyLimit)
	}
}

func TestLambdaConfigFrom_ZeroDisablesLimit(t *testing.T) {
	t.Setenv("JAISCLOUD_LAMBDA_CONCURRENCY_LIMIT", "0")

	got := lambdaexec.LambdaConfigFrom(lambdaexec.LambdaConfig{})

	if got.ConcurrencyLimit != 0 {
		t.Errorf("ConcurrencyLimit: got %d, want 0 (unlimited)", got.ConcurrencyLimit)
	}
}

func TestLambdaConfigFrom_PreservesBaseFields(t *testing.T) {
	t.Setenv("JAISCLOUD_LAMBDA_CONCURRENCY_LIMIT", "")

	base := lambdaexec.LambdaConfig{
		Mode:       "docker",
		InstanceID: "abc-123",
		Region:     "eu-west-1",
	}
	got := lambdaexec.LambdaConfigFrom(base)

	if got.Mode != "docker" {
		t.Errorf("Mode: got %q, want docker", got.Mode)
	}
	if got.InstanceID != "abc-123" {
		t.Errorf("InstanceID: got %q, want abc-123", got.InstanceID)
	}
	if got.Region != "eu-west-1" {
		t.Errorf("Region: got %q, want eu-west-1", got.Region)
	}
}

// TestDefaultLambdaConfigCodeURL locks the generic CodeURL plumbing: the K8s
// code-fetch init container is enabled only when JAISCLOUD_LAMBDA_CODE_URL is
// set, and a trailing slash is trimmed so the executor can append
// /lambda/code/... without a double separator.
func TestDefaultLambdaConfigCodeURL(t *testing.T) {
	t.Setenv("JAISCLOUD_LAMBDA_CODE_URL", "")
	if got := lambdaexec.DefaultLambdaConfig().CodeURL; got != "" {
		t.Errorf("CodeURL: got %q, want empty (code mount disabled)", got)
	}

	t.Setenv("JAISCLOUD_LAMBDA_CODE_URL", "http://jaiscloud:8080/_jaiscloud/")
	if got := lambdaexec.DefaultLambdaConfig().CodeURL; got != "http://jaiscloud:8080/_jaiscloud" {
		t.Errorf("CodeURL: got %q, want http://jaiscloud:8080/_jaiscloud", got)
	}
}
