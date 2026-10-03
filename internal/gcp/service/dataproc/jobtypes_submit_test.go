package dataproc

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/store"
)

// unsupportedJobTypesAtSubmit mirrors the fail-loud set in jobtypes.go
// (unsupportedJobTypes). Kept as an explicit list so the wire-level matrix is
// pinned independently of the service's own lookup table.
var unsupportedJobTypesAtSubmit = []string{
	"hadoopJob", "hiveJob", "pigJob",
	"prestoJob", "trinoJob", "flinkJob",
}

// submitTestService builds a mock-mode core with one cluster so SubmitJob has a
// placement target. No k8s client is installed, so the executor is the mock.
func submitTestService(t *testing.T) *Service {
	t.Helper()
	ctx := context.Background()
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore())
	_, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{})
	require.NoError(t, err, "create cluster")
	return p
}

// TestSubmitJob_JobTypeMatrix pins the submit-level contract for the Dataproc
// job-type matrix. An unsupported type is accepted (no RPC error) but the
// created job immediately lands in ERROR with the fail-loud detail; a
// Spark-family job with a valid entry point is accepted and left non-terminal
// by the mock executor. This complements jobtypes_test.go, which only asserts
// fail-loud at the jobToEntryPoint level.
func TestSubmitJob_JobTypeMatrix(t *testing.T) {
	ctx := context.Background()
	p := submitTestService(t)

	for _, jobType := range unsupportedJobTypesAtSubmit {
		jobType := jobType
		t.Run("unsupported/"+jobType, func(t *testing.T) {
			jobID := "fail-" + jobType
			submitted, err := p.SubmitJob(ctx, "proj", "us-central1", JobInput{
				JobID:                jobID,
				PlacementClusterName: "c1",
				Type:                 jobType,
				TypeJob:              json.RawMessage(`{}`),
			})
			require.NoError(t, err, "SubmitJob must not return an RPC error for %s", jobType)
			require.Equal(t, jobStateError, submitted.Status.State, "returned job state for %s", jobType)

			// The persisted record must carry the same ERROR state and detail:
			// a poller does not get a transport error, only a terminal job.
			stored, err := p.store.GetJob(ctx, "proj", "us-central1", jobID)
			require.NoError(t, err, "GetJob for %s", jobType)
			require.Equal(t, jobStateError, stored.Status.State, "stored job state for %s", jobType)
			require.Equal(t, "job type "+jobType+" is not supported by the emulator", stored.Status.Details, "stored details for %s", jobType)
		})
	}

	supported := []struct {
		jobType string
		typeJob string
	}{
		{"sparkJob", `{"mainJarFileUri":"gs://b/a.jar"}`},
		{"pysparkJob", `{"mainPythonFileUri":"gs://b/main.py"}`},
		{"sparkRJob", `{"mainRFileUri":"gs://b/main.R"}`},
		{"sparkSqlJob", `{"queryList":{"queries":["SELECT 1"]}}`},
	}
	for _, tc := range supported {
		tc := tc
		t.Run("supported/"+tc.jobType, func(t *testing.T) {
			jobID := "ok-" + tc.jobType
			_, err := p.SubmitJob(ctx, "proj", "us-central1", JobInput{
				JobID:                jobID,
				PlacementClusterName: "c1",
				Type:                 tc.jobType,
				TypeJob:              json.RawMessage(tc.typeJob),
			})
			require.NoError(t, err, "SubmitJob for %s", tc.jobType)

			stored, err := p.store.GetJob(ctx, "proj", "us-central1", jobID)
			require.NoError(t, err, "GetJob for %s", tc.jobType)
			require.NotEqual(t, jobStateError, stored.Status.State, "supported %s must not fail loud", tc.jobType)
		})
	}
}

// TestSubmitJob_HiveJob_FailsLoud names the Hive case explicitly: hiveJob is
// engine-bearing (it needs a Hive runtime) while the emulator is
// metadata-not-engine, so it must not be silently skipped or run. The job is
// created and immediately set to ERROR with the standard detail; no RPC error.
func TestSubmitJob_HiveJob_FailsLoud(t *testing.T) {
	ctx := context.Background()
	p := submitTestService(t)

	j, err := p.SubmitJob(ctx, "proj", "us-central1", JobInput{
		JobID:                "hive-1",
		PlacementClusterName: "c1",
		Type:                 "hiveJob",
		TypeJob:              json.RawMessage(`{"queryList":{"queries":["SELECT 1"]}}`),
	})
	require.NoError(t, err, "SubmitJob must not return an RPC error")
	require.Equal(t, jobStateError, j.Status.State)
	require.Equal(t, "job type hiveJob is not supported by the emulator", j.Status.Details)

	stored, err := p.store.GetJob(ctx, "proj", "us-central1", "hive-1")
	require.NoError(t, err)
	require.Equal(t, jobStateError, stored.Status.State)
	require.Equal(t, "job type hiveJob is not supported by the emulator", stored.Status.Details)
}
