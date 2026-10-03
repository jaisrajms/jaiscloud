// Package sdk_dataproc_test exercises the jaiscloud-gcp emulator's Cloud
// Dataproc surface (dataproc.googleapis.com/v1) through the official Google
// REST apiary client. This validates wire-level parity with the real SDK.
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_dataproc_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/dataproc/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/pubsub/v1"
)

func endpoint() string {
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8080/"
}

func opts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(endpoint()), option.WithoutAuthentication()}
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func TestSDKDataproc(t *testing.T) {
	ctx := context.Background()
	svc, err := dataproc.NewService(ctx, opts()...)
	require.NoError(t, err)

	const project = "proj"
	const region = "us-central1"
	clusterName := unique("c")

	// CreateCluster returns an in-flight LRO; it completes when polled through
	// operations.get.
	op, err := svc.Projects.Regions.Clusters.Create(project, region, &dataproc.Cluster{
		ProjectId:   project,
		ClusterName: clusterName,
		Config: &dataproc.ClusterConfig{
			GceClusterConfig: &dataproc.GceClusterConfig{ZoneUri: "us-central1-a"},
			SoftwareConfig:   &dataproc.SoftwareConfig{ImageVersion: "2.2"},
		},
	}).Do()
	require.NoError(t, err)
	require.False(t, op.Done)
	require.NotEmpty(t, op.Name)

	createdOp := pollOperation(t, svc, op.Name)
	require.True(t, createdOp.Done)
	var created dataproc.Cluster
	require.NoError(t, json.Unmarshal(createdOp.Response, &created))
	require.Equal(t, clusterName, created.ClusterName)
	require.Equal(t, "RUNNING", created.Status.State)

	// GetCluster returns the logical cluster (created synchronously).
	cluster, err := svc.Projects.Regions.Clusters.Get(project, region, clusterName).Do()
	require.NoError(t, err)
	require.Equal(t, clusterName, cluster.ClusterName)
	require.Equal(t, project, cluster.ProjectId)
	require.Equal(t, "RUNNING", cluster.Status.State)

	// ListClusters includes it.
	list, err := svc.Projects.Regions.Clusters.List(project, region).Do()
	require.NoError(t, err)
	require.NotEmpty(t, list.Clusters)

	// Submit a pyspark job. It is asynchronous: returned PENDING, it advances
	// through SETUP_DONE/RUNNING/DONE as GetJob is polled (mock executor).
	jobID := unique("job")
	submitted, err := svc.Projects.Regions.Jobs.Submit(project, region, &dataproc.SubmitJobRequest{
		Job: &dataproc.Job{
			Reference: &dataproc.JobReference{ProjectId: project, JobId: jobID},
			Placement: &dataproc.JobPlacement{ClusterName: clusterName},
			PysparkJob: &dataproc.PySparkJob{
				MainPythonFileUri: "gs://jaiscloud-bucket/main.py",
			},
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "PENDING", submitted.Status.State)
	require.False(t, submitted.Done)

	// GetJob polls the job to completion.
	got := pollJob(t, svc, project, region, jobID)
	require.Equal(t, "DONE", got.Status.State)
	require.Equal(t, clusterName, got.Placement.ClusterName)

	// Submit a sparkSqlJob. It runs the Spark SQL CLI in k8s mode (simulated
	// to DONE in mock mode); either way it must not fail loud.
	sqlID := unique("sql")
	sqlJob, err := svc.Projects.Regions.Jobs.Submit(project, region, &dataproc.SubmitJobRequest{
		Job: &dataproc.Job{
			Reference: &dataproc.JobReference{ProjectId: project, JobId: sqlID},
			Placement: &dataproc.JobPlacement{ClusterName: clusterName},
			SparkSqlJob: &dataproc.SparkSqlJob{
				QueryList: &dataproc.QueryList{Queries: []string{"SELECT 1"}},
			},
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "PENDING", sqlJob.Status.State)
	require.Equal(t, "DONE", pollJob(t, svc, project, region, sqlID).Status.State)

	// Submit an unsupported hadoop job → ERROR (fail-loud, never silent).
	hadoopID := unique("hadoop")
	hadoop, err := svc.Projects.Regions.Jobs.Submit(project, region, &dataproc.SubmitJobRequest{
		Job: &dataproc.Job{
			Reference: &dataproc.JobReference{ProjectId: project, JobId: hadoopID},
			Placement: &dataproc.JobPlacement{ClusterName: clusterName},
			HadoopJob: &dataproc.HadoopJob{
				MainJarFileUri: "gs://jaiscloud-bucket/mr.jar",
			},
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "ERROR", hadoop.Status.State)
	require.Contains(t, hadoop.Status.Details, "not supported")

	// DeleteCluster returns an in-flight LRO that completes when polled.
	del, err := svc.Projects.Regions.Clusters.Delete(project, region, clusterName).Do()
	require.NoError(t, err)
	require.False(t, del.Done)
	require.True(t, pollOperation(t, svc, del.Name).Done)

	// The cluster is gone.
	_, err = svc.Projects.Regions.Clusters.Get(project, region, clusterName).Do()
	require.Error(t, err)
}

// TestSDKDataprocJobEvents verifies that job state transitions publish
// CloudEvents to the configured Pub/Sub topic. The emulator must be started
// with JAISCLOUD_DATAPROC_EVENTS_TOPIC set (the same env var names the topic
// this test subscribes to); otherwise the test skips.
func TestSDKDataprocJobEvents(t *testing.T) {
	topicID := os.Getenv("JAISCLOUD_DATAPROC_EVENTS_TOPIC")
	if topicID == "" {
		t.Skip("JAISCLOUD_DATAPROC_EVENTS_TOPIC not set; skipping Dataproc event test")
	}
	ctx := context.Background()
	const project = "proj"
	const region = "us-central1"

	svc, err := dataproc.NewService(ctx, opts()...)
	require.NoError(t, err)
	ps, err := pubsub.NewService(ctx, opts()...)
	require.NoError(t, err)

	// The topic is created idempotently: a prior run in the same emulator may
	// already have it.
	topicName := "projects/" + project + "/topics/" + topicID
	if _, err := ps.Projects.Topics.Create(topicName, &pubsub.Topic{}).Do(); err != nil {
		var gerr *googleapi.Error
		if !errors.As(err, &gerr) || gerr.Code != 409 {
			require.NoError(t, err)
		}
	}
	subName := "projects/" + project + "/subscriptions/" + unique("sub")
	_, err = ps.Projects.Subscriptions.Create(subName, &pubsub.Subscription{Topic: topicName}).Do()
	require.NoError(t, err)
	t.Cleanup(func() { ps.Projects.Subscriptions.Delete(subName).Do() })

	clusterName := unique("c")
	op, err := svc.Projects.Regions.Clusters.Create(project, region, &dataproc.Cluster{
		ProjectId:   project,
		ClusterName: clusterName,
		Config: &dataproc.ClusterConfig{
			GceClusterConfig: &dataproc.GceClusterConfig{ZoneUri: "us-central1-a"},
		},
	}).Do()
	require.NoError(t, err)
	require.True(t, pollOperation(t, svc, op.Name).Done)

	jobID := unique("job")
	_, err = svc.Projects.Regions.Jobs.Submit(project, region, &dataproc.SubmitJobRequest{
		Job: &dataproc.Job{
			Reference: &dataproc.JobReference{ProjectId: project, JobId: jobID},
			Placement: &dataproc.JobPlacement{ClusterName: clusterName},
			PysparkJob: &dataproc.PySparkJob{
				MainPythonFileUri: "gs://jaiscloud-bucket/main.py",
			},
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "DONE", pollJob(t, svc, project, region, jobID).Status.State)

	// Pull the topic subscription and collect the job's and cluster's
	// state-change events (both publish to the same topic).
	jobStates := map[string]bool{}
	clusterStates := map[string]bool{}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if jobStates["PENDING"] && jobStates["RUNNING"] && jobStates["DONE"] &&
			clusterStates["CREATING"] && clusterStates["RUNNING"] {
			break
		}
		require.False(t, time.Now().After(deadline), "missing state events; job=%v cluster=%v", jobStates, clusterStates)
		resp, err := ps.Projects.Subscriptions.Pull(subName, &pubsub.PullRequest{
			MaxMessages:       50,
			ReturnImmediately: true,
		}).Do()
		require.NoError(t, err)
		var ackIDs []string
		for _, rm := range resp.ReceivedMessages {
			ackIDs = append(ackIDs, rm.AckId)
			body, err := base64.StdEncoding.DecodeString(rm.Message.Data)
			require.NoError(t, err)
			var ce map[string]any
			require.NoError(t, json.Unmarshal(body, &ce))
			require.Equal(t, "1.0", ce["specversion"])
			inner, _ := ce["data"].(map[string]any)
			switch ce["type"] {
			case "google.cloud.dataproc.v1.job.v1.stateChange":
				if inner["jobId"] != jobID {
					continue
				}
				require.Equal(t, clusterName, inner["clusterName"])
				require.Equal(t, project, inner["projectId"])
				if state, ok := inner["state"].(string); ok {
					jobStates[state] = true
				}
			case "google.cloud.dataproc.v1.cluster.v1.stateChange":
				if inner["clusterName"] != clusterName {
					continue
				}
				if state, ok := inner["state"].(string); ok {
					clusterStates[state] = true
				}
			}
		}
		if len(ackIDs) > 0 {
			_, err = ps.Projects.Subscriptions.Acknowledge(subName, &pubsub.AcknowledgeRequest{AckIds: ackIDs}).Do()
			require.NoError(t, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// pollJob polls jobs.get until the job reaches a terminal state.
func pollJob(t *testing.T, svc *dataproc.Service, project, region, jobID string) *dataproc.Job {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got, err := svc.Projects.Regions.Jobs.Get(project, region, jobID).Do()
		require.NoError(t, err)
		switch got.Status.State {
		case "DONE", "ERROR", "CANCELLED":
			return got
		}
		require.False(t, time.Now().After(deadline), "job %s did not reach a terminal state", jobID)
		time.Sleep(50 * time.Millisecond)
	}
}

// pollOperation polls operations.get until the named operation reports done.
func pollOperation(t *testing.T, svc *dataproc.Service, name string) *dataproc.Operation {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got, err := svc.Projects.Regions.Operations.Get(name).Do()
		require.NoError(t, err)
		if got.Done {
			return got
		}
		require.False(t, time.Now().After(deadline), "operation %s did not complete", name)
		time.Sleep(50 * time.Millisecond)
	}
}
