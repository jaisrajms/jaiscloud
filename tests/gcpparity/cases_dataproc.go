//go:build gcp_parity

package gcpparity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	dataproc "cloud.google.com/go/dataproc/v2/apiv1"
	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const dataprocRegion = "us-central1"

// dpMetainfoUUID matches the server-generated cluster uuid segment Dataproc
// embeds in a job's driver-output/control-file URIs
// (gs://<bucket>/google-cloud-dataproc-metainfo/<uuid>/jobs/<id>/...). It is
// folded for mutation parity, where the two transports submit to independent
// twin clusters with different uuids; the same-resource read steps still compare
// the URIs verbatim across transports.
var dpMetainfoUUID = regexp.MustCompile(`(google-cloud-dataproc-metainfo/)[0-9a-f]{32}(/)`)

// dpFoldClusterUUID folds the cluster uuid embedded in a job's driver URIs so
// two independent twin jobs compare equal. Placement.clusterUuid itself is
// covered by the normalizer's clusterUuid volatile key.
func dpFoldClusterUUID(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return raw, nil
	}
	return dpMetainfoUUID.ReplaceAll(raw, []byte("${1}<uuid>${2}")), nil
}

// dataprocOperation is the google.longrunning.Operation members the REST LRO
// poll needs.
type dataprocOperation struct {
	Name     string          `json:"name"`
	Done     bool            `json:"done"`
	Response json.RawMessage `json:"response"`
}

func decodeDataprocOperation(raw json.RawMessage) (dataprocOperation, error) {
	var op dataprocOperation
	err := json.Unmarshal(raw, &op)
	return op, err
}

// dataprocScenario compares the Cloud Dataproc v1 surface over REST and gRPC.
// Dataproc is region-scoped (projects/{p}/regions/{r}/...) and cluster
// mutations are long-running operations whose REST response is in flight, so
// the scenario drives the LRO to completion on both transports. It covers the
// cluster lifecycle (create -> get -> list -> update -> stop -> start -> delete)
// and a job on the cluster (submit -> settle -> get -> list -> update -> delete),
// plus mutation-parity Create/Update steps for both resources (AUD3-12).
//
// The emulator's cluster and job state machines advance one hop per read (with
// the default zero delays), so a job is settled to a terminal state with
// explicit reads before any cross-transport GetJob: otherwise the gRPC read
// would settle it one hop further than the REST read and the two would
// legitimately disagree.
func dataprocScenario() Scenario {
	clusterID := "parity-dp-cluster"
	jobID := "parity-dp-job"

	project := func(e *Env) string { return e.Cfg.Project }
	parent := func(e *Env) string { return fmt.Sprintf("projects/%s/regions/%s", project(e), dataprocRegion) }
	clusterName := func(e *Env) string { return e.Resource(clusterID) }
	clusterFull := func(e *Env) string { return parent(e) + "/clusters/" + clusterName(e) }
	jobName := func(e *Env) string { return e.Resource(jobID) }
	jobFull := func(e *Env) string { return parent(e) + "/jobs/" + jobName(e) }

	newClusterClient := func(ctx context.Context, e *Env) (*dataproc.ClusterControllerClient, error) {
		return dataproc.NewClusterControllerClient(ctx, e.GRPCClientOptions()...)
	}
	newJobClient := func(ctx context.Context, e *Env) (*dataproc.JobControllerClient, error) {
		return dataproc.NewJobControllerClient(ctx, e.GRPCClientOptions()...)
	}

	clusterProto := func(name string, labels map[string]string) *dataprocpb.Cluster {
		return &dataprocpb.Cluster{ClusterName: name, Labels: labels}
	}
	jobProto := func(name, cluster string, labels map[string]string) *dataprocpb.Job {
		return &dataprocpb.Job{
			Reference: &dataprocpb.JobReference{JobId: name},
			Placement: &dataprocpb.JobPlacement{ClusterName: cluster},
			Labels:    labels,
			TypeJob: &dataprocpb.Job_PysparkJob{
				PysparkJob: &dataprocpb.PySparkJob{MainPythonFileUri: "gs://bucket/main.py"},
			},
		}
	}
	jobBody := func(e *Env, labels string) string {
		return `{"job":{"reference":{"projectId":"` + project(e) + `","jobId":"` + jobName(e) +
			`"},"placement":{"clusterName":"` + clusterName(e) + `"},"labels":{` + labels +
			`},"pysparkJob":{"mainPythonFileUri":"gs://bucket/main.py"}}}`
	}
	clusterBody := func(e *Env, labels string) string {
		return `{"clusterName":"` + clusterName(e) + `","labels":{` + labels + `}}`
	}

	// restOperationSettle drives a REST long-running operation to completion.
	// Unlike the services whose REST mutation completes inline, Dataproc returns
	// a not-yet-done Operation (the cluster settles lazily on a later read), so
	// its wrapped RestOperationResource cannot apply; poll the operation by name
	// (which advances the state machine) and return the settled response.
	restOperationSettle := func(ctx context.Context, e *Env, method, path, body string) (json.RawMessage, error) {
		raw, err := e.Rest(ctx, method, path, body)
		if err != nil {
			return nil, err
		}
		op, err := decodeDataprocOperation(raw)
		if err != nil {
			return nil, fmt.Errorf("%s %s: decode operation: %w", method, path, err)
		}
		for i := 0; !op.Done; i++ {
			if i >= 10 {
				return nil, fmt.Errorf("%s %s: operation %q did not finish", method, path, op.Name)
			}
			polled, err := e.Rest(ctx, "GET", "/v1/"+op.Name, "")
			if err != nil {
				return nil, err
			}
			if op, err = decodeDataprocOperation(polled); err != nil {
				return nil, fmt.Errorf("%s %s: decode polled operation: %w", method, path, err)
			}
		}
		return stripAnyType(op.Response), nil
	}

	// createClusterGRPC creates the cluster and waits for the create LRO.
	createClusterGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClusterClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		op, err := c.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
			ProjectId: project(e),
			Region:    dataprocRegion,
			Cluster:   clusterProto(clusterName(e), map[string]string{"parity": "true"}),
		})
		if err != nil {
			return nil, err
		}
		return op.Wait(ctx)
	}
	createClusterREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return restOperationSettle(ctx, e, "POST", "/v1/"+parent(e)+"/clusters", clusterBody(e, `"parity":"true"`))
	}
	updateClusterGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClusterClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		op, err := c.UpdateCluster(ctx, &dataprocpb.UpdateClusterRequest{
			ProjectId:   project(e),
			Region:      dataprocRegion,
			ClusterName: clusterName(e),
			Cluster:     &dataprocpb.Cluster{Labels: map[string]string{"parity": "updated"}},
			UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		})
		if err != nil {
			return nil, err
		}
		return op.Wait(ctx)
	}
	updateClusterREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return restOperationSettle(ctx, e, "PATCH", "/v1/"+clusterFull(e)+"?updateMask=labels", `{"labels":{"parity":"updated"}}`)
	}
	submitJobGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		jc, err := newJobClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer jc.Close()
		return jc.SubmitJob(ctx, &dataprocpb.SubmitJobRequest{
			ProjectId: project(e),
			Region:    dataprocRegion,
			Job:       jobProto(jobName(e), clusterName(e), map[string]string{"parity": "true"}),
		})
	}
	submitJobREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, "POST", "/v1/"+parent(e)+"/jobs:submit", jobBody(e, `"parity":"true"`))
	}
	updateJobGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		jc, err := newJobClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer jc.Close()
		return jc.UpdateJob(ctx, &dataprocpb.UpdateJobRequest{
			ProjectId:  project(e),
			Region:     dataprocRegion,
			JobId:      jobName(e),
			Job:        &dataprocpb.Job{Labels: map[string]string{"parity": "updated"}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		})
	}
	updateJobREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, "PATCH", "/v1/"+jobFull(e)+"?updateMask=labels", `{"labels":{"parity":"updated"}}`)
	}

	// settleJob advances the lazy job state machine to a terminal state, so a
	// following cross-transport GetJob compares a stable job.
	settleJob := func(ctx context.Context, e *Env) error {
		for i := 0; i < 8; i++ {
			jc, err := newJobClient(ctx, e)
			if err != nil {
				return err
			}
			j, err := jc.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: project(e), Region: dataprocRegion, JobId: jobName(e)})
			jc.Close()
			if err != nil {
				return err
			}
			switch j.GetStatus().GetState() {
			case dataprocpb.JobStatus_DONE, dataprocpb.JobStatus_ERROR, dataprocpb.JobStatus_CANCELLED:
				return nil
			}
		}
		return fmt.Errorf("job %s did not reach a terminal state", jobName(e))
	}
	// deleteClusterREST deletes the cluster and settles its delete operation so
	// the record is reaped.
	deleteClusterREST := func(ctx context.Context, e *Env) error {
		_, err := restOperationSettle(ctx, e, "DELETE", "/v1/"+clusterFull(e), "")
		return err
	}
	// cleanupJobParity settles and deletes a twin job, then its twin cluster.
	cleanupJobParity := func(ctx context.Context, e *Env) error {
		_ = settleJob(ctx, e)
		_ = e.RestDelete(ctx, "/v1/"+jobFull(e))
		return deleteClusterREST(ctx, e)
	}

	return Scenario{Service: "dataproc", Steps: []Step{
		// --- Cluster lifecycle ---
		{
			Op: "CreateCluster",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := createClusterGRPC(ctx, e)
				return err
			},
		},
		{
			Op: "GetCluster",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClusterClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: project(e), Region: dataprocRegion, ClusterName: clusterName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+clusterFull(e), "")
			},
		},
		{
			Op:    "ListClusters",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClusterClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				it := c.ListClusters(ctx, &dataprocpb.ListClustersRequest{ProjectId: project(e), Region: dataprocRegion})
				out := &dataprocpb.ListClustersResponse{}
				for {
					cl, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Clusters = append(out.Clusters, cl)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+parent(e)+"/clusters", "")
			},
		},
		{
			Op: "UpdateCluster (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "PATCH", "/v1/"+clusterFull(e)+"?updateMask=labels", `{"labels":{"parity":"updated"}}`)
				return err
			},
		},
		{
			Op: "GetClusterAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClusterClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: project(e), Region: dataprocRegion, ClusterName: clusterName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+clusterFull(e), "")
			},
		},
		{
			Op: "StopCluster",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClusterClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.StopCluster(ctx, &dataprocpb.StopClusterRequest{ProjectId: project(e), Region: dataprocRegion, ClusterName: clusterName(e)})
				if err != nil {
					return err
				}
				_, err = op.Wait(ctx)
				return err
			},
		},
		{
			Op: "GetClusterStopped",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClusterClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: project(e), Region: dataprocRegion, ClusterName: clusterName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+clusterFull(e), "")
			},
		},
		{
			Op: "StartCluster",
			Mutate: func(ctx context.Context, e *Env) error {
				c, err := newClusterClient(ctx, e)
				if err != nil {
					return err
				}
				defer c.Close()
				op, err := c.StartCluster(ctx, &dataprocpb.StartClusterRequest{ProjectId: project(e), Region: dataprocRegion, ClusterName: clusterName(e)})
				if err != nil {
					return err
				}
				_, err = op.Wait(ctx)
				return err
			},
		},
		{
			Op: "GetClusterRunning",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				c, err := newClusterClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer c.Close()
				return c.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: project(e), Region: dataprocRegion, ClusterName: clusterName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+clusterFull(e), "")
			},
		},

		// --- Job lifecycle on the fixture cluster ---
		{
			Op: "SubmitJob",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := submitJobGRPC(ctx, e)
				return err
			},
		},
		{
			Op:     "SettleJob",
			Mutate: func(ctx context.Context, e *Env) error { return settleJob(ctx, e) },
		},
		{
			Op: "GetJob",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				jc, err := newJobClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer jc.Close()
				return jc.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: project(e), Region: dataprocRegion, JobId: jobName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+jobFull(e), "")
			},
		},
		{
			Op:    "ListJobs",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				jc, err := newJobClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer jc.Close()
				it := jc.ListJobs(ctx, &dataprocpb.ListJobsRequest{ProjectId: project(e), Region: dataprocRegion, ClusterName: clusterName(e)})
				out := &dataprocpb.ListJobsResponse{}
				for {
					j, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						return nil, err
					}
					out.Jobs = append(out.Jobs, j)
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+parent(e)+"/jobs?clusterName="+clusterName(e), "")
			},
		},
		{
			Op: "UpdateJob (REST)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := e.Rest(ctx, "PATCH", "/v1/"+jobFull(e)+"?updateMask=labels", `{"labels":{"parity":"updated"}}`)
				return err
			},
		},
		{
			Op: "GetJobAfterUpdate",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				jc, err := newJobClient(ctx, e)
				if err != nil {
					return nil, err
				}
				defer jc.Close()
				return jc.GetJob(ctx, &dataprocpb.GetJobRequest{ProjectId: project(e), Region: dataprocRegion, JobId: jobName(e)})
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", "/v1/"+jobFull(e), "")
			},
		},
		{
			Op: "DeleteJob",
			Mutate: func(ctx context.Context, e *Env) error {
				return e.RestDelete(ctx, "/v1/"+jobFull(e))
			},
		},

		// --- Mutation parity (AUD3-12): each transport mutates its own twin ---
		{
			Op: "CreateCluster (parity)",
			Mutation: &MutationParity{
				GRPC:    createClusterGRPC,
				REST:    createClusterREST,
				Cleanup: deleteClusterREST,
			},
		},
		{
			Op: "UpdateCluster (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					if _, err := createClusterGRPC(ctx, e); err != nil {
						return nil, err
					}
					return updateClusterGRPC(ctx, e)
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					if _, err := createClusterREST(ctx, e); err != nil {
						return nil, err
					}
					return updateClusterREST(ctx, e)
				},
				Cleanup: deleteClusterREST,
			},
		},
		{
			Op: "SubmitJob (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					if _, err := createClusterGRPC(ctx, e); err != nil {
						return nil, err
					}
					return submitJobGRPC(ctx, e)
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					if _, err := createClusterREST(ctx, e); err != nil {
						return nil, err
					}
					return submitJobREST(ctx, e)
				},
				Cleanup: cleanupJobParity,
				Project: dpFoldClusterUUID,
			},
		},
		{
			Op: "UpdateJob (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					if _, err := createClusterGRPC(ctx, e); err != nil {
						return nil, err
					}
					if _, err := submitJobGRPC(ctx, e); err != nil {
						return nil, err
					}
					return updateJobGRPC(ctx, e)
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					if _, err := createClusterREST(ctx, e); err != nil {
						return nil, err
					}
					if _, err := submitJobREST(ctx, e); err != nil {
						return nil, err
					}
					return updateJobREST(ctx, e)
				},
				Cleanup: cleanupJobParity,
				Project: dpFoldClusterUUID,
			},
		},

		// --- Cleanup the fixture cluster ---
		{
			Op: "DeleteCluster",
			Mutate: func(ctx context.Context, e *Env) error {
				return deleteClusterREST(ctx, e)
			},
		},
	}}
}
