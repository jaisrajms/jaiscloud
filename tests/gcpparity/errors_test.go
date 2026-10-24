//go:build gcp_parity && gcp_conformance

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"testing"
	"time"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	containerpb "cloud.google.com/go/container/apiv1/containerpb"
	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	eventarcpb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	runpb "cloud.google.com/go/run/apiv2/runpb"
	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	workflowspb "cloud.google.com/go/workflows/apiv1/workflowspb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"

	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// This file is the live half of the AUD8 error-mapping audit: for every service
// the emulator serves over BOTH transports it issues one representative
// not-found (or otherwise canonical) operation over REST and the analogous Get
// over gRPC, then asserts the REST envelope's code/status agree with the gRPC
// code. The offline half (internal/gcp/adapter/error_mapping_audit_test.go)
// proves the mapping exhaustively at the codec level; this proves the real
// routing → codec → envelope and handler → interceptor → status wiring.

// errorProbe is one dual service's representative error operation. wantHTTP is
// the REST status, wantRPC the gRPC code; both must name the same canonical
// google.rpc status.
type errorProbe struct {
	Service  string
	wantHTTP int
	wantRPC  codes.Code
	// legacy marks a service whose REST error envelope is the legacy GCS form
	// (errors[] with a reason, no google.rpc status); the canonical name is then
	// asserted through the gRPC code only.
	legacy bool
	Rest   func(ctx context.Context, e *Env) (int, []byte, error)
	GRPC   func(ctx context.Context, e *Env) error
}

// errorProbeExemptions documents a dual service with no meaningful
// representative error operation, so the coverage gate does not demand one.
var errorProbeExemptions = map[string]string{
	"iam":             "IAMPolicy is a shared authorization surface with no resource of its own; a missing-resource error belongs to the owning service's probe (pubsub, secretmanager, kms, ...)",
	"iamcredentials":  "token-only surface (generateAccessToken/generateIdToken) with no resource lifecycle to probe",
	"resourcemanager": "GetProject synthesizes an ACTIVE placeholder for any project id (documented backward compatibility), so a missing project is not an error; the REST surface is also v1 while gRPC is v3",
	"serviceusage":    "the emulator synthesizes a service descriptor for any name, so a missing service is not an error",
}

// grpcGet dials the emulator and hands the caller a connection, closing it on
// return. Every probe owns its connection, mirroring the scenario cases.
func grpcDial(e *Env, fn func(*grpc.ClientConn) error) error {
	conn, err := grpc.NewClient(e.Cfg.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(conn)
}

// restGet builds a REST probe that issues a GET for a missing resource.
func restGet(path func(e *Env) string) func(context.Context, *Env) (int, []byte, error) {
	return func(ctx context.Context, e *Env) (int, []byte, error) {
		body, statusCode, err := e.restDo(ctx, http.MethodGet, path(e), "", "")
		return statusCode, body, err
	}
}

// errorProbes returns one probe per dual service. Each selects a resource name
// that is guaranteed absent (the run suffix) and asks both transports for it.
func errorProbes() []errorProbe {
	// p is the project id; miss is a run-unique absent resource id.
	p := func(e *Env) string { return e.Cfg.Project }
	miss := func(e *Env) string { return e.Resource("probe") }
	loc := func(e *Env) string { return "projects/" + p(e) + "/locations/us-central1" }
	region := func(e *Env) string { return "projects/" + p(e) + "/regions/us-central1" }

	return []errorProbe{
		{
			Service: "storage", wantHTTP: 404, wantRPC: codes.NotFound, legacy: true,
			Rest: restGet(func(e *Env) string { return "/storage/v1/b/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := storagepb.NewStorageClient(conn).GetBucket(ctx, &storagepb.GetBucketRequest{Name: "projects/_/buckets/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "pubsub", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v1/projects/" + p(e) + "/topics/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := pubsubpb.NewPublisherClient(conn).GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: "projects/" + p(e) + "/topics/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "secretmanager", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v1/projects/" + p(e) + "/secrets/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := secretmanagerpb.NewSecretManagerServiceClient(conn).GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: "projects/" + p(e) + "/secrets/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "kms", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v1/projects/" + p(e) + "/locations/global/keyRings/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := kmspb.NewKeyManagementServiceClient(conn).GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: "projects/" + p(e) + "/locations/global/keyRings/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "container", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/container/v1/" + loc(e) + "/clusters/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := containerpb.NewClusterManagerClient(conn).GetCluster(ctx, &containerpb.GetClusterRequest{Name: loc(e) + "/clusters/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "dataproc", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v1/" + region(e) + "/clusters/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := dataprocpb.NewClusterControllerClient(conn).GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: p(e), Region: "us-central1", ClusterName: miss(e)})
					return err
				})
			},
		},
		{
			Service: "eventarc", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v1/" + loc(e) + "/triggers/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := eventarcpb.NewEventarcClient(conn).GetTrigger(ctx, &eventarcpb.GetTriggerRequest{Name: loc(e) + "/triggers/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "functions", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v1/" + loc(e) + "/functions/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := functionspb.NewCloudFunctionsServiceClient(conn).GetFunction(ctx, &functionspb.GetFunctionRequest{Name: loc(e) + "/functions/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "logging", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v2/projects/" + p(e) + "/metrics/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := loggingpb.NewMetricsServiceV2Client(conn).GetLogMetric(ctx, &loggingpb.GetLogMetricRequest{MetricName: "projects/" + p(e) + "/metrics/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "managedkafka", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v1/" + loc(e) + "/clusters/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := managedkafkapb.NewManagedKafkaClient(conn).GetCluster(ctx, &managedkafkapb.GetClusterRequest{Name: loc(e) + "/clusters/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "metastore", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v1/" + loc(e) + "/services/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := metastorepb.NewDataprocMetastoreClient(conn).GetService(ctx, &metastorepb.GetServiceRequest{Name: loc(e) + "/services/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "monitoring", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v3/projects/" + p(e) + "/alertPolicies/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := monitoringpb.NewAlertPolicyServiceClient(conn).GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{Name: "projects/" + p(e) + "/alertPolicies/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "firestore", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string {
				return "/v1/projects/" + p(e) + "/databases/(default)/documents/coll-" + miss(e) + "/doc-" + miss(e)
			}),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					name := "projects/" + p(e) + "/databases/(default)/documents/coll-" + miss(e) + "/doc-" + miss(e)
					_, err := firestorepb.NewFirestoreClient(conn).GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name})
					return err
				})
			},
		},
		{
			Service: "run", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v2/" + loc(e) + "/services/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := runpb.NewServicesClient(conn).GetService(ctx, &runpb.GetServiceRequest{Name: loc(e) + "/services/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "scheduler", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v1/" + loc(e) + "/jobs/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := schedulerpb.NewCloudSchedulerClient(conn).GetJob(ctx, &schedulerpb.GetJobRequest{Name: loc(e) + "/jobs/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "tasks", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v2/" + loc(e) + "/queues/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := taskspb.NewCloudTasksClient(conn).GetQueue(ctx, &taskspb.GetQueueRequest{Name: loc(e) + "/queues/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "workflows", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v1/" + loc(e) + "/workflows/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := workflowspb.NewWorkflowsClient(conn).GetWorkflow(ctx, &workflowspb.GetWorkflowRequest{Name: loc(e) + "/workflows/" + miss(e)})
					return err
				})
			},
		},
		{
			Service: "workflowexecutions", wantHTTP: 404, wantRPC: codes.NotFound,
			Rest: restGet(func(e *Env) string { return "/v1/" + loc(e) + "/workflows/wf-" + miss(e) + "/executions/" + miss(e) }),
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					name := loc(e) + "/workflows/wf-" + miss(e) + "/executions/" + miss(e)
					_, err := executionspb.NewExecutionsClient(conn).GetExecution(ctx, &executionspb.GetExecutionRequest{Name: name})
					return err
				})
			},
		},
		{
			Service: "datastore", wantHTTP: 400, wantRPC: codes.InvalidArgument,
			Rest: func(ctx context.Context, e *Env) (int, []byte, error) {
				body := fmt.Sprintf(`{"keys":[{"projectId":%q}]}`, p(e))
				b, code, err := e.restDo(ctx, http.MethodPost, "/v1/projects/"+p(e)+":lookup", body, "")
				return code, b, err
			},
			GRPC: func(ctx context.Context, e *Env) error {
				return grpcDial(e, func(conn *grpc.ClientConn) error {
					_, err := datastorepb.NewDatastoreClient(conn).Lookup(ctx, &datastorepb.LookupRequest{
						ProjectId: p(e),
						Keys:      []*datastorepb.Key{{PartitionId: &datastorepb.PartitionId{ProjectId: p(e)}}},
					})
					return err
				})
			},
		},
	}
}

// TestErrorMappingParity drives every probe against the live emulator and
// asserts the REST envelope and the gRPC code name the same canonical status.
func TestErrorMappingParity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping error-mapping parity in -short mode")
	}
	if os.Getenv("GCP_PARITY_SKIP") == "1" {
		t.Skip("GCP_PARITY_SKIP=1")
	}
	cfg := ConfigFromEnv()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := probeREST(ctx, cfg.REST); err != nil {
		t.Skipf("emulator REST endpoint %s not reachable: %v", cfg.REST, err)
	}
	if err := probeGRPC(ctx, cfg.GRPCAddr()); err != nil {
		t.Skipf("emulator gRPC endpoint %s not reachable: %v", cfg.GRPC, err)
	}
	env, err := NewEnv(ctx, cfg)
	if err != nil {
		t.Fatalf("new env: %v", err)
	}
	defer env.Close()

	for _, p := range errorProbes() {
		p := p
		t.Run(p.Service, func(t *testing.T) {
			httpStatus, body, err := p.Rest(ctx, env)
			if err != nil {
				t.Fatalf("REST probe: %v", err)
			}
			if httpStatus != p.wantHTTP {
				t.Fatalf("REST HTTP %d, want %d (body=%s)", httpStatus, p.wantHTTP, truncateBody(body))
			}
			var envelope struct {
				Error struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
					Status  string `json:"status"`
				} `json:"error"`
			}
			if err := json.Unmarshal(body, &envelope); err != nil {
				t.Fatalf("decode REST envelope: %v (body=%s)", err, truncateBody(body))
			}
			if envelope.Error.Code != httpStatus {
				t.Fatalf("REST envelope code %d != HTTP status %d (body=%s)", envelope.Error.Code, httpStatus, truncateBody(body))
			}
			wantName := errorRPCName(p.wantRPC)
			if p.legacy {
				// The legacy GCS envelope carries errors[] (reason/domain/message)
				// instead of a google.rpc status; the canonical name is asserted
				// through the gRPC code below.
				if envelope.Error.Status != "" {
					t.Fatalf("legacy envelope must omit google.rpc status, got %q", envelope.Error.Status)
				}
			} else if envelope.Error.Status != wantName {
				t.Fatalf("REST error.status = %q, want %q (body=%s)", envelope.Error.Status, wantName, truncateBody(body))
			}

			gerr := p.GRPC(ctx, env)
			if gerr == nil {
				t.Fatalf("gRPC probe returned a nil error, want %v", p.wantRPC)
			}
			if got := status.Code(gerr); got != p.wantRPC {
				t.Fatalf("gRPC code = %v, want %v (%v)", got, p.wantRPC, gerr)
			}
			if !p.legacy {
				if got := errorRPCName(status.Code(gerr)); got != envelope.Error.Status {
					t.Fatalf("cross-transport mismatch: REST status %q, gRPC %q", envelope.Error.Status, got)
				}
			}
		})
	}
}

// TestErrorProbeCoverage fails if a dual service has neither a probe nor a
// reasoned exemption, so the live matrix cannot drift as services gain
// transports.
func TestErrorProbeCoverage(t *testing.T) {
	covered := map[string]bool{}
	for _, p := range errorProbes() {
		covered[p.Service] = true
	}
	var missing, stale []string
	for _, svc := range dualServices(t) {
		if covered[svc] {
			if _, ok := errorProbeExemptions[svc]; ok {
				stale = append(stale, svc)
			}
			continue
		}
		if _, ok := errorProbeExemptions[svc]; ok {
			continue
		}
		missing = append(missing, svc)
	}
	if len(stale) > 0 {
		sort.Strings(stale)
		t.Fatalf("error-probe coverage: %d service(s) have both a probe and an exemption: %v", len(stale), stale)
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("error-probe coverage: %d dual service(s) have no probe and no exemption: %v", len(missing), missing)
	}
}

// errorRPCName maps a gRPC code to its canonical google.rpc name, written
// independently of the production tables.
func errorRPCName(c codes.Code) string {
	switch c {
	case codes.OK:
		return "OK"
	case codes.Canceled:
		return "CANCELLED"
	case codes.Unknown:
		return "UNKNOWN"
	case codes.InvalidArgument:
		return "INVALID_ARGUMENT"
	case codes.DeadlineExceeded:
		return "DEADLINE_EXCEEDED"
	case codes.NotFound:
		return "NOT_FOUND"
	case codes.AlreadyExists:
		return "ALREADY_EXISTS"
	case codes.PermissionDenied:
		return "PERMISSION_DENIED"
	case codes.ResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case codes.FailedPrecondition:
		return "FAILED_PRECONDITION"
	case codes.Aborted:
		return "ABORTED"
	case codes.OutOfRange:
		return "OUT_OF_RANGE"
	case codes.Unimplemented:
		return "UNIMPLEMENTED"
	case codes.Internal:
		return "INTERNAL"
	case codes.Unavailable:
		return "UNAVAILABLE"
	case codes.DataLoss:
		return "DATA_LOSS"
	case codes.Unauthenticated:
		return "UNAUTHENTICATED"
	default:
		return c.String()
	}
}
