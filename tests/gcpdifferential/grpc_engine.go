//go:build gcp_differential

package gcpdifferential

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/api/option"
	gtransport "google.golang.org/api/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// grpcGoldenDir and grpcReportDir are the gRPC analogues of goldenDir() and
// reportDir(): the gRPC goldens live beside the REST ones but in their own
// directory so neither manifest can be polluted by the other transport.
func grpcGoldenDir() string {
	return envOr("GCP_DIFFERENTIAL_GRPC_GOLDEN_DIR", "testdata/golden-grpc")
}
func grpcReportDir() string {
	return envOr("GCP_DIFFERENTIAL_GRPC_REPORT_DIR", "testdata/report-grpc")
}

// grpcServiceEndpoint maps a gRPC differential service to its real-GCP endpoint
// host:port. It is consulted when recording (and by the scenario contract); in
// emulator mode every service is served from the single GRPCTarget.Endpoint, so
// the :authority token comes from grpcServiceAuthority instead. Real GCP serves
// google.longrunning.Operations on each service's own host, so an Operations
// scenario's service is the host that owns it (e.g. "dataproc").
var grpcServiceEndpoint = map[string]string{
	"datastore":  "datastore.googleapis.com:443",
	"firestore":  "firestore.googleapis.com:443",
	"logging":    "logging.googleapis.com:443",
	"monitoring": "monitoring.googleapis.com:443",
	// AUD6-4: google.longrunning.Operations. Regional Dataproc operations are
	// served only on the region-scoped host ({region}-dataproc.googleapis.com);
	// the global host rejects the region and the other reachable endpoints deny
	// the parity principal.
	"dataproc": "us-central1-dataproc.googleapis.com:443",
	// SDK-tour differential: the gRPC surfaces the official Go tour drives.
	"kms":           "cloudkms.googleapis.com:443",
	"secretmanager": "secretmanager.googleapis.com:443",
	"pubsub":        "pubsub.googleapis.com:443",
}

// grpcServiceAuthority is the emulator :authority token for a service whose
// google.longrunning.Operations the differential exercises. Real GCP serves
// Dataproc operations on a region-scoped host, while the emulator's single
// listener registers the service under "dataproc" (cmd/jaiscloud-gcp); setting
// the authority makes the Operations request endpoint-scoped to that service.
// Services without an entry are not endpoint-scoped (and never call Operations).
var grpcServiceAuthority = map[string]string{
	"dataproc": "dataproc",
}

// GRPCTarget is where a gRPC scenario run is sent: real GCP with Application
// Default Credentials (via the official gRPC dialer), or the emulator's single
// insecure listener. The same scenario Call closures drive both.
type GRPCTarget struct {
	Name    string
	Project string
	// ProjectNumber, when set, is folded to the same <project> placeholder as
	// the project id: some services (Secret Manager) canonicalize resource
	// names to the number while others echo the id.
	ProjectNumber string
	Suffix        string
	Names         ResourceNames
	Emulator      bool
	// Endpoint is the emulator's gRPC host:port (no scheme); ignored by real GCP.
	Endpoint string
	// Timeout bounds a single RPC (default 60s).
	Timeout time.Duration
}

// RealGRPCTarget builds a record target for real GCP. Credentials come from
// Application Default Credentials through the official dialer; no token is read
// or held by this package.
func RealGRPCTarget(project, suffix string, names ResourceNames) *GRPCTarget {
	return RealGRPCTargetFor(project, "", suffix, names)
}

// RealGRPCTargetFor is RealGRPCTarget carrying the project number, so the
// normalizer folds the number alias a service may canonicalize to.
func RealGRPCTargetFor(project, projectNumber, suffix string, names ResourceNames) *GRPCTarget {
	return &GRPCTarget{
		Name:          "real-gcp-grpc",
		Project:       project,
		ProjectNumber: projectNumber,
		Suffix:        suffix,
		Names:         names,
		Timeout:       60 * time.Second,
	}
}

// EmulatorGRPCTarget builds a replay target for the local emulator's gRPC
// listener. Every service is served from one plaintext endpoint.
func EmulatorGRPCTarget(endpoint, project, suffix string, names ResourceNames) *GRPCTarget {
	return &GRPCTarget{
		Name:     "emulator-grpc",
		Project:  project,
		Suffix:   suffix,
		Names:    names,
		Emulator: true,
		Endpoint: endpoint,
		Timeout:  60 * time.Second,
	}
}

// dial returns a client connection for one service. Emulator mode dials the
// single plaintext listener with insecure credentials; record mode dials the
// service's real-GCP endpoint with ADC.
//
// In emulator mode a service with an endpoint-scoped Operations surface is
// dialed with its :authority token (grpcServiceAuthority). Real GCP serves
// google.longrunning.Operations per service host, and the emulator's single
// listener reproduces that split by :authority token
// (internal/gcp/grpc/operations), so setting the authority makes an Operations
// request addressed to the service answer as that service.
func (t *GRPCTarget) dial(ctx context.Context, service string) (*grpc.ClientConn, error) {
	if t.Emulator {
		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
		if tok, ok := grpcServiceAuthority[service]; ok {
			opts = append(opts, grpc.WithAuthority(tok))
		}
		return grpc.NewClient(t.Endpoint, opts...)
	}
	ep, ok := grpcServiceEndpoint[service]
	if !ok {
		return nil, fmt.Errorf("no real-GCP gRPC endpoint for service %q", service)
	}
	return gtransport.Dial(ctx, option.WithEndpoint(ep))
}

// GRPCScenario is one curated gRPC request. Unlike the REST Scenario it carries
// a Call closure because each official client has a different constructor; the
// same closure is issued against real GCP (record) and the emulator (replay).
//
// Call returns the request and response proto messages. The request is captured
// for the golden (it proves the same logical input was sent); the response — or,
// on error, the google.rpc.Status — is what the diff compares.
type GRPCScenario struct {
	// Service and Op are the stable identity used in golden filenames, reports
	// and (Service, Op) matching.
	Service string
	Op      string
	// Method is the full gRPC method name for the report, e.g.
	// "google.datastore.v1.Datastore/Commit".
	Method string
	// Path is a logical resource label for the report; empty defaults to
	// "projects/${project}".
	Path string
	Call func(ctx context.Context, t *GRPCTarget) (req, resp proto.Message, err error)
	// Wait, when set, re-runs Call until Until(resp) holds or Timeout expires,
	// for the eventually-consistent reads (Cloud Logging).
	Wait *GRPCWait
}

// GRPCWait configures the polling behaviour for GRPCScenario.Wait.
type GRPCWait struct {
	// Until reports whether the response is terminal.
	Until func(proto.Message) bool
	// Interval is the delay between polls (default 1s).
	Interval time.Duration
	// Timeout bounds the wait (default the target's Timeout); on expiry the last
	// response is recorded so the divergence surfaces rather than hanging.
	Timeout time.Duration
}

// RunGRPC executes every scenario against the target in order, capturing a
// normalized Exchange for each. The Exchange is the same type the REST runner
// produces, so normalization, the diff, triage and reporting are shared.
func (t *GRPCTarget) RunGRPC(ctx context.Context, scenarios []GRPCScenario) ([]Exchange, error) {
	if t.Timeout <= 0 {
		t.Timeout = 60 * time.Second
	}
	norm := NewNormalizer(t.Project, t.ProjectNumber, t.Suffix, t.Names)

	exs := make([]Exchange, 0, len(scenarios))
	for i, sc := range scenarios {
		req, resp, err := t.runScenario(ctx, sc)

		ex := Exchange{
			Index:     i,
			Service:   sc.Service,
			Op:        sc.Op,
			Method:    sc.Method,
			Transport: "grpc",
			Path:      norm.Path(grpcPath(sc.Path, t.Project)),
		}
		if err != nil {
			st := status.Convert(err)
			ex.Status = int(st.Code())
			if b, merr := marshalProtoJSON(st.Proto()); merr == nil {
				ex.Response = norm.Bytes(b)
			}
		} else {
			ex.Status = int(codes.OK)
			if resp != nil {
				b, merr := marshalProtoJSON(resp)
				if merr != nil {
					return exs, fmt.Errorf("%s/%s: marshal response: %w", sc.Service, sc.Op, merr)
				}
				ex.Response = norm.Bytes(b)
			}
		}
		if req != nil {
			if b, merr := marshalProtoJSON(req); merr == nil {
				ex.Request = norm.RequestBytes(b)
			}
		}
		exs = append(exs, ex)
	}
	return exs, nil
}

// runScenario issues one scenario, polling when it carries a Wait spec. It
// always issues the call at least once so a response already at the terminal
// state is captured immediately.
func (t *GRPCTarget) runScenario(ctx context.Context, sc GRPCScenario) (proto.Message, proto.Message, error) {
	call := func() (proto.Message, proto.Message, error) {
		cctx, cancel := context.WithTimeout(ctx, t.Timeout)
		defer cancel()
		return sc.Call(cctx, t)
	}
	if sc.Wait == nil {
		return call()
	}
	interval := sc.Wait.Interval
	if interval <= 0 {
		interval = time.Second
	}
	timeout := sc.Wait.Timeout
	if timeout <= 0 {
		timeout = t.Timeout
	}
	deadline := time.Now().Add(timeout)
	for {
		req, resp, err := call()
		if err == nil && sc.Wait.Until != nil && sc.Wait.Until(resp) {
			return req, resp, nil
		}
		if time.Now().After(deadline) {
			return req, resp, err
		}
		time.Sleep(interval)
	}
}

// marshalProtoJSON renders a proto message the camelCase way the generated
// clients encode it, so both record and replay normalize identical field names.
func marshalProtoJSON(m proto.Message) ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}.Marshal(m)
}

// grpcPath resolves the report path label, defaulting to the project root.
func grpcPath(p, project string) string {
	if p == "" {
		return "projects/" + project
	}
	return p
}
