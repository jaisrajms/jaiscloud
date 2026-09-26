// Package functions is the transport-neutral core of Cloud Functions
// (cloudfunctions.googleapis.com, both v1 and v2). It owns all function,
// location, IAM, and long-running-operation business logic over
// internal/gcp/store/functions, and it owns the Lambda executor that backs
// synchronous CallFunction invocation (mock echo by default, Docker/K8s under
// JAISCLOUD_EXECUTOR_MODE).
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// REST transport (internal/gcp/transport/rest/functions) and the gRPC transport
// (internal/gcp/transport/grpc/functions) both transcode their wire format into
// this package's typed API and then call the SAME Service instance. That is the
// dual-protocol invariant: one core, one store, one executor, so the two
// transports cannot drift.
//
// Cloud Functions v1 and v2 share one store record but render differently on the
// wire (v1: top-level runtime/entryPoint/availableMemoryMb/status; v2: nested
// buildConfig/serviceConfig and a "state" field). The Version argument selects
// the render shape; the stored record is version-independent.
package functions

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/resource"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"github.com/google/uuid"
)

// Version identifies the Cloud Functions wire API version whose shape a
// request/response uses.
type Version string

const (
	// V1 is the Cloud Functions v1 API (cloudfunctions.googleapis.com/v1).
	V1 Version = "v1"
	// V2 is the Cloud Functions v2 API (cloudfunctions.googleapis.com/v2).
	V2 Version = "v2"
)

// VersionFromAPI returns the version named by an "apiVersion" param value.
// Anything other than "v2" is treated as v1, the default.
func VersionFromAPI(s string) Version {
	if s == "v2" {
		return V2
	}
	return V1
}

// rtFunctionPolicy is the generic ResourceStore type for function IAM policies.
const rtFunctionPolicy = "gcp_function_policy"

// operationMetadataType is the proto @type of the Cloud Functions v1
// OperationMetadataV1 carried on the long-running operations that Create, Update,
// and Delete return. The v1 proto (and Discovery schema) name the message
// OperationMetadataV1; OperationMetadata is the v2 name.
const operationMetadataType = "type.googleapis.com/google.cloud.functions.v1.OperationMetadataV1"

// operationMetadataTypeV2 is the v2 equivalent. A v2 client (e.g. gcloud 585)
// rejects the v1 @type.
const operationMetadataTypeV2 = "type.googleapis.com/google.cloud.functions.v2.OperationMetadata"

// The google.longrunning.Operation `response` is a google.protobuf.Any and must
// carry a resolvable type URL, or gax clients fail to unpack it ("Missing type
// url when parsing"). Create/Update respond with the Function (a CloudFunction
// on v1); Delete responds with google.protobuf.Empty.
const (
	functionTypeURLV1 = "type.googleapis.com/google.cloud.functions.v1.CloudFunction"
	functionTypeURLV2 = "type.googleapis.com/google.cloud.functions.v2.Function"
	emptyTypeURL      = "type.googleapis.com/google.protobuf.Empty"
)

// defaultMemoryMB and defaultTimeout are the Cloud Functions defaults applied
// when a create/update body omits them.
const (
	defaultMemoryMB = 256
	defaultTimeout  = "60s"
)

// defaultFunctionTimeout is the invocation timeout fallback when a stored
// function carries no parseable timeout.
const defaultFunctionTimeout = 60 * time.Second

// Service is the transport-neutral Cloud Functions core.
type Service struct {
	functions functionsstore.Store
	resources store.ResourceStore // IAM policies (control plane)
	executor  lambdaexec.LambdaExecutor
}

// Option configures Service.
type Option func(*Service)

// WithExecutor sets the Lambda executor backing CallFunction. A nil executor
// falls back to a MockExecutor (echo) so CallFunction works out of the box.
func WithExecutor(e lambdaexec.LambdaExecutor) Option {
	return func(s *Service) { s.executor = e }
}

// NewService returns a Functions core backed by the given store. resources backs
// the function IAM policy surface. The executor defaults to a MockExecutor.
func NewService(fs functionsstore.Store, resources store.ResourceStore, opts ...Option) *Service {
	s := &Service{functions: fs, resources: resources, executor: &lambdaexec.MockExecutor{}}
	for _, o := range opts {
		o(s)
	}
	if s.executor == nil {
		s.executor = &lambdaexec.MockExecutor{}
	}
	return s
}

// Reset wipes the underlying store.
func (s *Service) Reset(ctx context.Context) { s.functions.Reset(ctx) }

// --- errors ---

// mapErr maps a functions store error onto a canonical provider error.
func mapErr(err error) error {
	if errors.Is(err, functionsstore.ErrNoSuchFunction) {
		return model.NewProviderError("NotFound", "function not found", 404)
	}
	return err
}

// invalidArgument builds the canonical InvalidArgument provider error.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// --- resource-name helpers ---

// ParseFunctionName validates a function resource name and returns its project,
// location, and id. Both the full form
// ("projects/{p}/locations/{l}/functions/{id}") and the relative form
// ("locations/{l}/functions/{id}") are accepted. Source archive references are
// deliberately not validated. A malformed name is InvalidArgument.
func ParseFunctionName(name string) (project, location, id string, err error) {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		switch parts[i] {
		case "projects":
			project = parts[i+1]
		case "locations":
			location = parts[i+1]
		case "functions":
			id = parts[i+1]
		}
	}
	if location == "" || id == "" {
		return "", "", "", invalidArgument("malformed function resource name " + name)
	}
	return project, location, id, nil
}

// ParseLocationParent validates a location parent name
// ("projects/{p}/locations/{l}") and returns its project and location.
func ParseLocationParent(parent string) (project, location string, err error) {
	parts := strings.Split(strings.Trim(parent, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		switch parts[i] {
		case "projects":
			project = parts[i+1]
		case "locations":
			location = parts[i+1]
		}
	}
	if location == "" {
		return "", "", invalidArgument("malformed location parent " + parent)
	}
	return project, location, nil
}

// ParseOperationName validates a long-running operation name and returns its
// location and id. Both the full and relative forms are accepted.
func ParseOperationName(name string) (location, id string, err error) {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		switch parts[i] {
		case "locations":
			location = parts[i+1]
		case "operations":
			id = parts[i+1]
		}
	}
	if location == "" || id == "" {
		return "", "", invalidArgument("malformed operation resource name " + name)
	}
	return location, id, nil
}

// resolveCreateID cross-checks an optional request-body function name against
// the requested location and returns the function id. An explicit id (the
// functionId query/field parameter) wins over the name-derived one, but the
// name's location must still match when present.
func resolveCreateID(name, location, explicitID string) (string, error) {
	id := explicitID
	if name != "" {
		_, loc, fid, err := ParseFunctionName(name)
		if err != nil {
			return "", err
		}
		if location != "" && loc != location {
			return "", invalidArgument("function name location " + loc + " does not match request location " + location)
		}
		if id == "" {
			id = fid
		}
	}
	return id, nil
}

// defaultHttpsTriggerURL derives the deployed HTTPS URL for an HTTP-triggered
// function (region-project.cloudfunctions.net/{name}).
func defaultHttpsTriggerURL(project, location, id string) string {
	return fmt.Sprintf("https://%s-%s.cloudfunctions.net/%s", location, project, id)
}

// --- map helpers (used by the typed input extraction in render.go) ---

func bodyString(body map[string]any, key string) string {
	if body == nil {
		return ""
	}
	s, _ := body[key].(string)
	return s
}

func stringOf(v any) string {
	s, _ := v.(string)
	return s
}

func bodyStringMap(body map[string]any, key string) map[string]string {
	if body == nil {
		return nil
	}
	m, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// nestedMap returns body[key] as a map, or nil when absent/malformed.
func nestedMap(body map[string]any, key string) map[string]any {
	if body == nil {
		return nil
	}
	m, _ := body[key].(map[string]any)
	return m
}

// parseMemoryMB parses a Cloud Functions v2 memory string ("256M", "1G",
// "512Mi") into megabytes. Unknown forms return 0.
func parseMemoryMB(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	mult := 1
	switch {
	case strings.HasSuffix(s, "Gi"):
		mult, s = 1024, strings.TrimSuffix(s, "Gi")
	case strings.HasSuffix(s, "Mi"):
		s = strings.TrimSuffix(s, "Mi")
	case strings.HasSuffix(s, "G"):
		mult, s = 1024, strings.TrimSuffix(s, "G")
	case strings.HasSuffix(s, "M"):
		s = strings.TrimSuffix(s, "M")
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return 0
	}
	return n * mult
}

// timeoutSeconds parses a duration string ("60s") into whole seconds. A
// non-positive or unparsable value returns 0.
func timeoutSeconds(s string) int {
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0
	}
	return int(d.Seconds())
}

// formatTimestamp renders a business timestamp as the RFC3339Nano string the
// Discovery JSON shape uses.
func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// pageParams builds the shared cursor-pagination parameter map accepted by
// internal/gcp/paging.
func pageParams(pageSize int, pageToken string) map[string]any {
	params := map[string]any{"pageSize": pageSize}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	return params
}

// resourceID is the project-scoped resource-name formatter used by the render
// helpers. It mirrors NormalizedRequest.ResourceID for the REST transport.
func resourceID(project string) func(string, string) string {
	return resource.ResourceID(project)
}

// iamID is the store key for a function's IAM policy (location/id).
func iamID(location, id string) string { return location + "/" + id }

// loadPolicy returns the function's stored IAM policy, or an empty default.
func (s *Service) loadPolicy(ctx context.Context, project, location, id string) policy.Policy {
	return policy.Load(ctx, s.resources, project, rtFunctionPolicy, iamID(location, id))
}

// now is the indirection that keeps business timestamps on clock.Now.
func now() time.Time { return clock.Now().UTC() }

// newUUID returns a random v4 UUID for operation ids.
func newUUID() string { return uuid.New().String() }

// fmtIntM renders a megabyte count in the v2 availableMemory string form.
func fmtIntM(n int) string { return strconv.Itoa(n) + "M" }
