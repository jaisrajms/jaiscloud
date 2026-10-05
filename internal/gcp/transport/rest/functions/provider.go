// Package functions is the REST transport for Cloud Functions v1 and v2
// (cloudfunctions.googleapis.com/v1 and /v2). It is a thin adapter: every
// handler resolves the NormalizedRequest params into the shared core's typed
// API (internal/gcp/service/functions) and encodes the result as
// Discovery-shaped JSON. No business logic and no state live here — the REST
// and gRPC transports call the SAME core Service (one store, one Lambda
// executor), so they cannot drift.
//
// Unlike the other /v1/projects/{project}/... services, the Cloud Functions
// control plane is detected by the generic JSONCodec{Service: "functions"} plus
// v2 path detection in the adapter router. This package also owns the codec for
// the data-plane HTTPS-trigger URL (trigger.go), which the adapter selects by
// request Host.
package functions

import (
	"context"
	"net/http"

	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles the Cloud Functions REST data plane.
type Provider struct {
	core        *core.Service
	defaultProj string
	// operationResolvers resolve top-level operations/{id} names owned by
	// another service that shares the namespace with Cloud Functions v1 (Service
	// Usage). The /v1/operations/{id} route is decoded as a Functions operation,
	// so without them a Service Usage poll would 404. Empty (the default) leaves
	// the surface unchanged; main wires them only in the opt-in async LRO mode.
	operationResolvers []OperationResolver
}

// OperationResolver resolves a top-level google.longrunning operation name
// (operations/{id}) owned by another service that shares the namespace with
// Cloud Functions v1. ResolveOperation returns handled=false when the name is
// not owned by the implementing service, so the caller can keep the canonical
// Functions NotFound.
type OperationResolver interface {
	ResolveOperation(ctx context.Context, project, name string) (map[string]any, bool, error)
}

// NewProvider returns a Functions REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// SetOperationResolvers wires cross-service top-level operation resolution. It
// is called only when the opt-in async LRO mode is enabled: in the default
// synchronous mode the create response is already done and no client polls, so
// the REST contract is left byte-for-byte unchanged.
func (p *Provider) SetOperationResolvers(rs ...OperationResolver) {
	p.operationResolvers = rs
}

// Routes maps "Function.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Function.CreateFunction":             p.CreateFunction,
		"Function.GetFunction":                p.GetFunction,
		"Function.ListFunctions":              p.ListFunctions,
		"Function.UpdateFunction":             p.UpdateFunction,
		"Function.DeleteFunction":             p.DeleteFunction,
		"Function.CallFunction":               p.CallFunction,
		"Function.InvokeTrigger":              p.InvokeTrigger,
		"Function.ListRuntimes":               p.ListRuntimes,
		"Function.GenerateUploadUrl":          p.GenerateUploadUrl,
		"Function.GenerateDownloadUrl":        p.GenerateDownloadUrl,
		"Function.ListLocations":              p.ListLocations,
		"Function.GetLocation":                p.GetLocation,
		"Function.FunctionGetIamPolicy":       p.FunctionGetIamPolicy,
		"Function.FunctionSetIamPolicy":       p.FunctionSetIamPolicy,
		"Function.FunctionTestIamPermissions": p.FunctionTestIamPermissions,
		"Function.GetOperation":               p.GetOperation,
		"Function.ListOperations":             p.ListOperations,
		"Function.WaitOperation":              p.WaitOperation,
		"Function.CancelOperation":            p.CancelOperation,
		"Function.DeleteOperation":            p.DeleteOperation,
		// v2 1st→2nd gen upgrade / traffic control plane (FD5).
		"Function.SetupFunctionUpgradeConfig":     p.SetupFunctionUpgradeConfig,
		"Function.RedirectFunctionUpgradeTraffic": p.RedirectFunctionUpgradeTraffic,
		"Function.RollbackFunctionUpgradeTraffic": p.RollbackFunctionUpgradeTraffic,
		"Function.CommitFunctionUpgrade":          p.CommitFunctionUpgrade,
		"Function.CommitFunctionUpgradeAsGen2":    p.CommitFunctionUpgradeAsGen2,
		"Function.AbortFunctionUpgrade":           p.AbortFunctionUpgrade,
		"Function.DetachFunction":                 p.DetachFunction,
	}
}

// version returns the wire API version of the request (v2 when the decoded
// path carried an apiVersion of "v2", v1 otherwise).
func (p *Provider) version(nr *model.NormalizedRequest) core.Version {
	return core.VersionFromAPI(strParam(nr, "apiVersion"))
}

// project resolves the owning project: the path project, else the request's
// account (project) scope, else the configured default.
func (p *Provider) project(nr *model.NormalizedRequest) string {
	if s := strParam(nr, "project"); s != "" {
		return s
	}
	if nr.AccountID != "" {
		return nr.AccountID
	}
	return p.defaultProj
}

// --- Function CRUD ---

func (p *Provider) CreateFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	v := p.version(nr)
	body := bodyOf(nr)
	_, op, err := p.core.CreateFunction(ctx, project, strParam(nr, "location"), strParam(nr, "functionId"), core.FunctionInputFromMap(body, v), v)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(v, project, op)), nil
}

func (p *Provider) GetFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	_, location, id, err := core.ParseFunctionName(name)
	if err != nil {
		return nil, err
	}
	project := p.project(nr)
	f, err := p.core.GetFunction(ctx, project, location, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.FunctionJSON(p.version(nr), project, f)), nil
}

func (p *Provider) ListFunctions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	v := p.version(nr)
	page, next, err := p.core.ListFunctions(ctx, project, strParam(nr, "location"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, f := range page {
		items = append(items, core.FunctionJSON(v, project, f))
	}
	resp := map[string]any{"functions": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	_, location, id, err := core.ParseFunctionName(name)
	if err != nil {
		return nil, err
	}
	project := p.project(nr)
	v := p.version(nr)
	mask := splitMask(strParam(nr, "updateMask"))
	if len(mask) == 0 {
		mask = splitMask(strParam(nr, "update_mask"))
	}
	_, op, err := p.core.UpdateFunction(ctx, project, location, id, core.FunctionInputFromMap(bodyOf(nr), v), mask, v)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(v, project, op)), nil
}

func (p *Provider) DeleteFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	_, location, id, err := core.ParseFunctionName(name)
	if err != nil {
		return nil, err
	}
	project := p.project(nr)
	op, err := p.core.DeleteFunction(ctx, project, location, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(p.version(nr), project, op)), nil
}

// CallFunction invokes a function synchronously via the core's Lambda executor.
// An executor error is returned in-band (HTTP 200 with an "error" field),
// matching real Cloud Functions.
func (p *Provider) CallFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	_, location, id, err := core.ParseFunctionName(name)
	if err != nil {
		return nil, err
	}
	executionID, result, invokeErr, err := p.core.CallFunction(ctx, p.project(nr), location, id, core.CallInput{Data: bodyString(bodyOf(nr), "data")})
	if err != nil {
		return nil, err
	}
	if invokeErr != "" {
		return provider.OK(map[string]any{"executionId": executionID, "error": invokeErr}), nil
	}
	return provider.OK(map[string]any{"executionId": executionID, "result": result}), nil
}

// InvokeTrigger serves a function at its synthesized HTTPS-trigger URL
// ("{location}-{project}.cloudfunctions.net/{functionId}"). The trigger codec
// (trigger.go) resolves the project/location from the host when the location is
// in the advertised region catalog; otherwise the ambiguous label is resolved
// against the stored functions (core.ResolveHTTPTriggerFunction), so a function
// created in a region outside the catalog still serves its synthesized URL. The
// raw request body is the function payload (any HTTP method). Only
// HTTP-triggered functions have such a URL, so an event-only function is
// NotFound, matching real Cloud Functions. The function's result is returned as
// the raw HTTP response; an executor failure — including a function timeout,
// enforced by the core — is an HTTP 500, matching an unhandled exception.
func (p *Provider) InvokeTrigger(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id := strParam(nr, "functionId")
	if id == "" {
		return nil, triggerNotFound()
	}
	project, f, err := p.triggerTarget(ctx, nr, id)
	if err != nil {
		return nil, err
	}
	if f.HttpsTriggerURL == "" {
		// Event-triggered functions have no HTTPS endpoint.
		return nil, triggerNotFound()
	}
	_, result, invokeErr, err := p.core.CallFunction(ctx, project, f.Location, id, core.CallInput{Data: strParam(nr, "payload")})
	if err != nil {
		return nil, err
	}
	if invokeErr != "" {
		return triggerResponse(http.StatusInternalServerError, invokeErr), nil
	}
	return triggerResponse(http.StatusOK, result), nil
}

// triggerTarget resolves the (project, function) a trigger request addresses.
// When the codec resolved a location from the advertised region catalog it does
// a direct lookup; otherwise it resolves the ambiguous trigger label against the
// stored functions.
func (p *Provider) triggerTarget(ctx context.Context, nr *model.NormalizedRequest, id string) (string, functionsstore.Function, error) {
	if location := strParam(nr, "location"); location != "" {
		project := p.project(nr)
		f, err := p.core.GetFunction(ctx, project, location, id)
		return project, f, err
	}
	return p.core.ResolveHTTPTriggerFunction(ctx, strParam(nr, "triggerLabel"), id)
}

// triggerResponse builds the raw HTTP response the trigger codec writes: the
// status plus the function's result as text/plain.
func triggerResponse(status int, body string) *model.ProviderResponse {
	return &model.ProviderResponse{
		HTTPStatus: status,
		Data:       map[string]any{"body": []byte(body)},
	}
}

// ListRuntimes returns the v2 runtime catalog
// (GET /v2/projects/{p}/locations/{l}/runtimes). It is the v2-deploy enabler:
// gcloud resolves a function's runtime from this list before uploading source.
func (p *Provider) ListRuntimes(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	runtimes, err := p.core.ListRuntimes(p.project(nr), strParam(nr, "location"), strParam(nr, "filter"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(runtimes))
	for _, rt := range runtimes {
		items = append(items, core.RuntimeJSON(rt))
	}
	return provider.OK(map[string]any{"runtimes": items}), nil
}

// GenerateUploadUrl provisions a GCS-backed upload target for v2 source
// deployment. The v2 response carries storageSource (bucket/object) which the
// client echoes back through buildConfig.source.storageSource after uploading;
// v1 carries only uploadUrl. The URL points at the emulator's GCS so the
// subsequent PUT lands locally.
func (p *Provider) GenerateUploadUrl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	up, err := p.core.GenerateUploadURL(ctx, p.project(nr), strParam(nr, "location"), baseURL(nr))
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"uploadUrl": up.URL}
	if p.version(nr) == core.V2 {
		resp["storageSource"] = map[string]any{"bucket": up.Bucket, "object": up.Object}
	}
	return provider.OK(resp), nil
}

// GenerateDownloadUrl returns a fake signed download URL for a function's
// source archive. The function must exist (NotFound otherwise).
func (p *Provider) GenerateDownloadUrl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	_, location, id, err := core.ParseFunctionName(name)
	if err != nil {
		return nil, err
	}
	url, err := p.core.GenerateDownloadURL(ctx, p.project(nr), location, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"downloadUrl": url}), nil
}

// --- Locations ---

func (p *Provider) GetLocation(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, err := p.core.GetLocation(p.project(nr), strParam(nr, "location"))
	if err != nil {
		return nil, err
	}
	return provider.OK(loc), nil
}

func (p *Provider) ListLocations(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	page, next := p.core.ListLocations(p.project(nr), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	items := make([]any, 0, len(page))
	for _, l := range page {
		items = append(items, l)
	}
	resp := map[string]any{"locations": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// --- IAM ---

func (p *Provider) FunctionGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, location, id, err := functionNameParts(nr)
	if err != nil {
		return nil, err
	}
	pol, err := p.core.GetIamPolicy(ctx, p.project(nr), location, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) FunctionSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, location, id, err := functionNameParts(nr)
	if err != nil {
		return nil, err
	}
	pol, err := p.core.SetIamPolicy(ctx, p.project(nr), location, id, bodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) FunctionTestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, location, id, err := functionNameParts(nr)
	if err != nil {
		return nil, err
	}
	perms, err := p.core.TestIamPermissions(ctx, p.project(nr), location, id, policy.Permissions(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"permissions": perms}), nil
}

// functionNameParts parses the request's function name into project, location,
// and id (project is returned for symmetry; callers use p.project for the
// account/project scope).
func functionNameParts(nr *model.NormalizedRequest) (project, location, id string, err error) {
	name, err := resourceName(nr)
	if err != nil {
		return "", "", "", err
	}
	return core.ParseFunctionName(name)
}

// --- Operations ---

func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	op, err := p.core.GetOperationJSON(ctx, p.project(nr), name, p.version(nr))
	if err != nil {
		// A top-level operations/{id} that Functions does not own may belong to
		// another service sharing the namespace (Service Usage). Ask their
		// resolvers before surfacing the Functions NotFound.
		if core.IsNotFound(err) {
			for _, r := range p.operationResolvers {
				resolved, handled, rerr := r.ResolveOperation(ctx, p.project(nr), name)
				if rerr != nil {
					return nil, rerr
				}
				if handled {
					return provider.OK(resolved), nil
				}
			}
		}
		return nil, err
	}
	return provider.OK(op), nil
}

// WaitOperation serves the google.longrunning.Operations.WaitOperation custom
// method (POST /v2/projects/{p}/locations/{l}/operations/{id}:wait). It returns
// the persisted operation immediately: done inline by default, or settled once
// the async delay has elapsed.
func (p *Provider) WaitOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	op, err := p.core.WaitOperation(ctx, p.project(nr), name, p.version(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) ListOperations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	v := p.version(nr)
	// returnPartialSuccess is not supported: the per-service Discovery document
	// states the field "will result in an UNIMPLEMENTED error if set unless
	// explicitly documented otherwise", and Cloud Functions does not document
	// support. Fail loud rather than silently returning a full page.
	if boolParam(nr, "returnPartialSuccess") {
		return nil, model.NewProviderError("Unimplemented", "ListOperations returnPartialSuccess is not supported", 501)
	}
	page, next, err := p.core.ListOperations(ctx, project, strParam(nr, "location"), v, strParam(nr, "filter"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, op := range page {
		items = append(items, core.OperationJSON(v, project, op))
	}
	resp := map[string]any{"operations": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) CancelOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if err := p.core.CancelOperation(ctx, p.project(nr), name); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DeleteOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if err := p.core.DeleteOperation(ctx, p.project(nr), name); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// --- v2 upgrade / traffic control plane (FD5) ---
//
// The seven v2 gen1→gen2 upgrade methods are custom POST verbs on a function
// name. Each returns a done google.longrunning.Operation carrying the updated
// Function, exactly like create/update. They are served REST-only: Google's RPC
// reference documents them as google.cloud.functions.v2.FunctionService RPCs,
// but the public googleapis proto mirror (and every generated client) omits
// them, so the emulator's generated-proto gRPC transport cannot register them.

// upgradeTarget parses a function name into its project/location/id plus the
// requested wire version.
func (p *Provider) upgradeTarget(nr *model.NormalizedRequest) (project, location, id string, v core.Version, err error) {
	name, err := resourceName(nr)
	if err != nil {
		return "", "", "", "", err
	}
	_, location, id, err = core.ParseFunctionName(name)
	if err != nil {
		return "", "", "", "", err
	}
	return p.project(nr), location, id, p.version(nr), nil
}

func (p *Provider) SetupFunctionUpgradeConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, location, id, v, err := p.upgradeTarget(nr)
	if err != nil {
		return nil, err
	}
	op, err := p.core.SetupFunctionUpgradeConfig(ctx, project, location, id, core.UpgradeInputFromMap(bodyOf(nr)), v)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(v, project, op)), nil
}

func (p *Provider) RedirectFunctionUpgradeTraffic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, location, id, v, err := p.upgradeTarget(nr)
	if err != nil {
		return nil, err
	}
	op, err := p.core.RedirectFunctionUpgradeTraffic(ctx, project, location, id, v)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(v, project, op)), nil
}

func (p *Provider) RollbackFunctionUpgradeTraffic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, location, id, v, err := p.upgradeTarget(nr)
	if err != nil {
		return nil, err
	}
	op, err := p.core.RollbackFunctionUpgradeTraffic(ctx, project, location, id, v)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(v, project, op)), nil
}

func (p *Provider) CommitFunctionUpgrade(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, location, id, v, err := p.upgradeTarget(nr)
	if err != nil {
		return nil, err
	}
	op, err := p.core.CommitFunctionUpgrade(ctx, project, location, id, v)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(v, project, op)), nil
}

func (p *Provider) CommitFunctionUpgradeAsGen2(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, location, id, v, err := p.upgradeTarget(nr)
	if err != nil {
		return nil, err
	}
	op, err := p.core.CommitFunctionUpgradeAsGen2(ctx, project, location, id, v)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(v, project, op)), nil
}

func (p *Provider) AbortFunctionUpgrade(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, location, id, v, err := p.upgradeTarget(nr)
	if err != nil {
		return nil, err
	}
	op, err := p.core.AbortFunctionUpgrade(ctx, project, location, id, v)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(v, project, op)), nil
}

func (p *Provider) DetachFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, location, id, v, err := p.upgradeTarget(nr)
	if err != nil {
		return nil, err
	}
	op, err := p.core.DetachFunction(ctx, project, location, id, v)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(v, project, op)), nil
}
