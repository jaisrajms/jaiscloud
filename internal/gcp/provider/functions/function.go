// Package functions implements the Google Cloud Functions v1 provider.
package functions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"

	"github.com/google/uuid"
)

// rtFunctionPolicy is the generic ResourceStore type for function IAM policies.
const rtFunctionPolicy = "gcp_function_policy"

// Provider handles Cloud Functions v1 functions.
type Provider struct {
	functions functionsstore.Store
	resources store.ResourceStore // IAM policies (control-plane)
	executor  lambdaexec.LambdaExecutor
}

// New returns a Provider backed by the given store. executor defaults to a
// MockExecutor (echo) when nil so CallFunction works out of the box.
func New(functions functionsstore.Store, resources store.ResourceStore, executor lambdaexec.LambdaExecutor) *Provider {
	if executor == nil {
		executor = &lambdaexec.MockExecutor{}
	}
	return &Provider{functions: functions, resources: resources, executor: executor}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Function.CreateFunction":             p.CreateFunction,
		"Function.GetFunction":                p.GetFunction,
		"Function.ListFunctions":              p.ListFunctions,
		"Function.UpdateFunction":             p.UpdateFunction,
		"Function.DeleteFunction":             p.DeleteFunction,
		"Function.CallFunction":               p.CallFunction,
		"Function.GenerateUploadUrl":          p.GenerateUploadUrl,
		"Function.FunctionGetIamPolicy":       p.FunctionGetIamPolicy,
		"Function.FunctionSetIamPolicy":       p.FunctionSetIamPolicy,
		"Function.FunctionTestIamPermissions": p.FunctionTestIamPermissions,
	}
}

// defaultHttpsTriggerURL derives the deployed HTTPS URL for an HTTP-triggered
// function (region-project.cloudfunctions.net/{name}).
func defaultHttpsTriggerURL(project, location, id string) string {
	return fmt.Sprintf("https://%s-%s.cloudfunctions.net/%s", location, project, id)
}

// functionID extracts the function ID from a relative name
// ("locations/{l}/functions/{id}") or a full GCP name.
func functionID(name string) string {
	if i := strings.Index(name, "/functions/"); i >= 0 {
		return name[i+len("/functions/"):]
	}
	return strings.TrimPrefix(name, "functions/")
}

// resourceName returns the "name" path param, or a 400 when absent.
func resourceName(nr *model.NormalizedRequest) (string, error) {
	n, ok := nr.Params["name"].(string)
	if !ok || n == "" {
		return "", model.NewProviderError("InvalidArgument", "missing resource name", 400)
	}
	return n, nil
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyString(body map[string]any, key string) string {
	if body == nil {
		return ""
	}
	s, _ := body[key].(string)
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

func bodyEventTrigger(body map[string]any) *functionsstore.EventTrigger {
	if body == nil {
		return nil
	}
	et, ok := body["eventTrigger"].(map[string]any)
	if !ok {
		return nil
	}
	out := &functionsstore.EventTrigger{
		EventType: stringOf(et["eventType"]),
		Resource:  stringOf(et["resource"]),
		Service:   stringOf(et["service"]),
	}
	return out
}

func stringOf(v any) string {
	s, _ := v.(string)
	return s
}

// functionFromBody builds the store Function from a request body, deriving the
// HTTPS trigger URL when no eventTrigger is present.
func functionFromBody(nr *model.NormalizedRequest, body map[string]any, location, id string) functionsstore.Function {
	now := clock.Now().UTC()
	f := functionsstore.Function{
		ID:                   id,
		Location:             location,
		Runtime:              bodyString(body, "runtime"),
		EntryPoint:           bodyString(body, "entryPoint"),
		SourceUploadURL:      bodyString(body, "sourceUploadUrl"),
		SourceArchiveURL:     bodyString(body, "sourceArchiveUrl"),
		EnvironmentVariables: bodyStringMap(body, "environmentVariables"),
		Status:               "ACTIVE",
		CreateTime:           now,
		UpdateTime:           now,
		Labels:               bodyStringMap(body, "labels"),
		AvailableMemoryMB:    256,
		Timeout:              "60s",
		Description:          bodyString(body, "description"),
	}
	if v := bodyString(body, "timeout"); v != "" {
		f.Timeout = v
	}
	if n, ok := body["availableMemoryMb"].(float64); ok && n > 0 {
		f.AvailableMemoryMB = int(n)
	}
	if et := bodyEventTrigger(body); et != nil {
		f.EventTrigger = et
	} else {
		f.HttpsTriggerURL = defaultHttpsTriggerURL(nr.AccountID, location, id)
	}
	return f
}

// functionToMap renders a store Function as a CloudFunction wire map.
func functionToMap(nr *model.NormalizedRequest, f functionsstore.Function) map[string]any {
	out := map[string]any{
		"name":   nr.ResourceID("cloud-function", f.Location+"/"+f.ID),
		"status": f.Status,
	}
	if !f.UpdateTime.IsZero() {
		out["updateTime"] = f.UpdateTime.Format(time.RFC3339Nano)
	}
	if f.Runtime != "" {
		out["runtime"] = f.Runtime
	}
	if f.EntryPoint != "" {
		out["entryPoint"] = f.EntryPoint
	}
	if f.SourceUploadURL != "" {
		out["sourceUploadUrl"] = f.SourceUploadURL
	}
	if f.SourceArchiveURL != "" {
		out["sourceArchiveUrl"] = f.SourceArchiveURL
	}
	if f.EventTrigger != nil {
		out["eventTrigger"] = map[string]any{
			"eventType": f.EventTrigger.EventType,
			"resource":  f.EventTrigger.Resource,
			"service":   f.EventTrigger.Service,
		}
	} else if f.HttpsTriggerURL != "" {
		out["httpsTrigger"] = map[string]any{
			"url":           f.HttpsTriggerURL,
			"securityLevel": "SECURE_ALWAYS",
		}
	}
	if f.EnvironmentVariables != nil {
		out["environmentVariables"] = f.EnvironmentVariables
	}
	if f.Labels != nil {
		out["labels"] = f.Labels
	}
	if f.AvailableMemoryMB > 0 {
		out["availableMemoryMb"] = f.AvailableMemoryMB
	}
	if f.Timeout != "" {
		out["timeout"] = f.Timeout
	}
	if f.Description != "" {
		out["description"] = f.Description
	}
	return out
}

func mapErr(err error) error {
	if errors.Is(err, functionsstore.ErrNoSuchFunction) {
		return model.NewProviderError("NotFound", "function not found", 404)
	}
	return err
}

func (p *Provider) CreateFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	id := strParam(nr, "functionId")
	if id == "" {
		if n := bodyString(body, "name"); n != "" {
			id = functionID(n)
		}
	}
	if id == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing functionId", 400)
	}
	f := functionFromBody(nr, body, location, id)
	if err := p.functions.CreateFunction(ctx, nr.AccountID, location, id, f); err != nil {
		if errors.Is(err, functionsstore.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "function already exists", 409)
		}
		return nil, err
	}
	return provider.OK(functionToMap(nr, f)), nil
}

func (p *Provider) GetFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	location := strParam(nr, "location")
	f, err := p.functions.GetFunction(ctx, nr.AccountID, location, functionID(name))
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(functionToMap(nr, f)), nil
}

func (p *Provider) ListFunctions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	// "-" is GCP's all-locations wildcard (locations/-/functions): aggregate
	// across every region for the project instead of an exact-match lookup,
	// which would always be empty since no function is ever stored under the
	// literal location "-".
	var fns []functionsstore.Function
	var err error
	if location == "-" {
		fns, err = p.functions.ListFunctionsAllLocations(ctx, nr.AccountID)
	} else {
		fns, err = p.functions.ListFunctions(ctx, nr.AccountID, location)
	}
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(fns, func(f functionsstore.Function) string { return f.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, f := range page {
		items = append(items, functionToMap(nr, f))
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
	location := strParam(nr, "location")
	id := functionID(name)
	body, _ := nr.Params["body"].(map[string]any)
	f, err := p.functions.UpdateFunctionAtomic(ctx, nr.AccountID, location, id, func(f functionsstore.Function) (functionsstore.Function, error) {
		// PATCH is a merge: apply only the fields present in the body.
		if v := bodyString(body, "runtime"); v != "" {
			f.Runtime = v
		}
		if v := bodyString(body, "entryPoint"); v != "" {
			f.EntryPoint = v
		}
		if v := bodyString(body, "sourceUploadUrl"); v != "" {
			f.SourceUploadURL = v
		}
		if v := bodyString(body, "sourceArchiveUrl"); v != "" {
			f.SourceArchiveURL = v
		}
		if env := bodyStringMap(body, "environmentVariables"); env != nil {
			f.EnvironmentVariables = env
		}
		if labels := bodyStringMap(body, "labels"); labels != nil {
			f.Labels = labels
		}
		if v := bodyString(body, "description"); v != "" {
			f.Description = v
		}
		if v := bodyString(body, "timeout"); v != "" {
			f.Timeout = v
		}
		if n, ok := body["availableMemoryMb"].(float64); ok && n > 0 {
			f.AvailableMemoryMB = int(n)
		}
		if et := bodyEventTrigger(body); et != nil {
			f.EventTrigger = et
		}
		f.UpdateTime = clock.Now().UTC()
		return f, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(functionToMap(nr, f)), nil
}

func (p *Provider) DeleteFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	location := strParam(nr, "location")
	if err := p.functions.DeleteFunction(ctx, nr.AccountID, location, functionID(name)); err != nil {
		return nil, mapErr(err)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// CallFunction invokes a function synchronously via the Lambda executor. The
// request payload is the CallFunctionRequest.data string; the executor (mock
// echo by default, Docker/K8s under JAISCLOUD_EXECUTOR_MODE) runs the
// function's entryPoint and returns the result as a string.
func (p *Provider) CallFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	location := strParam(nr, "location")
	id := functionID(name)
	f, err := p.functions.GetFunction(ctx, nr.AccountID, location, id)
	if err != nil {
		return nil, mapErr(err)
	}
	body, _ := nr.Params["body"].(map[string]any)
	data := bodyString(body, "data")

	timeout, err := time.ParseDuration(f.Timeout)
	if err != nil || timeout <= 0 {
		timeout = 60 * time.Second
	}
	invCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req := lambdaexec.InvokeRequest{
		FunctionName: f.ID,
		Runtime:      f.Runtime,
		Handler:      f.EntryPoint,
		EnvVars:      f.EnvironmentVariables,
		Payload:      []byte(data),
		AccountID:    nr.AccountID,
		MemoryMB:     f.AvailableMemoryMB,
		TimeoutSecs:  int(timeout.Seconds()),
	}
	executionID := uuid.New().String()
	result, err := p.executor.Invoke(invCtx, req)
	if err != nil {
		return provider.OK(map[string]any{
			"executionId": executionID,
			"error":       err.Error(),
		}), nil
	}
	return provider.OK(map[string]any{
		"executionId": executionID,
		"result":      string(result.Payload),
	}), nil
}

// GenerateUploadUrl returns a fake signed upload URL for source deployment.
func (p *Provider) GenerateUploadUrl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	project := nr.AccountID
	return provider.OK(map[string]any{
		"uploadUrl": fmt.Sprintf("https://storage.googleapis.com/uploads/%s/%s/%s.zip",
			project, location, uuid.New().String()),
	}), nil
}

func (p *Provider) requireFunction(ctx context.Context, nr *model.NormalizedRequest) error {
	name, err := resourceName(nr)
	if err != nil {
		return err
	}
	if _, err := p.functions.GetFunction(ctx, nr.AccountID, strParam(nr, "location"), functionID(name)); err != nil {
		return mapErr(err)
	}
	return nil
}

func (p *Provider) FunctionGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if err := p.requireFunction(ctx, nr); err != nil {
		return nil, err
	}
	id := strParam(nr, "location") + "/" + functionID(name)
	return provider.OK(policy.ToMap(policy.Load(ctx, p.resources, nr.AccountID, rtFunctionPolicy, id))), nil
}

func (p *Provider) FunctionSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if err := p.requireFunction(ctx, nr); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, rtFunctionPolicy, strParam(nr, "location")+"/"+functionID(name), body)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) FunctionTestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, err := resourceName(nr); err != nil {
		return nil, err
	}
	if err := p.requireFunction(ctx, nr); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	return provider.OK(map[string]any{"permissions": policy.TestPermissions(policy.Permissions(body))}), nil
}
