package ui

import (
	"context"
	"strconv"
	"strings"

	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/resource"
	functionscore "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	functionsui "jaiscloud/internal/gcp/ui/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// typedFunctions serves the Cloud Functions console through the plugin chain when
// a plugin covers the key (see typed.go). The console deploys 2nd gen functions, so
// every call is made at API version v2. ListDeliveries, the emulator's own log of
// event deliveries, is not a Cloud Functions API action and stays on the built-in.
type typedFunctions struct {
	functionsui.ProviderInterface
	c typedCaller
}

func routeFunctions(c typedCaller, p functionsui.ProviderInterface) functionsui.ProviderInterface {
	return typedFunctions{ProviderInterface: p, c: c}
}

// v2 decodes a Cloud Functions v2 Function. Fields the REST shape does not carry
// (the stored source's hash and blob key, the revision counter) stay zero.
func decodeFunction(m map[string]any) functionsstore.Function {
	parts := nameParts(strField(m, "name"))
	f := functionsstore.Function{
		ID: parts["functions"], Location: parts["locations"], Status: strField(m, "state"),
		HttpsTriggerURL: strField(m, "url"), Labels: strMapField(m, "labels"), Description: strField(m, "description"),
		CreateTime: timeField(m, "createTime"), UpdateTime: timeField(m, "updateTime"),
	}
	if b, ok := m["buildConfig"].(map[string]any); ok {
		f.Runtime, f.EntryPoint = strField(b, "runtime"), strField(b, "entryPoint")
		if src, ok := b["source"].(map[string]any); ok {
			if ss, ok := src["storageSource"].(map[string]any); ok {
				f.SourceArchiveURL = "gs://" + strField(ss, "bucket") + "/" + strField(ss, "object")
			}
			f.SourceUploadURL = strField(src, "sourceUploadUrl")
		}
		f.EnvironmentVariables = strMapField(b, "environmentVariables")
	}
	if s, ok := m["serviceConfig"].(map[string]any); ok {
		if env := strMapField(s, "environmentVariables"); env != nil {
			f.EnvironmentVariables = env
		}
		f.AvailableMemoryMB = memoryMB(strField(s, "availableMemory"))
		if secs, _ := s["timeoutSeconds"].(float64); secs > 0 {
			f.Timeout = strconv.Itoa(int(secs)) + "s"
		}
		f.MinInstanceCount = intField(s, "minInstanceCount")
		f.MaxInstanceCount = intField(s, "maxInstanceCount")
		f.MaxInstanceRequestConcurrency = intField(s, "maxInstanceRequestConcurrency")
		f.AvailableCPU = strField(s, "availableCpu")
	}
	if et, ok := m["eventTrigger"].(map[string]any); ok {
		f.EventTrigger = &functionsstore.EventTrigger{
			EventType: strField(et, "eventType"), Resource: strField(et, "pubsubTopic"),
			RetryPolicy: strField(et, "retryPolicy"), Trigger: strField(et, "trigger"),
		}
		for _, flt := range items(et, "eventFilters") {
			if strField(flt, "attribute") == "bucket" {
				f.EventTrigger.Resource = strField(flt, "value")
			}
		}
	}
	return f
}

func intField(m map[string]any, field string) int {
	n, _ := m[field].(float64)
	return int(n)
}

// memoryMB reads "256M" (or "256Mi", "1G") as megabytes.
func memoryMB(s string) int {
	if s == "" {
		return 0
	}
	unit := 1
	switch {
	case strings.HasSuffix(s, "Gi"), strings.HasSuffix(s, "G"):
		unit = 1024
	}
	n, _ := strconv.Atoi(strings.TrimRight(s, "MiGB"))
	return n * unit
}

func (p typedFunctions) fnName(project, location, id string) string {
	return resource.ResourceID(project)("cloud-function", location+"/"+id)
}

func (p typedFunctions) ListFunctions(ctx context.Context, project string) ([]functionsstore.Function, error) {
	all := func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		fns, err := p.ProviderInterface.ListFunctions(ctx, project)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, len(fns))
		for _, f := range fns {
			out = append(out, functionscore.FunctionJSON(functionscore.V2, project, f))
		}
		return provider.OK(map[string]any{"functions": out}), nil
	}
	rows, covered, err := p.c.doList(ctx, "Function.ListFunctions", project, "",
		map[string]any{"project": project, "location": "-", "apiVersion": "v2"}, all, "functions")
	if !covered {
		return p.ProviderInterface.ListFunctions(ctx, project)
	}
	if err != nil {
		return nil, err
	}
	var out []functionsstore.Function
	for _, m := range rows {
		out = append(out, decodeFunction(m))
	}
	return out, nil
}

func (p typedFunctions) GetFunction(ctx context.Context, project, location, id string) (functionsstore.Function, error) {
	data, covered, err := p.c.do(ctx, "Function.GetFunction", project, location,
		map[string]any{"project": project, "location": location, "name": p.fnName(project, location, id), "apiVersion": "v2"}, nil)
	if !covered {
		return p.ProviderInterface.GetFunction(ctx, project, location, id)
	}
	if err != nil {
		return functionsstore.Function{}, err
	}
	return decodeFunction(data), nil
}

func (p typedFunctions) DeleteFunction(ctx context.Context, project, location, id string) error {
	_, covered, err := p.c.do(ctx, "Function.DeleteFunction", project, location,
		map[string]any{"project": project, "location": location, "name": p.fnName(project, location, id), "apiVersion": "v2"}, nil)
	if !covered {
		return p.ProviderInterface.DeleteFunction(ctx, project, location, id)
	}
	return err
}

// createBody renders the console form as a v2 Function body.
func createBody(in functionsui.CreateInput, bucket, object string) map[string]any {
	build := map[string]any{}
	if in.Runtime != "" {
		build["runtime"] = in.Runtime
	}
	if in.EntryPoint != "" {
		build["entryPoint"] = in.EntryPoint
	}
	if bucket != "" && object != "" {
		build["source"] = map[string]any{"storageSource": map[string]any{"bucket": bucket, "object": object}}
	}
	body := map[string]any{"buildConfig": build}
	svc := map[string]any{}
	if in.AvailableMemoryMB > 0 {
		svc["availableMemory"] = strconv.Itoa(in.AvailableMemoryMB) + "M"
	}
	if in.Timeout != "" {
		if secs, err := strconv.Atoi(strings.TrimSuffix(in.Timeout, "s")); err == nil {
			svc["timeoutSeconds"] = float64(secs)
		}
	}
	if in.MinInstanceCount > 0 {
		svc["minInstanceCount"] = float64(in.MinInstanceCount)
	}
	if in.MaxInstanceCount > 0 {
		svc["maxInstanceCount"] = float64(in.MaxInstanceCount)
	}
	if in.MaxInstanceRequestConcurrency > 0 {
		svc["maxInstanceRequestConcurrency"] = float64(in.MaxInstanceRequestConcurrency)
	}
	if in.AvailableCPU != "" {
		svc["availableCpu"] = in.AvailableCPU
	}
	if in.EnvironmentVariables != nil {
		svc["environmentVariables"] = in.EnvironmentVariables
	}
	if len(svc) > 0 {
		body["serviceConfig"] = svc
	}
	if in.Description != "" {
		body["description"] = in.Description
	}
	if in.Labels != nil {
		body["labels"] = in.Labels
	}
	if in.TriggerType == "event" {
		et := map[string]any{"eventType": in.EventType}
		if in.EventResource != "" {
			et["pubsubTopic"] = in.EventResource
		}
		if in.Retry {
			et["retryPolicy"] = "RETRY_POLICY_RETRY"
		}
		body["eventTrigger"] = et
	}
	return body
}

func (p typedFunctions) CreateFunction(ctx context.Context, project, location string, in functionsui.CreateInput) (functionsstore.Function, error) {
	if !p.c.chain.Covers("Function.CreateFunction") {
		return p.ProviderInterface.CreateFunction(ctx, project, location, in)
	}
	bucket, object := in.SourceBucket, in.SourceObject
	if (bucket == "") != (object == "") {
		return functionsstore.Function{}, model.NewProviderError("InvalidArgument", "sourceBucket and sourceObject must be supplied together", 400)
	}
	// An inline source is staged the way a client does it: ask for an upload target,
	// put the archive there, and deploy from it.
	if archive, err := functionsui.InlineArchive(in.SourceInline, in.SourceFilename); err != nil {
		return functionsstore.Function{}, err
	} else if len(archive) > 0 {
		target, err := p.GenerateUploadURL(ctx, project, location)
		if err != nil {
			return functionsstore.Function{}, err
		}
		if err := p.PutSourceArchive(ctx, project, location, target.Object, archive); err != nil {
			return functionsstore.Function{}, err
		}
		bucket, object = target.Bucket, target.Object
	}
	data, covered, err := p.c.do(ctx, "Function.CreateFunction", project, location, map[string]any{
		"project": project, "location": location, "functionId": in.ID, "apiVersion": "v2", "body": createBody(in, bucket, object)}, nil)
	if !covered {
		return p.ProviderInterface.CreateFunction(ctx, project, location, in)
	}
	if err != nil {
		return functionsstore.Function{}, err
	}
	if r, ok := data["response"].(map[string]any); ok {
		return decodeFunction(r), nil
	}
	// An operation still in flight carries no function yet: read it back.
	if done, isOp := data["done"].(bool); isOp && !done {
		return p.GetFunction(ctx, project, location, in.ID)
	}
	return decodeFunction(data), nil
}

func (p typedFunctions) GenerateUploadURL(ctx context.Context, project, location string) (functionscore.UploadTarget, error) {
	data, covered, err := p.c.do(ctx, "Function.GenerateUploadUrl", project, location,
		map[string]any{"project": project, "location": location, "apiVersion": "v2"}, nil)
	if !covered {
		return p.ProviderInterface.GenerateUploadURL(ctx, project, location)
	}
	if err != nil {
		return functionscore.UploadTarget{}, err
	}
	t := functionscore.UploadTarget{URL: strField(data, "uploadUrl")}
	if ss, ok := data["storageSource"].(map[string]any); ok {
		t.Bucket, t.Object = strField(ss, "bucket"), strField(ss, "object")
	}
	return t, nil
}

func (p typedFunctions) CallFunction(ctx context.Context, project, location, id, data string) (string, string, string, error) {
	out, covered, err := p.c.do(ctx, "Function.CallFunction", project, location, map[string]any{
		"project": project, "location": location, "name": p.fnName(project, location, id), "apiVersion": "v2", "body": map[string]any{"data": data}}, nil)
	if !covered {
		return p.ProviderInterface.CallFunction(ctx, project, location, id, data)
	}
	if err != nil {
		return "", "", "", err
	}
	return strField(out, "executionId"), strField(out, "result"), strField(out, "error"), nil
}

func (p typedFunctions) GetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error) {
	data, covered, err := p.c.do(ctx, "Function.FunctionGetIamPolicy", project, location,
		map[string]any{"project": project, "location": location, "name": p.fnName(project, location, id), "apiVersion": "v2"}, nil)
	if !covered {
		return p.ProviderInterface.GetIamPolicy(ctx, project, location, id)
	}
	if err != nil {
		return policy.Policy{}, err
	}
	return decodePolicy(data)
}

func (p typedFunctions) SetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	data, covered, err := p.c.do(ctx, "Function.FunctionSetIamPolicy", project, location,
		map[string]any{"project": project, "location": location, "name": p.fnName(project, location, id), "apiVersion": "v2", "body": body}, nil)
	if !covered {
		return p.ProviderInterface.SetIamPolicy(ctx, project, location, id, body)
	}
	if err != nil {
		return policy.Policy{}, err
	}
	return decodePolicy(data)
}
