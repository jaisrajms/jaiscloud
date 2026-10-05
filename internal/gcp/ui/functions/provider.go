package functionsui

import (
	"archive/zip"
	"bytes"
	"context"
	"sort"

	"jaiscloud/internal/gcp/policy"
	functionscore "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
)

// ProviderInterface is the subset of *functionscore.Service used by the
// Functions UI handlers. The core is transport-neutral and addressable
// directly, so the UI reuses its typed API rather than going through a REST
// adapter. It keeps the UI decoupled from the core's concrete type and hides
// the (usually done) long-running operations the core returns from mutations.
type ProviderInterface interface {
	// ListFunctions lists every function in a project across all locations,
	// sorted by location then id. It backs the location-optional console list.
	ListFunctions(ctx context.Context, project string) ([]functionsstore.Function, error)

	// GetFunction returns one function.
	GetFunction(ctx context.Context, project, location, id string) (functionsstore.Function, error)

	// CreateFunction deploys a function from the create-form input.
	CreateFunction(ctx context.Context, project, location string, in CreateInput) (functionsstore.Function, error)

	// DeleteFunction removes a function.
	DeleteFunction(ctx context.Context, project, location, id string) error

	// CallFunction invokes a function synchronously (the console "Test"
	// action). An executor failure is returned in invokeErr with a nil error.
	CallFunction(ctx context.Context, project, location, id, data string) (executionID, result, invokeErr string, err error)

	// GetIamPolicy/SetIamPolicy manage the function's IAM policy.
	GetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error)
	SetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error)

	// ListDeliveries returns a function's persisted event-delivery records
	// (the emulator-only "Executions" tab). The result is filtered to the
	// function id by the provider.
	ListDeliveries(ctx context.Context, project, location, id string) ([]functionsstore.Delivery, error)
}

// Provider adapts the transport-neutral Functions core onto ProviderInterface,
// discarding the synchronous mutation operations and the wire version (the
// console renders the modern v2 shape).
type Provider struct {
	svc *functionscore.Service
}

// NewProvider returns a UI provider over the Functions core.
func NewProvider(svc *functionscore.Service) *Provider { return &Provider{svc: svc} }

var _ ProviderInterface = (*Provider)(nil)

// ListFunctions implements ProviderInterface. The core's "-" location is GCP's
// all-locations wildcard; the page size is left at the core default so the
// console shows the whole project.
func (p *Provider) ListFunctions(ctx context.Context, project string) ([]functionsstore.Function, error) {
	fns, _, err := p.svc.ListFunctions(ctx, project, "-", 0, "")
	if err != nil {
		return nil, err
	}
	sort.Slice(fns, func(i, j int) bool {
		if fns[i].Location != fns[j].Location {
			return fns[i].Location < fns[j].Location
		}
		return fns[i].ID < fns[j].ID
	})
	return fns, nil
}

// GetFunction implements ProviderInterface.
func (p *Provider) GetFunction(ctx context.Context, project, location, id string) (functionsstore.Function, error) {
	return p.svc.GetFunction(ctx, project, location, id)
}

// CreateFunction implements ProviderInterface; the create LRO is discarded. The
// console deploys 2nd gen functions, so the input is interpreted as V2.
func (p *Provider) CreateFunction(ctx context.Context, project, location string, in CreateInput) (functionsstore.Function, error) {
	fin := functionscore.FunctionInput{
		Runtime:              in.Runtime,
		EntryPoint:           in.EntryPoint,
		Description:          in.Description,
		AvailableMemoryMB:    in.AvailableMemoryMB,
		Timeout:              in.Timeout,
		EnvironmentVariables: in.EnvironmentVariables,
		Labels:               in.Labels,
		SourceArchiveURL:     in.SourceArchiveURL,
	}
	if in.TriggerType == "event" {
		fin.EventTrigger = &functionsstore.EventTrigger{
			EventType: in.EventType,
			Resource:  in.EventResource,
		}
		if in.Retry {
			fin.EventTrigger.RetryPolicy = "RETRY_POLICY_RETRY"
		}
	}
	// v2 ServiceConfig instance/concurrency settings. The Has* flags are set so
	// the core persists exactly what the form carried.
	if in.MinInstanceCount > 0 {
		fin.MinInstanceCount = in.MinInstanceCount
		fin.HasMinInstanceCount = true
	}
	if in.MaxInstanceCount > 0 {
		fin.MaxInstanceCount = in.MaxInstanceCount
		fin.HasMaxInstanceCount = true
	}
	if in.MaxInstanceRequestConcurrency > 0 {
		fin.MaxInstanceRequestConcurrency = in.MaxInstanceRequestConcurrency
		fin.HasMaxInstanceRequestConcurrency = true
	}
	if in.AvailableCPU != "" {
		fin.AvailableCPU = in.AvailableCPU
		fin.HasAvailableCPU = true
	}
	archive, err := inlineArchive(in.SourceInline, in.SourceFilename)
	if err != nil {
		return functionsstore.Function{}, err
	}
	fin.SourceArchive = archive

	f, _, err := p.svc.CreateFunction(ctx, project, location, in.ID, fin, functionscore.V2)
	return f, err
}

// DeleteFunction implements ProviderInterface; the delete LRO is discarded.
func (p *Provider) DeleteFunction(ctx context.Context, project, location, id string) error {
	_, err := p.svc.DeleteFunction(ctx, project, location, id)
	return err
}

// CallFunction implements ProviderInterface.
func (p *Provider) CallFunction(ctx context.Context, project, location, id, data string) (string, string, string, error) {
	return p.svc.CallFunction(ctx, project, location, id, functionscore.CallInput{Data: data})
}

// GetIamPolicy implements ProviderInterface.
func (p *Provider) GetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error) {
	return p.svc.GetIamPolicy(ctx, project, location, id)
}

// SetIamPolicy implements ProviderInterface.
func (p *Provider) SetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	return p.svc.SetIamPolicy(ctx, project, location, id, body)
}

// ListDeliveries implements ProviderInterface. The core lists deliveries per
// location; the handler filters them to one function.
func (p *Provider) ListDeliveries(ctx context.Context, project, location, id string) ([]functionsstore.Delivery, error) {
	all, err := p.svc.ListDeliveries(ctx, project, location)
	if err != nil {
		return nil, err
	}
	out := make([]functionsstore.Delivery, 0, len(all))
	for _, d := range all {
		if d.FunctionID == id {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreateTime.Before(out[j].CreateTime) })
	return out, nil
}

// inlineArchive packages an inline source file into a zip archive for the
// create path. A blank source yields a nil archive (metadata-only create, an
// emulator convenience), so callers never persist an empty archive.
func inlineArchive(source, filename string) ([]byte, error) {
	if source == "" {
		return nil, nil
	}
	if filename == "" {
		filename = "index.js"
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	fw, err := zw.Create(filename)
	if err != nil {
		return nil, err
	}
	if _, err := fw.Write([]byte(source)); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
