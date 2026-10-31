package ui

import (
	"context"
	"strconv"

	runcore "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	runui "jaiscloud/internal/gcp/ui/run"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// typedRun serves the Cloud Run console through the plugin chain when a plugin
// covers the key (see typed.go).
type typedRun struct {
	runui.ProviderInterface
	c typedCaller
}

func routeRun(c typedCaller, p runui.ProviderInterface) runui.ProviderInterface {
	return typedRun{ProviderInterface: p, c: c}
}

// runSystemFields are the fields the service renderer adds to the stored template.
var runSystemFields = map[string]bool{
	"name": true, "uid": true, "generation": true, "observedGeneration": true, "etag": true,
	"createTime": true, "updateTime": true, "deleteTime": true, "uri": true, "urls": true,
	"latestReadyRevision": true, "latestCreatedRevision": true, "terminalCondition": true,
	"conditions": true, "reconciling": true, "trafficStatuses": true, "service": true, "@type": true,
}

// templateOf is what the stored model keeps: the body minus the rendered system fields.
func templateOf(m map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		if !runSystemFields[k] {
			out[k] = v
		}
	}
	return out
}

func int64Field(m map[string]any, field string) int64 {
	n, _ := strconv.ParseInt(strField(m, field), 10, 64)
	return n
}

func decodeRunService(m map[string]any) runstore.Service {
	parts := nameParts(strField(m, "name"))
	return runstore.Service{
		ProjectID: parts["projects"], Location: parts["locations"], ID: parts["services"],
		UID: strField(m, "uid"), Generation: int64Field(m, "generation"), Etag: strField(m, "etag"),
		Uri: strField(m, "uri"), LatestReadyRevision: strField(m, "latestReadyRevision"),
		LatestCreatedRevision: strField(m, "latestCreatedRevision"),
		CreateTime:            timeField(m, "createTime"), UpdateTime: timeField(m, "updateTime"),
		DeleteTime: timeField(m, "deleteTime"), Data: templateOf(m),
	}
}

func decodeRunRevision(m map[string]any) runstore.Revision {
	parts := nameParts(strField(m, "name"))
	return runstore.Revision{
		ProjectID: parts["projects"], Location: parts["locations"], Service: parts["services"], ID: parts["revisions"],
		UID: strField(m, "uid"), Generation: int64Field(m, "generation"), Etag: strField(m, "etag"),
		CreateTime: timeField(m, "createTime"), UpdateTime: timeField(m, "updateTime"),
		DeleteTime: timeField(m, "deleteTime"), Data: templateOf(m),
	}
}

func decodeRunOperation(m map[string]any) runstore.Operation {
	parts := nameParts(strField(m, "name"))
	op := runstore.Operation{ProjectID: parts["projects"], Location: parts["locations"], ID: parts["operations"]}
	op.Done, _ = m["done"].(bool)
	snapshot, _ := m["response"].(map[string]any)
	if snapshot == nil {
		snapshot, _ = m["metadata"].(map[string]any)
	}
	if snapshot != nil {
		if _, isRevision := snapshot["service"]; isRevision {
			r := decodeRunRevision(snapshot)
			op.Revision = &r
		} else {
			s := decodeRunService(snapshot)
			op.Service = &s
		}
	}
	return op
}

func (p typedRun) ListAllServices(ctx context.Context, project string) ([]runstore.Service, error) {
	all := func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		svcs, err := p.ProviderInterface.ListAllServices(ctx, project)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, len(svcs))
		for _, s := range svcs {
			out = append(out, runcore.ServiceJSON(s))
		}
		return provider.OK(map[string]any{"services": out}), nil
	}
	data, covered, err := p.c.do(ctx, "Run.ListServices", project, "", map[string]any{"project": project, "location": "-"}, all)
	if !covered {
		return p.ProviderInterface.ListAllServices(ctx, project)
	}
	if err != nil {
		return nil, err
	}
	var out []runstore.Service
	for _, m := range items(data, "services") {
		out = append(out, decodeRunService(m))
	}
	return out, nil
}

func (p typedRun) GetService(ctx context.Context, project, location, id string) (runstore.Service, error) {
	data, covered, err := p.c.do(ctx, "Run.GetService", project, location, map[string]any{"project": project, "location": location, "service": id}, nil)
	if !covered {
		return p.ProviderInterface.GetService(ctx, project, location, id)
	}
	if err != nil {
		return runstore.Service{}, err
	}
	return decodeRunService(data), nil
}

func (p typedRun) ListRevisions(ctx context.Context, project, location, service string) ([]runstore.Revision, error) {
	data, covered, err := p.c.do(ctx, "Run.ListRevisions", project, location, map[string]any{"project": project, "location": location, "service": service}, nil)
	if !covered {
		return p.ProviderInterface.ListRevisions(ctx, project, location, service)
	}
	if err != nil {
		return nil, err
	}
	var out []runstore.Revision
	for _, m := range items(data, "revisions") {
		out = append(out, decodeRunRevision(m))
	}
	return out, nil
}

func (p typedRun) GetRevision(ctx context.Context, project, location, service, id string) (runstore.Revision, error) {
	data, covered, err := p.c.do(ctx, "Run.GetRevision", project, location, map[string]any{"project": project, "location": location, "service": service, "revision": id}, nil)
	if !covered {
		return p.ProviderInterface.GetRevision(ctx, project, location, service, id)
	}
	if err != nil {
		return runstore.Revision{}, err
	}
	return decodeRunRevision(data), nil
}

func (p typedRun) DeleteService(ctx context.Context, project, location, id string, validateOnly bool, etag string) (runstore.Operation, error) {
	params := map[string]any{"project": project, "location": location, "service": id}
	if validateOnly {
		params["validateOnly"] = true
	}
	if etag != "" {
		params["etag"] = etag
	}
	data, covered, err := p.c.do(ctx, "Run.DeleteService", project, location, params, nil)
	if !covered {
		return p.ProviderInterface.DeleteService(ctx, project, location, id, validateOnly, etag)
	}
	if err != nil {
		return runstore.Operation{}, err
	}
	return decodeRunOperation(data), nil
}
