package logging

import (
	"context"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"
)

// Provider handles the Cloud Logging v2 REST data plane. It is a thin adapter:
// every handler decodes the NormalizedRequest body/params into the core's typed
// API, calls the shared core Service, and encodes the result as Discovery-shaped
// JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Logging REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "Logging.<Action>" keys to their handlers. The action names are
// chosen so the fidelity resolver derives the Discovery method ids
// (entries.write, entries.list, logs.list, logs.delete,
// monitoredResourceDescriptors.list).
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Logging.EntryWrite":                      p.EntryWrite,
		"Logging.EntryList":                       p.EntryList,
		"Logging.LogList":                         p.LogList,
		"Logging.LogDelete":                       p.LogDelete,
		"Logging.MonitoredResourceDescriptorList": p.MonitoredResourceDescriptorList,

		"Logging.SinkCreate": p.SinkCreate,
		"Logging.SinkGet":    p.SinkGet,
		"Logging.SinkList":   p.SinkList,
		"Logging.SinkUpdate": p.SinkUpdate,
		"Logging.SinkPatch":  p.SinkPatch,
		"Logging.SinkDelete": p.SinkDelete,

		"Logging.ExclusionCreate": p.ExclusionCreate,
		"Logging.ExclusionGet":    p.ExclusionGet,
		"Logging.ExclusionList":   p.ExclusionList,
		"Logging.ExclusionPatch":  p.ExclusionPatch,
		"Logging.ExclusionDelete": p.ExclusionDelete,

		"Logging.MetricCreate": p.MetricCreate,
		"Logging.MetricGet":    p.MetricGet,
		"Logging.MetricList":   p.MetricList,
		"Logging.MetricUpdate": p.MetricUpdate,
		"Logging.MetricDelete": p.MetricDelete,
	}
}

// project resolves the owning project: the request's account (project) scope,
// else the configured default.
func (p *Provider) project(nr *model.NormalizedRequest) string {
	if s := strParam(nr, "project"); s != "" {
		return s
	}
	if nr.AccountID != "" {
		return nr.AccountID
	}
	return p.defaultProj
}

// defaultScope resolves the fallback scope parent ("projects/{p}") from the
// request project, or "" when no project resolves.
func (p *Provider) defaultScope(nr *model.NormalizedRequest) string {
	project := p.project(nr)
	if project == "" {
		return ""
	}
	scope, err := core.ParseScopeParent(project)
	if err != nil {
		return ""
	}
	return scope
}

// ─── handlers ─────────────────────────────────────────────────────────────────

func (p *Provider) EntryWrite(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	req := &core.WriteRequest{
		PartialSuccess: boolFrom(body["partialSuccess"]),
		DryRun:         boolFrom(body["dryRun"]),
		LogName:        strFrom(body["logName"]),
		Labels:         stringMapFrom(body["labels"]),
	}
	if res, ok := body["resource"].(map[string]any); ok {
		req.ResourceType = strFrom(res["type"])
		req.ResourceLabels = stringMapFrom(res["labels"])
	}
	if arr, ok := body["entries"].([]any); ok {
		req.Entries = make([]loggingstore.LogEntry, 0, len(arr))
		for _, e := range arr {
			req.Entries = append(req.Entries, entryFromWire(e))
		}
	}
	if err := p.core.WriteEntries(ctx, req); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) EntryList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)

	resourceNames := strListFrom(body["resourceNames"])
	// projectIds is the legacy request field; the proto specifies it is added to
	// resourceNames, so both spellings resolve to the same scope set.
	for _, pid := range strListFrom(body["projectIds"]) {
		resourceNames = append(resourceNames, "projects/"+pid)
	}

	res, err := p.core.ListEntries(ctx, &core.ListEntriesRequest{
		ResourceNames: resourceNames,
		Filter:        strFrom(body["filter"]),
		OrderBy:       strFrom(body["orderBy"]),
		PageSize:      intFrom(body["pageSize"]),
		PageToken:     strFrom(body["pageToken"]),
		Scope:         p.defaultScope(nr),
	})
	if err != nil {
		return nil, err
	}

	out := map[string]any{}
	if len(res.Entries) > 0 {
		entries := make([]any, 0, len(res.Entries))
		for _, e := range res.Entries {
			entries = append(entries, entryToWire(e))
		}
		out["entries"] = entries
	}
	if res.NextPageToken != "" {
		out["nextPageToken"] = res.NextPageToken
	}
	return provider.OK(out), nil
}

func (p *Provider) LogList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	names, next, err := p.core.ListLogs(ctx,
		strParam(nr, "parent"),
		p.defaultScope(nr),
		intFrom(nr.Params["pageSize"]),
		strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(names) > 0 {
		out["logNames"] = names
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) LogDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteLog(ctx, strParam(nr, "logName")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) MonitoredResourceDescriptorList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	page, next := p.core.ListMonitoredResourceDescriptors(
		intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	out := map[string]any{}
	if len(page) > 0 {
		descs := make([]any, 0, len(page))
		for _, d := range page {
			descs = append(descs, descriptorToWire(d))
		}
		out["resourceDescriptors"] = descs
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

// ─── sinks ────────────────────────────────────────────────────────────────────

func (p *Provider) SinkCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	sink, err := p.core.CreateSink(ctx, parent, sinkFromWire(bodyOf(nr)),
		boolParam(nr, "uniqueWriterIdentity"), strParam(nr, "customWriterIdentity"))
	if err != nil {
		return nil, err
	}
	return provider.OK(sinkToWire(sink, sinkResourceName(parent, sink.Name))), nil
}

func (p *Provider) SinkGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "sinkName")
	sink, err := p.core.GetSink(ctx, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(sinkToWire(sink, name)), nil
}

func (p *Provider) SinkList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	sinks, next, err := p.core.ListSinks(ctx, parent,
		intParam(nr, "pageSize"), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(sinks) > 0 {
		list := make([]any, 0, len(sinks))
		for _, s := range sinks {
			list = append(list, sinkToWire(s, sinkResourceName(parent, s.Name)))
		}
		out["sinks"] = list
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) SinkUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.updateSink(ctx, nr)
}

func (p *Provider) SinkPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.updateSink(ctx, nr)
}

func (p *Provider) updateSink(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "sinkName")
	sink, err := p.core.UpdateSink(ctx, name, sinkFromWire(bodyOf(nr)),
		updateMaskFromQuery(nr.Params["updateMask"]), boolParam(nr, "uniqueWriterIdentity"),
		strParam(nr, "customWriterIdentity"))
	if err != nil {
		return nil, err
	}
	return provider.OK(sinkToWire(sink, name)), nil
}

// sinkResourceName builds a sink's full resource name from a parent if the
// parent parses; "" otherwise (the sink's own name field still renders).
func sinkResourceName(parent, id string) string {
	scope, err := core.ParseScopeParent(parent)
	if err != nil {
		return ""
	}
	return core.SinkResourceName(scope, id)
}

func (p *Provider) SinkDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteSink(ctx, strParam(nr, "sinkName")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// ─── exclusions ───────────────────────────────────────────────────────────────

func (p *Provider) ExclusionCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.core.CreateExclusion(ctx, strParam(nr, "parent"), exclusionFromWire(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(exclusionToWire(e)), nil
}

func (p *Provider) ExclusionGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.core.GetExclusion(ctx, strParam(nr, "name"))
	if err != nil {
		return nil, err
	}
	return provider.OK(exclusionToWire(e)), nil
}

func (p *Provider) ExclusionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	list, next, err := p.core.ListExclusions(ctx, strParam(nr, "parent"),
		intParam(nr, "pageSize"), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(list) > 0 {
		items := make([]any, 0, len(list))
		for _, e := range list {
			items = append(items, exclusionToWire(e))
		}
		out["exclusions"] = items
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) ExclusionPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	e, err := p.core.UpdateExclusion(ctx, strParam(nr, "name"), exclusionFromWire(bodyOf(nr)),
		updateMaskFromQuery(nr.Params["updateMask"]))
	if err != nil {
		return nil, err
	}
	return provider.OK(exclusionToWire(e)), nil
}

func (p *Provider) ExclusionDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteExclusion(ctx, strParam(nr, "name")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// ─── logs-based metrics ───────────────────────────────────────────────────────

func (p *Provider) MetricCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	m, err := p.core.CreateMetric(ctx, parent, metricFromWire(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(metricToWire(metricScopeParent(parent), m)), nil
}

func (p *Provider) MetricGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "metricName")
	m, err := p.core.GetMetric(ctx, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(metricToWire(metricScopeParent(name), m)), nil
}

func (p *Provider) MetricList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	metrics, next, err := p.core.ListMetrics(ctx, parent,
		intParam(nr, "pageSize"), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(metrics) > 0 {
		list := make([]any, 0, len(metrics))
		scope := metricScopeParent(parent)
		for _, m := range metrics {
			list = append(list, metricToWire(scope, m))
		}
		out["metrics"] = list
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) MetricUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "metricName")
	m, err := p.core.UpdateMetric(ctx, name, metricFromWire(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(metricToWire(metricScopeParent(name), m)), nil
}

func (p *Provider) MetricDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteMetric(ctx, strParam(nr, "metricName")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// metricScopeParent resolves the canonical scope parent of a metric resource
// name, or "" when the name does not parse (a create/list parent parses via
// ParseScopeParent, a full metric name via ParseMetricName).
func metricScopeParent(name string) string {
	if scope, _, err := core.ParseMetricName(name); err == nil {
		return scope
	}
	if scope, err := core.ParseScopeParent(name); err == nil {
		return scope
	}
	return ""
}
