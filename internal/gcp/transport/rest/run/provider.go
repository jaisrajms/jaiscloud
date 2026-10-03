package run

import (
	"context"
	"errors"
	"net/http"

	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles the Cloud Run v2 REST data plane. It is a thin adapter: every
// handler resolves the NormalizedRequest params into the core's typed API, calls
// the shared core Service, and encodes the result as Cloud Run JSON.
type Provider struct {
	core *core.Service
}

// NewProvider returns a Cloud Run REST provider over the shared core.
func NewProvider(c *core.Service) *Provider { return &Provider{core: c} }

// Routes maps "Run.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Run.CreateService":      p.CreateService,
		"Run.GetService":         p.GetService,
		"Run.ListServices":       p.ListServices,
		"Run.UpdateService":      p.UpdateService,
		"Run.DeleteService":      p.DeleteService,
		"Run.GetIamPolicy":       p.GetIamPolicy,
		"Run.SetIamPolicy":       p.SetIamPolicy,
		"Run.TestIamPermissions": p.TestIamPermissions,
		"Run.ListRevisions":      p.ListRevisions,
		"Run.GetRevision":        p.GetRevision,
		"Run.GetOperation":       p.GetOperation,
		"Run.ListOperations":     p.ListOperations,
		"Run.WaitOperation":      p.WaitOperation,
		"Run.DeleteOperation":    p.DeleteOperation,
		"Run.CancelOperation":    p.CancelOperation,
		"Run.Invoke":             p.Invoke,
	}
}

// Reset delegates to the core so /_jaiscloud/reset clears Cloud Run state.
func (p *Provider) Reset(ctx context.Context) { p.core.Reset(ctx) }

func (p *Provider) CreateService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.CreateService(ctx, strParam(nr, "project"), strParam(nr, "location"),
		strParam(nr, "serviceId"), bodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

func (p *Provider) GetService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	svc, err := p.core.GetService(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "service"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.ServiceJSON(svc)), nil
}

func (p *Provider) ListServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	svcs, err := p.core.ListServices(ctx, strParam(nr, "project"), strParam(nr, "location"))
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(svcs, func(s runstore.Service) string { return s.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, s := range page {
		items = append(items, core.ServiceJSON(s))
	}
	resp := map[string]any{"services": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.UpdateService(ctx, strParam(nr, "project"), strParam(nr, "location"),
		strParam(nr, "service"), bodyOf(nr), strParam(nr, "updateMask"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

func (p *Provider) DeleteService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.DeleteService(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "service"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

func (p *Provider) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	pol, err := p.core.ServiceGetIamPolicy(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "service"))
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	pol, err := p.core.ServiceSetIamPolicy(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "service"), bodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	perms, err := p.core.ServiceTestIamPermissions(ctx, strParam(nr, "project"), strParam(nr, "location"),
		strParam(nr, "service"), policy.Permissions(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"permissions": perms}), nil
}

func (p *Provider) ListRevisions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	revs, err := p.core.ListRevisions(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "service"))
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(revs, func(r runstore.Revision) string { return r.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, r := range page {
		items = append(items, core.RevisionJSON(r))
	}
	resp := map[string]any{"revisions": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) GetRevision(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	rev, err := p.core.GetRevision(ctx, strParam(nr, "project"), strParam(nr, "location"),
		strParam(nr, "service"), strParam(nr, "revision"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.RevisionJSON(rev)), nil
}

func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.GetOperation(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "operation"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

func (p *Provider) ListOperations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ops, err := p.core.ListOperations(ctx, strParam(nr, "project"), strParam(nr, "location"))
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(ops, func(o runstore.Operation) string { return o.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, op := range page {
		items = append(items, core.OperationJSON(op))
	}
	resp := map[string]any{"operations": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) WaitOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.WaitOperation(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "operation"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

func (p *Provider) DeleteOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteOperation(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "operation")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) CancelOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.CancelOperation(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "operation")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// Invoke forwards a host-routed data-plane request to the service's runtime.
func (p *Provider) Invoke(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	r := nr.Raw
	if r == nil {
		return nil, model.NewProviderError("InvalidRequest", "missing raw request", 400)
	}
	inv, err := p.core.Invoke(ctx, core.InvocationRequest{
		Host:    r.Host,
		Method:  r.Method,
		Path:    r.URL.Path,
		Query:   r.URL.RawQuery,
		Headers: forwardHeaders(r),
		Body:    bodyBytes(nr),
	})
	if err != nil {
		if errors.Is(err, core.ErrNoReadyRuntime) {
			return nil, model.NewProviderError("Unavailable", err.Error(), 503)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: inv.Status, Data: map[string]any{
		"headers": inv.Headers,
		"body":    inv.Body,
	}}, nil
}

// bodyBytes returns the decoded request body stored by the invocation codec.
func bodyBytes(nr *model.NormalizedRequest) []byte {
	b, _ := nr.Params["body"].([]byte)
	return b
}

// forwardHeaders copies the request headers that are safe to forward upstream.
func forwardHeaders(r *http.Request) map[string]string {
	out := map[string]string{}
	for k, vs := range r.Header {
		if len(vs) == 0 || isHopByHop(k) {
			continue
		}
		out[k] = vs[0]
	}
	return out
}

func isHopByHop(header string) bool {
	switch http.CanonicalHeaderKey(header) {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
		"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Host", "Content-Length":
		return true
	}
	return false
}
