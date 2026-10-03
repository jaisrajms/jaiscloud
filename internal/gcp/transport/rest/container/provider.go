package container

import (
	"context"
	"strconv"

	core "jaiscloud/internal/gcp/service/container"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles the GKE v1 REST data plane. It is a thin adapter: every
// handler resolves the NormalizedRequest params into the core's typed API,
// calls the shared core Service, and encodes the result as GKE JSON.
type Provider struct {
	core *core.Service
}

// NewProvider returns a GKE REST provider over the shared core.
func NewProvider(c *core.Service) *Provider { return &Provider{core: c} }

// Routes maps "Container.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Container.CreateCluster":  p.CreateCluster,
		"Container.GetCluster":     p.GetCluster,
		"Container.ListClusters":   p.ListClusters,
		"Container.DeleteCluster":  p.DeleteCluster,
		"Container.GetOperation":   p.GetOperation,
		"Container.ListOperations": p.ListOperations,
	}
}

// Reset delegates to the core so /_jaiscloud/reset clears GKE state.
func (p *Provider) Reset(ctx context.Context) { p.core.Reset(ctx) }

func (p *Provider) CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterMap, _ := bodyOf(nr)["cluster"].(map[string]any)
	if clusterMap == nil {
		return nil, model.NewProviderError("InvalidArgument", "missing root 'cluster' object", 400)
	}
	cluster, err := clusterFromBody(clusterMap)
	if err != nil {
		return nil, err
	}
	op, err := p.core.CreateCluster(ctx, strParam(nr, "project"), strParam(nr, "location"), cluster)
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) GetCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	c, err := p.core.GetCluster(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"))
	if err != nil {
		return nil, err
	}
	return provider.OK(clusterToJSON(c)), nil
}

func (p *Provider) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusters, err := p.core.ListClusters(ctx, strParam(nr, "project"), strParam(nr, "location"))
	if err != nil {
		return nil, err
	}
	page, next := paginate(len(clusters), nr)
	items := make([]any, 0, len(page))
	for _, i := range page {
		items = append(items, clusterToJSON(clusters[i]))
	}
	resp := map[string]any{"clusters": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.DeleteCluster(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.GetOperation(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "operation"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) ListOperations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ops, err := p.core.ListOperations(ctx, strParam(nr, "project"), strParam(nr, "location"))
	if err != nil {
		return nil, err
	}
	page, next := paginate(len(ops), nr)
	items := make([]any, 0, len(page))
	for _, i := range page {
		items = append(items, operationToJSON(ops[i]))
	}
	resp := map[string]any{"operations": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// paginate applies GKE's pageSize/pageToken (a numeric offset, as the emulator
// has no opaque cursor store) and returns the selected indices plus the next
// token ("" when the page is the last).
func paginate(total int, nr *model.NormalizedRequest) ([]int, string) {
	pageSize := intFrom(nr.Params["pageSize"])
	if pageSize <= 0 || pageSize > 500 {
		pageSize = 500
	}
	offset := 0
	if t := strParam(nr, "pageToken"); t != "" {
		offset, _ = strconv.Atoi(t)
	}
	if offset < 0 {
		offset = 0
	}
	if offset > total {
		offset = total
	}
	end := offset + pageSize
	if end > total {
		end = total
	}
	idxs := make([]int, 0, end-offset)
	for i := offset; i < end; i++ {
		idxs = append(idxs, i)
	}
	if end < total {
		return idxs, strconv.Itoa(end)
	}
	return idxs, ""
}
