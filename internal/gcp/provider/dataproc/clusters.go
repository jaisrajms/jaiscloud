package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"jaiscloud/internal/clock"
	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// clusterFromBody builds the store Cluster from a request body (the REST create
// sends the Cluster object directly, with projectId/clusterName inside).
func clusterFromBody(nr *model.NormalizedRequest, body map[string]any, region, name string) dataprocstore.Cluster {
	now := clock.Now().UTC()
	c := dataprocstore.Cluster{
		ProjectID:     nr.AccountID,
		Region:        region,
		Name:          name,
		Status:        dataprocstore.ClusterStatus{State: "RUNNING", StateStartTime: now},
		StatusHistory: []dataprocstore.ClusterStatus{{State: "CREATING", StateStartTime: now}},
		ClusterUUID:   randomHex(32),
		CreateTime:    now,
		UpdateTime:    now,
		Labels:        bodyStringMap(body, "labels"),
	}
	if config, ok := body["config"].(map[string]any); ok {
		if data, err := json.Marshal(config); err == nil {
			c.Config = data
		}
	}
	return c
}

func (p *Provider) CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	region := strParam(nr, "region")
	if region == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	name := bodyString(body, "clusterName")
	if name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing clusterName", 400)
	}
	c := clusterFromBody(nr, body, region, name)
	if err := p.store.CreateCluster(ctx, nr.AccountID, region, c); err != nil {
		if errors.Is(err, dataprocstore.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "cluster already exists", 409)
		}
		return nil, err
	}
	target := nr.ResourceID("dataproc-cluster", region+"/"+name)
	op, err := p.storeOperation(ctx, nr, region, "create", target,
		clusterOperationMetadata(name, c.ClusterUUID, "CREATE"),
		clusterToMap(c))
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) GetCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	region := strParam(nr, "region")
	name := strParam(nr, "clusterName")
	if region == "" || name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region or clusterName", 400)
	}
	c, err := p.store.GetCluster(ctx, nr.AccountID, region, name)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(clusterToMap(c)), nil
}

func (p *Provider) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	region := strParam(nr, "region")
	if region == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region", 400)
	}
	clusters, err := p.store.ListClusters(ctx, nr.AccountID, region)
	if err != nil {
		return nil, err
	}
	// filter is accepted but ignored (documented limitation, matches workflows).
	resp, err := p.pageClusters(nr, clusters)
	if err != nil {
		return nil, err
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	region := strParam(nr, "region")
	name := strParam(nr, "clusterName")
	if region == "" || name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region or clusterName", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	mask := strParam(nr, "updateMask")

	apply := func(field string) bool {
		return mask == "" || containsMaskField(mask, field)
	}

	c, err := p.store.UpdateClusterAtomic(ctx, nr.AccountID, region, name, func(c dataprocstore.Cluster) (dataprocstore.Cluster, error) {
		// labels
		if apply("labels") {
			if labels := bodyStringMap(body, "labels"); labels != nil {
				c.Labels = labels
			}
		}
		// config.* fields are applied onto the stored config verbatim (the
		// updateMask selects the path; only top-level config.* keys are honored).
		if c.Config != nil && bodyConfig(body) != nil {
			var stored map[string]any
			if json.Unmarshal(c.Config, &stored) == nil {
				applyConfigMask(stored, bodyConfig(body), mask)
				if data, err := json.Marshal(stored); err == nil {
					c.Config = data
				}
			}
		}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("dataproc-cluster", region+"/"+name)
	op, err := p.storeOperation(ctx, nr, region, "update", target,
		clusterOperationMetadata(name, c.ClusterUUID, "UPDATE"),
		clusterToMap(c))
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	region := strParam(nr, "region")
	name := strParam(nr, "clusterName")
	if region == "" || name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region or clusterName", 400)
	}
	c, err := p.store.GetCluster(ctx, nr.AccountID, region, name)
	if err != nil {
		return nil, mapErr(err)
	}
	if err := p.store.DeleteCluster(ctx, nr.AccountID, region, name); err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("dataproc-cluster", region+"/"+name)
	// DeleteCluster's LRO response type is google.protobuf.Empty.
	op, err := p.storeOperation(ctx, nr, region, "delete", target,
		clusterOperationMetadata(name, c.ClusterUUID, "DELETE"),
		map[string]any{})
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) startStopCluster(ctx context.Context, nr *model.NormalizedRequest, toState, verb, operationType string) (*model.ProviderResponse, error) {
	region := strParam(nr, "region")
	name := strParam(nr, "clusterName")
	if region == "" || name == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region or clusterName", 400)
	}
	c, err := p.store.UpdateClusterAtomic(ctx, nr.AccountID, region, name, func(c dataprocstore.Cluster) (dataprocstore.Cluster, error) {
		c.StatusHistory = append(c.StatusHistory, c.Status)
		c.Status = dataprocstore.ClusterStatus{State: toState, StateStartTime: clock.Now().UTC()}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("dataproc-cluster", region+"/"+name)
	op, err := p.storeOperation(ctx, nr, region, verb, target,
		clusterOperationMetadata(name, c.ClusterUUID, operationType),
		clusterToMap(c))
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) StartCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.startStopCluster(ctx, nr, "RUNNING", "start", "START")
}

func (p *Provider) StopCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.startStopCluster(ctx, nr, "STOPPED", "stop", "STOP")
}

// DiagnoseCluster is an unimplemented stub: it fails loud rather than silently
// succeeding, so SDK callers observe a real UNIMPLEMENTED error.
func (p *Provider) DiagnoseCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("Unimplemented", "DiagnoseCluster is not supported by the emulator", 501)
}

// containsMaskField reports whether a comma-separated updateMask contains the
// given top-level field (or a sub-path of it).
func containsMaskField(mask, field string) bool {
	for _, part := range splitMask(mask) {
		if part == field || strings.HasPrefix(part, field+".") {
			return true
		}
	}
	return false
}

func splitMask(mask string) []string {
	var out []string
	for _, p := range strings.Split(mask, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func bodyConfig(body map[string]any) map[string]any {
	if body == nil {
		return nil
	}
	c, _ := body["config"].(map[string]any)
	return c
}

// applyConfigMask merges fields from the request config onto the stored config,
// honoring the updateMask's config.* paths. Only the sub-fields present in the
// request config (under the masked path) are applied — the emulator models the
// documented updatable fields (labels, config.worker_config.num_instances,
// config.secondary_worker_config.num_instances, and any config sub-field
// explicitly named by the mask).
func applyConfigMask(stored, incoming map[string]any, mask string) {
	if mask == "" {
		// No mask: merge all incoming top-level keys verbatim.
		for k, v := range incoming {
			stored[k] = v
		}
		return
	}
	for _, field := range splitMask(mask) {
		if !strings.HasPrefix(field, "config.") {
			continue
		}
		sub := field[len("config."):]
		if v, ok := incoming[sub]; ok {
			stored[sub] = v
		}
	}
}
