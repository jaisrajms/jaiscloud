package container

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	core "jaiscloud/internal/gcp/service/container"
	containerstore "jaiscloud/internal/gcp/store/container"
	"jaiscloud/internal/model"
)

// ─── request helpers ──────────────────────────────────────────────────────────

func splitEscaped(path string) []string {
	raw := strings.Split(strings.TrimPrefix(path, "/"), "/")
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		if u, err := url.PathUnescape(s); err == nil {
			out = append(out, u)
		} else {
			out = append(out, s)
		}
	}
	return out
}

func queryToParams(r *http.Request, params map[string]any) {
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}
}

func parseJSON(body []byte) (map[string]any, error) {
	if len(body) == 0 {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func intFrom(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case string:
		n, _ := strconv.Atoi(x)
		return n
	default:
		return 0
	}
}

func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func int32From(m map[string]any, key string) int32 {
	return int32(intFrom(m[key]))
}

// int64From reads an int64 that GKE encodes as a JSON string (int64 fields are
// strings on the REST surface) or, tolerantly, as a JSON number.
func int64From(v any) int64 {
	switch x := v.(type) {
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	default:
		return 0
	}
}

// boolFrom reads a JSON boolean, treating a missing value as false.
func boolFrom(v any) bool {
	b, _ := v.(bool)
	return b
}

// clusterFromBody decodes a GKE Cluster JSON object into the store model's
// writable subset. Input-only/derived fields are ignored; the core fills the
// defaults.
func clusterFromBody(m map[string]any) (containerstore.Cluster, error) {
	var c containerstore.Cluster
	raw := str(m, "name")
	if raw == "" {
		return c, model.NewProviderError("InvalidArgument", "cluster name is required", 400)
	}
	if strings.Contains(raw, "/") {
		// The canonical path is parsed by the core, the same parser the gRPC
		// codec uses, so both transports accept exactly the same names and
		// answer the same message for a malformed one.
		_, _, id, ok := core.ParseClusterName(raw)
		if !ok {
			return c, model.NewProviderError("InvalidArgument", "invalid cluster name: "+raw, 400)
		}
		c.Name = id
	} else {
		c.Name = raw
	}
	c.InitialClusterVersion = str(m, "initialClusterVersion")
	c.Network = str(m, "network")
	c.Subnetwork = str(m, "subnetwork")
	c.InitialNodeCount = int32From(m, "initialNodeCount")
	if cfg, ok := m["nodeConfig"].(map[string]any); ok {
		c.NodeConfig = nodeConfigFromMap(cfg)
	}
	if sn, ok := m["shieldedNodes"].(map[string]any); ok {
		c.ShieldedNodes = &containerstore.ShieldedNodes{Enabled: boolFrom(sn["enabled"])}
	}
	if dpc, ok := m["defaultMaxPodsConstraint"].(map[string]any); ok {
		if n := int64From(dpc["maxPodsPerNode"]); n > 0 {
			c.MaxPodsPerNode = n
		}
	}
	if labels := strMap(m["resourceLabels"]); labels != nil {
		c.ResourceLabels = labels
	}
	if ap, ok := m["autopilot"].(map[string]any); ok {
		c.Autopilot = &containerstore.Autopilot{Enabled: boolVal(ap, "enabled")}
	}
	if wi, ok := m["workloadIdentityConfig"].(map[string]any); ok {
		// An empty object still enables Workload Identity (the core defaults the
		// workload pool from the project), matching the API's meaning.
		c.WorkloadIdentityConfig = &containerstore.WorkloadIdentityConfig{WorkloadPool: str(wi, "workloadPool")}
	}
	if pools, ok := m["nodePools"].([]any); ok {
		for _, raw := range pools {
			if pm, ok := raw.(map[string]any); ok {
				c.NodePools = append(c.NodePools, nodePoolFromBody(pm))
			}
		}
	}
	return c, nil
}

func strMap(v any) map[string]string {
	m, _ := v.(map[string]any)
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

// ─── wire encoding ────────────────────────────────────────────────────────────

func clusterToJSON(c containerstore.Cluster) map[string]any {
	c = core.WithReadDefaults(c)
	network, subnetwork := core.NetworkConfigPaths(c)
	out := map[string]any{
		"name":                  c.Name,
		"location":              c.Location,
		"status":                c.Status,
		"endpoint":              c.Endpoint,
		"selfLink":              c.SelfLink,
		"currentMasterVersion":  c.CurrentMasterVersion,
		"currentNodeVersion":    c.CurrentNodeVersion,
		"initialClusterVersion": c.InitialClusterVersion,
		"network":               c.Network,
		"subnetwork":            c.Subnetwork,
		// Real GKE always renders these three response messages, and clients
		// (the hashicorp/google provider among them) dereference them
		// unconditionally, so they are never omitted.
		"networkConfig": map[string]any{
			"network":    network,
			"subnetwork": subnetwork,
		},
		"shieldedNodes": map[string]any{
			"enabled": c.ShieldedNodes != nil && c.ShieldedNodes.Enabled,
		},
		"defaultMaxPodsConstraint": map[string]any{
			"maxPodsPerNode": strconv.FormatInt(c.MaxPodsPerNode, 10),
		},
		"masterAuth": map[string]any{
			"clusterCaCertificate": c.CaCertificate,
		},
	}
	if c.InitialNodeCount > 0 {
		out["initialNodeCount"] = c.InitialNodeCount
	}
	if len(c.NodePools) > 0 {
		pools := make([]any, 0, len(c.NodePools))
		for _, p := range c.NodePools {
			pools = append(pools, nodePoolToJSON(p))
		}
		out["nodePools"] = pools
	}
	// Discovery: on read this is "populated with the node configuration of the
	// first node pool" (filled by core.WithReadDefaults), so a create that sent
	// the legacy nodeConfig + initialNodeCount shape reads it back.
	if c.NodeConfig != nil {
		out["nodeConfig"] = nodeConfigToJSON(c.NodeConfig)
	}
	if c.LoggingService != "" {
		out["loggingService"] = c.LoggingService
	}
	if c.MonitoringService != "" {
		out["monitoringService"] = c.MonitoringService
	}
	if len(c.Locations) > 0 {
		out["locations"] = c.Locations
	}
	if c.AddonsConfig != nil {
		out["addonsConfig"] = addonsConfigToJSON(c.AddonsConfig)
	}
	if c.LegacyAbac != nil {
		out["legacyAbac"] = map[string]any{"enabled": c.LegacyAbac.Enabled}
	}
	if c.NetworkPolicy != nil {
		np := map[string]any{"enabled": c.NetworkPolicy.Enabled}
		if c.NetworkPolicy.Provider != "" {
			np["provider"] = c.NetworkPolicy.Provider
		}
		out["networkPolicy"] = np
	}
	if c.MaintenancePolicy != nil {
		out["maintenancePolicy"] = maintenancePolicyToJSON(c.MaintenancePolicy)
	}
	if c.Autopilot != nil {
		out["autopilot"] = map[string]any{"enabled": c.Autopilot.Enabled}
	}
	if c.WorkloadIdentityConfig != nil {
		out["workloadIdentityConfig"] = map[string]any{"workloadPool": c.WorkloadIdentityConfig.WorkloadPool}
	}
	if c.AdminUsername != "" {
		out["masterAuth"].(map[string]any)["username"] = c.AdminUsername
	}
	if len(c.ResourceLabels) > 0 {
		out["resourceLabels"] = c.ResourceLabels
	}
	if !c.CreateTime.IsZero() {
		out["createTime"] = c.CreateTime.UTC().Format(time.RFC3339Nano)
	}
	return out
}

// nodePoolToJSON renders a stored node pool as the GKE NodePool JSON shape.
func nodePoolToJSON(p containerstore.NodePool) map[string]any {
	out := map[string]any{"name": p.Name}
	if p.Status != "" {
		out["status"] = p.Status
	}
	if p.InitialNodeCount > 0 {
		out["initialNodeCount"] = p.InitialNodeCount
	}
	if p.Version != "" {
		out["version"] = p.Version
	}
	if len(p.Locations) > 0 {
		out["locations"] = p.Locations
	}
	if p.SelfLink != "" {
		out["selfLink"] = p.SelfLink
	}
	if p.Config != nil {
		out["config"] = nodeConfigToJSON(p.Config)
	}
	if p.Autoscaling != nil {
		out["autoscaling"] = nodePoolAutoscalingToJSON(p.Autoscaling)
	}
	if p.Management != nil {
		out["management"] = map[string]any{"autoUpgrade": p.Management.AutoUpgrade, "autoRepair": p.Management.AutoRepair}
	}
	if p.UpgradeSettings != nil {
		us := map[string]any{
			"maxSurge":       p.UpgradeSettings.MaxSurge,
			"maxUnavailable": p.UpgradeSettings.MaxUnavailable,
		}
		if p.UpgradeSettings.Strategy != "" {
			us["strategy"] = p.UpgradeSettings.Strategy
		}
		out["upgradeSettings"] = us
	}
	return out
}

func nodeConfigToJSON(c *containerstore.NodeConfig) map[string]any {
	out := map[string]any{}
	if c.MachineType != "" {
		out["machineType"] = c.MachineType
	}
	if c.DiskSizeGb != 0 {
		out["diskSizeGb"] = c.DiskSizeGb
	}
	if c.DiskType != "" {
		out["diskType"] = c.DiskType
	}
	if c.ImageType != "" {
		out["imageType"] = c.ImageType
	}
	if len(c.OauthScopes) > 0 {
		out["oauthScopes"] = c.OauthScopes
	}
	if c.ServiceAccount != "" {
		out["serviceAccount"] = c.ServiceAccount
	}
	if len(c.Metadata) > 0 {
		out["metadata"] = c.Metadata
	}
	if len(c.Labels) > 0 {
		out["labels"] = c.Labels
	}
	if len(c.ResourceLabels) > 0 {
		out["resourceLabels"] = c.ResourceLabels
	}
	if len(c.Tags) > 0 {
		out["tags"] = c.Tags
	}
	if c.MinCpuPlatform != "" {
		out["minCpuPlatform"] = c.MinCpuPlatform
	}
	if c.LocalSsdCount != 0 {
		out["localSsdCount"] = c.LocalSsdCount
	}
	if c.Preemptible {
		out["preemptible"] = true
	}
	if c.Spot {
		out["spot"] = true
	}
	return out
}

func nodePoolAutoscalingToJSON(a *containerstore.NodePoolAutoscaling) map[string]any {
	out := map[string]any{
		"enabled":      a.Enabled,
		"minNodeCount": a.MinNodeCount,
		"maxNodeCount": a.MaxNodeCount,
	}
	if a.Autoprovisioned {
		out["autoprovisioned"] = true
	}
	if a.LocationPolicy != "" {
		out["locationPolicy"] = a.LocationPolicy
	}
	if a.TotalMinNodeCount != 0 {
		out["totalMinNodeCount"] = a.TotalMinNodeCount
	}
	if a.TotalMaxNodeCount != 0 {
		out["totalMaxNodeCount"] = a.TotalMaxNodeCount
	}
	return out
}

func addonsConfigToJSON(a *containerstore.AddonsConfig) map[string]any {
	out := map[string]any{}
	if a.HttpLoadBalancing != nil {
		out["httpLoadBalancing"] = map[string]any{"disabled": a.HttpLoadBalancing.Disabled}
	}
	if a.HorizontalPodAutoscaling != nil {
		out["horizontalPodAutoscaling"] = map[string]any{"disabled": a.HorizontalPodAutoscaling.Disabled}
	}
	if a.NetworkPolicyConfig != nil {
		out["networkPolicyConfig"] = map[string]any{"disabled": a.NetworkPolicyConfig.Disabled}
	}
	return out
}

func maintenancePolicyToJSON(mp *containerstore.MaintenancePolicy) map[string]any {
	out := map[string]any{}
	if mp.ResourceVersion != "" {
		out["resourceVersion"] = mp.ResourceVersion
	}
	if mp.Window != nil {
		w := map[string]any{}
		if mp.Window.DailyMaintenanceWindow != nil {
			w["dailyMaintenanceWindow"] = map[string]any{
				"startTime": mp.Window.DailyMaintenanceWindow.StartTime,
				"duration":  mp.Window.DailyMaintenanceWindow.Duration,
			}
		}
		if mp.Window.RecurringWindow != nil {
			rw := map[string]any{"recurrence": mp.Window.RecurringWindow.Recurrence}
			if mp.Window.RecurringWindow.Window != nil {
				rw["window"] = map[string]any{
					"startTime": mp.Window.RecurringWindow.Window.StartTime,
					"endTime":   mp.Window.RecurringWindow.Window.EndTime,
				}
			}
			w["recurringWindow"] = rw
		}
		if len(mp.Window.MaintenanceExclusions) > 0 {
			ex := map[string]any{}
			for k, v := range mp.Window.MaintenanceExclusions {
				if v == nil {
					continue
				}
				ex[k] = map[string]any{"startTime": v.StartTime, "endTime": v.EndTime}
			}
			w["maintenanceExclusions"] = ex
		}
		out["window"] = w
	}
	return out
}

// ─── request decoding ─────────────────────────────────────────────────────────

// nodePoolFromBody decodes a GKE NodePool JSON object into the store model.
func nodePoolFromBody(m map[string]any) containerstore.NodePool {
	p := containerstore.NodePool{
		Name:             str(m, "name"),
		InitialNodeCount: int32From(m, "initialNodeCount"),
		Version:          str(m, "version"),
		Locations:        strSlice(m["locations"]),
	}
	if p.Name != "" && strings.Contains(p.Name, "/") {
		// The core parser is the one the gRPC codec uses, so a canonical pool
		// name is reduced to its id identically on both transports. An
		// unparseable name is left as sent and rejected by the core's grammar
		// check with the same 400.
		if _, _, _, id, ok := core.ParseNodePoolName(p.Name); ok {
			p.Name = id
		}
	}
	if cfg, ok := m["config"].(map[string]any); ok {
		p.Config = nodeConfigFromMap(cfg)
	}
	if a, ok := m["autoscaling"].(map[string]any); ok {
		p.Autoscaling = nodePoolAutoscalingFromMap(a)
	}
	if mg, ok := m["management"].(map[string]any); ok {
		p.Management = &containerstore.NodeManagement{AutoUpgrade: boolVal(mg, "autoUpgrade"), AutoRepair: boolVal(mg, "autoRepair")}
	}
	if us, ok := m["upgradeSettings"].(map[string]any); ok {
		p.UpgradeSettings = &containerstore.NodePoolUpgradeSettings{
			MaxSurge:       int32From(us, "maxSurge"),
			MaxUnavailable: int32From(us, "maxUnavailable"),
			Strategy:       str(us, "strategy"),
		}
	}
	return p
}

func nodeConfigFromMap(m map[string]any) *containerstore.NodeConfig {
	c := &containerstore.NodeConfig{
		MachineType:    str(m, "machineType"),
		DiskSizeGb:     int32From(m, "diskSizeGb"),
		DiskType:       str(m, "diskType"),
		ImageType:      str(m, "imageType"),
		OauthScopes:    strSlice(m["oauthScopes"]),
		ServiceAccount: str(m, "serviceAccount"),
		Metadata:       strMap(m["metadata"]),
		Labels:         strMap(m["labels"]),
		ResourceLabels: strMap(m["resourceLabels"]),
		Tags:           strSlice(m["tags"]),
		MinCpuPlatform: str(m, "minCpuPlatform"),
		LocalSsdCount:  int32From(m, "localSsdCount"),
		Preemptible:    boolVal(m, "preemptible"),
		Spot:           boolVal(m, "spot"),
	}
	return c
}

func nodePoolAutoscalingFromMap(m map[string]any) *containerstore.NodePoolAutoscaling {
	return &containerstore.NodePoolAutoscaling{
		Enabled:           boolVal(m, "enabled"),
		MinNodeCount:      int32From(m, "minNodeCount"),
		MaxNodeCount:      int32From(m, "maxNodeCount"),
		Autoprovisioned:   boolVal(m, "autoprovisioned"),
		LocationPolicy:    str(m, "locationPolicy"),
		TotalMinNodeCount: int32From(m, "totalMinNodeCount"),
		TotalMaxNodeCount: int32From(m, "totalMaxNodeCount"),
	}
}

func addonsConfigFromMap(m map[string]any) containerstore.AddonsConfig {
	var a containerstore.AddonsConfig
	if h, ok := m["httpLoadBalancing"].(map[string]any); ok {
		a.HttpLoadBalancing = &containerstore.AddonConfig{Disabled: boolVal(h, "disabled")}
	}
	if h, ok := m["horizontalPodAutoscaling"].(map[string]any); ok {
		a.HorizontalPodAutoscaling = &containerstore.AddonConfig{Disabled: boolVal(h, "disabled")}
	}
	if n, ok := m["networkPolicyConfig"].(map[string]any); ok {
		a.NetworkPolicyConfig = &containerstore.AddonConfig{Disabled: boolVal(n, "disabled")}
	}
	return a
}

func networkPolicyFromMap(m map[string]any) containerstore.NetworkPolicy {
	return containerstore.NetworkPolicy{Provider: str(m, "provider"), Enabled: boolVal(m, "enabled")}
}

func maintenancePolicyFromMap(m map[string]any) containerstore.MaintenancePolicy {
	mp := containerstore.MaintenancePolicy{ResourceVersion: str(m, "resourceVersion")}
	w, ok := m["window"].(map[string]any)
	if !ok {
		return mp
	}
	win := &containerstore.MaintenanceWindow{}
	if d, ok := w["dailyMaintenanceWindow"].(map[string]any); ok {
		win.DailyMaintenanceWindow = &containerstore.DailyMaintenanceWindow{StartTime: str(d, "startTime"), Duration: str(d, "duration")}
	}
	if r, ok := w["recurringWindow"].(map[string]any); ok {
		rw := &containerstore.RecurringWindow{Recurrence: str(r, "recurrence")}
		if tw, ok := r["window"].(map[string]any); ok {
			rw.Window = &containerstore.TimeWindow{StartTime: str(tw, "startTime"), EndTime: str(tw, "endTime")}
		}
		win.RecurringWindow = rw
	}
	if ex, ok := w["maintenanceExclusions"].(map[string]any); ok && len(ex) > 0 {
		win.MaintenanceExclusions = make(map[string]*containerstore.TimeWindow, len(ex))
		for k, v := range ex {
			tw, ok := v.(map[string]any)
			if !ok {
				continue
			}
			win.MaintenanceExclusions[k] = &containerstore.TimeWindow{StartTime: str(tw, "startTime"), EndTime: str(tw, "endTime")}
		}
	}
	mp.Window = win
	return mp
}

func strSlice(v any) []string {
	arr, _ := v.([]any)
	if len(arr) == 0 {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func boolVal(m map[string]any, key string) bool {
	b, _ := m[key].(bool)
	return b
}

func operationToJSON(op containerstore.Operation) map[string]any {
	out := map[string]any{
		"name":          op.Name,
		"operationType": op.OperationType,
		"status":        op.Status,
		"location":      op.Location,
	}
	if op.TargetLink != "" {
		out["targetLink"] = op.TargetLink
	}
	if op.SelfLink != "" {
		out["selfLink"] = op.SelfLink
	}
	if !op.StartTime.IsZero() {
		out["startTime"] = op.StartTime.UTC().Format(time.RFC3339Nano)
	}
	if !op.EndTime.IsZero() {
		out["endTime"] = op.EndTime.UTC().Format(time.RFC3339Nano)
	}
	return out
}
