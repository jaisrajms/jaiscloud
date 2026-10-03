package container

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

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
		_, _, id, ok := ParseClusterPath(raw)
		if !ok {
			return c, model.NewProviderError("InvalidArgument", "invalid cluster name", 400)
		}
		c.Name = id
	} else {
		c.Name = raw
	}
	c.InitialClusterVersion = str(m, "initialClusterVersion")
	c.Network = str(m, "network")
	c.Subnetwork = str(m, "subnetwork")
	c.InitialNodeCount = int32From(m, "initialNodeCount")
	if labels := strMap(m["resourceLabels"]); labels != nil {
		c.ResourceLabels = labels
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

// ParseClusterPath parses a GKE cluster name that may be either the short id
// ("my-cluster") or the canonical path.
func ParseClusterPath(name string) (project, location, cluster string, ok bool) {
	if !strings.Contains(name, "/") {
		return "", "", name, true
	}
	return parseCanonical(name, "clusters")
}

func parseCanonical(name, resource string) (project, location, id string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 6 {
		return "", "", "", false
	}
	if segs[0] != "projects" || segs[2] != "locations" || segs[4] != resource {
		return "", "", "", false
	}
	return segs[1], segs[3], segs[5], true
}

// ─── wire encoding ────────────────────────────────────────────────────────────

func clusterToJSON(c containerstore.Cluster) map[string]any {
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
		"masterAuth": map[string]any{
			"clusterCaCertificate": c.CaCertificate,
		},
	}
	if len(c.NodePools) > 0 {
		pools := make([]any, 0, len(c.NodePools))
		for _, p := range c.NodePools {
			pool := map[string]any{"name": p.Name}
			if p.Status != "" {
				pool["status"] = p.Status
			}
			if p.InitialNodeCount > 0 {
				pool["initialNodeCount"] = p.InitialNodeCount
			}
			pools = append(pools, pool)
		}
		out["nodePools"] = pools
	}
	if len(c.ResourceLabels) > 0 {
		out["resourceLabels"] = c.ResourceLabels
	}
	if !c.CreateTime.IsZero() {
		out["createTime"] = c.CreateTime.UTC().Format(time.RFC3339Nano)
	}
	return out
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
