package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
)

// ClusterInput carries the caller-supplied fields of a cluster create/update.
// Config (a GCE ClusterConfig) and VirtualClusterConfig (a Dataproc-on-GKE
// VirtualClusterConfig) are the raw wire JSON stored verbatim; the API treats
// them as mutually exclusive.
type ClusterInput struct {
	Labels               map[string]string
	Config               json.RawMessage
	VirtualClusterConfig json.RawMessage
}

// ClusterInputFromMap builds a ClusterInput from a Discovery/proto Cluster map.
func ClusterInputFromMap(body map[string]any) ClusterInput {
	in := ClusterInput{Labels: bodyStringMap(body, "labels")}
	if cfg, ok := body["config"].(map[string]any); ok {
		if data, err := json.Marshal(cfg); err == nil {
			in.Config = data
		}
	}
	if vcc, ok := body["virtualClusterConfig"].(map[string]any); ok {
		if data, err := json.Marshal(vcc); err == nil {
			in.VirtualClusterConfig = data
		}
	}
	return in
}

// validateVirtualClusterConfig validates the required shape of a caller-supplied
// dataproc.v1.VirtualClusterConfig. The emulator models the GKE placement as
// metadata only (there is no GKE control plane), so this checks the API's
// required structure: kubernetesClusterConfig.gkeClusterConfig must name a
// target cluster (gkeClusterTarget) or at least one node pool
// (nodePoolTarget). Unknown sub-fields are accepted and preserved verbatim.
func validateVirtualClusterConfig(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var vcc map[string]any
	if err := json.Unmarshal(raw, &vcc); err != nil {
		return invalidArgument("virtualClusterConfig must be an object")
	}
	kcc, ok := vcc["kubernetesClusterConfig"].(map[string]any)
	if !ok {
		return invalidArgument("virtualClusterConfig requires kubernetesClusterConfig")
	}
	gke, ok := kcc["gkeClusterConfig"].(map[string]any)
	if !ok {
		return invalidArgument("kubernetesClusterConfig requires gkeClusterConfig")
	}
	target, _ := gke["gkeClusterTarget"].(string)
	pools, _ := gke["nodePoolTarget"].([]any)
	if target == "" && len(pools) == 0 {
		return invalidArgument("gkeClusterConfig requires gkeClusterTarget or nodePoolTarget")
	}
	return nil
}

// clusterPlacement is the human-readable placement of a cluster for logs.
func clusterPlacement(c dpstore.Cluster) string {
	if c.IsGKEBacked() {
		return "GKE"
	}
	return "GCE"
}

// ParseMask splits a comma-separated update mask (REST query form) into paths.
func ParseMask(mask string) []string { return splitMask(mask) }

// CreateCluster creates a cluster in CREATING state and returns its (not yet
// done) create operation. The cluster settles to RUNNING lazily on the first
// cluster or operation read after the configured ready delay, mirroring real
// Dataproc's asynchronous provisioning without a background goroutine.
func (s *Service) CreateCluster(ctx context.Context, project, region, name string, in ClusterInput) (dpstore.Cluster, dpstore.Operation, error) {
	if region == "" || name == "" {
		return dpstore.Cluster{}, dpstore.Operation{}, invalidArgument("missing region or clusterName")
	}
	if len(in.Config) > 0 && len(in.VirtualClusterConfig) > 0 {
		return dpstore.Cluster{}, dpstore.Operation{}, invalidArgument("cluster must specify exactly one of config or virtualClusterConfig")
	}
	if err := validateVirtualClusterConfig(in.VirtualClusterConfig); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, err
	}
	if err := validateMetastoreConfigShape(in); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, err
	}
	if err := s.validateClusterMetastore(ctx, project, region, in); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, err
	}
	now := clock.Now().UTC()
	c := dpstore.Cluster{
		ProjectID:            project,
		Region:               region,
		Name:                 name,
		Status:               dpstore.ClusterStatus{State: "CREATING", StateStartTime: now},
		ClusterUUID:          randomHex(32),
		CreateTime:           now,
		UpdateTime:           now,
		Labels:               in.Labels,
		Config:               in.Config,
		VirtualClusterConfig: in.VirtualClusterConfig,
	}
	if err := s.store.CreateCluster(ctx, project, region, c); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	slog.Info("dataproc: cluster creating", "project", project, "region", region, "cluster", name, "placement", clusterPlacement(c))
	s.emitClusterStateChange(ctx, project, region, c, dpstore.ClusterStatus{})
	op, err := s.createClusterOperation(ctx, project, region, "create", ClusterName(project, region, name),
		clusterOperationMetadata(name, c.ClusterUUID, "CREATE", "RUNNING"))
	if err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	return c, op, nil
}

// GetCluster returns one cluster, lazily settling a transitional state first.
func (s *Service) GetCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, error) {
	if region == "" || name == "" {
		return dpstore.Cluster{}, invalidArgument("missing region or clusterName")
	}
	c, err := s.advanceCluster(ctx, project, region, name)
	if err != nil {
		return dpstore.Cluster{}, mapErr(err)
	}
	return c, nil
}

// ListClusters returns a cursor page of the clusters in a region, settling each
// transitional cluster first and dropping any whose delayed delete completed.
func (s *Service) ListClusters(ctx context.Context, project, region string, pageSize int, pageToken string) ([]dpstore.Cluster, string, error) {
	if region == "" {
		return nil, "", invalidArgument("missing region")
	}
	clusters, err := s.store.ListClusters(ctx, project, region)
	if err != nil {
		return nil, "", err
	}
	// filter is accepted but ignored (documented limitation, matches workflows).
	settled := make([]dpstore.Cluster, 0, len(clusters))
	for _, c := range clusters {
		advanced, err := s.advanceCluster(ctx, project, region, c.Name)
		if errors.Is(err, dpstore.ErrNoSuchCluster) {
			continue // a delayed delete completed during this read
		}
		if err != nil {
			return nil, "", err
		}
		settled = append(settled, advanced)
	}
	page, next := pageClusters(settled, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateCluster merges the caller's config/labels into the stored cluster and
// returns its (not yet done) update operation. The mask selects which top-level
// fields apply; an empty mask applies everything supplied. Field changes are
// applied synchronously; only the cluster status transition (UPDATING -> the
// prior state) is deferred and completed lazily by a later read.
func (s *Service) UpdateCluster(ctx context.Context, project, region, name string, in ClusterInput, mask []string) (dpstore.Cluster, dpstore.Operation, error) {
	if region == "" || name == "" {
		return dpstore.Cluster{}, dpstore.Operation{}, invalidArgument("missing region or clusterName")
	}
	apply := func(field string) bool {
		return len(mask) == 0 || containsMaskField(mask, field)
	}
	if in.VirtualClusterConfig != nil && apply("virtualClusterConfig") {
		if err := validateVirtualClusterConfig(in.VirtualClusterConfig); err != nil {
			return dpstore.Cluster{}, dpstore.Operation{}, err
		}
	}
	if err := validateMetastoreConfigShape(in); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, err
	}
	// Settle any pending create/update before applying this one, so the update
	// starts from a stable state and its operation can reach a target.
	current, err := s.advanceCluster(ctx, project, region, name)
	if err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	// Validate the attachment of the cluster this update will actually produce
	// (the effective merged config under the mask), not the raw request body.
	if err := s.validateClusterMetastoreUpdate(ctx, project, region, current, in, mask); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, err
	}
	var transitioned bool
	var prev dpstore.ClusterStatus
	c, err := s.store.UpdateClusterAtomic(ctx, project, region, name, func(c dpstore.Cluster) (dpstore.Cluster, error) {
		if apply("labels") && in.Labels != nil {
			c.Labels = in.Labels
		}
		if in.VirtualClusterConfig != nil && apply("virtualClusterConfig") {
			c.VirtualClusterConfig = in.VirtualClusterConfig
		}
		if in.Config != nil {
			stored := map[string]any{}
			if len(c.Config) > 0 {
				_ = json.Unmarshal(c.Config, &stored)
			}
			applyConfigMask(stored, mustJSONMap(in.Config), mask)
			if data, err := json.Marshal(stored); err == nil {
				c.Config = data
			}
		}
		if len(c.Config) > 0 && len(c.VirtualClusterConfig) > 0 {
			return c, invalidArgument("cluster must specify exactly one of config or virtualClusterConfig")
		}
		if !clusterTransitional(c.Status.State) {
			transitioned = true
			prev = c.Status
			c.StatusHistory = append(c.StatusHistory, c.Status)
			c.Status = dpstore.ClusterStatus{State: "UPDATING", StateStartTime: clock.Now().UTC()}
		}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	if transitioned {
		s.emitClusterStateChange(ctx, project, region, c, prev)
	}
	op, err := s.createClusterOperation(ctx, project, region, "update", ClusterName(project, region, name),
		clusterOperationMetadata(name, c.ClusterUUID, "UPDATE", "RUNNING"))
	if err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	return c, op, nil
}

// DeleteCluster marks the cluster DELETING and returns its (not yet done)
// delete operation. The record is kept until the delayed transition fires; a
// later cluster or operation read removes it and completes the operation with
// an empty (google.protobuf.Empty) response.
func (s *Service) DeleteCluster(ctx context.Context, project, region, name string) (dpstore.Operation, error) {
	if region == "" || name == "" {
		return dpstore.Operation{}, invalidArgument("missing region or clusterName")
	}
	if _, err := s.advanceCluster(ctx, project, region, name); err != nil {
		return dpstore.Operation{}, mapErr(err)
	}
	var prev dpstore.ClusterStatus
	c, err := s.store.UpdateClusterAtomic(ctx, project, region, name, func(c dpstore.Cluster) (dpstore.Cluster, error) {
		prev = c.Status
		if !clusterTransitional(c.Status.State) {
			c.StatusHistory = append(c.StatusHistory, c.Status)
		}
		c.Status = dpstore.ClusterStatus{State: "DELETING", StateStartTime: clock.Now().UTC()}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if err != nil {
		return dpstore.Operation{}, mapErr(err)
	}
	if prev.State != "DELETING" {
		s.emitClusterStateChange(ctx, project, region, c, prev)
	}
	op, err := s.createClusterOperation(ctx, project, region, "delete", ClusterName(project, region, name),
		clusterOperationMetadata(name, c.ClusterUUID, "DELETE", "RUNNING"))
	if err != nil {
		return dpstore.Operation{}, mapErr(err)
	}
	return op, nil
}

// startStopCluster moves a cluster into a transitional state (STARTING or
// STOPPING) and returns its not-yet-done operation; the read-time state machine
// settles it to the target state.
func (s *Service) startStopCluster(ctx context.Context, project, region, name, toState, verb, operationType string) (dpstore.Cluster, dpstore.Operation, error) {
	if region == "" || name == "" {
		return dpstore.Cluster{}, dpstore.Operation{}, invalidArgument("missing region or clusterName")
	}
	if _, err := s.advanceCluster(ctx, project, region, name); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	var prev dpstore.ClusterStatus
	c, err := s.store.UpdateClusterAtomic(ctx, project, region, name, func(c dpstore.Cluster) (dpstore.Cluster, error) {
		prev = c.Status
		if !clusterTransitional(c.Status.State) {
			c.StatusHistory = append(c.StatusHistory, c.Status)
		}
		c.Status = dpstore.ClusterStatus{State: toState, StateStartTime: clock.Now().UTC()}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	if prev.State != toState {
		s.emitClusterStateChange(ctx, project, region, c, prev)
	}
	op, err := s.createClusterOperation(ctx, project, region, verb, ClusterName(project, region, name),
		clusterOperationMetadata(name, c.ClusterUUID, operationType, "RUNNING"))
	if err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	return c, op, nil
}

// StartCluster begins a cluster start and returns the not-yet-done operation.
func (s *Service) StartCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, dpstore.Operation, error) {
	return s.startStopCluster(ctx, project, region, name, "STARTING", "start", "START")
}

// StopCluster begins a cluster stop and returns the not-yet-done operation.
func (s *Service) StopCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, dpstore.Operation, error) {
	return s.startStopCluster(ctx, project, region, name, "STOPPING", "stop", "STOP")
}

// DiagnoseCluster is an unimplemented stub: it fails loud rather than silently
// succeeding, so SDK callers observe a real UNIMPLEMENTED error.
func (s *Service) DiagnoseCluster() error {
	return model.NewProviderError("Unimplemented", "DiagnoseCluster is not supported by the emulator", 501)
}

// --- Lazy cluster state machine ---

// clusterTransitionalStates are the in-flight cluster states a read settles.
var clusterTransitionalStates = map[string]bool{
	"CREATING":  true,
	"UPDATING":  true,
	"STARTING":  true,
	"STOPPING":  true,
	"DELETING":  true,
	"REPAIRING": true,
}

func clusterTransitional(state string) bool { return clusterTransitionalStates[state] }

// clusterStableState reports whether a state is settled (not advanced further
// by a read). ERROR is settled: a failed cluster stays failed until deleted.
func clusterStableState(state string) bool {
	switch state {
	case "RUNNING", "STOPPED", "ERROR":
		return true
	}
	return false
}

// clusterTargetState is the state a transitional cluster settles to. UPDATING
// restores the most recent stable state recorded before it (RUNNING/STOPPED);
// every other transition has a fixed target.
func clusterTargetState(c dpstore.Cluster) string {
	switch c.Status.State {
	case "STARTING", "REPAIRING":
		return "RUNNING"
	case "STOPPING":
		return "STOPPED"
	case "UPDATING":
		for i := len(c.StatusHistory) - 1; i >= 0; i-- {
			if clusterStableState(c.StatusHistory[i].State) {
				return c.StatusHistory[i].State
			}
		}
		return "RUNNING"
	default: // CREATING
		return "RUNNING"
	}
}

// errClusterDeleteDue signals UpdateClusterAtomic to abort without writing
// because the cluster's DELETING transition is due and the record must be
// removed instead.
var errClusterDeleteDue = errors.New("dataproc: cluster delete transition due")

// advanceCluster lazily settles a transitional cluster once clusterReadyDelay
// has elapsed since it entered that state. It is called on every cluster read
// (GetCluster/ListClusters) and from the operation poll path, and serializes
// with other mutations via UpdateClusterAtomic so concurrent readers cannot
// double-apply a transition. A DELETING cluster past its delay is removed and
// reported as ErrNoSuchCluster; any settled cluster is returned unchanged.
func (s *Service) advanceCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, error) {
	transitioned := false
	var prev dpstore.ClusterStatus
	var deleted dpstore.Cluster
	c, err := s.store.UpdateClusterAtomic(ctx, project, region, name, func(c dpstore.Cluster) (dpstore.Cluster, error) {
		if !clusterTransitional(c.Status.State) || clock.Now().UTC().Before(c.Status.StateStartTime.Add(s.clusterReadyDelay)) {
			return c, nil
		}
		if c.Status.State == "DELETING" {
			deleted = c
			return c, errClusterDeleteDue
		}
		next := clusterTargetState(c)
		if next == "RUNNING" && s.clusterErrorHook != nil && s.clusterErrorHook(project, region, name) {
			next = "ERROR"
		}
		transitioned = true
		prev = c.Status
		c.StatusHistory = append(c.StatusHistory, c.Status)
		c.Status = dpstore.ClusterStatus{State: next, StateStartTime: clock.Now().UTC()}
		c.UpdateTime = clock.Now().UTC()
		return c, nil
	})
	if errors.Is(err, errClusterDeleteDue) {
		delErr := s.store.DeleteCluster(ctx, project, region, name)
		if delErr != nil && !errors.Is(delErr, dpstore.ErrNoSuchCluster) {
			return dpstore.Cluster{}, delErr
		}
		// Emit the terminal DELETED transition only when this reader actually
		// removed the record, so two concurrent readers polling the same due
		// DELETING cluster cannot both publish it. The delete happens outside
		// the atomic mutate (which aborted), so existence is decided here.
		if delErr == nil {
			gone := deleted
			gone.Status = dpstore.ClusterStatus{State: clusterDeletedState, StateStartTime: clock.Now().UTC()}
			s.emitClusterStateChange(ctx, project, region, gone, deleted.Status)
		}
		return dpstore.Cluster{}, dpstore.ErrNoSuchCluster
	}
	if err != nil {
		// The mutate ran (so transitioned/prev/deleted may be set) but the write
		// failed: never publish a transition that did not persist.
		return c, err
	}
	if transitioned {
		s.emitClusterStateChange(ctx, project, region, c, prev)
	}
	return c, nil
}

// clusterNameFromTarget extracts the cluster name from a stored operation's
// resource-name target (projects/{p}/regions/{r}/clusters/{name}).
func clusterNameFromTarget(target string) string {
	const marker = "/clusters/"
	if i := strings.LastIndex(target, marker); i >= 0 {
		return target[i+len(marker):]
	}
	return ""
}

// pageClusters paginates cluster records using the shared GCP paging helper.
func pageClusters(clusters []dpstore.Cluster, params map[string]any) ([]dpstore.Cluster, string) {
	return paging.Page(clusters, func(c dpstore.Cluster) string { return c.Name }, params)
}

// --- mask helpers ---

// containsMaskField reports whether the mask contains the given top-level field
// (or a sub-path of it). Mask segments are camelCase-normalized first so a
// gRPC FieldMask's snake_case path (e.g. virtual_cluster_config) and a REST
// updateMask's camelCase path (virtualClusterConfig) match the same field.
func containsMaskField(mask []string, field string) bool {
	for _, part := range mask {
		norm := camelKey(part)
		if norm == field || strings.HasPrefix(norm, field+".") {
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

// applyConfigMask merges fields from the request config onto the stored config,
// honoring the updateMask's config.* paths. Paths may be nested (e.g.
// config.worker_config.num_instances, the documented updatable field) and may
// use either the proto's snake_case or the JSON shape's camelCase segments; both
// are normalized to the camelCase keys the stored config uses. Only sub-fields
// present in the request config (under the masked path) are applied. An empty
// mask merges every supplied top-level key.
func applyConfigMask(stored, incoming map[string]any, mask []string) {
	if incoming == nil {
		return
	}
	if len(mask) == 0 {
		// No mask: merge all incoming top-level keys verbatim.
		for k, v := range incoming {
			stored[k] = v
		}
		return
	}
	for _, field := range mask {
		if !strings.HasPrefix(field, "config.") {
			continue
		}
		path := strings.Split(field[len("config."):], ".")
		if len(path) == 0 || path[0] == "" {
			continue
		}
		cur := incoming
		ok := true
		for _, seg := range path[:len(path)-1] {
			next, isMap := cur[camelKey(seg)].(map[string]any)
			if !isMap {
				ok = false
				break
			}
			cur = next
		}
		if !ok {
			continue
		}
		leaf := camelKey(path[len(path)-1])
		v, exists := cur[leaf]
		if !exists {
			continue
		}
		setNestedCamel(stored, path[:len(path)-1], leaf, v)
	}
}

// camelKey converts a snake_case path segment to the camelCase JSON key the
// Discovery/proto shape uses. An already-camelCase segment passes through.
func camelKey(s string) string {
	if !strings.Contains(s, "_") {
		return s
	}
	parts := strings.Split(s, "_")
	out := parts[0]
	for _, p := range parts[1:] {
		if p == "" {
			continue
		}
		out += strings.ToUpper(p[:1]) + p[1:]
	}
	return out
}

// setNestedCamel stores value at the camelCase-normalized path under stored,
// creating intermediate maps as needed.
func setNestedCamel(stored map[string]any, parents []string, leaf string, value any) {
	cur := stored
	for _, seg := range parents {
		key := camelKey(seg)
		next, ok := cur[key].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[key] = next
		}
		cur = next
	}
	cur[leaf] = value
}
