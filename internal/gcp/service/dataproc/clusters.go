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
	// Discovery's Cluster.config/virtualClusterConfig descriptions make the two
	// mutually exclusive and one of them required: "Exactly one of ClusterConfig
	// or VirtualClusterConfig must be specified". A create carrying neither is
	// therefore as invalid as one carrying both — and staying tolerant would
	// hand the caller an invented default GCE config on the read.
	if (len(in.Config) > 0) == (len(in.VirtualClusterConfig) > 0) {
		return dpstore.Cluster{}, dpstore.Operation{}, invalidArgument("cluster must specify exactly one of config or virtualClusterConfig")
	}
	// Collect the request's InvalidArgument failures so one response lists every
	// problem, the way real Dataproc answers a create (DPG45). The checks run in
	// a fixed order — virtualClusterConfig, placement, metastore shape — so the
	// joined message is deterministic, and a request with one problem keeps its
	// original message.
	var errs validationErrors
	gkeVCC, vccErrs := collectVirtualClusterConfig(in.VirtualClusterConfig, name)
	errs.addAll(vccErrs)
	errs.addAll(collectGKEPlacement(gkeVCC, project, region))
	errs.add(validateMetastoreConfigShape(in))
	if err := errs.err(); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, err
	}
	if err := s.validateClusterMetastore(ctx, project, region, in); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, err
	}
	// A Kubernetes namespace belongs to one live virtual cluster per GKE
	// cluster: real Dataproc verifies that "another Dataproc VirtualCluster is
	// not installed into it", which is why the recreate guide drops
	// kubernetesNamespace before re-creating.
	if err := s.validateGKENamespaceOwnership(ctx, project, region, name, gkeVCC); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, err
	}
	// Persist the effective virtualClusterConfig: the namespace defaulted to the
	// cluster name and the SPARK componentVersion dereferenced from its alias, so
	// a read echoes the values the cluster actually runs with. The k8s placement
	// namespace keeps using the caller's value only (see resolveClusterNamespace).
	requestedNamespace := KubernetesNamespaceFromVirtualClusterConfig(in.VirtualClusterConfig)
	normalizedVCC, err := applyVirtualClusterDefaults(in.VirtualClusterConfig, name, gkeVCC)
	if err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, err
	}
	in.VirtualClusterConfig = normalizedVCC
	// Reject a duplicate name before any side effect: reconciling node pools and
	// provisioning the workload namespace both create state that a rejected
	// create must not leave behind (pools are never deleted), and real Dataproc
	// answers the duplicate before it provisions anything.
	if _, err := s.store.GetCluster(ctx, project, region, name); err == nil {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(dpstore.ErrAlreadyExists)
	} else if !errors.Is(err, dpstore.ErrNoSuchCluster) {
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	// Validate the Spark History Server attachment (if any) and add the
	// documented workload event-log properties to the cluster's effective
	// virtualClusterConfig (after the GKE defaults, so the injected log directory
	// sees the effective staging bucket).
	if in.VirtualClusterConfig != nil {
		prepared, err := s.preparePHSClusterConfig(ctx, project, region, name, in.VirtualClusterConfig)
		if err != nil {
			return dpstore.Cluster{}, dpstore.Operation{}, err
		}
		in.VirtualClusterConfig = prepared
	}
	now := clock.Now().UTC()
	// Provision the cluster's workload namespace before persisting the record, so
	// a job submitted immediately after create has a placement. Mock execution
	// yields ("", false); an RBAC/tech failure falls back to the process-wide
	// namespace (see resolveClusterNamespace).
	ns, nsOwned := s.resolveClusterNamespace(ctx, project, region, name, requestedNamespace)
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
		Namespace:            ns,
		NamespaceOwned:       nsOwned,
	}
	// Reconcile the Dataproc-on-GKE placement against the emulated GKE service:
	// resolve gkeClusterTarget, then create or verify each nodePoolTarget. A
	// provisioning failure is not a transport error — real Dataproc fails the
	// create operation, so the cluster is persisted in ERROR with a
	// status.detail naming the pool and field, and its pools survive a delete. A
	// resolution failure (unknown/Autopilot/no-Workload-Identity target) rejects
	// the request instead: nothing was created.
	reconcileErr := s.reconcileGKEVirtualCluster(ctx, project, region, name, c.ClusterUUID, gkeVCC, in.Labels)
	var poolFailure *gkeReconcileFailure
	if reconcileErr != nil {
		if !errors.As(reconcileErr, &poolFailure) {
			// Nothing was created — including the workload namespace provisioned
			// above, which a rejected request must not leak.
			s.rollbackNamespace(ctx, ns, nsOwned)
			return dpstore.Cluster{}, dpstore.Operation{}, reconcileErr
		}
		c.Status = gkeErrorState(poolFailure.err, now)
	}
	if err := s.store.CreateCluster(ctx, project, region, c); err != nil {
		s.rollbackNamespace(ctx, ns, nsOwned)
		return dpstore.Cluster{}, dpstore.Operation{}, mapErr(err)
	}
	if reconcileErr != nil {
		slog.Warn("dataproc: cluster create failed GKE node-pool reconciliation",
			"project", project, "region", region, "cluster", name, "detail", c.Status.Detail)
	} else {
		slog.Info("dataproc: cluster creating", "project", project, "region", region, "cluster", name,
			"placement", clusterPlacement(c), "namespace", ns, "namespaceOwned", nsOwned)
	}
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
// filter is the bounded dataproc.v1.ListClustersRequest.filter grammar
// (status.state, clusterName, labels.<key>); it is applied before pagination so
// pageToken cursors stay stable. A malformed or unsupported filter is
// InvalidArgument — never a silently unfiltered list.
func (s *Service) ListClusters(ctx context.Context, project, region, filter string, pageSize int, pageToken string) ([]dpstore.Cluster, string, error) {
	if region == "" {
		return nil, "", invalidArgument("missing region")
	}
	filter = strings.TrimSpace(filter)
	var compiled clusterFilter
	if filter != "" {
		var err error
		if compiled, err = compileClusterFilter(filter); err != nil {
			return nil, "", err
		}
	}
	clusters, err := s.store.ListClusters(ctx, project, region)
	if err != nil {
		return nil, "", err
	}
	settled := make([]dpstore.Cluster, 0, len(clusters))
	for _, c := range clusters {
		advanced, err := s.advanceCluster(ctx, project, region, c.Name)
		if errors.Is(err, dpstore.ErrNoSuchCluster) {
			continue // a delayed delete completed during this read
		}
		if err != nil {
			return nil, "", err
		}
		if filter != "" && !compiled.match(advanced) {
			continue
		}
		settled = append(settled, advanced)
	}
	page, next, err := pageClusters(settled, pageParams(pageSize, pageToken))
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// ListAllClusters lists every cluster in a project across all regions, sorted
// by region then name, settling each transitional cluster first (and dropping
// any whose delayed delete completed). It backs the region-optional console
// list.
func (s *Service) ListAllClusters(ctx context.Context, project string) ([]dpstore.Cluster, error) {
	clusters, err := s.store.ListClustersByProject(ctx, project)
	if err != nil {
		return nil, err
	}
	settled := make([]dpstore.Cluster, 0, len(clusters))
	for _, c := range clusters {
		advanced, err := s.advanceCluster(ctx, c.ProjectID, c.Region, c.Name)
		if errors.Is(err, dpstore.ErrNoSuchCluster) {
			continue // a delayed delete completed during this read
		}
		if err != nil {
			return nil, mapErr(err)
		}
		settled = append(settled, advanced)
	}
	return settled, nil
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
		if _, err := validateVirtualClusterConfig(in.VirtualClusterConfig, name); err != nil {
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
	// Dataproc on GKE is recreate-only: real GCP disallows update operations on a
	// virtual cluster, so a patch may not touch virtualClusterConfig or add a GCE
	// config to a GKE-backed cluster.
	if err := rejectGKEVirtualClusterUpdate(current, in, mask); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, err
	}
	// Validate the attachment of the cluster this update will actually produce
	// (the effective merged config under the mask), not the raw request body.
	if err := s.validateClusterMetastoreUpdate(ctx, project, region, current, in, mask); err != nil {
		return dpstore.Cluster{}, dpstore.Operation{}, err
	}
	// A virtualClusterConfig update is rejected above (recreate-only), so a PHS
	// attachment is validated at create time only; there is no update path that
	// could introduce or change one.
	var transitioned bool
	var prev dpstore.ClusterStatus
	c, err := s.store.UpdateClusterAtomic(ctx, project, region, name, func(c dpstore.Cluster) (dpstore.Cluster, error) {
		if apply("labels") && in.Labels != nil {
			c.Labels = in.Labels
		}
		if in.VirtualClusterConfig != nil && apply("virtualClusterConfig") {
			c.VirtualClusterConfig = in.VirtualClusterConfig
		}
		// Only merge a supplied config when the mask applies it: a full resource
		// sent under a labels-only mask must not install an (empty) config, which
		// would then trip the "exactly one of config or virtualClusterConfig"
		// invariant on a GKE-backed cluster.
		if in.Config != nil && apply("config") {
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
			// The record is gone; reap the cluster's workloads and, when the
			// emulator owns it, its namespace. Dispatched in the background so a
			// read never blocks on namespace deletion.
			s.dispatchClusterTeardown(deleted)
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
func pageClusters(clusters []dpstore.Cluster, params map[string]any) ([]dpstore.Cluster, string, error) {
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
		if camelKey(field) == "config" {
			// A bare `config` mask names the whole field. Treat it like an
			// omitted mask (merge every supplied top-level key) rather than
			// matching no path at all: silently discarding the caller's config
			// while answering 200 is the one behaviour no client can detect.
			for k, v := range incoming {
				stored[k] = v
			}
			continue
		}
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
