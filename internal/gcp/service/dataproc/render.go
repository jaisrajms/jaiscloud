package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"jaiscloud/internal/clock"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// substateRunning is the dataproc.v1.JobStatus.Substate rendered while a job is
// in the RUNNING state. A submitted job walks PENDING -> SETUP_DONE -> RUNNING,
// and once RUNNING it reports the real QUEUED substate ("the Job has been
// received and is awaiting execution"; dataproc.v1 documents QUEUED as applying
// to RUNNING). The other defined substates (SUBMITTED, STALE_STATUS) describe
// agent hand-off/staleness the emulator does not model, and every non-RUNNING
// state omits substate entirely because every defined substate applies only to
// RUNNING.
const substateRunning = "QUEUED"

// formatTimestamp renders a business timestamp as the RFC3339Nano string the
// Discovery JSON shape uses.
func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// --- Cluster rendering ---

func clusterStatusMap(s dpstore.ClusterStatus) map[string]any {
	out := map[string]any{
		"state":          s.State,
		"stateStartTime": formatTimestamp(s.StateStartTime),
	}
	if s.Detail != "" {
		out["detail"] = s.Detail
	}
	return out
}

// ClusterJSON renders a stored Cluster as a dataproc.v1.Cluster wire map.
func ClusterJSON(c dpstore.Cluster) map[string]any {
	out := map[string]any{
		"projectId":   c.ProjectID,
		"clusterName": c.Name,
		"status":      clusterStatusMap(c.Status),
	}
	if c.ClusterUUID != "" {
		out["clusterUuid"] = c.ClusterUUID
	}
	// A GKE-backed cluster has a virtualClusterConfig and no GCE config, and a
	// GCE cluster the reverse. An empty stored object (e.g. the Postgres JSONB
	// "{}" sentinel) is treated as unset, so neither transport invents config:{}.
	if len(c.Config) > 0 {
		var config map[string]any
		if json.Unmarshal(c.Config, &config) == nil && len(config) > 0 {
			out["config"] = config
		}
	}
	if len(c.VirtualClusterConfig) > 0 {
		var vcc map[string]any
		if json.Unmarshal(c.VirtualClusterConfig, &vcc) == nil && len(vcc) > 0 {
			out["virtualClusterConfig"] = vcc
		}
	}
	if c.Labels != nil {
		out["labels"] = c.Labels
	}
	if len(c.StatusHistory) > 0 {
		history := make([]any, 0, len(c.StatusHistory))
		for _, s := range c.StatusHistory {
			history = append(history, clusterStatusMap(s))
		}
		out["statusHistory"] = history
	}
	return out
}

// --- Job rendering ---

func jobStatusMap(s dpstore.JobStatus) map[string]any {
	out := map[string]any{
		"state":          s.State,
		"stateStartTime": formatTimestamp(s.StateStartTime),
	}
	if s.Details != "" {
		out["details"] = s.Details
	}
	// substate is an enum-valued field in dataproc.v1.JobStatus; proto3 JSON
	// omits it at its default (UNSPECIFIED), so only render a set value.
	if s.Substate != "" {
		out["substate"] = s.Substate
	}
	return out
}

// jobSchedulingMap renders a job's restart policy. Zero-valued counters are
// omitted (protojson omits zero int32 fields), so an explicitly-present but
// zero policy renders as an empty object.
func jobSchedulingMap(s *dpstore.JobScheduling) map[string]any {
	out := map[string]any{}
	if s.MaxFailuresPerHour != 0 {
		out["maxFailuresPerHour"] = s.MaxFailuresPerHour
	}
	if s.MaxFailuresTotal != 0 {
		out["maxFailuresTotal"] = s.MaxFailuresTotal
	}
	return out
}

// JobJSON renders a stored Job as a dataproc.v1.Job wire map.
func JobJSON(j dpstore.Job) map[string]any {
	placement := map[string]any{"clusterName": j.PlacementClusterName}
	// clusterUuid is the output-only UUID of the cluster the job was submitted
	// to (dataproc.v1.JobPlacement); render it only when it was captured.
	if j.PlacementClusterUUID != "" {
		placement["clusterUuid"] = j.PlacementClusterUUID
	}
	out := map[string]any{
		"reference": map[string]any{"projectId": j.ProjectID, "jobId": j.JobID},
		"placement": placement,
		"status":    jobStatusMap(j.Status),
		"done":      jobTerminal(j.Status.State),
	}
	if j.JobUUID != "" {
		out["jobUuid"] = j.JobUUID
	}
	if j.Type != "" && len(j.TypeJob) > 0 {
		var typeJob map[string]any
		if json.Unmarshal(j.TypeJob, &typeJob) == nil && typeJob != nil {
			out[j.Type] = typeJob
		}
	}
	if j.DriverOutputResourceURI != "" {
		out["driverOutputResourceUri"] = j.DriverOutputResourceURI
	}
	if j.DriverControlFilesURI != "" {
		out["driverControlFilesUri"] = j.DriverControlFilesURI
	}
	if j.Labels != nil {
		out["labels"] = j.Labels
	}
	if j.Scheduling != nil {
		out["scheduling"] = jobSchedulingMap(j.Scheduling)
	}
	if len(j.StatusHistory) > 0 {
		history := make([]any, 0, len(j.StatusHistory))
		for _, s := range j.StatusHistory {
			history = append(history, jobStatusMap(s))
		}
		out["statusHistory"] = history
	}
	return out
}

// --- Operations ---

// clusterOperationMetadata renders ClusterOperationMetadata for an operation.
// opState is the operation-status enum (PENDING|RUNNING|DONE), not the cluster
// state: dataproc's ClusterOperationStatus tracks the *operation*, while the
// cluster's own state (CREATING/DELETING/UPDATING/...) is exposed on the
// Cluster resource. protojson rejects unknown enum values, so this must stay
// within ClusterOperationStatus.State.
func clusterOperationMetadata(clusterName, clusterUUID, operationType, opState string) map[string]any {
	now := clock.Now().UTC()
	entry := func(state string) map[string]any {
		return map[string]any{"state": state, "stateStartTime": formatTimestamp(now)}
	}
	pending, running, done := entry("PENDING"), entry("RUNNING"), entry("DONE")
	var status map[string]any
	var history []any
	switch opState {
	case "PENDING":
		status, history = pending, []any{pending}
	case "DONE":
		status, history = done, []any{pending, running, done}
	default: // RUNNING — an in-flight create/update/start/stop/delete
		status, history = running, []any{pending, running}
	}
	return map[string]any{
		"@type":         "type.googleapis.com/google.cloud.dataproc.v1.ClusterOperationMetadata",
		"clusterName":   clusterName,
		"clusterUuid":   clusterUUID,
		"operationType": operationType,
		"status":        status,
		"statusHistory": history,
	}
}

// jobOperationMetadata renders JobMetadata for a submit-as-operation. Its
// status is the job's full JobStatus (dataproc.v1.JobMetadata.status), so a
// poller sees the job state advance, not just the first state.
func jobOperationMetadata(jobID, operationType string, status dpstore.JobStatus, start time.Time) map[string]any {
	return map[string]any{
		"@type":         "type.googleapis.com/google.cloud.dataproc.v1.JobMetadata",
		"jobId":         jobID,
		"operationType": operationType,
		"startTime":     formatTimestamp(start),
		"status":        jobStatusMap(status),
	}
}

// refreshJobOperationMetadata rewrites a persisted JobMetadata's status to the
// job's current JobStatus, keeping an in-flight submit operation's polled
// metadata in step with the job as it walks the state machine.
func refreshJobOperationMetadata(metadata string, status dpstore.JobStatus) string {
	if metadata == "" {
		return metadata
	}
	meta := map[string]any{}
	if err := json.Unmarshal([]byte(metadata), &meta); err != nil {
		return metadata
	}
	meta["status"] = jobStatusMap(status)
	if b, err := json.Marshal(meta); err == nil {
		return string(b)
	}
	return metadata
}

// OperationJSON renders a stored Operation as a google.longrunning.Operation.
func OperationJSON(op dpstore.Operation) map[string]any {
	name := OperationName(op.ProjectID, op.Region, op.ID)
	var metadata any = map[string]any{}
	if op.Metadata != "" {
		_ = json.Unmarshal([]byte(op.Metadata), &metadata)
	}
	out := map[string]any{
		"name":     name,
		"metadata": metadata,
		"done":     op.Done,
	}
	if op.Done && op.Response != "" {
		var response any = map[string]any{}
		if json.Unmarshal([]byte(op.Response), &response) == nil {
			out["response"] = response
		}
	}
	return out
}

// createClusterOperation persists a not-yet-done operation for a cluster
// mutation. The response is filled in when a later read finalizes the
// operation (see advanceClusterOperation).
func (s *Service) createClusterOperation(ctx context.Context, project, region, verb, target string, metadata map[string]any) (dpstore.Operation, error) {
	now := clock.Now().UTC()
	op := dpstore.Operation{
		ID:         randomHex(12),
		ProjectID:  project,
		Region:     region,
		Done:       false,
		Verb:       verb,
		Target:     target,
		CreateTime: now,
	}
	if metadata != nil {
		if b, err := json.Marshal(metadata); err == nil {
			op.Metadata = string(b)
		}
	}
	s.sweepOperations(ctx)
	if err := s.store.CreateOperation(ctx, project, region, op); err != nil {
		return dpstore.Operation{}, err
	}
	return op, nil
}

// advanceClusterOperation settles the cluster an in-flight cluster operation
// targets and, once the cluster is stable, finalizes the operation with the
// cluster (or an empty object for a delete). It is the read-time half of the
// cluster LRO, invoked from GetOperation.
func (s *Service) advanceClusterOperation(ctx context.Context, op dpstore.Operation) (dpstore.Operation, error) {
	name := clusterNameFromTarget(op.Target)
	if name == "" {
		return op, nil
	}
	c, err := s.advanceCluster(ctx, op.ProjectID, op.Region, name)
	if errors.Is(err, dpstore.ErrNoSuchCluster) {
		if op.Verb == "delete" {
			return s.finishClusterOperation(ctx, op, map[string]any{}), nil
		}
		// The cluster disappeared underneath the operation (e.g. deleted while
		// still creating); terminate so the poll cannot hang forever.
		return s.finishClusterOperation(ctx, op, nil), nil
	}
	if err != nil {
		return op, err
	}
	if clusterTransitional(c.Status.State) {
		return op, nil // transition not due yet
	}
	return s.finishClusterOperation(ctx, op, ClusterJSON(c)), nil
}

// finishClusterOperation marks an operation done, records its response, and
// flips the cluster-operation metadata's status to DONE. It is best-effort on
// the metadata rewrite (the operation is still returned done even if the store
// update fails).
func (s *Service) finishClusterOperation(ctx context.Context, op dpstore.Operation, response map[string]any) dpstore.Operation {
	if op.Done {
		return op
	}
	now := clock.Now().UTC()
	op.Done = true
	op.EndTime = now
	if response != nil {
		if b, err := json.Marshal(response); err == nil {
			op.Response = string(b)
		}
	}
	var meta map[string]any
	if op.Metadata != "" {
		_ = json.Unmarshal([]byte(op.Metadata), &meta)
	}
	if meta != nil {
		done := map[string]any{"state": "DONE", "stateStartTime": formatTimestamp(now)}
		meta["status"] = done
		if history, ok := meta["statusHistory"].([]any); ok {
			meta["statusHistory"] = append(history, done)
		}
		if b, err := json.Marshal(meta); err == nil {
			op.Metadata = string(b)
		}
	}
	if err := s.store.UpdateOperation(ctx, op.ProjectID, op.Region, op); err != nil {
		slog.Warn("dataproc: finishClusterOperation failed", "operation", op.ID, "err", err)
	}
	return op
}

// sweepOperations lazily deletes completed operations older than the retention
// window. It runs on each operation creation so jc_dataproc_operations stays
// bounded without a background goroutine.
func (s *Service) sweepOperations(ctx context.Context) {
	if s.operationTTL <= 0 {
		return
	}
	if _, err := s.store.DeleteStaleOperations(ctx, clock.Now().UTC().Add(-s.operationTTL)); err != nil {
		slog.Warn("dataproc: operation sweep failed", "err", err)
	}
}
