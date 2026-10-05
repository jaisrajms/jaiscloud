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

// SubmitJob submits a job to a cluster. The job is stored PENDING and walks the
// full state machine to a terminal state. In mock mode the transition is lazy
// and clock-driven (advanceJob); in k8s mode a background goroutine runs it and
// writes SETUP_DONE/RUNNING/terminal back to the store.
func (s *Service) SubmitJob(ctx context.Context, project, region string, in JobInput) (dpstore.Job, error) {
	return s.submitJob(ctx, project, region, in)
}

func (s *Service) submitJob(ctx context.Context, project, region string, in JobInput) (dpstore.Job, error) {
	if region == "" {
		return dpstore.Job{}, invalidArgument("missing region")
	}
	// placement.clusterName is REQUIRED (dataproc.v1.JobPlacement). Validate it
	// before the cluster existence check.
	if in.PlacementClusterName == "" {
		return dpstore.Job{}, invalidArgument("missing placement.clusterName")
	}
	// scheduling is optional; when present its counters must be within the API
	// maxima (0 = the default "no restarts").
	if err := validateScheduling(in.Scheduling); err != nil {
		return dpstore.Job{}, err
	}

	j := jobToStore(project, region, in)
	cluster, err := s.store.GetCluster(ctx, project, region, j.PlacementClusterName)
	if err != nil {
		return dpstore.Job{}, model.NewProviderError("NotFound", "cluster not found: "+j.PlacementClusterName, 404)
	}
	// placement.clusterUuid is the output-only UUID of the cluster the job runs
	// on (dataproc.v1.JobPlacement). Capture it at submit so the field survives
	// the cluster being deleted before the job is terminal.
	j.PlacementClusterUUID = cluster.ClusterUUID
	// Allocate the driver-output/control-file URIs (and provision the staging
	// bucket) now, so they are persisted with the job and survive the cluster
	// being deleted before the job reaches a terminal state.
	s.prepareDriverOutput(ctx, cluster, &j)

	// Derive the cluster's Hive Metastore attachment endpoint, if any. The
	// reference was validated when the cluster was created; here it is only
	// formatted, so the job inherits the attachment fixed at cluster creation
	// (matching real Dataproc, which does not re-check the metastore per job).
	metastoreEndpoint, err := s.clusterMetastoreEndpoint(project, region, cluster)
	if err != nil {
		return dpstore.Job{}, err
	}

	// Fail-loud on unsupported job types — never silently succeed.
	if unsupportedJobTypes[j.Type] {
		now := clock.Now().UTC()
		j.Status = dpstore.JobStatus{
			State:          jobStateError,
			Details:        "job type " + j.Type + " is not supported by the emulator",
			StateStartTime: now,
		}
		if err := s.store.CreateJob(ctx, project, region, j); err != nil {
			return dpstore.Job{}, mapCreateJobErr(err)
		}
		s.emitJobStateChange(ctx, project, region, j, dpstore.JobStatus{})
		s.materializeDriverOutput(ctx, j, nil)
		return j, nil
	}

	// Validate the entry point before storing (malformed Spark job → ERROR).
	if _, _, err := jobToEntryPoint(j.Type, mustJSONMap(j.TypeJob)); err != nil {
		now := clock.Now().UTC()
		j.Status = dpstore.JobStatus{State: jobStateError, Details: err.Error(), StateStartTime: now}
		if createErr := s.store.CreateJob(ctx, project, region, j); createErr != nil {
			return dpstore.Job{}, mapCreateJobErr(createErr)
		}
		s.emitJobStateChange(ctx, project, region, j, dpstore.JobStatus{})
		s.materializeDriverOutput(ctx, j, nil)
		return j, nil
	}

	if err := s.store.CreateJob(ctx, project, region, j); err != nil {
		return dpstore.Job{}, mapCreateJobErr(err)
	}
	s.emitJobStateChange(ctx, project, region, j, dpstore.JobStatus{})

	// Mock mode: the job stays PENDING and advances through SETUP_DONE /
	// RUNNING / DONE lazily on reads (advanceJob), so pollers observe the state
	// machine. A real executor runs the job and lets the driver signals drive it.
	if s.mockMode() {
		return j, nil
	}

	// Register the cancel func before launching the executor. submitJob used to
	// return as soon as the goroutine was spawned, but runJob registered its
	// cancel func only a moment later; a CancelJob landing in that window moved
	// the store to CANCEL_PENDING but found no cancel func, so the driver
	// started and kept running. Registering here closes that window.
	s.wg.Add(1)
	runCtx, runCancel := context.WithCancel(s.ctx)
	key := cancelKey(project, region, j.JobID)
	s.registerCancel(key, runCancel)
	go func() {
		defer s.wg.Done()
		defer runCancel()
		defer s.unregisterCancel(key)
		s.runJobWithCtx(runCtx, project, region, j, metastoreEndpoint, cluster.Namespace)
	}()
	return j, nil
}

// mapCreateJobErr translates store.CreateJob's sentinel errors into the
// matching wire error. Used by every CreateJob call site in submitJob so a
// duplicate client-specified jobId always surfaces as 409 AlreadyExists,
// regardless of which job-type branch triggered the create.
func mapCreateJobErr(err error) error {
	if errors.Is(err, dpstore.ErrAlreadyExists) {
		return model.NewProviderError("AlreadyExists", "job already exists", 409)
	}
	return err
}

// SubmitJobAsOperation wraps SubmitJob in a long-running operation.
func (s *Service) SubmitJobAsOperation(ctx context.Context, project, region string, in JobInput) (dpstore.Operation, error) {
	if region == "" {
		return dpstore.Operation{}, invalidArgument("missing region")
	}
	j, err := s.submitJob(ctx, project, region, in)
	if err != nil {
		return dpstore.Operation{}, err
	}
	target := JobName(project, region, j.JobID)
	now := clock.Now().UTC()
	terminal := jobTerminal(j.Status.State)
	op := dpstore.Operation{
		ID:         j.JobID,
		ProjectID:  project,
		Region:     region,
		Done:       terminal,
		Verb:       "submit",
		Target:     target,
		CreateTime: now,
	}
	if terminal {
		op.EndTime = now
	}
	if meta, err := json.Marshal(jobOperationMetadata(j.JobID, "SUBMIT", j.Status, now)); err == nil {
		op.Metadata = string(meta)
	}
	if terminal {
		if resp, err := json.Marshal(JobJSON(j)); err == nil {
			op.Response = string(resp)
		}
	}
	if err := s.store.CreateOperation(ctx, project, region, op); err != nil {
		return dpstore.Operation{}, err
	}
	return op, nil
}

// GetJob returns one job, lazily settling a transitional state first.
func (s *Service) GetJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error) {
	if region == "" || jobID == "" {
		return dpstore.Job{}, invalidArgument("missing region or jobId")
	}
	j, err := s.advanceJob(ctx, project, region, jobID)
	if err != nil {
		return dpstore.Job{}, mapErr(err)
	}
	return j, nil
}

// ListJobs returns a cursor page of the jobs in a region, settling each
// transitional job first so a poller sees it progress. clusterName narrows the
// result to jobs submitted to a cluster; filter is the bounded
// dataproc.v1.ListJobsRequest.filter grammar and, when non-empty, overrides
// matcher (the API's "If filter is provided, jobStateMatcher will be ignored"
// rule). Filtering is applied before pagination so pageToken cursors stay
// stable; paging.Page keeps the page sorted by job id. A malformed filter is
// InvalidArgument.
func (s *Service) ListJobs(ctx context.Context, project, region, clusterName, filter string, matcher JobStateMatcher, pageSize int, pageToken string) ([]dpstore.Job, string, error) {
	if region == "" {
		return nil, "", invalidArgument("missing region")
	}
	filter = strings.TrimSpace(filter)
	var compiled jobFilter
	if filter != "" {
		var err error
		if compiled, err = compileJobFilter(filter); err != nil {
			return nil, "", err
		}
	}

	jobs, err := s.store.ListJobs(ctx, project, region)
	if err != nil {
		return nil, "", err
	}
	filtered := make([]dpstore.Job, 0, len(jobs))
	for _, j := range jobs {
		advanced, advErr := s.advanceJob(ctx, project, region, j.JobID)
		if errors.Is(advErr, dpstore.ErrNoSuchJob) {
			continue
		}
		if advErr != nil {
			return nil, "", mapErr(advErr)
		}
		if clusterName != "" && advanced.PlacementClusterName != clusterName {
			continue
		}
		if filter != "" {
			if !compiled.match(advanced) {
				continue
			}
		} else if !matchJobStateMatcher(advanced, matcher) {
			continue
		}
		filtered = append(filtered, advanced)
	}
	page, next := paging.Page(filtered, func(j dpstore.Job) string { return j.JobID }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// ListAllJobs lists every job in a project across all regions, sorted by region
// then job id, settling each transitional job first. It backs the
// region-optional console list.
func (s *Service) ListAllJobs(ctx context.Context, project string) ([]dpstore.Job, error) {
	jobs, err := s.store.ListJobsByProject(ctx, project)
	if err != nil {
		return nil, err
	}
	settled := make([]dpstore.Job, 0, len(jobs))
	for _, j := range jobs {
		advanced, advErr := s.advanceJob(ctx, j.ProjectID, j.Region, j.JobID)
		if errors.Is(advErr, dpstore.ErrNoSuchJob) {
			continue
		}
		if advErr != nil {
			return nil, mapErr(advErr)
		}
		settled = append(settled, advanced)
	}
	return settled, nil
}

// UpdateJob applies the masked fields of a job update. Dataproc jobs are
// immutable except for labels.
func (s *Service) UpdateJob(ctx context.Context, project, region, jobID string, in JobInput, mask []string) (dpstore.Job, error) {
	if region == "" || jobID == "" {
		return dpstore.Job{}, invalidArgument("missing region or jobId")
	}
	applyLabels := len(mask) == 0 || containsMaskField(mask, "labels")
	j, err := s.store.UpdateJobAtomic(ctx, project, region, jobID, func(j dpstore.Job) (dpstore.Job, error) {
		if applyLabels && in.Labels != nil {
			j.Labels = in.Labels
		}
		return j, nil
	})
	if err != nil {
		return dpstore.Job{}, mapErr(err)
	}
	return j, nil
}

// DeleteJob removes a terminal job. An active job cannot be deleted
// (FAILED_PRECONDITION).
func (s *Service) DeleteJob(ctx context.Context, project, region, jobID string) error {
	if region == "" || jobID == "" {
		return invalidArgument("missing region or jobId")
	}
	j, err := s.store.GetJob(ctx, project, region, jobID)
	if err != nil {
		return mapErr(err)
	}
	if !jobTerminal(j.Status.State) {
		return &model.ProviderError{
			Code:       "FailedPrecondition",
			Message:    "job is active and cannot be deleted",
			HTTPStatus: 400,
			Status:     "FAILED_PRECONDITION",
		}
	}
	if err := s.store.DeleteJob(ctx, project, region, jobID); err != nil {
		return mapErr(err)
	}
	return nil
}

// CancelJob starts the cancel progression for a non-terminal job: it moves the
// job to CANCEL_PENDING and stops the executor, and later reads settle it
// CANCEL_STARTED -> CANCELLED (closing its SubmitJobAsOperation LRO). A job
// that is already terminal, or already cancelling, is a no-op.
func (s *Service) CancelJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error) {
	if region == "" || jobID == "" {
		return dpstore.Job{}, invalidArgument("missing region or jobId")
	}
	now := clock.Now().UTC()
	var transitioned bool
	var prev dpstore.JobStatus
	j, err := s.store.UpdateJobAtomic(ctx, project, region, jobID, func(j dpstore.Job) (dpstore.Job, error) {
		if jobTerminal(j.Status.State) || j.Status.State == jobStateCancelPending || j.Status.State == jobStateCancelStarted {
			return j, nil // already terminal / already cancelling — nothing to do
		}
		transitioned = true
		prev = j.Status
		j.StatusHistory = append(j.StatusHistory, j.Status)
		j.Status = dpstore.JobStatus{State: jobStateCancelPending, StateStartTime: now}
		return j, nil
	})
	if err != nil {
		return dpstore.Job{}, mapErr(err)
	}
	if !transitioned {
		return j, nil
	}
	s.emitJobStateChange(ctx, project, region, j, prev)

	// Stop the executor (k8s mode). The cancel progression itself is lazy and
	// settles on later reads / the operation poll, so finishJob cannot resurrect
	// the job: it sees CANCEL_PENDING and lands on CANCELLED instead.
	s.cancelsMu.Lock()
	cancel, ok := s.cancels[cancelKey(project, region, jobID)]
	s.cancelsMu.Unlock()
	if ok {
		cancel()
	}
	return j, nil
}

// GetOperation returns a persisted long-running operation. Polling is what
// drives the lazy state machine: a cluster mutation advances the cluster and a
// submit advances the job, then the operation is finalized with its response.
func (s *Service) GetOperation(ctx context.Context, project, region, opID string) (dpstore.Operation, error) {
	if region == "" || opID == "" {
		return dpstore.Operation{}, invalidArgument("missing region or operationId")
	}
	op, err := s.store.GetOperation(ctx, project, region, opID)
	if err != nil {
		return dpstore.Operation{}, mapErr(err)
	}
	// A submit operation is advanced by the job state machine (its id is the
	// job id); a cluster mutation by the cluster state machine.
	if !op.Done && op.Verb == "submit" {
		advanced, advErr := s.advanceSubmitOperation(ctx, op)
		if advErr != nil {
			return dpstore.Operation{}, mapErr(advErr)
		}
		return advanced, nil
	}
	// A workflow operation drives its inline/stored DAG through the job core.
	if op.Verb == "workflow" {
		advanced, advErr := s.advanceWorkflowOperation(ctx, op)
		if advErr != nil {
			return dpstore.Operation{}, mapErr(advErr)
		}
		return advanced, nil
	}
	if !op.Done {
		advanced, advErr := s.advanceClusterOperation(ctx, op)
		if advErr != nil {
			return dpstore.Operation{}, mapErr(advErr)
		}
		return advanced, nil
	}
	return op, nil
}

// advanceSubmitOperation advances the job backing a SubmitJobAsOperation LRO
// and reflects it on the operation: while the job is non-terminal the polled
// JobMetadata.status tracks the job, and once terminal the operation is closed
// with the job as its response.
func (s *Service) advanceSubmitOperation(ctx context.Context, op dpstore.Operation) (dpstore.Operation, error) {
	j, err := s.advanceJob(ctx, op.ProjectID, op.Region, op.ID)
	if err != nil {
		return op, err
	}
	if jobTerminal(j.Status.State) {
		s.completeSubmitOperation(ctx, op.ProjectID, op.Region, j)
		if done, gerr := s.store.GetOperation(ctx, op.ProjectID, op.Region, op.ID); gerr == nil {
			return done, nil
		}
		return op, nil
	}
	// Refresh the polled metadata atomically: a concurrent poll may have
	// completed the operation, and a blind overwrite of a stale snapshot would
	// resurrect it.
	refreshed, uerr := s.store.UpdateOperationAtomic(ctx, op.ProjectID, op.Region, op.ID, func(cur dpstore.Operation) (dpstore.Operation, error) {
		if cur.Done {
			return cur, nil
		}
		cur.Metadata = refreshJobOperationMetadata(cur.Metadata, j.Status)
		return cur, nil
	})
	if uerr != nil {
		if errors.Is(uerr, dpstore.ErrNoSuchOperation) {
			return op, nil
		}
		return op, uerr
	}
	return refreshed, nil
}

// finishJob writes the terminal state back to the jobs store. The get-check-
// set cycle is atomic (UpdateJobAtomic) so it can't race with a concurrent
// CancelJob: whichever of the two acquires the lock first wins the terminal-
// state transition, and the other sees the already-terminal state inside its
// own mutate and no-ops instead of overwriting it.
func (s *Service) finishJob(project, region string, j dpstore.Job, state, details string, driverOutput []byte) {
	now := clock.Now().UTC()
	var transitioned bool
	var prev dpstore.JobStatus
	fresh, err := s.store.UpdateJobAtomic(context.Background(), project, region, j.JobID, func(fresh dpstore.Job) (dpstore.Job, error) {
		if jobTerminal(fresh.Status.State) {
			return fresh, nil // already terminal (e.g. cancelled) — first write wins
		}
		transitioned = true
		prev = fresh.Status
		fresh.StatusHistory = append(fresh.StatusHistory, fresh.Status)
		if fresh.Status.State == jobStateCancelPending || fresh.Status.State == jobStateCancelStarted {
			// A cancel request won the race: the job lands on CANCELLED, not
			// the executor's own result.
			fresh.Status = dpstore.JobStatus{State: jobStateCancelled, StateStartTime: now}
			return fresh, nil
		}
		fresh.Status = dpstore.JobStatus{State: state, Details: details, StateStartTime: now, Substate: jobSubstateFor(state)}
		ensureDriverOutputURIs(project, region, &fresh)
		return fresh, nil
	})
	if err != nil {
		slog.Warn("dataproc: finishJob update failed", "job", j.JobID, "err", err)
		return
	}
	if !transitioned {
		return
	}
	s.emitJobStateChange(context.Background(), project, region, fresh, prev)
	s.materializeDriverOutput(context.Background(), fresh, driverOutput)
	s.completeSubmitOperation(context.Background(), project, region, fresh)
}

// completeSubmitOperation flips the SubmitJobAsOperation long-running operation
// (id == job id) to done=true with the terminal job as its response. No-op when
// the job was submitted without an operation (plain SubmitJob) or the operation
// is already terminal.
func (s *Service) completeSubmitOperation(ctx context.Context, project, region string, j dpstore.Job) {
	_, err := s.store.UpdateOperationAtomic(ctx, project, region, j.JobID, func(cur dpstore.Operation) (dpstore.Operation, error) {
		if cur.Done {
			return cur, nil
		}
		resp, _ := json.Marshal(JobJSON(j))
		cur.Done = true
		cur.Response = string(resp)
		cur.EndTime = clock.Now().UTC()
		if meta, mErr := json.Marshal(jobOperationMetadata(j.JobID, "SUBMIT", j.Status, cur.CreateTime)); mErr == nil {
			cur.Metadata = string(meta)
		}
		return cur, nil
	})
	// No operation exists for a plain SubmitJob (only SubmitJobAsOperation
	// persists one); that is not an error.
	if err != nil && !errors.Is(err, dpstore.ErrNoSuchOperation) {
		slog.Warn("dataproc: completeSubmitOperation failed", "job", j.JobID, "err", err)
	}
}
