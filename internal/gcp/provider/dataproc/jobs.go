package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// submitJobFromBody extracts the nested "job" object from a SubmitJobRequest
// body and dispatches it. Returns the created job wire map.
func (p *Provider) submitJob(ctx context.Context, nr *model.NormalizedRequest) (map[string]any, error) {
	region := strParam(nr, "region")
	if region == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	jobBody, _ := body["job"].(map[string]any)
	if jobBody == nil {
		return nil, model.NewProviderError("InvalidArgument", "missing job", 400)
	}

	// placement.clusterName is REQUIRED (dataproc.v1.JobPlacement). Validate it
	// before the cluster existence check.
	placement, _ := jobBody["placement"].(map[string]any)
	if placement == nil {
		return nil, model.NewProviderError("InvalidArgument", "missing placement", 400)
	}
	if bodyString(placement, "clusterName") == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing placement.clusterName", 400)
	}

	j, err := jobToStore(nr, jobBody, region)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", err.Error(), 400)
	}

	if j.PlacementClusterName != "" {
		if _, err := p.store.GetCluster(ctx, nr.AccountID, region, j.PlacementClusterName); err != nil {
			return nil, model.NewProviderError("NotFound", "cluster not found: "+j.PlacementClusterName, 404)
		}
	}

	// Fail-loud on unsupported job types — never silently succeed.
	if unsupportedJobTypes[j.Type] {
		now := clock.Now().UTC()
		j.Status = dataprocstore.JobStatus{
			State:          "ERROR",
			Details:        "job type " + j.Type + " is not supported by the emulator",
			StateStartTime: now,
		}
		if err := p.store.CreateJob(ctx, nr.AccountID, region, j); err != nil {
			return nil, mapCreateJobErr(err)
		}
		return jobToMap(j), nil
	}

	// Validate the entry point before storing (malformed Spark job → ERROR).
	if _, _, err := jobToEntryPoint(j.Type, mustJSONMap(j.TypeJob)); err != nil {
		now := clock.Now().UTC()
		j.Status = dataprocstore.JobStatus{State: "ERROR", Details: err.Error(), StateStartTime: now}
		if createErr := p.store.CreateJob(ctx, nr.AccountID, region, j); createErr != nil {
			return nil, mapCreateJobErr(createErr)
		}
		return jobToMap(j), nil
	}

	if err := p.store.CreateJob(ctx, nr.AccountID, region, j); err != nil {
		return nil, mapCreateJobErr(err)
	}

	// Mock mode: complete synchronously (no goroutine). K8s mode: run for real.
	if p.k8sClient == nil {
		now := clock.Now().UTC()
		j.Status = dataprocstore.JobStatus{State: "DONE", StateStartTime: now}
		j.StatusHistory = append(j.StatusHistory, dataprocstore.JobStatus{State: "RUNNING", StateStartTime: j.CreateTime})
		j.DriverOutputResourceURI = "gs://jaiscloud-dataproc/" + j.JobUUID + "/driveroutput"
		if err := p.store.UpdateJob(ctx, nr.AccountID, region, j); err != nil {
			slog.Warn("dataproc: mock SubmitJob update failed", "job", j.JobID, "err", err)
		}
		return jobToMap(j), nil
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.runJob(p.ctx, nr.AccountID, region, j)
	}()
	return jobToMap(j), nil
}

// mapCreateJobErr translates store.CreateJob's sentinel errors into the
// matching wire error. Used by every CreateJob call site in submitJob so a
// duplicate client-specified jobId always surfaces as 409 AlreadyExists,
// regardless of which job-type branch triggered the create.
func mapCreateJobErr(err error) error {
	if errors.Is(err, dataprocstore.ErrAlreadyExists) {
		return model.NewProviderError("AlreadyExists", "job already exists", 409)
	}
	return err
}

func (p *Provider) SubmitJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	j, err := p.submitJob(ctx, nr)
	if err != nil {
		return nil, err
	}
	return provider.OK(j), nil
}

// SubmitJobAsOperation wraps SubmitJob in a long-running operation.
func (p *Provider) SubmitJobAsOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	region := strParam(nr, "region")
	if region == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region", 400)
	}
	j, err := p.submitJob(ctx, nr)
	if err != nil {
		return nil, err
	}
	jobID, _ := j["reference"].(map[string]any)["jobId"].(string)
	state, _ := j["status"].(map[string]any)["state"].(string)
	target := nr.ResourceID("dataproc-job", region+"/"+jobID)
	now := clock.Now().UTC()
	op := dataprocstore.Operation{
		ID:         jobID,
		ProjectID:  nr.AccountID,
		Region:     region,
		Done:       jobTerminal(state),
		Verb:       "submit",
		Target:     target,
		CreateTime: now,
		EndTime:    now,
	}
	if meta, err := json.Marshal(jobOperationMetadata(jobID, state, "SUBMIT", now)); err == nil {
		op.Metadata = string(meta)
	}
	if op.Done {
		if resp, err := json.Marshal(j); err == nil {
			op.Response = string(resp)
		}
	}
	if err := p.store.CreateOperation(ctx, nr.AccountID, region, op); err != nil {
		return nil, err
	}
	return provider.OK(p.operationMap(nr, op)), nil
}

func (p *Provider) GetJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	region := strParam(nr, "region")
	jobID := strParam(nr, "jobId")
	if region == "" || jobID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region or jobId", 400)
	}
	j, err := p.store.GetJob(ctx, nr.AccountID, region, jobID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(jobToMap(j)), nil
}

func (p *Provider) ListJobs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	region := strParam(nr, "region")
	if region == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region", 400)
	}
	jobs, err := p.store.ListJobs(ctx, nr.AccountID, region)
	if err != nil {
		return nil, err
	}
	// clusterName and filter are accepted but ignored (documented limitation).
	page, next := paging.Page(jobs, func(j dataprocstore.Job) string { return j.JobID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, j := range page {
		items = append(items, jobToMap(j))
	}
	resp := map[string]any{"jobs": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) DeleteJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	region := strParam(nr, "region")
	jobID := strParam(nr, "jobId")
	if region == "" || jobID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region or jobId", 400)
	}
	j, err := p.store.GetJob(ctx, nr.AccountID, region, jobID)
	if err != nil {
		return nil, mapErr(err)
	}
	// Active jobs cannot be deleted (FAILED_PRECONDITION).
	if !jobTerminal(j.Status.State) {
		return nil, &model.ProviderError{
			Code:       "FailedPrecondition",
			Message:    "job is active and cannot be deleted",
			HTTPStatus: 400,
			Status:     "FAILED_PRECONDITION",
		}
	}
	if err := p.store.DeleteJob(ctx, nr.AccountID, region, jobID); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) CancelJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	region := strParam(nr, "region")
	jobID := strParam(nr, "jobId")
	if region == "" || jobID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region or jobId", 400)
	}
	now := clock.Now().UTC()
	var transitioned bool
	j, err := p.store.UpdateJobAtomic(ctx, nr.AccountID, region, jobID, func(j dataprocstore.Job) (dataprocstore.Job, error) {
		if jobTerminal(j.Status.State) {
			return j, nil // already terminal — nothing to cancel
		}
		transitioned = true
		j.StatusHistory = append(j.StatusHistory, j.Status)
		j.Status = dataprocstore.JobStatus{State: "CANCELLED", StateStartTime: now}
		return j, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	if !transitioned {
		return provider.OK(jobToMap(j)), nil
	}

	p.cancelsMu.Lock()
	cancel, ok := p.cancels[cancelKey(nr.AccountID, region, jobID)]
	p.cancelsMu.Unlock()
	if ok {
		cancel()
	}
	// Close out the SubmitJobAsOperation LRO for this transition — finishJob
	// won't run it (or will see the job already terminal and no-op) once
	// CancelJob has won the race for the terminal-state transition.
	p.completeSubmitOperation(nr.AccountID, region, j)
	return provider.OK(jobToMap(j)), nil
}

func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	region := strParam(nr, "region")
	opID := strParam(nr, "operationId")
	if region == "" || opID == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing region or operationId", 400)
	}
	op, err := p.store.GetOperation(ctx, nr.AccountID, region, opID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.operationMap(nr, op)), nil
}

// mustJSONMap unmarshals raw JSON bytes into a map, returning nil on failure.
func mustJSONMap(b []byte) map[string]any {
	if len(b) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	return m
}
