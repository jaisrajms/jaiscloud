package scheduler

import (
	"context"
	"strconv"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	core "jaiscloud/internal/gcp/service/scheduler"
)

// Provider handles the Cloud Scheduler v1 REST data plane. It is a thin
// adapter: every handler resolves the NormalizedRequest params into the core's
// typed API, calls the shared core Service, and encodes the result as
// Discovery-shaped JSON.
type Provider struct {
	core *core.Service
}

// NewProvider returns a Cloud Scheduler REST provider over the shared core.
func NewProvider(c *core.Service) *Provider { return &Provider{core: c} }

// Routes maps "Scheduler.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Scheduler.JobsList":   p.ListJobs,
		"Scheduler.JobsCreate": p.CreateJob,
		"Scheduler.JobsGet":    p.GetJob,
		"Scheduler.JobsPatch":  p.UpdateJob,
		"Scheduler.JobsDelete": p.DeleteJob,
		"Scheduler.JobsPause":  p.PauseJob,
		"Scheduler.JobsResume": p.ResumeJob,
		"Scheduler.JobsRun":    p.RunJob,
	}
}

// Reset delegates to the core so /_jaiscloud/reset clears scheduler state.
func (p *Provider) Reset(ctx context.Context) { p.core.Reset(ctx) }

func (p *Provider) ListJobs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	jobs, err := p.core.ListJobs(ctx, strParam(nr, "project"), strParam(nr, "location"))
	if err != nil {
		return nil, err
	}
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
	if offset > len(jobs) {
		offset = len(jobs)
	}
	end := offset + pageSize
	if end > len(jobs) {
		end = len(jobs)
	}
	items := make([]any, 0, end-offset)
	for _, j := range jobs[offset:end] {
		items = append(items, jobToJSON(j))
	}
	resp := map[string]any{"jobs": items}
	if end < len(jobs) {
		resp["nextPageToken"] = strconv.Itoa(end)
	}
	return provider.OK(resp), nil
}

func (p *Provider) CreateJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	job, err := jobFromBody(bodyOf(nr))
	if err != nil {
		return nil, err
	}
	created, err := p.core.CreateJob(ctx, strParam(nr, "project"), strParam(nr, "location"), job)
	if err != nil {
		return nil, err
	}
	return provider.OK(jobToJSON(created)), nil
}

func (p *Provider) GetJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	j, err := p.core.GetJob(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "job"))
	if err != nil {
		return nil, err
	}
	return provider.OK(jobToJSON(j)), nil
}

func (p *Provider) UpdateJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	upd, err := jobFromBody(bodyOf(nr))
	if err != nil {
		return nil, err
	}
	mask := splitMask(nr.Params["updateMask"])
	j, err := p.core.UpdateJob(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "job"), upd, mask)
	if err != nil {
		return nil, err
	}
	return provider.OK(jobToJSON(j)), nil
}

func (p *Provider) DeleteJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteJob(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "job")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) PauseJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	j, err := p.core.PauseJob(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "job"))
	if err != nil {
		return nil, err
	}
	return provider.OK(jobToJSON(j)), nil
}

func (p *Provider) ResumeJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	j, err := p.core.ResumeJob(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "job"))
	if err != nil {
		return nil, err
	}
	return provider.OK(jobToJSON(j)), nil
}

func (p *Provider) RunJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	j, err := p.core.RunJob(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "job"))
	if err != nil {
		return nil, err
	}
	return provider.OK(jobToJSON(j)), nil
}
