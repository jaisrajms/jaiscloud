package ui

import (
	"context"
	"strings"

	schedstore "jaiscloud/internal/gcp/store/scheduler"
	restscheduler "jaiscloud/internal/gcp/transport/rest/scheduler"
	schedulerui "jaiscloud/internal/gcp/ui/scheduler"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// typedScheduler serves the Cloud Scheduler console through the plugin chain when
// a plugin covers the key (see typed.go). Every method has the REST key it maps to.
type typedScheduler struct {
	schedulerui.ProviderInterface
	c typedCaller
}

func routeScheduler(c typedCaller, p schedulerui.ProviderInterface) schedulerui.ProviderInterface {
	return typedScheduler{ProviderInterface: p, c: c}
}

// decodeJob decodes a REST job and restores the project and location the body
// only carries in its resource name.
func decodeJob(m map[string]any) (schedstore.Job, error) {
	j, err := decodeTyped(m, restscheduler.JobFromBody)
	if err != nil {
		return j, err
	}
	if n, _ := m["name"].(string); n != "" {
		p := nameParts(n)
		j.ProjectID, j.Location = p["projects"], p["locations"]
	}
	return j, nil
}

func (p typedScheduler) ListJobsByProject(ctx context.Context, project string) ([]schedstore.Job, error) {
	// The REST list is per location; "-" is the API's all-locations wildcard, which
	// the built-in under the chain answers from the project-wide console list.
	all := func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		jobs, err := p.ProviderInterface.ListJobsByProject(ctx, project)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, len(jobs))
		for _, j := range jobs {
			out = append(out, restscheduler.JobToJSON(j))
		}
		return provider.OK(map[string]any{"jobs": out}), nil
	}
	rows, covered, err := p.c.doList(ctx, "Scheduler.JobsList", project, "", map[string]any{"project": project, "location": "-"}, all, "jobs")
	if !covered {
		return p.ProviderInterface.ListJobsByProject(ctx, project)
	}
	if err != nil {
		return nil, err
	}
	var jobs []schedstore.Job
	for _, m := range rows {
		j, err := decodeJob(m)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

func (p typedScheduler) one(ctx context.Context, key, project, location, name string, direct func() (schedstore.Job, error)) (schedstore.Job, error) {
	data, covered, err := p.c.do(ctx, key, project, location, map[string]any{"project": project, "location": location, "job": name}, nil)
	if !covered {
		return direct()
	}
	if err != nil {
		return schedstore.Job{}, err
	}
	return decodeJob(data)
}

func (p typedScheduler) GetJob(ctx context.Context, project, location, name string) (schedstore.Job, error) {
	return p.one(ctx, "Scheduler.JobsGet", project, location, name, func() (schedstore.Job, error) {
		return p.ProviderInterface.GetJob(ctx, project, location, name)
	})
}

func (p typedScheduler) PauseJob(ctx context.Context, project, location, name string) (schedstore.Job, error) {
	return p.one(ctx, "Scheduler.JobsPause", project, location, name, func() (schedstore.Job, error) {
		return p.ProviderInterface.PauseJob(ctx, project, location, name)
	})
}

func (p typedScheduler) ResumeJob(ctx context.Context, project, location, name string) (schedstore.Job, error) {
	return p.one(ctx, "Scheduler.JobsResume", project, location, name, func() (schedstore.Job, error) {
		return p.ProviderInterface.ResumeJob(ctx, project, location, name)
	})
}

func (p typedScheduler) RunJob(ctx context.Context, project, location, name string) (schedstore.Job, error) {
	return p.one(ctx, "Scheduler.JobsRun", project, location, name, func() (schedstore.Job, error) {
		return p.ProviderInterface.RunJob(ctx, project, location, name)
	})
}

func (p typedScheduler) DeleteJob(ctx context.Context, project, location, name string) error {
	_, covered, err := p.c.do(ctx, "Scheduler.JobsDelete", project, location, map[string]any{"project": project, "location": location, "job": name}, nil)
	if !covered {
		return p.ProviderInterface.DeleteJob(ctx, project, location, name)
	}
	return err
}

func (p typedScheduler) CreateJob(ctx context.Context, project, location string, j schedstore.Job) (schedstore.Job, error) {
	data, covered, err := p.c.do(ctx, "Scheduler.JobsCreate", project, location,
		map[string]any{"project": project, "location": location, "body": restscheduler.JobToJSON(j)}, nil)
	if !covered {
		return p.ProviderInterface.CreateJob(ctx, project, location, j)
	}
	if err != nil {
		return schedstore.Job{}, err
	}
	return decodeJob(data)
}

func (p typedScheduler) UpdateJob(ctx context.Context, project, location, name string, upd schedstore.Job, mask []string) (schedstore.Job, error) {
	upd.ProjectID, upd.Location, upd.Name = project, location, name // the body names the job it patches
	params := map[string]any{"project": project, "location": location, "job": name, "body": restscheduler.JobToJSON(upd)}
	if len(mask) > 0 {
		params["updateMask"] = strings.Join(mask, ",")
	}
	data, covered, err := p.c.do(ctx, "Scheduler.JobsPatch", project, location, params, nil)
	if !covered {
		return p.ProviderInterface.UpdateJob(ctx, project, location, name, upd, mask)
	}
	if err != nil {
		return schedstore.Job{}, err
	}
	return decodeJob(data)
}
