package ui

import (
	"context"
	"encoding/json"
	"strings"

	dataproccore "jaiscloud/internal/gcp/service/dataproc"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	dataprocui "jaiscloud/internal/gcp/ui/dataproc"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// typedDataproc serves the Dataproc console through the plugin chain when a plugin
// covers the key (see typed.go). The REST shape carries no region on a cluster, job
// or template (it is part of the request path), so a project-wide list shows the
// region only when the answer includes it (the built-in's answer does, as "region").
type typedDataproc struct {
	dataprocui.ProviderInterface
	c typedCaller
}

func routeDataproc(c typedCaller, p dataprocui.ProviderInterface) dataprocui.ProviderInterface {
	return typedDataproc{ProviderInterface: p, c: c}
}

func rawField(m map[string]any, field string) json.RawMessage {
	v, ok := m[field]
	if !ok {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

func decodeInto(m map[string]any, v any) {
	if b, err := json.Marshal(m); err == nil {
		_ = json.Unmarshal(b, v)
	}
}

// regionOf prefers an explicit region in the answer, then the region of the
// cluster's zone, then the region the call was made for.
func regionOf(m map[string]any, fallback string) string {
	if r := strField(m, "region"); r != "" {
		return r
	}
	if cfg, ok := m["config"].(map[string]any); ok {
		if g, ok := cfg["gceClusterConfig"].(map[string]any); ok {
			zone := strField(g, "zoneUri")
			if i := strings.LastIndex(zone, "/"); i >= 0 {
				zone = zone[i+1:]
			}
			if j := strings.LastIndex(zone, "-"); j > 0 {
				return zone[:j]
			}
		}
	}
	return fallback
}

func decodeCluster(m map[string]any, region string) dpstore.Cluster {
	var c dpstore.Cluster
	decodeInto(m, &c)
	c.Name = strField(m, "clusterName")
	c.ProjectID = strField(m, "projectId")
	c.Region = regionOf(m, region)
	c.Config = rawField(m, "config")
	c.VirtualClusterConfig = rawField(m, "virtualClusterConfig")
	return c
}

var dataprocJobTypes = []string{"hadoopJob", "sparkJob", "pysparkJob", "hiveJob", "pigJob", "sparkRJob", "sparkSqlJob", "prestoJob", "trinoJob", "flinkJob"}

func decodeDataprocJob(m map[string]any, region string) dpstore.Job {
	var j dpstore.Job
	decodeInto(m, &j)
	if ref, ok := m["reference"].(map[string]any); ok {
		j.ProjectID, j.JobID = strField(ref, "projectId"), strField(ref, "jobId")
	}
	if pl, ok := m["placement"].(map[string]any); ok {
		j.PlacementClusterName, j.PlacementClusterUUID = strField(pl, "clusterName"), strField(pl, "clusterUuid")
	}
	j.Region = regionOf(m, region)
	for _, t := range dataprocJobTypes {
		if _, ok := m[t]; ok {
			j.Type, j.TypeJob = t, rawField(m, t)
			break
		}
	}
	return j
}

func decodeTemplate(m map[string]any, region string) dpstore.WorkflowTemplate {
	t := dpstore.WorkflowTemplate{
		ProjectID: nameParts(strField(m, "name"))["projects"], Region: regionOf(m, region),
		TemplateID: strField(m, "id"), CreateTime: timeField(m, "createTime"), UpdateTime: timeField(m, "updateTime"),
	}
	if loc := nameParts(strField(m, "name"))["regions"]; loc != "" {
		t.Region = loc
	}
	if v, ok := m["version"].(float64); ok {
		t.Version = int32(v)
	}
	def := map[string]any{}
	for k, v := range m {
		switch k {
		case "id", "name", "version", "createTime", "updateTime", "region":
		default:
			def[k] = v
		}
	}
	t.Definition, _ = json.Marshal(def)
	return t
}

// operationCluster returns the cluster a long-running cluster operation carries; an
// operation still in flight carries none, so the cluster is read back.
func (p typedDataproc) operationCluster(ctx context.Context, project, region, name string, data map[string]any) (dpstore.Cluster, error) {
	if r, ok := data["response"].(map[string]any); ok {
		return decodeCluster(r, region), nil
	}
	return p.GetCluster(ctx, project, region, name)
}

func (p typedDataproc) ListAllClusters(ctx context.Context, project string) ([]dpstore.Cluster, error) {
	all := func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		cs, err := p.ProviderInterface.ListAllClusters(ctx, project)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, len(cs))
		for _, c := range cs {
			m := dataproccore.ClusterJSON(c)
			m["region"] = c.Region
			out = append(out, m)
		}
		return provider.OK(map[string]any{"clusters": out}), nil
	}
	rows, covered, err := p.c.doList(ctx, "Dataproc.ListClusters", project, "", map[string]any{"project": project, "region": "-"}, all, "clusters")
	if !covered {
		return p.ProviderInterface.ListAllClusters(ctx, project)
	}
	if err != nil {
		return nil, err
	}
	var out []dpstore.Cluster
	for _, m := range rows {
		out = append(out, decodeCluster(m, ""))
	}
	return out, nil
}

func (p typedDataproc) GetCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, error) {
	data, covered, err := p.c.do(ctx, "Dataproc.GetCluster", project, region,
		map[string]any{"project": project, "region": region, "clusterName": name}, nil)
	if !covered {
		return p.ProviderInterface.GetCluster(ctx, project, region, name)
	}
	if err != nil {
		return dpstore.Cluster{}, err
	}
	return decodeCluster(data, region), nil
}

func (p typedDataproc) CreateCluster(ctx context.Context, project, region, name string, in dataproccore.ClusterInput) (dpstore.Cluster, error) {
	body := map[string]any{"clusterName": name}
	if in.Labels != nil {
		body["labels"] = in.Labels
	}
	if len(in.Config) > 0 {
		body["config"] = bodyOfConfig(in.Config)
	}
	if len(in.VirtualClusterConfig) > 0 {
		body["virtualClusterConfig"] = bodyOfConfig(in.VirtualClusterConfig)
	}
	data, covered, err := p.c.do(ctx, "Dataproc.CreateCluster", project, region,
		map[string]any{"project": project, "region": region, "body": body}, nil)
	if !covered {
		return p.ProviderInterface.CreateCluster(ctx, project, region, name, in)
	}
	if err != nil {
		return dpstore.Cluster{}, err
	}
	return p.operationCluster(ctx, project, region, name, data)
}

func (p typedDataproc) lifecycle(ctx context.Context, key, project, region, name string, direct func() (dpstore.Cluster, error)) (dpstore.Cluster, error) {
	data, covered, err := p.c.do(ctx, key, project, region, map[string]any{"project": project, "region": region, "clusterName": name}, nil)
	if !covered {
		return direct()
	}
	if err != nil {
		return dpstore.Cluster{}, err
	}
	return p.operationCluster(ctx, project, region, name, data)
}

func (p typedDataproc) StartCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, error) {
	return p.lifecycle(ctx, "Dataproc.StartCluster", project, region, name, func() (dpstore.Cluster, error) {
		return p.ProviderInterface.StartCluster(ctx, project, region, name)
	})
}

func (p typedDataproc) StopCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, error) {
	return p.lifecycle(ctx, "Dataproc.StopCluster", project, region, name, func() (dpstore.Cluster, error) {
		return p.ProviderInterface.StopCluster(ctx, project, region, name)
	})
}

func (p typedDataproc) DeleteCluster(ctx context.Context, project, region, name string) error {
	_, covered, err := p.c.do(ctx, "Dataproc.DeleteCluster", project, region, map[string]any{"project": project, "region": region, "clusterName": name}, nil)
	if !covered {
		return p.ProviderInterface.DeleteCluster(ctx, project, region, name)
	}
	return err
}

func (p typedDataproc) ListAllJobs(ctx context.Context, project string) ([]dpstore.Job, error) {
	all := func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		js, err := p.ProviderInterface.ListAllJobs(ctx, project)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, len(js))
		for _, j := range js {
			m := dataproccore.JobJSON(j)
			m["region"] = j.Region
			out = append(out, m)
		}
		return provider.OK(map[string]any{"jobs": out}), nil
	}
	rows, covered, err := p.c.doList(ctx, "Dataproc.ListJobs", project, "", map[string]any{"project": project, "region": "-"}, all, "jobs")
	if !covered {
		return p.ProviderInterface.ListAllJobs(ctx, project)
	}
	if err != nil {
		return nil, err
	}
	var out []dpstore.Job
	for _, m := range rows {
		out = append(out, decodeDataprocJob(m, ""))
	}
	return out, nil
}

func (p typedDataproc) job(ctx context.Context, key, project, region, id string, direct func() (dpstore.Job, error)) (dpstore.Job, error) {
	data, covered, err := p.c.do(ctx, key, project, region, map[string]any{"project": project, "region": region, "jobId": id}, nil)
	if !covered {
		return direct()
	}
	if err != nil {
		return dpstore.Job{}, err
	}
	return decodeDataprocJob(data, region), nil
}

func (p typedDataproc) GetJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error) {
	return p.job(ctx, "Dataproc.GetJob", project, region, jobID, func() (dpstore.Job, error) {
		return p.ProviderInterface.GetJob(ctx, project, region, jobID)
	})
}

func (p typedDataproc) CancelJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error) {
	return p.job(ctx, "Dataproc.CancelJob", project, region, jobID, func() (dpstore.Job, error) {
		return p.ProviderInterface.CancelJob(ctx, project, region, jobID)
	})
}

func (p typedDataproc) ListAllWorkflowTemplates(ctx context.Context, project string) ([]dpstore.WorkflowTemplate, error) {
	all := func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		ts, err := p.ProviderInterface.ListAllWorkflowTemplates(ctx, project)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, len(ts))
		for _, t := range ts {
			out = append(out, dataproccore.WorkflowTemplateJSON(t))
		}
		return provider.OK(map[string]any{"templates": out}), nil
	}
	rows, covered, err := p.c.doList(ctx, "Dataproc.ListWorkflowTemplates", project, "", map[string]any{"project": project, "region": "-"}, all, "templates")
	if !covered {
		return p.ProviderInterface.ListAllWorkflowTemplates(ctx, project)
	}
	if err != nil {
		return nil, err
	}
	var out []dpstore.WorkflowTemplate
	for _, m := range rows {
		out = append(out, decodeTemplate(m, ""))
	}
	return out, nil
}

func (p typedDataproc) GetWorkflowTemplate(ctx context.Context, project, region, id string) (dpstore.WorkflowTemplate, error) {
	data, covered, err := p.c.do(ctx, "Dataproc.GetWorkflowTemplate", project, region,
		map[string]any{"project": project, "region": region, "workflowTemplateId": id}, nil)
	if !covered {
		return p.ProviderInterface.GetWorkflowTemplate(ctx, project, region, id)
	}
	if err != nil {
		return dpstore.WorkflowTemplate{}, err
	}
	return decodeTemplate(data, region), nil
}
