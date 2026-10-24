package dataproc

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/store"
)

func TestListAllClusters_AcrossRegionsSorted(t *testing.T) {
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore())
	ctx := context.Background()
	for _, c := range []struct{ project, region, name string }{
		{"proj", "us-central1", "b"},
		{"proj", "europe-west1", "a"},
		{"other", "us-central1", "c"},
	} {
		if _, _, err := p.CreateCluster(ctx, c.project, c.region, c.name, ClusterInput{}); err != nil {
			t.Fatalf("CreateCluster %s/%s/%s: %v", c.project, c.region, c.name, err)
		}
	}
	clusters, err := p.ListAllClusters(ctx, "proj")
	if err != nil {
		t.Fatalf("ListAllClusters: %v", err)
	}
	if len(clusters) != 2 {
		t.Fatalf("clusters = %d, want 2 (other project excluded)", len(clusters))
	}
	if clusters[0].Region != "europe-west1" || clusters[0].Name != "a" ||
		clusters[1].Region != "us-central1" || clusters[1].Name != "b" {
		t.Fatalf("clusters not ordered by region then name: %+v", clusters)
	}
	// CreateCluster leaves the cluster CREATING; the aggregated list must settle
	// it exactly like the per-region list does.
	for _, c := range clusters {
		if c.Status.State != "RUNNING" {
			t.Fatalf("cluster %s not settled: %s", c.Name, c.Status.State)
		}
	}
}

func TestListAllClusters_SettlesTransitional(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	p := newStateProvider(t, 30*time.Second)
	ctx := context.Background()

	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	clusters, err := p.ListAllClusters(ctx, "proj")
	if err != nil || len(clusters) != 1 {
		t.Fatalf("ListAllClusters: %v %d", err, len(clusters))
	}
	if clusters[0].Status.State != "CREATING" {
		t.Fatalf("state = %q, want CREATING before the delay elapses", clusters[0].Status.State)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(31 * time.Second)})
	clusters, err = p.ListAllClusters(ctx, "proj")
	if err != nil || len(clusters) != 1 {
		t.Fatalf("ListAllClusters after delay: %v %d", err, len(clusters))
	}
	if clusters[0].Status.State != "RUNNING" {
		t.Fatalf("state = %q, want RUNNING after the delay", clusters[0].Status.State)
	}
}

func TestListAllJobs_AcrossRegionsSorted(t *testing.T) {
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore())
	ctx := context.Background()
	for _, j := range []struct{ project, region, id string }{
		{"proj", "us-central1", "j-b"},
		{"proj", "europe-west1", "j-a"},
		{"other", "us-central1", "j-c"},
	} {
		if err := p.store.CreateJob(ctx, j.project, j.region, dpstore.Job{
			JobID:  j.id,
			Status: dpstore.JobStatus{State: "RUNNING"},
		}); err != nil {
			t.Fatalf("seed job %s/%s/%s: %v", j.project, j.region, j.id, err)
		}
	}
	jobs, err := p.ListAllJobs(ctx, "proj")
	if err != nil {
		t.Fatalf("ListAllJobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("jobs = %d, want 2 (other project excluded)", len(jobs))
	}
	if jobs[0].Region != "europe-west1" || jobs[0].JobID != "j-a" ||
		jobs[1].Region != "us-central1" || jobs[1].JobID != "j-b" {
		t.Fatalf("jobs not ordered by region then id: %+v", jobs)
	}
}

func TestListAllWorkflowTemplates_AcrossRegionsSorted(t *testing.T) {
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore())
	ctx := context.Background()
	for _, tmpl := range []struct{ project, region, id string }{
		{"proj", "us-central1", "t-b"},
		{"proj", "europe-west1", "t-a"},
		{"other", "us-central1", "t-c"},
	} {
		if err := p.store.CreateWorkflowTemplate(ctx, tmpl.project, tmpl.region, dpstore.WorkflowTemplate{TemplateID: tmpl.id}); err != nil {
			t.Fatalf("seed template %s/%s/%s: %v", tmpl.project, tmpl.region, tmpl.id, err)
		}
	}
	templates, err := p.ListAllWorkflowTemplates(ctx, "proj")
	if err != nil {
		t.Fatalf("ListAllWorkflowTemplates: %v", err)
	}
	if len(templates) != 2 {
		t.Fatalf("templates = %d, want 2 (other project excluded)", len(templates))
	}
	if templates[0].Region != "europe-west1" || templates[0].TemplateID != "t-a" ||
		templates[1].Region != "us-central1" || templates[1].TemplateID != "t-b" {
		t.Fatalf("templates not ordered by region then id: %+v", templates)
	}
}
