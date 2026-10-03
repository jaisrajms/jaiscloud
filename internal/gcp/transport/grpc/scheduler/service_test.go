package scheduler

import (
	"context"
	"testing"

	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	core "jaiscloud/internal/gcp/service/scheduler"
	schedstore "jaiscloud/internal/gcp/store/scheduler"
)

func newGRPCService(t *testing.T) (*Service, *core.Service) {
	t.Helper()
	mem := schedstore.NewMemoryStore()
	c := core.NewService(mem)
	return NewService(c, "p"), c
}

func pbJob(name string) *schedulerpb.Job {
	return &schedulerpb.Job{
		Name:     name,
		Schedule: "* * * * *",
		TimeZone: "UTC",
		Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{
			Uri:        "http://example.test/hook",
			HttpMethod: schedulerpb.HttpMethod_POST,
		}},
	}
}

func TestGRPCCRUD(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	parent := "projects/p/locations/l"
	name := parent + "/jobs/j1"

	if _, err := svc.CreateJob(ctx, &schedulerpb.CreateJobRequest{Parent: parent, Job: pbJob(name)}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := svc.GetJob(ctx, &schedulerpb.GetJobRequest{Name: name}); err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if resp, err := svc.ListJobs(ctx, &schedulerpb.ListJobsRequest{Parent: parent}); err != nil || len(resp.GetJobs()) != 1 {
		t.Fatalf("ListJobs = %v, %v", resp, err)
	}
	if _, err := svc.UpdateJob(ctx, &schedulerpb.UpdateJobRequest{Job: &schedulerpb.Job{Name: name, Description: "d"}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}}}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if _, err := svc.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: name}); err != nil {
		t.Fatalf("PauseJob: %v", err)
	}
	if _, err := svc.ResumeJob(ctx, &schedulerpb.ResumeJobRequest{Name: name}); err != nil {
		t.Fatalf("ResumeJob: %v", err)
	}
	if _, err := svc.RunJob(ctx, &schedulerpb.RunJobRequest{Name: name}); err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	if _, err := svc.DeleteJob(ctx, &schedulerpb.DeleteJobRequest{Name: name}); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	if _, err := svc.GetJob(ctx, &schedulerpb.GetJobRequest{Name: name}); err == nil {
		t.Fatalf("GetJob after delete: expected NotFound")
	}
}

func TestGRPCInvalidNames(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	if _, err := svc.CreateJob(ctx, &schedulerpb.CreateJobRequest{Parent: "bogus", Job: pbJob("")}); err == nil {
		t.Fatalf("CreateJob with bad parent: expected error")
	}
	if _, err := svc.GetJob(ctx, &schedulerpb.GetJobRequest{Name: "bogus"}); err == nil {
		t.Fatalf("GetJob with bad name: expected error")
	}
	if _, err := svc.ListJobs(ctx, &schedulerpb.ListJobsRequest{Parent: "bogus"}); err == nil {
		t.Fatalf("ListJobs with bad parent: expected error")
	}
}
