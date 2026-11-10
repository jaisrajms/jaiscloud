package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
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

// TestGRPCRetryConfigDefaults pins the gRPC read of the RetryConfig a job
// answers when its create body carried none. The documented defaults
// (retry_count 0, min_backoff_duration 5s, max_backoff_duration 1h,
// max_doublings 5, max_retry_duration 0 = unlimited) must be present, and a
// non-default config must be echoed verbatim (TF3-4-2).
func TestGRPCRetryConfigDefaults(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	parent := "projects/p/locations/l"

	for _, tc := range []struct {
		job  string
		cfg  *schedulerpb.RetryConfig
		want *schedulerpb.RetryConfig
	}{
		{job: "plain", want: &schedulerpb.RetryConfig{
			RetryCount:         0,
			MinBackoffDuration: durationpb.New(5 * time.Second),
			MaxBackoffDuration: durationpb.New(time.Hour),
			MaxRetryDuration:   durationpb.New(0),
			MaxDoublings:       5,
		}},
		{job: "custom", cfg: &schedulerpb.RetryConfig{
			RetryCount:         3,
			MaxDoublings:       2,
			MinBackoffDuration: durationpb.New(2 * time.Second),
			MaxBackoffDuration: durationpb.New(20 * time.Second),
			MaxRetryDuration:   durationpb.New(time.Minute),
		}, want: &schedulerpb.RetryConfig{
			RetryCount:         3,
			MaxDoublings:       2,
			MinBackoffDuration: durationpb.New(2 * time.Second),
			MaxBackoffDuration: durationpb.New(20 * time.Second),
			MaxRetryDuration:   durationpb.New(time.Minute),
		}},
	} {
		job := pbJob(parent + "/jobs/" + tc.job)
		job.RetryConfig = tc.cfg
		if _, err := svc.CreateJob(ctx, &schedulerpb.CreateJobRequest{Parent: parent, Job: job}); err != nil {
			t.Fatalf("CreateJob(%s): %v", tc.job, err)
		}
		got, err := svc.GetJob(ctx, &schedulerpb.GetJobRequest{Name: parent + "/jobs/" + tc.job})
		if err != nil {
			t.Fatalf("GetJob(%s): %v", tc.job, err)
		}
		rc := got.GetRetryConfig()
		if rc == nil {
			t.Fatalf("%s: retryConfig = nil, want %v", tc.job, tc.want)
		}
		if rc.GetRetryCount() != tc.want.GetRetryCount() ||
			rc.GetMaxDoublings() != tc.want.GetMaxDoublings() ||
			rc.GetMinBackoffDuration().AsDuration() != tc.want.GetMinBackoffDuration().AsDuration() ||
			rc.GetMaxBackoffDuration().AsDuration() != tc.want.GetMaxBackoffDuration().AsDuration() ||
			rc.GetMaxRetryDuration().AsDuration() != tc.want.GetMaxRetryDuration().AsDuration() {
			t.Fatalf("%s: retryConfig = %v, want %v", tc.job, rc, tc.want)
		}
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

// TestGRPCListJobsNegativePageSize pins AIP-158 on the gRPC surface: a negative
// page_size is INVALID_ARGUMENT, not a silent first page.
func TestGRPCListJobsNegativePageSize(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	_, err := svc.ListJobs(ctx, &schedulerpb.ListJobsRequest{Parent: "projects/p/locations/l", PageSize: -1})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("ListJobs(page_size=-1) err = %v (%v), want InvalidArgument", err, code)
	}
	if _, err := svc.ListJobs(ctx, &schedulerpb.ListJobsRequest{Parent: "projects/p/locations/l", PageSize: 0}); err != nil {
		t.Fatalf("ListJobs(page_size=0): %v", err)
	}
}
