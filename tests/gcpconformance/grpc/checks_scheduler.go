package grpcconformance

import (
	"context"
	"fmt"

	scheduler "cloud.google.com/go/scheduler/apiv1"
	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// schedulerLocation is a canonical Cloud Scheduler location; the emulator does
// not validate the location list.
const schedulerLocation = "us-central1"

// schedulerChecks covers the Cloud Scheduler v1 surface
// (google.cloud.scheduler.v1.CloudScheduler) via the official generated
// cloud.google.com/go/scheduler/apiv1 client: CreateJob, GetJob, ListJobs,
// UpdateJob, PauseJob, ResumeJob, RunJob, DeleteJob.
//
// Every probe is self-contained and run-unique (cfg.ResourceName), and every
// probe that can fire uses the emulator's own /_jaiscloud/health endpoint as
// the HTTP target so delivery is fast and deterministic.
func schedulerChecks() []Check {
	return []Check{
		{Service: "scheduler", RPC: "CreateJob", Method: "CreateJob", KeyField: "name/state/httpTarget round-trip", Run: checkSchedulerCreateJob},
		{Service: "scheduler", RPC: "GetJob", Method: "GetJob", KeyField: "name + state ENABLED", Run: checkSchedulerGetJob},
		{Service: "scheduler", RPC: "ListJobs", Method: "ListJobs", KeyField: "created job present", Run: checkSchedulerListJobs},
		{Service: "scheduler", RPC: "UpdateJob", Method: "UpdateJob", KeyField: "description updated (masked)", Run: checkSchedulerUpdateJob},
		{Service: "scheduler", RPC: "PauseJob", Method: "PauseJob", KeyField: "state PAUSED", Run: checkSchedulerPauseJob},
		{Service: "scheduler", RPC: "ResumeJob", Method: "ResumeJob", KeyField: "state ENABLED", Run: checkSchedulerResumeJob},
		{Service: "scheduler", RPC: "RunJob", Method: "RunJob", KeyField: "lastAttemptTime set", Run: checkSchedulerRunJob},
		{Service: "scheduler", RPC: "DeleteJob", Method: "DeleteJob", KeyField: "subsequent GetJob NOT_FOUND", Run: checkSchedulerDeleteJob},
	}
}

// newSchedulerClient dials the emulator and returns the official generated
// Cloud Scheduler client.
func newSchedulerClient(ctx context.Context, cfg Config) (*scheduler.CloudSchedulerClient, error) {
	return scheduler.NewCloudSchedulerClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func schedulerParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s", cfg.Project, schedulerLocation)
}

func schedulerJobName(cfg Config, prefix string) string {
	return schedulerParent(cfg) + "/jobs/" + cfg.ResourceName(prefix)
}

// newSchedulerJob builds a job with an httpTarget pointing at the emulator's
// health endpoint (delivery succeeds immediately).
func newSchedulerJob(cfg Config, name string) *schedulerpb.Job {
	return &schedulerpb.Job{
		Name:     name,
		Schedule: "* * * * *",
		TimeZone: "UTC",
		Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{
			Uri:        cfg.RESTEndpoint + "/_jaiscloud/health",
			HttpMethod: schedulerpb.HttpMethod_GET,
		}},
	}
}

// createSchedulerJob creates a fresh job for the probe and returns it.
func createSchedulerJob(ctx context.Context, client *scheduler.CloudSchedulerClient, cfg Config, prefix string) (*schedulerpb.Job, error) {
	name := schedulerJobName(cfg, prefix)
	job, err := client.CreateJob(ctx, &schedulerpb.CreateJobRequest{Parent: schedulerParent(cfg), Job: newSchedulerJob(cfg, name)})
	if err != nil {
		return nil, fmt.Errorf("CreateJob: %w", err)
	}
	return job, nil
}

// Check 1: CreateJob returns the job in the ENABLED state with its httpTarget.
func checkSchedulerCreateJob(ctx context.Context, cfg Config) error {
	client, err := newSchedulerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	job, err := createSchedulerJob(ctx, client, cfg, "gcpc-grpc-sched-create")
	if err != nil {
		return err
	}
	if job.GetState() != schedulerpb.Job_ENABLED {
		return fmt.Errorf("CreateJob state = %v, want ENABLED", job.GetState())
	}
	if job.GetHttpTarget().GetUri() != cfg.RESTEndpoint+"/_jaiscloud/health" {
		return fmt.Errorf("CreateJob httpTarget.uri = %q", job.GetHttpTarget().GetUri())
	}
	if job.GetScheduleTime() == nil {
		return fmt.Errorf("CreateJob scheduleTime is unset")
	}
	return nil
}

// Check 2: GetJob returns the created job.
func checkSchedulerGetJob(ctx context.Context, cfg Config) error {
	client, err := newSchedulerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createSchedulerJob(ctx, client, cfg, "gcpc-grpc-sched-get")
	if err != nil {
		return err
	}
	got, err := client.GetJob(ctx, &schedulerpb.GetJobRequest{Name: created.GetName()})
	if err != nil {
		return fmt.Errorf("GetJob: %w", err)
	}
	if got.GetName() != created.GetName() || got.GetState() != schedulerpb.Job_ENABLED {
		return fmt.Errorf("GetJob = %q/%v, want %q/ENABLED", got.GetName(), got.GetState(), created.GetName())
	}
	return nil
}

// Check 3: ListJobs includes the created job.
func checkSchedulerListJobs(ctx context.Context, cfg Config) error {
	client, err := newSchedulerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createSchedulerJob(ctx, client, cfg, "gcpc-grpc-sched-list")
	if err != nil {
		return err
	}
	it := client.ListJobs(ctx, &schedulerpb.ListJobsRequest{Parent: schedulerParent(cfg)})
	for {
		job, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListJobs did not include %q", created.GetName())
		}
		if err != nil {
			return fmt.Errorf("ListJobs: %w", err)
		}
		if job.GetName() == created.GetName() {
			return nil
		}
	}
}

// Check 4: UpdateJob with a field mask changes only the masked field.
func checkSchedulerUpdateJob(ctx context.Context, cfg Config) error {
	client, err := newSchedulerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createSchedulerJob(ctx, client, cfg, "gcpc-grpc-sched-update")
	if err != nil {
		return err
	}
	mask := &fieldmaskpb.FieldMask{Paths: []string{"description"}}
	updated, err := client.UpdateJob(ctx, &schedulerpb.UpdateJobRequest{
		Job:        &schedulerpb.Job{Name: created.GetName(), Description: "updated"},
		UpdateMask: mask,
	})
	if err != nil {
		return fmt.Errorf("UpdateJob: %w", err)
	}
	if updated.GetDescription() != "updated" {
		return fmt.Errorf("UpdateJob description = %q, want updated", updated.GetDescription())
	}
	if updated.GetSchedule() != created.GetSchedule() {
		return fmt.Errorf("UpdateJob clobbered schedule: %q", updated.GetSchedule())
	}
	return nil
}

// Check 5: PauseJob transitions ENABLED -> PAUSED.
func checkSchedulerPauseJob(ctx context.Context, cfg Config) error {
	client, err := newSchedulerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createSchedulerJob(ctx, client, cfg, "gcpc-grpc-sched-pause")
	if err != nil {
		return err
	}
	paused, err := client.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: created.GetName()})
	if err != nil {
		return fmt.Errorf("PauseJob: %w", err)
	}
	if paused.GetState() != schedulerpb.Job_PAUSED {
		return fmt.Errorf("PauseJob state = %v, want PAUSED", paused.GetState())
	}
	return nil
}

// Check 6: ResumeJob transitions PAUSED -> ENABLED.
func checkSchedulerResumeJob(ctx context.Context, cfg Config) error {
	client, err := newSchedulerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createSchedulerJob(ctx, client, cfg, "gcpc-grpc-sched-resume")
	if err != nil {
		return err
	}
	if _, err := client.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: created.GetName()}); err != nil {
		return fmt.Errorf("PauseJob: %w", err)
	}
	resumed, err := client.ResumeJob(ctx, &schedulerpb.ResumeJobRequest{Name: created.GetName()})
	if err != nil {
		return fmt.Errorf("ResumeJob: %w", err)
	}
	if resumed.GetState() != schedulerpb.Job_ENABLED {
		return fmt.Errorf("ResumeJob state = %v, want ENABLED", resumed.GetState())
	}
	return nil
}

// Check 7: RunJob forces an attempt and records it.
func checkSchedulerRunJob(ctx context.Context, cfg Config) error {
	client, err := newSchedulerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createSchedulerJob(ctx, client, cfg, "gcpc-grpc-sched-run")
	if err != nil {
		return err
	}
	ran, err := client.RunJob(ctx, &schedulerpb.RunJobRequest{Name: created.GetName()})
	if err != nil {
		return fmt.Errorf("RunJob: %w", err)
	}
	if ran.GetLastAttemptTime() == nil {
		return fmt.Errorf("RunJob did not record lastAttemptTime")
	}
	return nil
}

// Check 8: DeleteJob removes the job (a subsequent GetJob is NOT_FOUND).
func checkSchedulerDeleteJob(ctx context.Context, cfg Config) error {
	client, err := newSchedulerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	created, err := createSchedulerJob(ctx, client, cfg, "gcpc-grpc-sched-delete")
	if err != nil {
		return err
	}
	if err := client.DeleteJob(ctx, &schedulerpb.DeleteJobRequest{Name: created.GetName()}); err != nil {
		return fmt.Errorf("DeleteJob: %w", err)
	}
	if _, err := client.GetJob(ctx, &schedulerpb.GetJobRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetJob after delete code = %v, want NotFound", status.Code(err))
	}
	return nil
}
