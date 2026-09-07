package dataproc

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/sparkgcp"
	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/sparkhelpers"
)

const tailLogsTimeout = 60 * time.Second

// cancelKey scopes a job cancellation to its project+region so a CancelJob in
// one region cannot cancel a same-named job in another.
func cancelKey(project, region, jobID string) string {
	return project + "/" + region + "/" + jobID
}

// runJob executes a stored job via sparkhelpers.SubmitClientMode. Runs in a
// goroutine; the terminal state is written back to the jobs store (first-write
// wins via the store UpdateJob) and a terminal snapshot is persisted for
// post-GC rehydration parity.
func (p *Provider) runJob(ctx context.Context, project, region string, j dataprocstore.Job) {
	jobID := j.JobID

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	cancelKey := cancelKey(project, region, jobID)
	p.cancelsMu.Lock()
	p.cancels[cancelKey] = runCancel
	p.cancelsMu.Unlock()
	defer func() {
		p.cancelsMu.Lock()
		delete(p.cancels, cancelKey)
		p.cancelsMu.Unlock()
	}()

	ep, sparkArgs, jarArgs, err := entryPointForJob(j)
	if err != nil {
		p.finishJob(project, region, j, "ERROR", err.Error())
		return
	}

	ns := p.namespace
	if ns == "" {
		ns = "jaiscloud"
	}

	var identityMutator k8shelpers.IdentityMutator
	if p.serviceAccountName != "" {
		identityMutator = sparkgcp.BuildWorkloadIdentityMutator(ns, p.serviceAccountName, project)
	}

	labels := map[string]string{
		"jaiscloud.io/provider":        "dataproc",
		"jaiscloud.io/project":         project,
		"jaiscloud.io/region":          region,
		"jaiscloud.io/cluster-name":    j.PlacementClusterName,
		"jaiscloud.io/job-id":          jobID,
		"jaiscloud.io/spark-id":        jobID,
		"app.kubernetes.io/managed-by": "jaiscloud",
	}
	if p.instanceID != "" {
		labels["jaiscloud.io/instance-id"] = p.instanceID
	}

	// Per-job emulator config so pods talk to the local emulator as the
	// submitting project.
	jobEmulator := p.gcpEmulator
	if jobEmulator != nil {
		copy := *jobEmulator
		copy.ProjectID = project
		jobEmulator = &copy
	}
	driverEnv := sparkgcp.DriverEnv(jobEmulator)

	clientJob := sparkhelpers.ClientModeJob{
		JobID:              jobID,
		Namespace:          ns,
		Image:              p.sparkImage,
		EntryPoint:         ep,
		SparkSubmitPath:    p.sparkSubmitPath,
		SparkSubmitArgs:    sparkArgs,
		JarArgs:            jarArgs,
		PlatformOverlay:    p.platformCfg,
		IdentityMutator:    identityMutator,
		ServiceAccountName: p.serviceAccountName,
		ExtraDriverEnv:     driverEnv,
		ExtraSparkConfs:    sparkgcp.DriverSparkConfsFromEnv(jobEmulator, driverEnv),
		Labels:             labels,
	}

	handle, err := sparkhelpers.SubmitClientMode(runCtx, p.k8sClient, clientJob)
	if err != nil {
		if runCtx.Err() != nil {
			return // cancelled by CancelJob
		}
		slog.Warn("dataproc: SubmitClientMode failed", "job", jobID, "err", err)
		p.persistSnapshot(runCtx, project, region, jobID, k8shelpers.BuildSnapshotFromError(err))
		p.finishJob(project, region, j, "ERROR", err.Error())
		return
	}

	final, err := sparkhelpers.WaitTerminal(runCtx, p.k8sClient, handle)
	if err != nil {
		if runCtx.Err() != nil {
			return // cancelled by CancelJob
		}
		slog.Warn("dataproc: WaitTerminal failed", "job", jobID, "err", err)
		p.persistSnapshot(runCtx, project, region, jobID, k8shelpers.BuildSnapshotFromError(err))
		p.finishJob(project, region, j, "ERROR", err.Error())
		return
	}

	tailCtx, tailCancel := context.WithTimeout(ctx, tailLogsTimeout)
	defer tailCancel()
	if tailErr := k8shelpers.TailLogs(tailCtx, p.k8sClient, handle, k8shelpers.LogKindMain, &discardWriter{}); tailErr != nil {
		slog.Warn("dataproc: TailLogs failed", "job", jobID, "err", tailErr)
	}

	state, reason := finalToJobState(final)
	p.persistSnapshot(runCtx, project, region, jobID, k8shelpers.BuildSnapshot(final.Final, state))
	p.finishJob(project, region, j, state, reason)
}

// entryPointForJob derives the sparkhelpers.EntryPoint + args for a stored job.
func entryPointForJob(j dataprocstore.Job) (sparkhelpers.EntryPoint, []string, []string, error) {
	ep, jarArgs, err := jobToEntryPoint(j.Type, mustJSONMap(j.TypeJob))
	if err != nil {
		return nil, nil, nil, err
	}
	return ep, propertiesToConfArgs(mustJSONMap(j.TypeJob)), jarArgs, nil
}

// finishJob writes the terminal state back to the jobs store. The get-check-
// set cycle is atomic (UpdateJobAtomic) so it can't race with a concurrent
// CancelJob: whichever of the two acquires the lock first wins the terminal-
// state transition, and the other sees the already-terminal state inside its
// own mutate and no-ops instead of overwriting it.
func (p *Provider) finishJob(project, region string, j dataprocstore.Job, state, details string) {
	now := clock.Now().UTC()
	var transitioned bool
	fresh, err := p.store.UpdateJobAtomic(context.Background(), project, region, j.JobID, func(fresh dataprocstore.Job) (dataprocstore.Job, error) {
		if jobTerminal(fresh.Status.State) {
			return fresh, nil // already terminal (e.g. cancelled) — first write wins
		}
		transitioned = true
		fresh.StatusHistory = append(fresh.StatusHistory, fresh.Status)
		fresh.Status = dataprocstore.JobStatus{State: state, Details: details, StateStartTime: now}
		if state == "DONE" && fresh.DriverOutputResourceURI == "" {
			fresh.DriverOutputResourceURI = "gs://jaiscloud-dataproc/" + fresh.JobUUID + "/driveroutput"
		}
		return fresh, nil
	})
	if err != nil {
		slog.Warn("dataproc: finishJob update failed", "job", j.JobID, "err", err)
		return
	}
	if !transitioned {
		return
	}
	p.completeSubmitOperation(project, region, fresh)
}

// completeSubmitOperation flips the SubmitJobAsOperation long-running operation
// (id == job id) to done=true with the terminal job as its response. No-op when
// the job was submitted without an operation (plain SubmitJob) or the operation
// is already terminal.
func (p *Provider) completeSubmitOperation(project, region string, j dataprocstore.Job) {
	op, err := p.store.GetOperation(context.Background(), project, region, j.JobID)
	if err != nil {
		return
	}
	if op.Done {
		return
	}
	resp, _ := json.Marshal(jobToMap(j))
	op.Done = true
	op.Response = string(resp)
	op.EndTime = clock.Now().UTC()
	if err := p.store.UpdateOperation(context.Background(), project, region, op); err != nil {
		slog.Warn("dataproc: completeSubmitOperation failed", "job", j.JobID, "err", err)
	}
}

func (p *Provider) persistSnapshot(ctx context.Context, project, region, jobID string, snap k8shelpers.Snapshot) {
	if err := k8shelpers.PersistTerminalSnapshot(ctx, p.resources, project, region, "dataproc/jobs", jobID, snap); err != nil {
		slog.Error("dataproc: PersistTerminalSnapshot failed", "prefix", "dataproc/jobs", "id", jobID, "err", err)
	}
}

// finalToJobState maps a sparkhelpers.Final to a Dataproc job state string.
func finalToJobState(f sparkhelpers.Final) (state, reason string) {
	if f.SparkSucceeded {
		return "DONE", ""
	}
	if f.Cancelled {
		return "CANCELLED", ""
	}
	return "ERROR", f.SparkReason
}

// discardWriter drops driver logs (Dataproc has no per-job log sink in the
// emulator; logs are best-effort tailed for Spark exit classification only).
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
