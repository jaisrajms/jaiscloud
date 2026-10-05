package dataproc

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/sparkgcp"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/sparkhelpers"
)

const tailLogsTimeout = 60 * time.Second

// driverReapTimeout bounds the best-effort deletion of a cancelled job's
// client-mode k8s Job. CancelJob's context is already cancelled, so the reap
// runs on a fresh bounded context and must not block job shutdown.
const driverReapTimeout = 30 * time.Second

// driverLogLimit caps how many driver stdout/stderr bytes the emulator buffers
// in memory (and stages into GCS). A chatty or looping driver must not exhaust
// emulator memory; the cap is a fidelity tradeoff (the tail beyond the limit is
// dropped).
const driverLogLimit = 4 << 20 // 4 MiB

// cappedBuffer is an io.Writer that keeps at most max bytes and silently drops
// the rest. It always reports a full write, so an over-limit log stream keeps
// draining rather than aborting.
type cappedBuffer struct {
	buf bytes.Buffer
	max int
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if rem := c.max - c.buf.Len(); rem > 0 {
		if len(p) > rem {
			p = p[:rem]
		}
		_, _ = c.buf.Write(p)
	}
	return n, nil
}

func (c *cappedBuffer) Bytes() []byte { return c.buf.Bytes() }

// cancelKey scopes a job cancellation to its project+region so a CancelJob in
// one region cannot cancel a same-named job in another.
func cancelKey(project, region, jobID string) string {
	return project + "/" + region + "/" + jobID
}

// waitTerminalFn waits for the driver pod's terminal state. It is a package
// var so tests can inject a synthetic non-context error and exercise the
// reap-on-error branch (the real fallback poll never returns a non-context
// error on its own).
var waitTerminalFn = sparkhelpers.WaitTerminalWith

// runJob registers the job's cancel func and executes it. Registration happens
// synchronously before runJobWithCtx blocks (or, when called from submitJob,
// before the executor goroutine is launched), so a CancelJob racing job
// submission always finds the cancel func.
func (s *Service) runJob(ctx context.Context, project, region string, j dpstore.Job, metastoreEndpoint, namespace string) {
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	key := cancelKey(project, region, j.JobID)
	s.registerCancel(key, runCancel)
	defer s.unregisterCancel(key)
	s.runJobWithCtx(runCtx, project, region, j, metastoreEndpoint, namespace)
}

// registerCancel records a running job's cancel func so a racing CancelJob
// finds it. unregisterCancel removes it once the executor has stopped.
func (s *Service) registerCancel(key string, cancel context.CancelFunc) {
	s.cancelsMu.Lock()
	s.cancels[key] = cancel
	s.cancelsMu.Unlock()
}

func (s *Service) unregisterCancel(key string) {
	s.cancelsMu.Lock()
	delete(s.cancels, key)
	s.cancelsMu.Unlock()
}

// runJobWithCtx executes a stored job via sparkhelpers.SubmitClientMode. Runs
// in a goroutine; the terminal state is written back to the jobs store
// (first-write wins via the store UpdateJob) and a terminal snapshot is
// persisted for post-GC rehydration parity. metastoreEndpoint is the cluster's
// Hive Metastore thrift endpoint ("" when the cluster has no attachment); it is
// injected as a spark-submit conf so driver and executor pods share the
// attachment. namespace is the cluster's effective workload namespace ("" falls
// back to the process-wide one).
//
// The caller owns ctx's cancel func and its registration in s.cancels. submitJob
// registers before launching the goroutine so a CancelJob landing in the
// submit/launch window cancels the driver instead of leaving it running.
//
// When the job carries a restart policy (Job.scheduling) a non-zero driver exit
// restarts the driver within maxFailuresPerHour/maxFailuresTotal and the
// documented thrash rule (>4 non-zero exits in a 10-minute window). Success is
// strictly driver exit 0. Only k8s mode runs here; mock mode has no executor.
func (s *Service) runJobWithCtx(ctx context.Context, project, region string, j dpstore.Job, metastoreEndpoint, namespace string) {
	jobID := j.JobID
	runCtx := ctx

	// Safety net: if the executor goroutine exits while the job is still
	// ATTEMPT_FAILURE (service shutdown, crash, a lost goroutine) and no read
	// will ever settle it, move it to ERROR so a poller cannot hang on a
	// permanently-transient state. A CancelJob writes CANCEL_PENDING before it
	// cancels this context, so a normal cancellation is unaffected.
	defer func() {
		cur, err := s.store.GetJob(context.Background(), project, region, jobID)
		if err == nil && cur.Status.State == jobStateAttemptFail {
			s.finishJob(project, region, cur, jobStateError, "executor stopped after an attempt failure", nil)
		}
	}()

	ep, sparkArgs, jarArgs, err := entryPointForJob(j)
	if err != nil {
		s.finishJob(project, region, j, jobStateError, err.Error(), nil)
		return
	}
	// The Spark SQL CLI prints a failed query's message without an "ERROR" log
	// line, so for sparkSqlJob the driver's exit code is authoritative (no
	// lenient "clean shutdown hook" success rule). Restartable jobs are likewise
	// strict: real Dataproc restarts on any non-zero exit, including signal
	// exits, so success is only exit 0.
	_, isSQLJob := ep.(sparkhelpers.SqlEntryPoint)

	ns := namespace
	if ns == "" {
		ns = s.defaultNamespace()
	}

	var identityMutator k8shelpers.IdentityMutator
	if s.serviceAccountName != "" {
		identityMutator = sparkgcp.BuildWorkloadIdentityMutator(ns, s.serviceAccountName, project)
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
	if s.instanceID != "" {
		labels["jaiscloud.io/instance-id"] = s.instanceID
	}

	// Per-job emulator config so pods talk to the local emulator as the
	// submitting project.
	jobEmulator := s.gcpEmulator
	if jobEmulator != nil {
		copy := *jobEmulator
		copy.ProjectID = project
		// The emulator config is process-wide; scope the location to the
		// cluster's region so driver/executor pods report the right
		// GOOGLE_CLOUD_LOCATION for this job.
		copy.Region = region
		jobEmulator = &copy
	}
	driverEnv := sparkgcp.DriverEnv(jobEmulator)

	sparkConfs := sparkgcp.DriverSparkConfsFromEnv(jobEmulator, driverEnv)
	sparkConfs = append(sparkConfs, metastoreSparkConfs(metastoreEndpoint)...)

	clientJob := sparkhelpers.ClientModeJob{
		JobID:              jobID,
		Namespace:          ns,
		Image:              s.sparkImage,
		EntryPoint:         ep,
		SparkSubmitPath:    s.sparkSubmitPath,
		SparkSqlPath:       s.sparkSqlPath,
		SparkSubmitArgs:    sparkArgs,
		JarArgs:            jarArgs,
		PlatformOverlay:    s.platformCfg,
		IdentityMutator:    identityMutator,
		ServiceAccountName: s.serviceAccountName,
		ExtraDriverEnv:     driverEnv,
		ExtraSparkConfs:    sparkConfs,
		Labels:             labels,
	}

	policy := restartPolicyFor(j.Scheduling)
	strict := isSQLJob || policy.enabled()

	// driverLog accumulates the (byte-capped) driver output across attempts.
	driverLog := &cappedBuffer{max: driverLogLimit}
	var failures []time.Time

	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			_, _ = fmt.Fprintf(driverLog, "\n--- restart attempt %d ---\n", attempt+1)
		}
		clientJob.Attempt = attempt

		handle, err := s.executor.Submit(runCtx, clientJob)
		if err != nil {
			if runCtx.Err() != nil {
				return // cancelled by CancelJob
			}
			// A submission failure is not a driver exit: real Dataproc restarts
			// only on a non-zero driver exit, so this is terminal and does not
			// consume the restart budget.
			slog.Warn("dataproc: SubmitClientMode failed", "job", jobID, "attempt", attempt, "err", err)
			s.persistSnapshot(runCtx, project, region, jobID, k8shelpers.BuildSnapshotFromError(err))
			s.finishJob(project, region, j, jobStateError, err.Error(), driverLog.Bytes())
			return
		}

		if attempt == 0 {
			// The executor accepted the job: PENDING -> SETUP_DONE, then the
			// driver pod is created -> RUNNING. Both transitions are no-ops if a
			// cancel won the race, so a cancelled job never runs again.
			s.setJobState(runCtx, project, region, jobID, map[string]bool{jobStatePending: true}, jobStateSetupDone, "")
			s.setJobState(runCtx, project, region, jobID, map[string]bool{jobStateSetupDone: true}, jobStateRunning, "")
		} else {
			// A restartable retry resumes from ATTEMPT_FAILURE.
			s.setJobState(runCtx, project, region, jobID, map[string]bool{jobStateAttemptFail: true}, jobStateRunning, "")
		}

		final, err := s.executor.WaitTerminal(runCtx, handle, sparkhelpers.TerminalOptions{StrictExitCode: strict})
		if err != nil {
			if runCtx.Err() != nil {
				// Cancelled (CancelJob, or service shutdown): the driver is
				// still running, so reap it before returning — otherwise the
				// driver keeps executing after the job reports CANCELLED.
				s.executor.Reap(handle)
				return
			}
			// The wait itself failed (e.g. the pod watch could not be
			// established) while the context is still live: the driver may
			// still be running, so reap it before reporting ERROR rather than
			// leaking it — the same class of leak STR3 fixed on the cancel path.
			s.executor.Reap(handle)
			slog.Warn("dataproc: WaitTerminal failed", "job", jobID, "attempt", attempt, "err", err)
			s.persistSnapshot(runCtx, project, region, jobID, k8shelpers.BuildSnapshotFromError(err))
			s.finishJob(project, region, j, jobStateError, err.Error(), driverLog.Bytes())
			return
		}

		// Capture the driver container's stdout/stderr and stage it into the
		// job's GCS driver-output object at terminal state. The capture is
		// byte-capped so a chatty driver cannot exhaust emulator memory.
		tailCtx, tailCancel := context.WithTimeout(ctx, tailLogsTimeout)
		if tailErr := s.executor.StreamLogs(tailCtx, handle, driverLog); tailErr != nil {
			slog.Warn("dataproc: StreamLogs failed", "job", jobID, "attempt", attempt, "err", tailErr)
		}
		tailCancel()

		if !policy.enabled() {
			// Non-restartable: keep the lenient Spark classification.
			state, reason := finalToJobState(final)
			s.persistSnapshot(runCtx, project, region, jobID, k8shelpers.BuildSnapshot(final.Final, state))
			s.finishJob(project, region, j, state, reason, driverLog.Bytes())
			return
		}

		// Restartable: success is strictly driver exit 0.
		if final.Succeeded && final.ExitCode == 0 {
			s.persistSnapshot(runCtx, project, region, jobID, k8shelpers.BuildSnapshot(final.Final, jobStateDone))
			s.finishJob(project, region, j, jobStateDone, "", driverLog.Bytes())
			return
		}
		now := clock.Now().UTC()
		failures = append(failures, now)
		if !policy.allowsRestart(failures, now) {
			reason := final.SparkReason
			if reason == "" {
				reason = final.Reason
			}
			s.persistSnapshot(runCtx, project, region, jobID, k8shelpers.BuildSnapshot(final.Final, jobStateError))
			s.finishJob(project, region, j, jobStateError, reason, driverLog.Bytes())
			return
		}
		// Record the failed attempt; the next loop iteration restarts it.
		s.setJobState(runCtx, project, region, jobID, map[string]bool{jobStateRunning: true}, jobStateAttemptFail, final.SparkReason)
		if runCtx.Err() != nil {
			return
		}
		// Reap the failed attempt's driver (its k8s Job, and — via the Job's
		// ownerReference — the executor-template ConfigMap and failed pod, or its
		// Docker container) so a long restart chain does not accumulate one
		// resource per attempt.
		s.executor.Reap(handle)
	}
}

// restartPolicy is the operative part of a job's dataproc.v1.JobScheduling:
// how many driver restarts are allowed after a non-zero exit. Both limits are
// optional; a zero limit (with the other also zero) disables restarts.
type restartPolicy struct {
	perHour int32
	total   int32
}

func restartPolicyFor(s *dpstore.JobScheduling) restartPolicy {
	if s == nil {
		return restartPolicy{}
	}
	return restartPolicy{perHour: s.MaxFailuresPerHour, total: s.MaxFailuresTotal}
}

func (p restartPolicy) enabled() bool { return p.perHour > 0 || p.total > 0 }

// thrashWindow and thrashLimit encode the documented thrash rule: a driver that
// exits non-zero more than thrashLimit times within thrashWindow is failing.
const (
	thrashWindow = 10 * time.Minute
	thrashLimit  = 4
)

// allowsRestart reports whether another attempt is permitted given every
// recorded non-zero driver exit (including the one just observed). A restart is
// refused when the thrash window, the per-hour limit or the total limit is
// exceeded. The check order is an implementation choice (the API does not
// document one).
func (p restartPolicy) allowsRestart(failures []time.Time, now time.Time) bool {
	if !p.enabled() {
		return false
	}
	recent := 0
	hourly := 0
	for _, t := range failures {
		if now.Sub(t) <= thrashWindow {
			recent++
		}
		if now.Sub(t) <= time.Hour {
			hourly++
		}
	}
	if recent > thrashLimit {
		return false
	}
	if p.total > 0 && int32(len(failures)) > p.total {
		return false
	}
	if p.perHour > 0 && int32(hourly) > p.perHour {
		return false
	}
	return true
}

// validateScheduling rejects a restart policy outside the API's documented
// maxima. 0 (and an omitted field) is valid and means "no restarts".
func validateScheduling(s *dpstore.JobScheduling) error {
	if s == nil {
		return nil
	}
	if s.MaxFailuresPerHour < 0 || s.MaxFailuresPerHour > 10 {
		return invalidArgument("scheduling.maxFailuresPerHour must be between 0 and 10")
	}
	if s.MaxFailuresTotal < 0 || s.MaxFailuresTotal > 240 {
		return invalidArgument("scheduling.maxFailuresTotal must be between 0 and 240")
	}
	return nil
}

// entryPointForJob derives the sparkhelpers.EntryPoint + args for a stored job.
func entryPointForJob(j dpstore.Job) (sparkhelpers.EntryPoint, []string, []string, error) {
	ep, jarArgs, err := jobToEntryPoint(j.Type, mustJSONMap(j.TypeJob))
	if err != nil {
		return nil, nil, nil, err
	}
	return ep, propertiesToConfArgs(mustJSONMap(j.TypeJob)), jarArgs, nil
}

func (s *Service) persistSnapshot(ctx context.Context, project, region, jobID string, snap k8shelpers.Snapshot) {
	if err := k8shelpers.PersistTerminalSnapshot(ctx, s.resources, project, region, "dataproc/jobs", jobID, snap); err != nil {
		slog.Error("dataproc: PersistTerminalSnapshot failed", "prefix", "dataproc/jobs", "id", jobID, "err", err)
	}
}

// finalToJobState maps a sparkhelpers.Final to a Dataproc job state string.
func finalToJobState(f sparkhelpers.Final) (state, reason string) {
	if f.SparkSucceeded {
		return jobStateDone, ""
	}
	if f.Cancelled {
		return jobStateCancelled, ""
	}
	return jobStateError, f.SparkReason
}
