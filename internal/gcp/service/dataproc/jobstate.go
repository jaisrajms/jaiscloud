package dataproc

import (
	"context"
	"log/slog"

	"jaiscloud/internal/clock"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// Dataproc job states (dataproc.v1.JobStatus.State). The emulator drives the
// full documented machine so pollers observe PENDING, SETUP_DONE, RUNNING, the
// cancel progression and ATTEMPT_FAILURE — not just the terminal states.
const (
	jobStatePending       = "PENDING"
	jobStateSetupDone     = "SETUP_DONE"
	jobStateRunning       = "RUNNING"
	jobStateCancelPending = "CANCEL_PENDING"
	jobStateCancelStarted = "CANCEL_STARTED"
	jobStateCancelled     = "CANCELLED"
	jobStateDone          = "DONE"
	jobStateError         = "ERROR"
	jobStateAttemptFail   = "ATTEMPT_FAILURE"
)

// jobTerminalStates are the states a job never leaves (Job.done == true).
var jobTerminalStates = map[string]bool{
	jobStateDone:      true,
	jobStateError:     true,
	jobStateCancelled: true,
}

// jobTransitionalStates are the states a read settles (advanceJob).
var jobTransitionalStates = map[string]bool{
	jobStatePending:       true,
	jobStateSetupDone:     true,
	jobStateRunning:       true,
	jobStateCancelPending: true,
	jobStateCancelStarted: true,
	jobStateAttemptFail:   true,
}

// jobTerminal reports whether a job state is terminal (done).
func jobTerminal(state string) bool { return jobTerminalStates[state] }

// jobTransitional reports whether a job state is settled by a later read.
func jobTransitional(state string) bool { return jobTransitionalStates[state] }

// jobNextState is the state a transitional job settles to on the next read.
// In k8s mode the executor goroutine drives the forward schedule
// (PENDING -> SETUP_DONE -> RUNNING -> terminal) with real driver signals, so
// advanceJob only settles the cancel progression; in mock mode there is no
// executor goroutine, so it walks the whole forward schedule on the clock.
func jobNextState(state string, longRunning, mock bool) string {
	switch state {
	case jobStateCancelPending:
		return jobStateCancelStarted
	case jobStateCancelStarted:
		return jobStateCancelled
	}
	if !mock {
		// k8s mode: the executor goroutine drives the forward schedule with real
		// driver signals. In particular ATTEMPT_FAILURE is left in place for a
		// restartable job to retry; only exhaustion reaches ERROR.
		return state
	}
	switch state {
	case jobStatePending:
		return jobStateSetupDone
	case jobStateSetupDone:
		return jobStateRunning
	case jobStateRunning:
		// A long-running (streaming) job's driver never exits, so a mock-mode
		// job stays RUNNING until it is cancelled. See store.Job.LongRunning.
		if longRunning {
			return jobStateRunning
		}
		return jobStateDone
	case jobStateAttemptFail:
		// Mock mode does not restart, so a failed attempt is terminal ERROR.
		return jobStateError
	}
	return state
}

// jobSubstateFor returns the substate a state carries on the wire. Per
// dataproc.v1.JobStatus only RUNNING has defined substates; the emulator
// reports QUEUED ("received, awaiting execution") while a job is live and omits
// substate entirely for every other state.
func jobSubstateFor(state string) string {
	if state == jobStateRunning {
		return substateRunning
	}
	return ""
}

// advanceJob lazily settles a transitional job once jobStateDelay has elapsed
// since it entered that state. It serializes with runJob/finishJob/CancelJob
// through UpdateJobAtomic, so a concurrent writer cannot double-apply a
// transition, and it closes the SubmitJobAsOperation LRO once the job reaches a
// terminal state.
func (s *Service) advanceJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error) {
	transitioned := false
	var prev dpstore.JobStatus
	j, err := s.store.UpdateJobAtomic(ctx, project, region, jobID, func(cur dpstore.Job) (dpstore.Job, error) {
		if !jobTransitional(cur.Status.State) || clock.Now().UTC().Before(cur.Status.StateStartTime.Add(s.jobStateDelay)) {
			return cur, nil
		}
		next := jobNextState(cur.Status.State, cur.LongRunning, s.mockMode())
		if next == cur.Status.State {
			return cur, nil
		}
		if next == jobStateDone && s.jobAttemptFailureHook != nil && s.jobAttemptFailureHook(project, region, jobID) {
			next = jobStateAttemptFail
		}
		transitioned = true
		prev = cur.Status
		cur.StatusHistory = append(cur.StatusHistory, cur.Status)
		cur.Status = dpstore.JobStatus{State: next, StateStartTime: clock.Now().UTC(), Substate: jobSubstateFor(next)}
		if jobTerminal(next) {
			ensureDriverOutputURIs(project, region, &cur)
		}
		return cur, nil
	})
	if err != nil {
		return j, err
	}
	if transitioned {
		s.emitJobStateChange(ctx, project, region, j, prev)
	}
	if transitioned && jobTerminal(j.Status.State) {
		// Mock mode has no executor goroutine to capture driver logs, so write a
		// synthetic output object (the URIs were allocated at submit) to keep
		// the advertised location resolvable.
		s.materializeDriverOutput(ctx, j, nil)
		s.completeSubmitOperation(ctx, project, region, j)
	}
	return j, nil
}

// setJobState moves a job from one of the from states to to, atomically. It is
// the executor side of the state machine (runJob writes SETUP_DONE/RUNNING as
// the driver progresses) and returns whether the transition applied: a state
// that changed underneath (e.g. a cancel landed first) leaves the job
// untouched rather than resurrecting it.
func (s *Service) setJobState(ctx context.Context, project, region, jobID string, from map[string]bool, to, details string) (dpstore.Job, bool) {
	applied := false
	var prev dpstore.JobStatus
	j, err := s.store.UpdateJobAtomic(ctx, project, region, jobID, func(cur dpstore.Job) (dpstore.Job, error) {
		if !from[cur.Status.State] {
			return cur, nil
		}
		applied = true
		prev = cur.Status
		cur.StatusHistory = append(cur.StatusHistory, cur.Status)
		cur.Status = dpstore.JobStatus{State: to, Details: details, StateStartTime: clock.Now().UTC(), Substate: jobSubstateFor(to)}
		return cur, nil
	})
	if err != nil {
		slog.Warn("dataproc: setJobState failed", "job", jobID, "state", to, "err", err)
		return dpstore.Job{}, false
	}
	if applied {
		s.emitJobStateChange(ctx, project, region, j, prev)
	}
	return j, applied
}
