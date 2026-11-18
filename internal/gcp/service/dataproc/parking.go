package dataproc

import "context"

// Name identifies the Dataproc hook in POST /_jaiscloud/park|resume.
func (s *Service) Name() string { return "dataproc" }

// Park quiesces Dataproc compute before the emulator is scaled down: every
// non-terminal job is cancelled (CancelJob moves it to CANCEL_PENDING and stops
// its executor). Dataproc clusters are control-plane-only records here — a
// RUNNING cluster with no driver is normal, and the next job creates a driver —
// so clusters are deliberately left alone and every record survives.
func (s *Service) Park(ctx context.Context, project string) (map[string]any, error) {
	jobs, err := s.ListAllJobs(ctx, project)
	if err != nil {
		return nil, err
	}
	cancelled := 0
	for _, j := range jobs {
		if jobTerminal(j.Status.State) ||
			j.Status.State == jobStateCancelPending ||
			j.Status.State == jobStateCancelStarted {
			continue
		}
		if _, err := s.CancelJob(ctx, j.ProjectID, j.Region, j.JobID); err != nil {
			return map[string]any{"jobsCancelled": cancelled}, err
		}
		cancelled++
	}
	return map[string]any{"jobsCancelled": cancelled}, nil
}

// Resume is a no-op: Dataproc has no always-on runtime to recreate. A paused
// job was cancelled, not suspended (real Dataproc has no job suspend), and the
// next submission starts its own driver.
func (s *Service) Resume(context.Context, string) error { return nil }
