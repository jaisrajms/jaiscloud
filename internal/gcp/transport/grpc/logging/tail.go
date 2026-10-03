package logging

import (
	"context"
	"errors"
	"io"
	"time"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	core "jaiscloud/internal/gcp/service/logging"
	"jaiscloud/internal/model"

	"google.golang.org/protobuf/types/known/durationpb"
)

// invalidFilterError wraps a CompileFilter parse error as the canonical
// InvalidArgument the gRPC layer maps to codes.InvalidArgument, matching the
// core's ListEntries error message.
func invalidFilterError(err error) error {
	return model.NewProviderError("InvalidArgument", "invalid filter: "+err.Error(), 400)
}

const (
	// defaultTailBufferWindow mirrors real Cloud Logging's default buffer_window.
	defaultTailBufferWindow = 2 * time.Second
	// maxTailBufferWindow caps a client-supplied buffer_window (spec: 0-60000 ms).
	maxTailBufferWindow = 60 * time.Second
	// minTailPollInterval keeps an explicit zero buffer_window from busy-spinning.
	minTailPollInterval = 50 * time.Millisecond
)

// tailPollInterval maps the request's buffer_window to the emulator's polling
// cadence. Real Cloud Logging holds entries server-side for up to buffer_window
// to smooth out late-arriving (out-of-order) entries; the emulator's store has
// no late-arrival reordering to absorb — List already returns every entry in
// (timestamp, id) order — so the window is realized as the bounded interval at
// which the tail re-reads the store. A nil/absent window uses the 2 s default.
func tailPollInterval(bw *durationpb.Duration) time.Duration {
	if bw == nil {
		return defaultTailBufferWindow
	}
	d := bw.AsDuration()
	if d < minTailPollInterval {
		return minTailPollInterval
	}
	if d > maxTailBufferWindow {
		return maxTailBufferWindow
	}
	return d
}

// tailScope resolves the scope parent from the request's resource_names,
// falling back to routing metadata and then the configured default — the same
// resolution ListLogEntries uses.
func (s *Service) tailScope(ctx context.Context, req *loggingpb.TailLogEntriesRequest) string {
	for _, rn := range req.GetResourceNames() {
		if scope, err := core.ParseScopeParent(rn); err == nil {
			return scope
		}
	}
	return s.defaultScope(ctx)
}

// TailLogEntries implements a bounded, store-polling approximation of Cloud
// Logging's streaming read. After accepting the initial request it streams log
// entries written after the stream began whose fields satisfy the same filter
// engine ListLogEntries uses. A client may send further requests to change the
// filter (and project); they are applied to subsequent polls.
//
// Approximation (documented in the core package): real Logging guarantees
// at-least-once delivery with per-response timestamp ordering and uses
// buffer_window to reorder late arrivals. The emulator instead records the
// store's monotonic write id at stream start and, on each poll, emits every
// entry with a larger id in (timestamp, id) order. This yields at-most-once
// delivery relative to the read snapshot, no server-side retention, and a
// latency bounded below by the poll interval derived from buffer_window (the
// 2 s default). It never returns Unimplemented and never busy-spins.
func (s *Service) TailLogEntries(stream loggingpb.LoggingServiceV2_TailLogEntriesServer) error {
	req, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}

	pred, err := core.CompileFilter(req.GetFilter())
	if err != nil {
		return mapError(invalidFilterError(err))
	}

	ctx := stream.Context()
	project := s.tailScope(ctx, req)
	lastID, err := s.core.LatestEntryID(ctx, project)
	if err != nil {
		return mapError(err)
	}
	interval := tailPollInterval(req.GetBufferWindow())

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// tailUpdate carries a client reconfiguration (a further
	// TailLogEntriesRequest) or the terminal Recv error. It is buffered so the
	// reader goroutine can always deliver one update and exit, even after the
	// main loop has returned.
	type tailUpdate struct {
		pred     core.Predicate
		project  string
		interval time.Duration
		err      error
	}
	updates := make(chan tailUpdate, 1)

	// Drain the client's send side on a dedicated goroutine: it applies filter
	// updates and, importantly, notices a half-close (Recv -> io.EOF) so the
	// session terminates instead of tailing forever.
	go func() {
		for {
			r, rerr := stream.Recv()
			if rerr != nil {
				select {
				case updates <- tailUpdate{err: rerr}:
				case <-ctx.Done():
				}
				return
			}
			p, perr := core.CompileFilter(r.GetFilter())
			if perr != nil {
				select {
				case updates <- tailUpdate{err: mapError(invalidFilterError(perr))}:
				case <-ctx.Done():
				}
				return
			}
			u := tailUpdate{
				pred:     p,
				project:  s.tailScope(ctx, r),
				interval: tailPollInterval(r.GetBufferWindow()),
			}
			select {
			case updates <- u:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case u := <-updates:
			if u.err != nil {
				if errors.Is(u.err, io.EOF) {
					return nil
				}
				return u.err
			}
			// A project change must re-seed the id cursor, otherwise the tail
			// would replay the new project's pre-existing backlog.
			if u.project != project {
				project = u.project
				lastID, err = s.core.LatestEntryID(ctx, project)
				if err != nil {
					return mapError(err)
				}
			}
			pred = u.pred
			if u.interval != interval {
				interval = u.interval
				ticker.Reset(interval)
			}
		case <-ticker.C:
			entries, lerr := s.core.ListScope(ctx, project)
			if lerr != nil {
				return mapError(lerr)
			}
			batch := make([]*loggingpb.LogEntry, 0)
			newLast := lastID
			for _, e := range entries {
				if e.ID <= lastID {
					continue
				}
				if e.ID > newLast {
					newLast = e.ID
				}
				if pred.Match(e) {
					batch = append(batch, entryToProto(e))
				}
			}
			lastID = newLast
			if len(batch) == 0 {
				continue
			}
			if serr := stream.Send(&loggingpb.TailLogEntriesResponse{Entries: batch}); serr != nil {
				if ctx.Err() != nil || errors.Is(serr, io.EOF) {
					return nil
				}
				return serr
			}
		}
	}
}
