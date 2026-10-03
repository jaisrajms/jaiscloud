package firestore

import (
	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// changeLogRetention is the maximum number of recent change events retained for
// resume-token replay. Real Firestore keeps change history only for a bounded
// window (resume tokens expire); this bounds the emulator's memory under a
// steady write load. History beyond the window is dropped oldest-first and
// ChangeFloor advances past it, so a resume token below the floor is treated as
// expired rather than silently replayed from a partial history.
const changeLogRetention = 10_000

// ChangeEvent describes a single document mutation published by the shared
// change-feed. Every write through the Service (REST or gRPC) publishes exactly
// one ChangeEvent, so Listen observes writes regardless of the transport that
// performed them. Doc is nil when the document was deleted.
type ChangeEvent struct {
	Seq  uint64
	Name string
	Doc  *firestorestore.Document
}

// changeRing is a fixed-capacity FIFO of ChangeEvents. Once full it overwrites
// the oldest entry, so its memory use is exactly capacity entries no matter how
// many writes the Service has observed.
type changeRing struct {
	buf   []ChangeEvent
	head  int // index of the oldest retained event
	count int
}

// newChangeRing returns a ring that retains at most capacity events. A
// non-positive capacity yields a ring that retains nothing (add is a no-op).
func newChangeRing(capacity int) changeRing {
	if capacity < 0 {
		capacity = 0
	}
	return changeRing{buf: make([]ChangeEvent, capacity)}
}

// add appends ev. When the ring is full it overwrites the oldest entry and
// returns that evicted event with ok=true; otherwise it returns the zero event
// and ok=false.
func (r *changeRing) add(ev ChangeEvent) (ChangeEvent, bool) {
	if len(r.buf) == 0 {
		return ChangeEvent{}, false
	}
	if r.count < len(r.buf) {
		r.buf[(r.head+r.count)%len(r.buf)] = ev
		r.count++
		return ChangeEvent{}, false
	}
	evicted := r.buf[r.head]
	r.buf[r.head] = ev
	r.head = (r.head + 1) % len(r.buf)
	return evicted, true
}

// reset empties the ring without releasing its backing array.
func (r *changeRing) reset() {
	r.head = 0
	r.count = 0
}

// each invokes fn for every retained event, oldest first.
func (r *changeRing) each(fn func(ChangeEvent)) {
	for i := 0; i < r.count; i++ {
		fn(r.buf[(r.head+i)%len(r.buf)])
	}
}

// changeSub is one subscriber to the shared change-feed.
type changeSub struct {
	ch   chan ChangeEvent
	done chan struct{}
}

// ChangeSubscription is a live subscription to the shared change-feed. C
// delivers each published ChangeEvent in sequence order; Cancel unregisters the
// subscription.
type ChangeSubscription struct {
	svc *Service
	sub *changeSub
	C   <-chan ChangeEvent
}

// Cancel unregisters the subscription from the change-feed. Safe to call more
// than once.
func (cs *ChangeSubscription) Cancel() {
	cs.svc.unsubscribeChange(cs.sub)
}

// SubscribeChange registers a new subscriber on the shared change-feed. The
// caller MUST call Cancel when done, or the subscription (and its buffered
// channel) will leak.
func (s *Service) SubscribeChange() *ChangeSubscription {
	sub := s.subscribeChange()
	return &ChangeSubscription{svc: s, sub: sub, C: sub.ch}
}

// CurrentSeq returns the change-feed's current monotonic sequence number. It is
// the sequence encoded into snapshot resume tokens.
func (s *Service) CurrentSeq() uint64 {
	s.changeMu.Lock()
	defer s.changeMu.Unlock()
	return s.changeSeq
}

// ChangeFloor returns the sequence number of the most recently evicted change
// event (0 when no history has been evicted yet). The feed can replay faithfully
// only from a sequence at or above the floor; a resume token below it must fall
// back to a snapshot because the deltas it depends on are gone.
func (s *Service) ChangeFloor() uint64 {
	s.changeMu.Lock()
	defer s.changeMu.Unlock()
	return s.changeFloor
}

// ReplayableFrom reports whether ChangesSince(seq) can reconstruct the feed
// faithfully. It is false when seq is below the retention floor (its deltas were
// evicted) or when seq is ahead of the current sequence (a token from before a
// Reset). Callers treat a non-replayable token as invalid and resnapshot.
func (s *Service) ReplayableFrom(seq uint64) bool {
	s.changeMu.Lock()
	defer s.changeMu.Unlock()
	return seq >= s.changeFloor && seq <= s.changeSeq
}

// ChangesSince returns the retained change-feed history for every event
// published after seq, in order. Listen uses it to replay deltas from a resume
// token. A seq below ChangeFloor() cannot be replayed completely; callers should
// check ReplayableFrom first.
func (s *Service) ChangesSince(seq uint64) []ChangeEvent {
	s.changeMu.Lock()
	defer s.changeMu.Unlock()
	out := make([]ChangeEvent, 0)
	s.changeLog.each(func(e ChangeEvent) {
		if e.Seq > seq {
			out = append(out, e)
		}
	})
	return out
}

func (s *Service) subscribeChange() *changeSub {
	s.changeMu.Lock()
	defer s.changeMu.Unlock()
	if s.changeSubs == nil {
		s.changeSubs = make(map[*changeSub]struct{})
	}
	sub := &changeSub{ch: make(chan ChangeEvent, 1024), done: make(chan struct{})}
	s.changeSubs[sub] = struct{}{}
	return sub
}

func (s *Service) unsubscribeChange(sub *changeSub) {
	s.changeMu.Lock()
	defer s.changeMu.Unlock()
	if _, ok := s.changeSubs[sub]; ok {
		delete(s.changeSubs, sub)
		close(sub.done)
	}
}

// retention returns the effective change-log capacity: the test override when
// set, otherwise the package default.
func (s *Service) retention() int {
	if s.changeRetentionOverride > 0 {
		return s.changeRetentionOverride
	}
	return changeLogRetention
}

// publishChange appends the event to the change-feed history (assigning the next
// monotonic sequence) and fans it out to every subscriber. The history keeps
// only the most recent retention() events; once full, the oldest entry is evicted
// and the floor advances to its sequence.
func (s *Service) publishChange(ev ChangeEvent) {
	s.changeMu.Lock()
	if len(s.changeLog.buf) == 0 {
		s.changeLog = newChangeRing(s.retention())
	}
	s.changeSeq++
	ev.Seq = s.changeSeq
	if evicted, ok := s.changeLog.add(ev); ok {
		s.changeFloor = evicted.Seq
	}
	subs := make([]*changeSub, 0, len(s.changeSubs))
	for sub := range s.changeSubs {
		subs = append(subs, sub)
	}
	s.changeMu.Unlock()

	for _, sub := range subs {
		select {
		case sub.ch <- ev:
		case <-sub.done:
		default:
			// The subscriber's buffered channel is full — it has fallen behind
			// and is not draining events. Drop this event and detach the
			// subscriber so the write path never blocks on a listener and no
			// further events are attempted for it. A subscriber that falls
			// behind misses a delta and will NOT receive a later NO_CHANGE
			// covering it, so it must re-listen (from a fresh or resume token)
			// to resync.
			s.detachChange(sub)
		}
	}
}

// detachChange removes a subscriber that has fallen behind from the change-feed
// and closes its done channel so a Listen stream can observe the teardown and
// exit. It is a no-op for an already-removed subscriber.
func (s *Service) detachChange(sub *changeSub) {
	s.changeMu.Lock()
	defer s.changeMu.Unlock()
	if _, ok := s.changeSubs[sub]; ok {
		delete(s.changeSubs, sub)
		close(sub.done)
	}
}
