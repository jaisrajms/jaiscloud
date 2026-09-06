package firestore

import (
	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// ChangeEvent describes a single document mutation published by the shared
// change-feed. Every write through the Service (REST or gRPC) publishes exactly
// one ChangeEvent, so Listen observes writes regardless of the transport that
// performed them. Doc is nil when the document was deleted.
type ChangeEvent struct {
	Seq  uint64
	Name string
	Doc  *firestorestore.Document
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

// ChangesSince returns the change-feed history for every event published after
// seq, in order. Listen uses it to replay deltas from a resume token.
func (s *Service) ChangesSince(seq uint64) []ChangeEvent {
	s.changeMu.Lock()
	defer s.changeMu.Unlock()
	out := make([]ChangeEvent, 0)
	for _, e := range s.changeLog {
		if e.Seq > seq {
			out = append(out, e)
		}
	}
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

// publishChange appends the event to the change-feed history (assigning the next
// monotonic sequence) and fans it out to every subscriber.
func (s *Service) publishChange(ev ChangeEvent) {
	s.changeMu.Lock()
	s.changeSeq++
	ev.Seq = s.changeSeq
	s.changeLog = append(s.changeLog, ev)
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
