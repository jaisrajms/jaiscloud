package firestore

import (
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"strings"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"jaiscloud/internal/clock"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EncodeResumeToken encodes a monotonic sequence number into an 8-byte resume
// token (big-endian uint64).
func EncodeResumeToken(seq uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, seq)
	return b
}

// DecodeResumeToken decodes an 8-byte resume token back into a sequence number.
// ok is false for any token that is not exactly 8 bytes.
func DecodeResumeToken(b []byte) (uint64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}

// listenTarget is a resolved watch target on a single Listen stream. Exactly
// one of query / documents is set.
type listenTarget struct {
	query     *firestoreprovider.StructuredQuery
	parent    string
	documents []string

	// seq is this target's own delivered sequence position: the highest
	// change-feed sequence it has been brought up to (by snapshot, incremental
	// replay, or a live delta). Real Firestore resume tokens are per-target;
	// encoding seq rather than the global head in this target's TargetChange
	// frames is the emulator's approximation of that cursor (see the package
	// doc's Listen fidelity note).
	seq uint64

	// view is the set of document names currently in this target's result set
	// (the snapshot, adjusted by every live delta since). A live change is
	// diffed against it to decide whether the document enters the view
	// (DocumentChange), leaves it (DocumentRemove), or was deleted
	// (DocumentDelete).
	view map[string]struct{}
}

func (t listenTarget) isQuery() bool { return t.query != nil }

// deltaKind classifies a per-document delta observed by one target while
// diffing a live change against its view.
type deltaKind int

const (
	// deltaChange means the document is (now) present in the target's result
	// set: an add, or a content update to a document already in it.
	deltaChange deltaKind = iota
	// deltaRemove means the document left the target's result set while still
	// existing in the store (e.g. it stopped matching a where predicate).
	deltaRemove
	// deltaDelete means the document was deleted from the store.
	deltaDelete
)

// listenDelta is the per-target effect of one live change on one document.
// handleChange collects these across every target before emitting canonical
// frames, so a document's deltas on all targets are aggregated into a single
// change epoch (see handleChange).
type listenDelta struct {
	kind deltaKind
	name string
	doc  *firestorestore.Document
}

// listenSession carries the per-stream target registry plus a helper to send
// responses. All sends happen from the single Listen loop goroutine, so the
// stream is never written concurrently.
type listenSession struct {
	srv     *Service
	stream  firestorepb.Firestore_ListenServer
	targets map[int32]*listenTarget
	nextID  int32

	// lastReadTime is the most recent read_time handed out on this stream.
	// read_time must be non-decreasing across the stream (the Go Firestore SDK
	// requires each snapshot's read_time to be valid and consistent), so it is
	// advanced monotonically rather than read directly from the wall clock,
	// which can jump backwards on NTP adjustment.
	lastReadTime time.Time
}

// nextReadTime returns a read_time strictly greater than every read_time
// previously returned on this stream. It reads the shared clock and bumps it
// past lastReadTime when the clock has not advanced (or has moved backwards),
// guaranteeing monotonicity.
func (ls *listenSession) nextReadTime() time.Time {
	now := clock.Now()
	if !now.After(ls.lastReadTime) {
		now = ls.lastReadTime.Add(time.Nanosecond)
	}
	ls.lastReadTime = now
	return now
}

// Listen implements the bidirectional streaming Firestore.Listen RPC. For each
// added target it streams an initial snapshot (or incremental deltas when a
// valid resume token is supplied), then real-time
// DocumentChange/DocumentDelete/DocumentRemove frames as writes are published
// by the shared provider Service's change-feed. A change epoch's frames are
// aggregated per document across every target on the stream (see handleChange).
func (s *Service) Listen(stream firestorepb.Firestore_ListenServer) error {
	ctx := stream.Context()
	ls := &listenSession{
		srv:     s,
		stream:  stream,
		targets: make(map[int32]*listenTarget),
	}

	sub := s.svc.SubscribeChange()
	defer sub.Cancel()

	reqCh := make(chan *firestorepb.ListenRequest, 8)
	recvErr := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case reqCh <- req:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-recvErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case req := <-reqCh:
			if err := ls.handleRequest(req); err != nil {
				return err
			}
		case ev := <-sub.C:
			ls.handleChange(ev)
		}
	}
}

func (ls *listenSession) send(resp *firestorepb.ListenResponse) error {
	return ls.stream.Send(resp)
}

func (ls *listenSession) handleRequest(req *firestorepb.ListenRequest) error {
	switch tc := req.GetTargetChange().(type) {
	case *firestorepb.ListenRequest_AddTarget:
		return ls.handleAddTarget(tc.AddTarget)
	case *firestorepb.ListenRequest_RemoveTarget:
		return ls.handleRemoveTarget(tc.RemoveTarget)
	}
	return nil
}

func (ls *listenSession) handleAddTarget(t *firestorepb.Target) error {
	if t == nil {
		return status.Error(codes.InvalidArgument, "add_target requires a target")
	}
	id := t.GetTargetId()
	if id == 0 {
		id = ls.assignTargetID()
	}

	target, err := resolveTarget(t)
	if err != nil {
		return err
	}
	lt := &target
	lt.view = map[string]struct{}{}
	ls.targets[id] = lt

	if err := ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_TargetChange{
		TargetChange: &firestorepb.TargetChange{
			TargetChangeType: firestorepb.TargetChange_ADD,
			TargetIds:        []int32{id},
		},
	}}); err != nil {
		return err
	}

	// Capture the feed position this target is about to observe, before reading
	// the snapshot/replay. Any write published after this point is still
	// buffered on the stream subscription and delivered by handleChange, so
	// advancing the cursor to it can never advertise a delta that was not (or
	// will not be) delivered — unlike reading the head after the snapshot, which
	// could run past a buffered event. Reads that land between this position and
	// the snapshot store-read are seen twice (snapshot + delta), never lost.
	base := ls.srv.svc.CurrentSeq()

	// A valid, unexpired resume token replays only this target's deltas after
	// it; an absent, malformed, evicted, or pre-reset token cannot be honored,
	// so the target is reset and given a fresh snapshot. seq == 0 is treated as
	// the start of time (not a resumable position) because the change log
	// records only writes, not a document history, so replaying from zero
	// cannot reconstruct documents that predate the log.
	incremental := false
	if rt, ok := t.GetResumeType().(*firestorepb.Target_ResumeToken); ok {
		if seq, valid := DecodeResumeToken(rt.ResumeToken); valid && seq > 0 && ls.srv.svc.ReplayableFrom(seq) {
			incremental = true
			for _, ev := range ls.srv.svc.ChangesSince(seq) {
				if ls.targetMatches(*lt, ev) {
					if err := ls.sendChange(id, ev); err != nil {
						return err
					}
					if ev.Seq > lt.seq {
						lt.seq = ev.Seq
					}
				}
			}
			// Seed the view from current state so the first live delta diffs
			// against the resumed result set instead of re-emitting all of it.
			if err := ls.seedView(lt); err != nil {
				return err
			}
		} else if err := ls.sendReset(id); err != nil {
			return err
		}
	}
	if !incremental {
		if err := ls.sendSnapshot(id, lt); err != nil {
			return err
		}
	}

	// The target is now caught up through `base` (and through any later delta it
	// replayed). Deltas published after `base` arrive via handleChange and advance
	// the cursor from there.
	if base > lt.seq {
		lt.seq = base
	}

	if err := ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_TargetChange{
		TargetChange: &firestorepb.TargetChange{
			TargetChangeType: firestorepb.TargetChange_CURRENT,
			TargetIds:        []int32{id},
			ResumeToken:      EncodeResumeToken(lt.seq),
			ReadTime:         timestamppb.New(ls.nextReadTime()),
		},
	}}); err != nil {
		return err
	}

	// Emit NO_CHANGE after CURRENT: the Go Firestore SDK concludes a snapshot
	// only on a TargetChange with TargetChangeType NO_CHANGE, empty target_ids
	// ("all targets on this stream"), a non-nil read_time, and a non-empty
	// resume token (see cloud.google.com/go/firestore watch.go). Without it,
	// Collection.Snapshots / DocumentRef.Snapshots / OnSnapshotsInSync hang.
	return ls.sendNoChange()
}

// sendNoChange emits a NO_CHANGE frame with empty target_ids (meaning "all
// targets on this stream"), a monotonic read_time, and a resume token covering
// all targets. The SDK concludes a snapshot epoch on this frame.
func (ls *listenSession) sendNoChange() error {
	return ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_TargetChange{
		TargetChange: &firestorepb.TargetChange{
			TargetChangeType: firestorepb.TargetChange_NO_CHANGE,
			ReadTime:         timestamppb.New(ls.nextReadTime()),
			ResumeToken:      EncodeResumeToken(ls.allTargetsSeq()),
		},
	}})
}

// allTargetsSeq returns the resume position advertised on the empty target_ids
// NO_CHANGE frame. The proto defines that token as covering "all targets", which
// a single 8-byte token cannot represent exactly once targets sit at different
// positions; real Firestore instead pairs each resume token with its own
// target_ids. As the safe approximation this returns the minimum of the live
// targets' cursors (or the global head when there are no targets): a position at
// or below every target's delivered seq means resuming all targets can replay a
// duplicate delta but never skip one. Note the Go SDK watches a single target per
// stream, so there this is exactly that target's cursor.
func (ls *listenSession) allTargetsSeq() uint64 {
	if len(ls.targets) == 0 {
		return ls.srv.svc.CurrentSeq()
	}
	var min uint64
	first := true
	for _, t := range ls.targets {
		if first || t.seq < min {
			min = t.seq
			first = false
		}
	}
	return min
}

func (ls *listenSession) handleRemoveTarget(id int32) error {
	delete(ls.targets, id)
	return ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_TargetChange{
		TargetChange: &firestorepb.TargetChange{
			TargetChangeType: firestorepb.TargetChange_REMOVE,
			TargetIds:        []int32{id},
		},
	}})
}

// handleChange applies one live change to every target on the stream, then
// emits one canonical frame per affected document aggregating that document's
// deltas across all targets (real Firestore's change-epoch semantics): a
// DocumentChange carries its target_ids plus any removed_target_ids, a
// DocumentRemove carries the targets it left, and a DocumentDelete carries the
// targets it was removed from. Accumulating per target and flushing once avoids
// the per-target duplicate frames an emulator would otherwise emit when a
// document is watched by more than one target.
func (ls *listenSession) handleChange(ev firestoreprovider.ChangeEvent) {
	// agg is a document's accumulated state across every target that observed
	// the change. doc is the last non-nil body seen (the present state);
	// deleted records that some target saw the document deleted.
	type agg struct {
		doc        *firestorestore.Document
		deleted    bool
		targetIDs  map[int32]bool
		removedIDs map[int32]bool
	}

	aggs := make(map[string]*agg)
	anyChanged := false
	for id, t := range ls.targets {
		deltas, changed := ls.computeChange(t, ev)
		if !changed {
			continue
		}
		if ev.Seq > t.seq {
			t.seq = ev.Seq
		}
		anyChanged = true
		for _, d := range deltas {
			a := aggs[d.name]
			if a == nil {
				a = &agg{targetIDs: map[int32]bool{}, removedIDs: map[int32]bool{}}
				aggs[d.name] = a
			}
			switch d.kind {
			case deltaChange:
				a.doc = d.doc
				a.targetIDs[id] = true
			case deltaRemove:
				a.removedIDs[id] = true
			case deltaDelete:
				a.deleted = true
				a.removedIDs[id] = true
			}
		}
	}
	if !anyChanged {
		return
	}

	// Flush in sorted document-name order so the frame order is deterministic
	// across runs (the target map is unordered).
	names := make([]string, 0, len(aggs))
	for name := range aggs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		a := aggs[name]
		removed := sortedTargetIDs(a.removedIDs)
		switch {
		case a.deleted:
			_ = ls.sendDocumentDelete(removed, name)
		case len(a.targetIDs) > 0:
			_ = ls.sendDocumentChange(sortedTargetIDs(a.targetIDs), removed, *a.doc)
		default:
			_ = ls.sendDocumentRemove(removed, name)
		}
	}

	// Conclude the change epoch with a NO_CHANGE frame so the SDK returns the
	// next snapshot: the SDK concludes every snapshot (initial and subsequent)
	// on NO_CHANGE, so real-time deltas would otherwise never surface through
	// Snapshots.
	_ = ls.sendNoChange()
}

// sortedTargetIDs returns the sorted, de-duplicated target ids in m (nil when
// empty) for a canonical, deterministic wire frame.
func sortedTargetIDs(m map[int32]bool) []int32 {
	if len(m) == 0 {
		return nil
	}
	ids := make([]int32, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// computeChange diffs one live change against a target's current view, updating
// the view and returning the deltas it produced. changed reports whether the
// target observed anything at all (a non-empty delta list). It does not send:
// handleChange aggregates the returned deltas across every target on the stream
// before emitting canonical frames, so a document's target_ids and
// removed_target_ids land on a single frame.
func (ls *listenSession) computeChange(t *listenTarget, ev firestoreprovider.ChangeEvent) (deltas []listenDelta, changed bool) {
	if !ls.targetMatches(*t, ev) {
		return nil, false
	}
	if !t.isQuery() {
		if ev.Doc == nil {
			delete(t.view, ev.Name)
			return []listenDelta{{kind: deltaDelete, name: ev.Name}}, true
		}
		t.view[ev.Name] = struct{}{}
		return []listenDelta{{kind: deltaChange, name: ev.Name, doc: ev.Doc}}, true
	}

	// Re-run the target's query so where/order_by/limit apply to the delta: a
	// document that stops matching (or is pushed out by a limit) is no longer in
	// the new result set, and one that starts matching (or moves into a limit)
	// is. A delete is delivered by scope alone (below) because its body is gone.
	docs, err := ls.queryDocs(t)
	if err != nil {
		return nil, false
	}
	newView := make(map[string]firestorestore.Document, len(docs))
	for _, d := range docs {
		newView[d.Name] = d
	}

	for name := range t.view {
		if _, ok := newView[name]; ok {
			continue
		}
		if ev.Doc == nil && name == ev.Name {
			continue // emitted as a delete below
		}
		deltas = append(deltas, listenDelta{kind: deltaRemove, name: name})
	}
	if ev.Doc == nil {
		deltas = append(deltas, listenDelta{kind: deltaDelete, name: ev.Name})
	}
	for name, d := range newView {
		if _, ok := t.view[name]; ok {
			if name == ev.Name && ev.Doc != nil {
				deltas = append(deltas, listenDelta{kind: deltaChange, name: name, doc: ev.Doc})
			}
			continue
		}
		doc := d
		if name == ev.Name && ev.Doc != nil {
			doc = *ev.Doc
		}
		deltas = append(deltas, listenDelta{kind: deltaChange, name: name, doc: &doc})
	}

	t.view = make(map[string]struct{}, len(newView))
	for name := range newView {
		t.view[name] = struct{}{}
	}
	return deltas, len(deltas) > 0
}

func (ls *listenSession) assignTargetID() int32 {
	ls.nextID++
	if ls.nextID == 0 {
		ls.nextID = 1
	}
	return ls.nextID
}

func resolveTarget(t *firestorepb.Target) (listenTarget, error) {
	switch tt := t.GetTargetType().(type) {
	case *firestorepb.Target_Query:
		q, err := decodeStructuredQuery(tt.Query.GetStructuredQuery())
		if err != nil {
			return listenTarget{}, mapError(model.NewProviderError("InvalidArgument", err.Error(), 400))
		}
		return listenTarget{query: q, parent: tt.Query.GetParent()}, nil
	case *firestorepb.Target_Documents:
		return listenTarget{documents: tt.Documents.GetDocuments()}, nil
	default:
		return listenTarget{}, status.Error(codes.InvalidArgument, "target must specify a query or documents")
	}
}

// sendSnapshot streams one DocumentChange per matching document for the initial
// state of a target and records those documents in the target's view.
func (ls *listenSession) sendSnapshot(id int32, t *listenTarget) error {
	if t.isQuery() {
		docs, err := ls.queryDocs(t)
		if err != nil {
			return mapError(err)
		}
		for _, d := range docs {
			if err := ls.sendDocumentChange([]int32{id}, nil, d); err != nil {
				return err
			}
			t.view[d.Name] = struct{}{}
		}
		return nil
	}
	for _, name := range t.documents {
		doc, err := ls.srv.svc.GetDocument(ls.stream.Context(), name, nil, nil)
		if err != nil {
			var pe *model.ProviderError
			if errors.As(err, &pe) && pe.HTTPStatus == 404 {
				continue
			}
			return mapError(err)
		}
		if err := ls.sendDocumentChange([]int32{id}, nil, doc); err != nil {
			return err
		}
		t.view[doc.Name] = struct{}{}
	}
	return nil
}

// queryDocs runs a query target's StructuredQuery against the current store
// state, applying collection scope, where, order_by, and limit.
func (ls *listenSession) queryDocs(t *listenTarget) ([]firestorestore.Document, error) {
	project, database, rel, ok := splitParent(t.parent)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid query parent resource name")
	}
	if project == "" {
		project = ls.srv.resolveProject(ls.stream.Context())
	}
	return ls.srv.svc.RunQuery(ls.stream.Context(), project, database, rel, t.query, nil)
}

// seedView populates a target's view from current state without emitting a
// snapshot. It is used after an incremental replay so subsequent live deltas
// diff against the resumed result set rather than re-emitting all of it.
func (ls *listenSession) seedView(t *listenTarget) error {
	if t.isQuery() {
		docs, err := ls.queryDocs(t)
		if err != nil {
			return mapError(err)
		}
		for _, d := range docs {
			t.view[d.Name] = struct{}{}
		}
		return nil
	}
	for _, name := range t.documents {
		doc, err := ls.srv.svc.GetDocument(ls.stream.Context(), name, nil, nil)
		if err != nil {
			var pe *model.ProviderError
			if errors.As(err, &pe) && pe.HTTPStatus == 404 {
				continue
			}
			return mapError(err)
		}
		t.view[doc.Name] = struct{}{}
	}
	return nil
}

func (ls *listenSession) sendReset(id int32) error {
	return ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_TargetChange{
		TargetChange: &firestorepb.TargetChange{
			TargetChangeType: firestorepb.TargetChange_RESET,
			TargetIds:        []int32{id},
		},
	}})
}

// sendDocumentChange emits one DocumentChange. targetIDs is the set of targets
// the document is (now) in; removedTargetIDs, when non-empty, names the targets
// the same change removes it from. The Go SDK watches a single target per
// stream, so it observes a one-element target_ids and an empty
// removed_target_ids.
func (ls *listenSession) sendDocumentChange(targetIDs, removedTargetIDs []int32, d firestorestore.Document) error {
	return ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_DocumentChange{
		DocumentChange: &firestorepb.DocumentChange{
			Document:         encodeDocument(d),
			TargetIds:        targetIDs,
			RemovedTargetIds: removedTargetIDs,
		},
	}})
}

// sendDocumentDelete emits one DocumentDelete for a document removed from the
// store, naming every target that was watching it in removed_target_ids.
func (ls *listenSession) sendDocumentDelete(targetIDs []int32, name string) error {
	return ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_DocumentDelete{
		DocumentDelete: &firestorepb.DocumentDelete{
			Document:         name,
			RemovedTargetIds: targetIDs,
		},
	}})
}

// sendDocumentRemove emits one DocumentRemove for a document that left the
// target(s)' result set but still exists, naming them in removed_target_ids.
func (ls *listenSession) sendDocumentRemove(targetIDs []int32, name string) error {
	return ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_DocumentRemove{
		DocumentRemove: &firestorepb.DocumentRemove{
			Document:         name,
			RemovedTargetIds: targetIDs,
		},
	}})
}

// sendChange emits the resume-replay delta for one target: a DocumentDelete
// when the replayed event was a deletion, otherwise a DocumentChange. It is the
// single-target analogue of the live handleChange path.
func (ls *listenSession) sendChange(id int32, ev firestoreprovider.ChangeEvent) error {
	if ev.Doc == nil {
		return ls.sendDocumentDelete([]int32{id}, ev.Name)
	}
	return ls.sendDocumentChange([]int32{id}, nil, *ev.Doc)
}

func (ls *listenSession) targetMatches(t listenTarget, ev firestoreprovider.ChangeEvent) bool {
	if t.documents != nil {
		for _, name := range t.documents {
			if name == ev.Name {
				return true
			}
		}
		return false
	}
	if t.query == nil {
		return false
	}
	return changeMatchesQuery(t.query, t.parent, ev.Name)
}

// changeMatchesQuery reports whether a document (identified by name) is in the
// collection scope of a query target. It mirrors the provider's
// matchesCollection semantics but operates on a bare name so deletes can be
// matched without a document body.
func changeMatchesQuery(q *firestoreprovider.StructuredQuery, parent, name string) bool {
	project, database, path, ok := firestorestore.ParseDocumentName(name)
	if !ok {
		return false
	}
	segs := strings.Split(path, "/")
	if len(segs) < 2 {
		return false
	}
	collID := segs[len(segs)-2]
	docParent := "projects/" + project + "/databases/" + database + "/documents/" + strings.Join(segs[:len(segs)-1], "/")

	if len(q.From) == 0 {
		return strings.HasPrefix(name, parent+"/")
	}
	for _, sel := range q.From {
		if sel.CollectionID != collID {
			continue
		}
		if sel.AllDescendants {
			if strings.HasPrefix(name, parent+"/") {
				return true
			}
			continue
		}
		if docParent == parent+"/"+sel.CollectionID {
			return true
		}
	}
	return false
}
