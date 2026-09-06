package firestore

import (
	"encoding/binary"
	"errors"
	"io"
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
}

func (t listenTarget) isQuery() bool { return t.query != nil }

// listenSession carries the per-stream target registry plus a helper to send
// responses. All sends happen from the single Listen loop goroutine, so the
// stream is never written concurrently.
type listenSession struct {
	srv     *Service
	stream  firestorepb.Firestore_ListenServer
	targets map[int32]listenTarget
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
// valid resume token is supplied), then real-time DocumentChange/DocumentDelete
// events as writes are published by the shared provider Service's change-feed.
func (s *Service) Listen(stream firestorepb.Firestore_ListenServer) error {
	ctx := stream.Context()
	ls := &listenSession{
		srv:     s,
		stream:  stream,
		targets: make(map[int32]listenTarget),
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
	ls.targets[id] = target

	if err := ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_TargetChange{
		TargetChange: &firestorepb.TargetChange{
			TargetChangeType: firestorepb.TargetChange_ADD,
			TargetIds:        []int32{id},
		},
	}}); err != nil {
		return err
	}

	// A valid resume token replays only the deltas after it; an absent or
	// invalid token falls back to the full initial snapshot.
	incremental := false
	if rt, ok := t.GetResumeType().(*firestorepb.Target_ResumeToken); ok {
		if seq, valid := DecodeResumeToken(rt.ResumeToken); valid {
			incremental = true
			for _, ev := range ls.srv.svc.ChangesSince(seq) {
				if ls.targetMatches(target, ev) {
					if err := ls.sendChange(id, ev); err != nil {
						return err
					}
				}
			}
		}
	}
	if !incremental {
		if err := ls.sendSnapshot(id, target); err != nil {
			return err
		}
	}

	if err := ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_TargetChange{
		TargetChange: &firestorepb.TargetChange{
			TargetChangeType: firestorepb.TargetChange_CURRENT,
			TargetIds:        []int32{id},
			ResumeToken:      EncodeResumeToken(ls.srv.svc.CurrentSeq()),
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
// targets on this stream"), a monotonic read_time, and the current change-feed
// resume token. The SDK concludes a snapshot epoch on this frame.
func (ls *listenSession) sendNoChange() error {
	return ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_TargetChange{
		TargetChange: &firestorepb.TargetChange{
			TargetChangeType: firestorepb.TargetChange_NO_CHANGE,
			ReadTime:         timestamppb.New(ls.nextReadTime()),
			ResumeToken:      EncodeResumeToken(ls.srv.svc.CurrentSeq()),
		},
	}})
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

func (ls *listenSession) handleChange(ev firestoreprovider.ChangeEvent) {
	sent := false
	for id, t := range ls.targets {
		if ls.targetMatches(t, ev) {
			_ = ls.sendChange(id, ev)
			sent = true
		}
	}
	if !sent {
		return
	}
	// Conclude the change epoch with a NO_CHANGE frame so the SDK returns the
	// next snapshot: the SDK concludes every snapshot (initial and subsequent)
	// on NO_CHANGE, so real-time deltas would otherwise never surface through
	// Snapshots.
	_ = ls.sendNoChange()
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
// state of a target.
func (ls *listenSession) sendSnapshot(id int32, t listenTarget) error {
	if t.isQuery() {
		project, database, rel, ok := splitParent(t.parent)
		if !ok {
			return status.Error(codes.InvalidArgument, "invalid query parent resource name")
		}
		if project == "" {
			project = ls.srv.resolveProject(ls.stream.Context())
		}
		docs, err := ls.srv.svc.RunQuery(ls.stream.Context(), project, database, rel, t.query, nil)
		if err != nil {
			return mapError(err)
		}
		for _, d := range docs {
			if err := ls.sendDocumentChange(id, d); err != nil {
				return err
			}
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
		if err := ls.sendDocumentChange(id, doc); err != nil {
			return err
		}
	}
	return nil
}

func (ls *listenSession) sendDocumentChange(id int32, d firestorestore.Document) error {
	return ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_DocumentChange{
		DocumentChange: &firestorepb.DocumentChange{
			Document:  encodeDocument(d),
			TargetIds: []int32{id},
		},
	}})
}

func (ls *listenSession) sendChange(id int32, ev firestoreprovider.ChangeEvent) error {
	if ev.Doc == nil {
		return ls.send(&firestorepb.ListenResponse{ResponseType: &firestorepb.ListenResponse_DocumentDelete{
			DocumentDelete: &firestorepb.DocumentDelete{Document: ev.Name},
		}})
	}
	return ls.sendDocumentChange(id, *ev.Doc)
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
