package firestore

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// writeStream carries the per-stream Write state. The Write RPC is a
// bidirectional stream with a strict handshake:
//
//  1. First client msg: database set; stream_id empty (new) or set (resume);
//     writes empty; stream_token empty.
//  2. Server first response: stream_id (new only) + stream_token (always,
//     monotonic) + write_results + commit_time.
//  3. Subsequent client msgs: writes non-empty (may be empty on the final msg);
//     stream_token echoes the token from the most recent WriteResponse (an ack
//     on every msg).
//  4. Final msg may carry labels; the server completes the batch and closes.
//
// The emulator commits each message's writes synchronously, so there are never
// unacknowledged responses to replay on resume: a resume handshake simply
// acknowledges the completed batch with an up-to-date token.
type writeStream struct {
	srv    *Service
	stream firestorepb.Firestore_WriteServer

	started   bool
	database  string
	streamID  string
	lastToken uint64
}

// Write implements the bidirectional streaming Firestore.Write RPC. Each
// non-handshake message's writes are applied atomically via the shared
// provider Service.Commit, and the per-write results are streamed back in a
// WriteResponse. The handler returns nil on a clean client close (EOF).
func (s *Service) Write(stream firestorepb.Firestore_WriteServer) error {
	ws := &writeStream{srv: s, stream: stream}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := ws.handle(req); err != nil {
			return err
		}
	}
}

// handle routes a single WriteRequest through the handshake or write path.
func (ws *writeStream) handle(req *firestorepb.WriteRequest) error {
	if !ws.started {
		return ws.handshake(req)
	}
	return ws.handleWrites(req)
}

// handshake processes the first message: it establishes (or resumes) the stream
// and returns the initial WriteResponse carrying the stream id (new streams
// only) and a monotonic stream token.
func (ws *writeStream) handshake(req *firestorepb.WriteRequest) error {
	if len(req.GetWrites()) != 0 {
		return status.Error(codes.InvalidArgument, "writes must be empty on the first WriteRequest")
	}
	ws.started = true
	if req.GetDatabase() == "" {
		return status.Error(codes.InvalidArgument, "database is required on the first WriteRequest")
	}
	ws.database = req.GetDatabase()

	resume := false
	if req.GetStreamId() != "" {
		ws.streamID = req.GetStreamId()
		resume = true
	} else {
		ws.streamID = newStreamID()
	}

	token := ws.srv.nextWriteToken()
	ws.lastToken = token

	resp := &firestorepb.WriteResponse{StreamToken: EncodeResumeToken(token)}
	if !resume {
		resp.StreamId = ws.streamID
	}
	// A resume handshake acknowledges the completed batch with only an
	// up-to-date token (no stream id, results, or commit time).
	return ws.stream.Send(resp)
}

// handleWrites applies one message's writes and streams back the per-write
// results plus a fresh stream token. An empty message (the final flush) is
// acknowledged with a token only.
func (ws *writeStream) handleWrites(req *firestorepb.WriteRequest) error {
	// The client echoes the token from the most recent WriteResponse. It is not
	// revalidated here: the emulator commits synchronously and always advances
	// its own token, so a stale or absent echo is harmless.
	if len(req.GetWrites()) == 0 {
		token := ws.srv.nextWriteToken()
		ws.lastToken = token
		return ws.stream.Send(&firestorepb.WriteResponse{StreamToken: EncodeResumeToken(token)})
	}

	writes, err := decodeWrites(req.GetWrites())
	if err != nil {
		return mapError(model.NewProviderError("InvalidArgument", err.Error(), 400))
	}
	commitTime, results, err := ws.srv.svc.Commit(ws.stream.Context(), nil, writes)
	if err != nil {
		return mapError(err)
	}
	wr, err := commitResultsToProto(results)
	if err != nil {
		return mapError(err)
	}
	token := ws.srv.nextWriteToken()
	ws.lastToken = token
	return ws.stream.Send(&firestorepb.WriteResponse{
		StreamToken:  EncodeResumeToken(token),
		WriteResults: wr,
		CommitTime:   timestamppb.New(commitTime),
	})
}

// nextWriteToken returns a strictly increasing stream-token sequence number.
func (s *Service) nextWriteToken() uint64 {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.writeSeq++
	return s.writeSeq
}

// newStreamID returns a fresh random write-stream identifier.
func newStreamID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return hex.EncodeToString([]byte(clock.Now().Format("20060102150405.000000000")))
	}
	return hex.EncodeToString(b)
}
