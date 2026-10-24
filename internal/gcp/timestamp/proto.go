package timestamp

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// Proto renders t as the proto google.protobuf.Timestamp a gRPC adapter returns
// for a google-datetime field, truncated to the microsecond resolution real
// GCP's server holds such an instant at. It is the proto-typed counterpart of
// Canonical: a gRPC adapter (timestamppb.New) and the REST renderer (Canonical)
// of the same field then hand a client the same instant, with no sub-microsecond
// digit the REST transport no longer emits.
//
// Do NOT use this for a field whose nanoseconds are the client's own — a
// Firestore Value.timestampValue (Firestore holds it at nanosecond resolution
// and echoes it unchanged) keeps its raw proto Timestamp.
func Proto(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(Truncate(t))
}
