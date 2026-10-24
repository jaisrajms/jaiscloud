package datastore

import (
	"testing"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"jaiscloud/internal/model"
)

// TestTimestampProtoCanonicalUTC pins that the gRPC transport writes a timestamp
// value in the shared canonical form (RFC 3339 UTC, the same form the REST
// boundary normalizes to) and reads it back as the same instant. (TD41)
func TestTimestampProtoCanonicalUTC(t *testing.T) {
	// 2019-12-31T19:00:00.5-05:00 == 2020-01-01T00:00:00.5Z, spelled in a
	// non-UTC location to prove the transcode canonicalizes to UTC.
	loc := time.FixedZone("EST", -5*3600)
	instant := time.Date(2019, 12, 31, 19, 0, 0, 500000000, loc)

	neutral, err := valueFromProto(&datastorepb.Value{
		ValueType: &datastorepb.Value_TimestampValue{TimestampValue: timestamppb.New(instant)},
	})
	if err != nil {
		t.Fatalf("valueFromProto: %v", err)
	}
	if neutral.TimestampValue == nil || *neutral.TimestampValue != "2020-01-01T00:00:00.500Z" {
		t.Fatalf("TimestampValue = %v, want 2020-01-01T00:00:00.500Z", neutral.TimestampValue)
	}

	back := valueToProto(neutral, "proj").GetTimestampValue()
	if got := back.AsTime(); !got.Equal(instant) {
		t.Fatalf("round-trip instant = %v, want %v", got, instant)
	}
}

// TestTimestampProtoRejectsOutOfRange pins TD46: a raw proto Timestamp that is
// invalid — outside the range [0001-01-01T00:00:00Z, 9999-12-31T23:59:59Z], or
// carrying out-of-range nanos — is INVALID_ARGUMENT at the gRPC decode
// boundary, so it never reaches the store as a string the read path could not
// parse back (it would otherwise read as the epoch). Both inclusive ends are
// accepted.
func TestTimestampProtoRejectsOutOfRange(t *testing.T) {
	bad := []struct {
		name string
		ts   *timestamppb.Timestamp
	}{
		{"after the upper bound", &timestamppb.Timestamp{Seconds: 253402300800}}, // 10000-01-01T00:00:00Z
		// The seconds field is out of range even though the negative nanos
		// would normalize the instant back to 9999-12-31T23:59:59Z.
		{"past the upper bound compensated by nanos", &timestamppb.Timestamp{Seconds: 253402300800, Nanos: -1000000000}},
		{"before the lower bound", &timestamppb.Timestamp{Seconds: -62135596801}}, // 0000-12-31T23:59:59Z
		// Likewise in the other direction: positive nanos normalize the instant
		// back to 0001-01-01T00:00:00Z.
		{"before the lower bound compensated by nanos", &timestamppb.Timestamp{Seconds: -62135596801, Nanos: 1000000000}},
		{"year zero", &timestamppb.Timestamp{Seconds: -62167219200}}, // 0000-01-01T00:00:00Z
		{"nanos out of range", &timestamppb.Timestamp{Seconds: 0, Nanos: 1500000000}},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := valueFromProto(&datastorepb.Value{
				ValueType: &datastorepb.Value_TimestampValue{TimestampValue: tc.ts},
			})
			perr, ok := err.(*model.ProviderError)
			if !ok {
				t.Fatalf("valueFromProto(%v) err = %v (%T), want *model.ProviderError", tc.ts, err, err)
			}
			if perr.HTTPStatus != 400 || perr.Code != "InvalidArgument" {
				t.Fatalf("valueFromProto(%v) err = %+v, want 400/InvalidArgument", tc.ts, perr)
			}
		})
	}

	accepted := []struct {
		name string
		ts   *timestamppb.Timestamp
		want string
	}{
		{
			"lower bound",
			timestamppb.New(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)),
			"0001-01-01T00:00:00Z",
		},
		{
			"upper bound with nanosecond fraction",
			timestamppb.New(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)),
			"9999-12-31T23:59:59.999999Z",
		},
	}
	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			neutral, err := valueFromProto(&datastorepb.Value{
				ValueType: &datastorepb.Value_TimestampValue{TimestampValue: tc.ts},
			})
			if err != nil {
				t.Fatalf("valueFromProto(%v): %v", tc.ts, err)
			}
			if neutral.TimestampValue == nil || *neutral.TimestampValue != tc.want {
				t.Fatalf("TimestampValue = %v, want %s", neutral.TimestampValue, tc.want)
			}
		})
	}
}

// TestMutationPreconditionUpdateTimeTruncated pins TD42: a gRPC
// Mutation.update_time precondition carrying sub-microsecond digits is truncated
// to Datastore's microsecond resolution before it is compared, matching the
// stored (microsecond) update_time.
func TestMutationPreconditionUpdateTimeTruncated(t *testing.T) {
	m := &datastorepb.Mutation{
		Operation: &datastorepb.Mutation_Upsert{Upsert: &datastorepb.Entity{}},
		ConflictDetectionStrategy: &datastorepb.Mutation_UpdateTime{
			UpdateTime: timestamppb.New(time.Date(2020, 1, 1, 0, 0, 0, 123456789, time.UTC)),
		},
	}
	pre := mutationPrecondition(m)
	if pre == nil || pre.UpdateTime == nil {
		t.Fatalf("precondition = %+v, want an updateTime precondition", pre)
	}
	want := time.Date(2020, 1, 1, 0, 0, 0, 123456000, time.UTC)
	if !pre.UpdateTime.Equal(want) {
		t.Fatalf("UpdateTime = %v, want %v", pre.UpdateTime, want)
	}
}

// TestTimestampProtoTruncatesSubMicrosecond pins TD42: a proto Timestamp written
// over gRPC is held at Datastore's microsecond resolution, so sub-microsecond
// digits are dropped on the way in (real Datastore stores them "precise only to
// microseconds; any additional precision is rounded down").
func TestTimestampProtoTruncatesSubMicrosecond(t *testing.T) {
	instant := time.Date(2020, 1, 1, 0, 0, 0, 123456789, time.UTC)

	neutral, err := valueFromProto(&datastorepb.Value{
		ValueType: &datastorepb.Value_TimestampValue{TimestampValue: timestamppb.New(instant)},
	})
	if err != nil {
		t.Fatalf("valueFromProto: %v", err)
	}
	if neutral.TimestampValue == nil || *neutral.TimestampValue != "2020-01-01T00:00:00.123456Z" {
		t.Fatalf("TimestampValue = %v, want 2020-01-01T00:00:00.123456Z", neutral.TimestampValue)
	}

	// The value read back is the truncated instant, not the original.
	back := valueToProto(neutral, "proj").GetTimestampValue().AsTime()
	want := time.Date(2020, 1, 1, 0, 0, 0, 123456000, time.UTC)
	if !back.Equal(want) {
		t.Fatalf("round-trip instant = %v, want %v", back, want)
	}
}
