package secretmanager

import (
	"errors"
	"testing"
	"time"

	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
)

func TestParseRotationDuration(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"3600s", time.Hour, true},
		{"2592000s", 2592000 * time.Second, true},
		{"3600.000s", time.Hour, true},
		{"3600.5s", 3600*time.Second + 500*time.Millisecond, true},
		{"3153600000s", MaxRotationSpan, true},
		{"", 0, false},
		{"1h", 0, false},  // Go's duration syntax is not the wire format
		{"60m", 0, false}, // ditto
		{"-3600s", 0, false},
		{"3600..5s", 0, false},
		{"abc", 0, false},
		{"1e30s", 0, false}, // past the int64-nanosecond range
	} {
		got, err := ParseRotationDuration(tc.in)
		if tc.ok {
			if err != nil || got != tc.want {
				t.Errorf("ParseRotationDuration(%q) = %v, %v; want %v", tc.in, got, err, tc.want)
			}
			continue
		}
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
			t.Errorf("ParseRotationDuration(%q) = %v, want a 400 InvalidArgument", tc.in, err)
		}
	}
}

// TestRotationPeriodString pins the rendering: the API returns a google-duration
// in whole seconds ("3600s"), where an older gRPC build stored Go's duration
// syntax ("1h0m0s").
func TestRotationPeriodString(t *testing.T) {
	for in, want := range map[string]string{
		"":            "",
		"3600s":       "3600s",
		"86400s":      "86400s",
		"1h0m0s":      "3600s",
		"3600.5s":     "3600.500s",
		"1h0m0.5s":    "3600.500s",
		"24h0m0s":     "86400s",
		"unparseable": "unparseable",
	} {
		if got := RotationPeriodString(in); got != want {
			t.Errorf("RotationPeriodString(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestValidateTopics(t *testing.T) {
	if err := ValidateTopics(nil); err != nil {
		t.Fatalf("empty topics = %v, want nil", err)
	}
	if err := ValidateTopics([]string{"projects/p/topics/t"}); err != nil {
		t.Fatalf("valid topic = %v, want nil", err)
	}
	for _, tc := range []struct {
		name   string
		topics []string
	}{
		{"too many", make11Topics()},
		{"bare id", []string{"my-topic"}},
		{"missing topic component", []string{"projects/p/topics/"}},
		{"subscription path", []string{"projects/p/subscriptions/s"}},
	} {
		err := ValidateTopics(tc.topics)
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
			t.Errorf("%s: ValidateTopics = %v, want a 400 InvalidArgument", tc.name, err)
		}
	}
}

func make11Topics() []string {
	out := make([]string, 0, MaxSecretTopics+1)
	for i := 0; i <= MaxSecretTopics; i++ {
		out = append(out, "projects/p/topics/t"+time.Duration(i).String())
	}
	return out
}

func TestValidateRotation(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	topics := []string{"projects/p/topics/t"}
	at := func(d time.Duration) string { return now.Add(d).UTC().Format(time.RFC3339) }

	for _, tc := range []struct {
		name     string
		rot      *secretmanagerstore.Rotation
		topics   []string
		supplied bool
		ok       bool
	}{
		{"no schedule", nil, nil, true, true},
		{"valid pair", &secretmanagerstore.Rotation{NextRotationTime: at(time.Hour), RotationPeriod: "3600s"}, topics, true, true},
		{"next only", &secretmanagerstore.Rotation{NextRotationTime: at(time.Hour)}, topics, true, true},
		{"period too short", &secretmanagerstore.Rotation{NextRotationTime: at(time.Hour), RotationPeriod: "3599s"}, topics, true, false},
		{"period too long", &secretmanagerstore.Rotation{NextRotationTime: at(time.Hour), RotationPeriod: "3153600001s"}, topics, true, false},
		{"period without next", &secretmanagerstore.Rotation{RotationPeriod: "3600s"}, topics, true, false},
		{"period not a google duration", &secretmanagerstore.Rotation{NextRotationTime: at(time.Hour), RotationPeriod: "1h"}, topics, true, false},
		{"next under five minutes", &secretmanagerstore.Rotation{NextRotationTime: at(4 * time.Minute)}, topics, true, false},
		{"next in the past", &secretmanagerstore.Rotation{NextRotationTime: at(-time.Hour)}, topics, true, false},
		{"next past 100 years", &secretmanagerstore.Rotation{NextRotationTime: at(101 * 365 * 24 * time.Hour)}, topics, true, false},
		{"bad timestamp", &secretmanagerstore.Rotation{NextRotationTime: "tomorrow"}, topics, true, false},
		{"schedule without topics", &secretmanagerstore.Rotation{NextRotationTime: at(time.Hour)}, nil, true, false},
		{"stored past time is not re-bounded", &secretmanagerstore.Rotation{NextRotationTime: at(-time.Hour)}, topics, false, true},
		{"managed rotation carries no schedule", &secretmanagerstore.Rotation{ManagedRotation: &secretmanagerstore.ManagedRotation{State: "ACTIVE"}}, nil, false, true},
		{"fieldless rotation configures nothing", &secretmanagerstore.Rotation{}, topics, true, true},
	} {
		err := ValidateRotation(tc.rot, tc.topics, now, tc.supplied)
		if tc.ok {
			if err != nil {
				t.Errorf("%s: ValidateRotation = %v, want nil", tc.name, err)
			}
			continue
		}
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
			t.Errorf("%s: ValidateRotation = %v, want a 400 InvalidArgument", tc.name, err)
		}
	}
}

// TestMergedRotation pins the update-mask semantics: a mask naming one subfield
// must not drop the other, and managedRotationStatus is never lost.
func TestMergedRotation(t *testing.T) {
	stored := &secretmanagerstore.Rotation{
		NextRotationTime: "2026-11-01T00:00:00Z", RotationPeriod: "86400s",
		ManagedRotation: &secretmanagerstore.ManagedRotation{State: "ACTIVE"},
	}

	// A period-only subfield update keeps the stored next rotation time.
	got := MergedRotation(stored, &secretmanagerstore.Rotation{RotationPeriod: "3600s"}, false, false, true)
	if got.NextRotationTime != stored.NextRotationTime || got.RotationPeriod != "3600s" {
		t.Fatalf("period-only merge = %#v, want next kept and period replaced", got)
	}
	if got.ManagedRotation == nil || got.ManagedRotation.State != "ACTIVE" {
		t.Fatalf("period-only merge dropped managedRotationStatus: %#v", got.ManagedRotation)
	}

	// A next-time-only update keeps the stored period.
	got = MergedRotation(stored, &secretmanagerstore.Rotation{NextRotationTime: "2026-12-01T00:00:00Z"}, false, true, false)
	if got.NextRotationTime != "2026-12-01T00:00:00Z" || got.RotationPeriod != "86400s" {
		t.Fatalf("next-time-only merge = %#v, want period kept", got)
	}

	// Clearing the next time leaves a period-only schedule (which validation
	// then rejects, but the merge itself must be faithful).
	got = MergedRotation(stored, &secretmanagerstore.Rotation{}, false, true, false)
	if got.NextRotationTime != "" || got.RotationPeriod != "86400s" {
		t.Fatalf("cleared next time = %#v, want only the next time cleared", got)
	}

	// A whole-`rotation` patch replaces the schedule and keeps the status.
	got = MergedRotation(stored, &secretmanagerstore.Rotation{NextRotationTime: "2026-12-01T00:00:00Z", RotationPeriod: "7200s"}, true, true, true)
	if got.NextRotationTime != "2026-12-01T00:00:00Z" || got.RotationPeriod != "7200s" {
		t.Fatalf("whole-schedule merge = %#v", got)
	}
	if got.ManagedRotation == nil || got.ManagedRotation.State != "ACTIVE" {
		t.Fatalf("whole-schedule merge dropped managedRotationStatus")
	}

	// A masked empty rotation object removes the schedule but keeps the
	// output-only managed-rotation status.
	got = MergedRotation(stored, nil, true, false, false)
	if got == nil || got.NextRotationTime != "" || got.RotationPeriod != "" || got.ManagedRotation == nil {
		t.Fatalf("clearing a whole rotation = %#v, want the status alone", got)
	}
	// A subfield merge onto a secret with no stored schedule starts from an
	// empty one, and a merge that leaves nothing configured collapses to nil.
	if got := MergedRotation(nil, &secretmanagerstore.Rotation{NextRotationTime: "2026-11-01T00:00:00Z"}, false, true, false); got == nil || got.NextRotationTime != "2026-11-01T00:00:00Z" {
		t.Fatalf("merge onto an empty schedule = %#v", got)
	}
	if got := MergedRotation(nil, nil, false, false, false); got != nil {
		t.Fatalf("empty merge = %#v, want nil", got)
	}

	// Without a managed-rotation status the schedule clears to nil.
	if got := MergedRotation(&secretmanagerstore.Rotation{NextRotationTime: "2026-11-01T00:00:00Z"}, nil, true, false, false); got != nil {
		t.Fatalf("clearing an unmanaged rotation = %#v, want nil", got)
	}
}
