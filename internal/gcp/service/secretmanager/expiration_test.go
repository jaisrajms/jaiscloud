package secretmanager

import (
	"testing"
	"time"

	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
)

func TestParseVersionDestroyTTL(t *testing.T) {
	good := map[string]time.Duration{
		"86400s":    24 * time.Hour,
		"864001s":   864001 * time.Second,
		"86400000s": 1000 * 24 * time.Hour,
		"86400.5s":  86400*time.Second + 500*time.Millisecond,
	}
	for in, want := range good {
		got, err := ParseVersionDestroyTTL(in)
		if err != nil {
			t.Fatalf("ParseVersionDestroyTTL(%q) = %v", in, err)
		}
		if got != want {
			t.Errorf("ParseVersionDestroyTTL(%q) = %v, want %v", in, got, want)
		}
	}
	for _, in := range []string{"86399s", "0s", "86400001s", "24h", "-86400s", "abc", ""} {
		if _, err := ParseVersionDestroyTTL(in); err == nil || providerErr(err) == nil || providerErr(err).HTTPStatus != 400 {
			t.Errorf("ParseVersionDestroyTTL(%q) = %v, want a 400", in, err)
		}
	}
}

func TestExpirationValidation(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	expire, err := ParseTTL("86400s", now)
	if err != nil {
		t.Fatalf("ParseTTL: %v", err)
	}
	if want := now.Add(24 * time.Hour); !expire.Equal(want) {
		t.Errorf("ParseTTL = %v, want %v", expire, want)
	}
	// The documented 60-second floor and 100-year ceiling apply to both arms.
	for _, in := range []string{"59s", "0s", "3153600001s", "1h", "abc"} {
		if _, err := ParseTTL(in, now); err == nil || providerErr(err) == nil || providerErr(err).HTTPStatus != 400 {
			t.Errorf("ParseTTL(%q) = %v, want a 400", in, err)
		}
	}
	if _, err := ParseExpireTime(now.Add(59*time.Second).Format(time.RFC3339), now); err == nil || providerErr(err) == nil || providerErr(err).HTTPStatus != 400 {
		t.Errorf("expireTime under 60s = %v, want a 400", err)
	}
	if _, err := ParseExpireTime(now.Add(101*365*24*time.Hour).Format(time.RFC3339), now); err == nil || providerErr(err) == nil || providerErr(err).HTTPStatus != 400 {
		t.Errorf("expireTime over 100 years = %v, want a 400", err)
	}
	if _, err := ParseExpireTime("not-a-time", now); err == nil || providerErr(err) == nil || providerErr(err).HTTPStatus != 400 {
		t.Errorf("malformed expireTime = %v, want a 400", err)
	}
}

func TestExpiredAndScheduledDestroyDue(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	if Expired(secretmanagerstore.Secret{}, now) {
		t.Error("a secret without an expiration is expired")
	}
	sec := secretmanagerstore.Secret{ExpireTime: now.Add(-time.Second)}
	if !Expired(sec, now) {
		t.Error("a past expiration is not expired")
	}
	if !Expired(secretmanagerstore.Secret{ExpireTime: now}, now) {
		t.Error("an expiration at the current instant is not expired")
	}
	if Expired(secretmanagerstore.Secret{ExpireTime: now}, now.Add(-time.Nanosecond)) {
		t.Error("an expiration a nanosecond in the future is expired")
	}

	v := secretmanagerstore.Version{State: "DISABLED", ScheduledDestroyTime: now.Add(time.Second)}
	if ScheduledDestroyDue(v, now) {
		t.Error("a future schedule is due")
	}
	if !ScheduledDestroyDue(v, now.Add(time.Second)) {
		t.Error("a reached schedule is not due")
	}
	v.State = "DESTROYED"
	if ScheduledDestroyDue(v, now.Add(time.Hour)) {
		t.Error("a destroyed version is due again")
	}
	if ScheduledDestroyDue(secretmanagerstore.Version{}, now) {
		t.Error("an unscheduled version is due")
	}
}

// TestDurationStringProtoJSON pins google-duration rendering to protojson's
// 0/3/6/9 fractional digits and proves the render/parse pair is exact (the
// parser is integer-based, so a canonical multi-day value cannot drift by a
// nanosecond).
func TestDurationStringProtoJSON(t *testing.T) {
	render := map[time.Duration]string{
		0:                                       "0s",
		time.Hour:                               "3600s",
		3600*time.Second + 500*time.Millisecond: "3600.500s",
		1200 * time.Millisecond:                 "1.200s",
		123456 * time.Microsecond:               "0.123456s",
		500 * time.Nanosecond:                   "0.000000500s",
		999999999 * time.Nanosecond:             "0.999999999s",
		86400*time.Second + 955557379*time.Nanosecond: "86400.955557379s",
	}
	for d, want := range render {
		if got := DurationString(d); got != want {
			t.Errorf("DurationString(%v) = %q, want %q", d, got, want)
		}
		got, err := parseDurationSeconds("test", DurationString(d))
		if err != nil || got != d {
			t.Errorf("round trip %v -> %q -> %v, %v", d, DurationString(d), got, err)
		}
	}
}
