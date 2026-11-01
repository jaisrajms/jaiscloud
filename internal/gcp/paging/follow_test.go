package paging

import (
	"context"
	"errors"
	"testing"
)

func TestFollowReadsEveryPage(t *testing.T) {
	pages := map[string]string{"": "a", "a": "b", "b": ""}
	var seen []string
	err := Follow(context.Background(), func(tok string) (string, error) { seen = append(seen, tok); return pages[tok], nil })
	if err != nil || len(seen) != 3 {
		t.Fatalf("Follow = %v, pages %v", err, seen)
	}
}

// A source that cannot be trusted (a plugin) must not keep the caller busy forever.
func TestFollowStopsOnARepeatedToken(t *testing.T) {
	calls := 0
	err := Follow(context.Background(), func(string) (string, error) { calls++; return "same", nil })
	if !errors.Is(err, ErrPageLoop) || calls != 2 {
		t.Fatalf("err = %v after %d calls, want ErrPageLoop after 2", err, calls)
	}
}

func TestFollowStopsOnAnEndlessCountOfDistinctTokens(t *testing.T) {
	calls := 0
	err := Follow(context.Background(), func(string) (string, error) { calls++; return string(rune('a'+calls%26)) + "-" + itoa(calls), nil })
	if !errors.Is(err, ErrPageLoop) || calls != MaxPages {
		t.Fatalf("err = %v after %d calls, want ErrPageLoop after %d", err, calls, MaxPages)
	}
}

func TestFollowHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := Follow(ctx, func(string) (string, error) { calls++; cancel(); return "x" + itoa(calls), nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}
