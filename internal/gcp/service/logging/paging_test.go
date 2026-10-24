package logging

import "testing"

// TestPaginateConfigPageSizeContract pins the shared config-list pager: a
// non-positive size selects the 50-item default, an over-large size is clamped
// to the 1000-result server-side cap (AIP-158), and an in-range size is
// honored. A single seeded caller cannot distinguish the clamp from an error,
// so this exercises the pager directly with more items than the cap.
func TestPaginateConfigPageSizeContract(t *testing.T) {
	items := make([]int, 1005)
	for i := range items {
		items[i] = i
	}

	for _, tc := range []struct {
		pageSize int
		want     int
	}{
		{-1, 50},
		{0, 50},
		{1, 1},
		{1000, 1000},
		{1001, 1000},
		{1005, 1000},
	} {
		page, _ := paginateConfig(items, tc.pageSize, "")
		if len(page) != tc.want {
			t.Fatalf("pageSize %d = %d items, want %d", tc.pageSize, len(page), tc.want)
		}
		if len(page) > 0 && page[0] != 0 {
			t.Fatalf("pageSize %d starts at item %d, want 0", tc.pageSize, page[0])
		}
	}
}

// TestDecodeOffsetClampsNegative pins the token guard: a forged token that
// decodes to a negative offset must not slice below zero.
func TestDecodeOffsetClampsNegative(t *testing.T) {
	if got := decodeOffset(encodeOffset(3)); got != 3 {
		t.Fatalf("decodeOffset(encodeOffset(3)) = %d, want 3", got)
	}
	if got := decodeOffset(encodeOffset(-5)); got != 0 {
		t.Fatalf("decodeOffset(encodeOffset(-5)) = %d, want 0", got)
	}
	if got := decodeOffset("not-base64!"); got != 0 {
		t.Fatalf("decodeOffset(garbage) = %d, want 0", got)
	}
}
