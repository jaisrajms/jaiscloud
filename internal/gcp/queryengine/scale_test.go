package queryengine

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestHydrateRowLimitFailsLoud proves the 1M-row guard rejects a scan before it
// hydrates anything, so a runaway query fails with InvalidQuery instead of
// exhausting the process. The slice is all-nil so no per-row memory is paid.
func TestHydrateRowLimitFailsLoud(t *testing.T) {
	rows := make([]map[string]any, maxHydratedRows+1)
	cat := memCatalog{tables: map[string]Table{
		"p\x00ds\x00huge": {
			Project: "p", Dataset: "ds", Table: "huge",
			Fields: []Field{{Name: "id", Type: "INT64"}},
			Rows:   rows,
		},
	}}
	_, err := Execute(context.Background(), cat, Request{
		Project: "p", Query: "SELECT id FROM `p.ds.huge`", DefaultProject: "p", DefaultDataset: "ds",
	})
	if err == nil {
		t.Fatalf("expected over-limit query to fail")
	}
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("expected an unsupported/invalid-query error, got %v", err)
	}
}

// BenchmarkHydrateSelect measures per-query scratch hydration + execution over a
// 100k-row table (the realistic upper end a single emulator query should carry).
// Recorded so a regression in the batched hydration path is visible.
func BenchmarkHydrateSelect(b *testing.B) {
	const n = 100_000
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i), "v": fmt.Sprintf("v%d", i)}
	}
	cat := memCatalog{tables: map[string]Table{
		"p\x00ds\x00big": {
			Project: "p", Dataset: "ds", Table: "big",
			Fields: []Field{{Name: "id", Type: "INT64"}, {Name: "v", Type: "STRING"}},
			Rows:   rows,
		},
	}}
	req := Request{
		Project: "p", Query: "SELECT COUNT(*) AS n, MAX(id) AS hi FROM `p.ds.big`",
		DefaultProject: "p", DefaultDataset: "ds",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Execute(context.Background(), cat, req); err != nil {
			b.Fatalf("Execute: %v", err)
		}
	}
}
