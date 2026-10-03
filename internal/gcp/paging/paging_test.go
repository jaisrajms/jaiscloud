package paging

import (
	"testing"

	"jaiscloud/internal/store"
)

func TestPagePagination(t *testing.T) {
	var entries []store.ResourceEntry
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		entries = append(entries, store.ResourceEntry{ID: id})
	}

	// Page 1 of size 2.
	page, next := Apply(entries, map[string]any{"pageSize": "2"})
	if len(page) != 2 || page[0].ID != "a" || page[1].ID != "b" {
		t.Fatalf("page 1 = %+v", page)
	}
	if next == "" {
		t.Fatal("expected nextPageToken")
	}

	// Page 2.
	page, next = Apply(entries, map[string]any{"pageSize": "2", "pageToken": next})
	if len(page) != 2 || page[0].ID != "c" || page[1].ID != "d" {
		t.Fatalf("page 2 = %+v", page)
	}
	if next == "" {
		t.Fatal("expected nextPageToken on page 2")
	}

	// Page 3 (final).
	page, next = Apply(entries, map[string]any{"pageSize": "2", "pageToken": next})
	if len(page) != 1 || page[0].ID != "e" {
		t.Fatalf("page 3 = %+v", page)
	}
	if next != "" {
		t.Fatalf("expected empty nextPageToken on final page, got %q", next)
	}
}

func TestPageGeneric(t *testing.T) {
	items := []string{"z", "a", "m"}
	page, next := Page(items, func(s string) string { return s }, map[string]any{"pageSize": "2"})
	if len(page) != 2 || page[0] != "a" || page[1] != "m" {
		t.Fatalf("generic page = %v", page)
	}
	if next == "" {
		t.Fatal("expected nextPageToken")
	}
}

func TestPageSizeFallback(t *testing.T) {
	if got := PageSize(map[string]any{"pageSize": "0"}); got != 1000 {
		t.Errorf("expected default 1000 for non-positive size, got %d", got)
	}
	if got := PageSize(map[string]any{"pageSize": "7"}); got != 7 {
		t.Errorf("expected 7, got %d", got)
	}
}
