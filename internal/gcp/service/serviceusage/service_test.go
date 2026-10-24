package serviceusage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newCore() *Service { return NewService(store.NewMemoryResourceStore()) }

func TestEnableGetListFiltersAndPagination(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	if _, err := s.GetAPI(ctx, "proj", "unknown.googleapis.com"); err != nil {
		t.Fatalf("GetAPI unknown: %v", err)
	}
	got, err := s.GetAPI(ctx, "proj", "unknown.googleapis.com")
	if err != nil {
		t.Fatalf("GetAPI: %v", err)
	}
	if got.State != StateDisabled || got.Name != "projects/proj/services/unknown.googleapis.com" {
		t.Fatalf("unknown API = %+v, want DISABLED with full name", got)
	}

	api, op, err := s.EnableAPI(ctx, "proj", "run.googleapis.com")
	if err != nil {
		t.Fatalf("EnableAPI: %v", err)
	}
	if !op.Done || len(op.ResourceNames) != 1 || op.ResourceNames[0] != api.Name {
		t.Fatalf("enable operation = %+v", op)
	}
	if api.State != StateEnabled {
		t.Fatalf("enable state = %q, want ENABLED", api.State)
	}

	for _, id := range []string{"a.googleapis.com", "b.googleapis.com"} {
		if _, _, err := s.EnableAPI(ctx, "proj", id); err != nil {
			t.Fatalf("EnableAPI %s: %v", id, err)
		}
	}

	page, next, err := s.ListAPIs(ctx, "proj", FilterEnabled, 2, "")
	if err != nil {
		t.Fatalf("ListAPIs: %v", err)
	}
	if len(page) != 2 || next == "" {
		t.Fatalf("page 1 = %d items, next=%q", len(page), next)
	}
	page2, next2, err := s.ListAPIs(ctx, "proj", FilterEnabled, 2, next)
	if err != nil {
		t.Fatalf("ListAPIs page 2: %v", err)
	}
	if len(page2) != 1 || next2 != "" {
		t.Fatalf("page 2 = %d items, next=%q", len(page2), next2)
	}

	disabled, next, err := s.ListAPIs(ctx, "proj", FilterDisabled, 0, "")
	if err != nil {
		t.Fatalf("ListAPIs disabled: %v", err)
	}
	if len(disabled) != 0 || next != "" {
		t.Fatalf("disabled = %+v, next=%q", disabled, next)
	}
}

func TestDisableRequiresEnabled(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	if _, _, err := s.DisableAPI(ctx, "proj", "run.googleapis.com"); err == nil {
		t.Fatal("expected FailedPrecondition for disabled service")
	}
	if _, _, err := s.EnableAPI(ctx, "proj", "run.googleapis.com"); err != nil {
		t.Fatalf("EnableAPI: %v", err)
	}
	api, op, err := s.DisableAPI(ctx, "proj", "run.googleapis.com")
	if err != nil {
		t.Fatalf("DisableAPI: %v", err)
	}
	if api.State != StateDisabled {
		t.Fatalf("disable = %+v / %+v", api, op)
	}
}

func TestBatchEnableValidation(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	apis, op, err := s.BatchEnableAPIs(ctx, "proj", []string{"a.googleapis.com", "b.googleapis.com"})
	if err != nil {
		t.Fatalf("BatchEnableAPIs: %v", err)
	}
	if len(apis) != 2 || !op.Done || len(op.ResourceNames) != 2 {
		t.Fatalf("batch = %d APIs / %+v", len(apis), op)
	}
	if _, _, err := s.BatchEnableAPIs(ctx, "proj", nil); err == nil {
		t.Error("expected error for empty batch")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.Code != "InvalidArgument" {
			t.Errorf("empty batch error = %v", err)
		}
	}
	// A resource name is not a service id; a slash or empty id is rejected
	// rather than persisted as a bogus name.
	for _, bad := range []string{"", "projects/proj/services/x.googleapis.com"} {
		if _, _, err := s.BatchEnableAPIs(ctx, "proj", []string{bad}); err == nil {
			t.Errorf("BatchEnableAPIs(%q) succeeded, want InvalidArgument", bad)
		}
	}
	ids := make([]string, maxBatchEnable+1)
	for i := range ids {
		ids[i] = "s.googleapis.com"
	}
	if _, _, err := s.BatchEnableAPIs(ctx, "proj", ids); err == nil {
		t.Error("expected error for oversized batch")
	}
}

func TestBatchGetAPIs(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	if _, _, err := s.EnableAPI(ctx, "proj", "a.googleapis.com"); err != nil {
		t.Fatalf("EnableAPI: %v", err)
	}
	// A mix of an enabled and a never-seen service keeps order and defaults the
	// unknown one to DISABLED (matching GetAPI, not NotFound).
	apis, err := s.BatchGetAPIs(ctx, "proj", []string{
		"projects/proj/services/a.googleapis.com",
		"projects/proj/services/b.googleapis.com",
	})
	if err != nil {
		t.Fatalf("BatchGetAPIs: %v", err)
	}
	if len(apis) != 2 {
		t.Fatalf("BatchGetAPIs returned %d APIs, want 2", len(apis))
	}
	if apis[0].Name != "projects/proj/services/a.googleapis.com" || apis[0].State != StateEnabled {
		t.Errorf("apis[0] = %+v, want enabled a.googleapis.com", apis[0])
	}
	if apis[1].Name != "projects/proj/services/b.googleapis.com" || apis[1].State != StateDisabled {
		t.Errorf("apis[1] = %+v, want disabled b.googleapis.com", apis[1])
	}
	// An empty parent lets each name resolve its own project.
	if _, err := s.BatchGetAPIs(ctx, "", []string{"projects/other/services/c.googleapis.com"}); err != nil {
		t.Errorf("BatchGetAPIs with empty parent: %v", err)
	}

	bad := []struct {
		name    string
		project string
		names   []string
	}{
		{"empty", "proj", nil},
		{"malformed name", "proj", []string{"a.googleapis.com"}},
		{"parent mismatch", "proj", []string{"projects/other/services/a.googleapis.com"}},
		{"oversized", "proj", oversizedNames(maxBatchGet + 1)},
	}
	for _, tc := range bad {
		if _, err := s.BatchGetAPIs(ctx, tc.project, tc.names); err == nil {
			t.Errorf("BatchGetAPIs(%s) succeeded, want InvalidArgument", tc.name)
		} else {
			var pe *model.ProviderError
			if !errors.As(err, &pe) || pe.Code != "InvalidArgument" {
				t.Errorf("BatchGetAPIs(%s) error = %v, want InvalidArgument", tc.name, err)
			}
		}
	}
}

func oversizedNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("projects/proj/services/s%d.googleapis.com", i)
	}
	return names
}

func TestListPageSizeBounds(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	for i := 0; i < 210; i++ {
		id := fmt.Sprintf("svc%03d.googleapis.com", i)
		if _, _, err := s.EnableAPI(ctx, "proj", id); err != nil {
			t.Fatalf("EnableAPI %s: %v", id, err)
		}
	}
	// Unset pageSize defaults to 50 (not the shared paging default of 1000).
	page, next, err := s.ListAPIs(ctx, "proj", FilterAll, 0, "")
	if err != nil {
		t.Fatalf("ListAPIs default: %v", err)
	}
	if len(page) != defaultPageSize || next == "" {
		t.Fatalf("default page = %d items (next=%q), want %d", len(page), next, defaultPageSize)
	}
	// A pageSize above the max is capped at 200.
	capped, _, err := s.ListAPIs(ctx, "proj", FilterAll, 500, "")
	if err != nil {
		t.Fatalf("ListAPIs capped: %v", err)
	}
	if len(capped) != maxPageSize {
		t.Fatalf("capped page = %d items, want %d", len(capped), maxPageSize)
	}
}

func TestParseFilter(t *testing.T) {
	cases := []struct {
		in      string
		want    StateFilter
		wantErr bool
	}{
		{"", FilterAll, false},
		{" state:ENABLED ", FilterEnabled, false},
		{"state:DISABLED", FilterDisabled, false},
		{"state:RUNNING", FilterAll, true},
	}
	for _, tc := range cases {
		got, err := ParseFilter(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseFilter(%q) = %v, want error", tc.in, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("ParseFilter(%q) = %v, %v", tc.in, got, err)
		}
	}
}

// failingStore surfaces a storage outage.
type failingStore struct{ store.ResourceStore }

func (failingStore) Get(context.Context, string, string, string, string) (store.ResourceEntry, error) {
	return store.ResourceEntry{}, store.ErrStorageUnavailable
}

func (failingStore) Upsert(context.Context, string, string, store.ResourceEntry) error {
	return store.ErrStorageUnavailable
}

func TestStorageErrorsPropagate(t *testing.T) {
	ctx := context.Background()
	s := NewService(failingStore{})

	if _, err := s.GetAPI(ctx, "proj", "a.googleapis.com"); !errors.Is(err, store.ErrStorageUnavailable) {
		t.Errorf("GetAPI error = %v, want ErrStorageUnavailable", err)
	}
	if _, _, err := s.EnableAPI(ctx, "proj", "a.googleapis.com"); !errors.Is(err, store.ErrStorageUnavailable) {
		t.Errorf("EnableAPI error = %v, want ErrStorageUnavailable", err)
	}
}

// TestListPageSizeNegativeRejected pins AIP-158 on serviceusage's two
// cursor-paged list surfaces: a negative pageSize is INVALID_ARGUMENT, not a
// fall-back to the documented default of 50.
func TestListPageSizeNegativeRejected(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	cases := []struct {
		name string
		call func() error
	}{
		{"ListAPIs", func() error { _, _, err := s.ListAPIs(ctx, "proj", FilterAll, -1, ""); return err }},
		{"ListOperations", func() error { _, _, err := s.ListOperations(ctx, "proj", -1, ""); return err }},
	}
	for _, tc := range cases {
		var pe *model.ProviderError
		if err := tc.call(); !errors.As(err, &pe) || pe.Code != "InvalidArgument" {
			t.Errorf("%s(pageSize=-1) = %v, want InvalidArgument", tc.name, err)
		}
	}
}
