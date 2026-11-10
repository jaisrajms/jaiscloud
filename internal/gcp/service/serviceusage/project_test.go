package serviceusage

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// fakeResolver is the ProjectResolver seam: it knows only the numbers the test
// registers, exactly like Resource Manager's synthesized-number index.
type fakeResolver struct {
	byNumber map[string]string
	err      error
	calls    []string
}

func (f *fakeResolver) ProjectByNumber(_ context.Context, number string) (string, bool, error) {
	f.calls = append(f.calls, number)
	if f.err != nil {
		return "", false, f.err
	}
	id, ok := f.byNumber[number]
	return id, ok, nil
}

func newResolvedCore(r ProjectResolver) *Service {
	return NewService(store.NewMemoryResourceStore(), WithProjectResolver(r))
}

// assertUnknownProject checks the documented 403: PERMISSION_DENIED, the real
// Service Usage message, and the legacy errors[] reason the apiary client and
// the Terraform Google provider surface.
func assertUnknownProject(t *testing.T, err error, ref string) {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("error = %v, want *model.ProviderError", err)
	}
	if pe.Code != "PermissionDenied" || pe.HTTPStatus != 403 {
		t.Fatalf("error = %+v, want PermissionDenied/403", pe)
	}
	want := "Project '" + ref + "' not found or permission denied."
	if pe.Message != want {
		t.Fatalf("message = %q, want %q", pe.Message, want)
	}
	if reason, _ := pe.Data["reason"].(string); reason != "forbidden" {
		t.Fatalf("legacy reason = %v, want forbidden", pe.Data["reason"])
	}
}

// TestUnknownProjectNumberRejected covers every entry point with the provider's
// dummy number: the zero number can never belong to a project, so no call may
// answer a synthesized 200 DISABLED. An arbitrary unresolvable number is the
// same rule.
func TestUnknownProjectNumberRejected(t *testing.T) {
	ctx := context.Background()
	s := newResolvedCore(&fakeResolver{})

	for _, ref := range []string{"00000000000", "123456789012", "7"} {
		if _, err := s.GetAPI(ctx, ref, "serviceusage.googleapis.com"); err == nil {
			t.Fatalf("GetAPI(%s): expected the unknown-project error", ref)
		} else {
			assertUnknownProject(t, err, ref)
		}
		if _, _, err := s.ListAPIs(ctx, ref, FilterAll, 0, ""); !isUnknownProject(err) {
			t.Fatalf("ListAPIs(%s) error = %v, want unknown project", ref, err)
		}
		if _, _, err := s.EnableAPI(ctx, ref, "run.googleapis.com"); !isUnknownProject(err) {
			t.Fatalf("EnableAPI(%s) error = %v, want unknown project", ref, err)
		}
		if _, _, err := s.DisableAPI(ctx, ref, "run.googleapis.com"); !isUnknownProject(err) {
			t.Fatalf("DisableAPI(%s) error = %v, want unknown project", ref, err)
		}
		if _, _, err := s.BatchEnableAPIs(ctx, ref, []string{"run.googleapis.com"}); !isUnknownProject(err) {
			t.Fatalf("BatchEnableAPIs(%s) error = %v, want unknown project", ref, err)
		}
	}
}

// TestZeroProjectNumberNeverResolves: even a registry that (pathologically)
// claims the zero number must not admit it — no project number is all zeros.
func TestZeroProjectNumberNeverResolves(t *testing.T) {
	ctx := context.Background()
	s := newResolvedCore(&fakeResolver{byNumber: map[string]string{"00000000000": "proj"}})
	_, err := s.GetAPI(ctx, "00000000000", "serviceusage.googleapis.com")
	assertUnknownProject(t, err, "00000000000")
}

// TestUnknownProjectNumberInBatchGet covers the names and parent forms: a
// service name under the dummy number is rejected, and so is a parent.
func TestUnknownProjectNumberInBatchGet(t *testing.T) {
	ctx := context.Background()
	s := newResolvedCore(&fakeResolver{})

	if _, err := s.BatchGetAPIs(ctx, "", []string{"projects/00000000000/services/run.googleapis.com"}); !isUnknownProject(err) {
		t.Fatalf("names form error = %v, want unknown project", err)
	}
	if _, err := s.BatchGetAPIs(ctx, "00000000000", []string{"projects/00000000000/services/run.googleapis.com"}); !isUnknownProject(err) {
		t.Fatalf("parent form error = %v, want unknown project", err)
	}
}

// TestProjectNumberResolvesToProjectID proves the number form keeps working for
// a known project: the number resolves to the project ID, and both forms share
// one stored state (real Service Usage treats the number as an alias).
func TestProjectNumberResolvesToProjectID(t *testing.T) {
	ctx := context.Background()
	number := resource.ProjectNumber("proj")
	r := &fakeResolver{byNumber: map[string]string{number: "proj"}}
	s := newResolvedCore(r)

	// Enable through the number form.
	api, _, err := s.EnableAPI(ctx, number, "run.googleapis.com")
	if err != nil {
		t.Fatalf("EnableAPI by number: %v", err)
	}
	if api.Name != "projects/proj/services/run.googleapis.com" {
		t.Fatalf("name = %q, want the resolved project ID", api.Name)
	}

	// The ID form sees the same state.
	got, err := s.GetAPI(ctx, "proj", "run.googleapis.com")
	if err != nil {
		t.Fatalf("GetAPI by id: %v", err)
	}
	if got.State != StateEnabled {
		t.Fatalf("state by id = %q, want ENABLED (one state per project)", got.State)
	}

	// List by number lists the same service.
	page, _, err := s.ListAPIs(ctx, number, FilterEnabled, 0, "")
	if err != nil {
		t.Fatalf("ListAPIs by number: %v", err)
	}
	if len(page) != 1 || page[0].Name != "projects/proj/services/run.googleapis.com" {
		t.Fatalf("ListAPIs by number = %+v", page)
	}

	// BatchGet with a numeric parent and matching numeric names resolves both
	// sides and compares the resolved IDs.
	apis, err := s.BatchGetAPIs(ctx, number, []string{"projects/" + number + "/services/run.googleapis.com"})
	if err != nil {
		t.Fatalf("BatchGetAPIs by number: %v", err)
	}
	if len(apis) != 1 || apis[0].State != StateEnabled || apis[0].Name != "projects/proj/services/run.googleapis.com" {
		t.Fatalf("BatchGetAPIs by number = %+v", apis)
	}

	if len(r.calls) == 0 {
		t.Fatal("resolver was never consulted")
	}
}

// TestProjectIDKeepsSynthesizeOnRead is the control: an id, including the
// documented "-" token, is still a project the emulator has never seen, so its
// services answer DISABLED and the resolver is not consulted.
func TestProjectIDKeepsSynthesizeOnRead(t *testing.T) {
	ctx := context.Background()
	r := &fakeResolver{}
	s := newResolvedCore(r)

	for _, project := range []string{"proj", "another-project", "-"} {
		api, err := s.GetAPI(ctx, project, "run.googleapis.com")
		if err != nil {
			t.Fatalf("GetAPI(%q): %v", project, err)
		}
		if api.State != StateDisabled || api.Name != "projects/"+project+"/services/run.googleapis.com" {
			t.Fatalf("GetAPI(%q) = %+v, want synthesized DISABLED", project, api)
		}
	}
	if len(r.calls) != 0 {
		t.Fatalf("resolver consulted for project IDs: %v", r.calls)
	}
}

// TestUnknownProjectNumberWithoutResolver: with no registry wired no number can
// resolve, so the zero project is still rejected rather than synthesized.
func TestUnknownProjectNumberWithoutResolver(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, err := s.GetAPI(ctx, "00000000000", "serviceusage.googleapis.com"); !isUnknownProject(err) {
		t.Fatalf("error = %v, want unknown project", err)
	}
}

// TestProjectResolverErrorPropagates: a registry outage is a 5xx, never a
// fabricated 403 or an invented project.
func TestProjectResolverErrorPropagates(t *testing.T) {
	ctx := context.Background()
	s := newResolvedCore(&fakeResolver{err: store.ErrStorageUnavailable})
	if _, err := s.GetAPI(ctx, "123456789012", "run.googleapis.com"); !errors.Is(err, store.ErrStorageUnavailable) {
		t.Fatalf("error = %v, want ErrStorageUnavailable", err)
	}
}

// isUnknownProject reports whether err is the 403 the rule returns.
func isUnknownProject(err error) bool {
	var pe *model.ProviderError
	return errors.As(err, &pe) && pe.Code == "PermissionDenied" && pe.HTTPStatus == 403
}
