package serviceaccount

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/store"
)

// TestDefaults covers the Google-managed service accounts the IAM provider
// renders without a create: an Eventarc trigger that names one must resolve,
// and the rendered shape must be stable across reads.
func TestDefaults(t *testing.T) {
	const project = "proj"
	number := resource.ProjectNumber(project)
	appspot := project + "@appspot.gserviceaccount.com"
	compute := number + "-compute@developer.gserviceaccount.com"

	defaults := Defaults(project)
	if len(defaults) != 2 {
		t.Fatalf("Defaults(%q) returned %d accounts, want 2", project, len(defaults))
	}
	if defaults[0].Email != appspot || defaults[0].DisplayName != "App Engine default service account" {
		t.Errorf("defaults[0] = %+v, want the App Engine default", defaults[0])
	}
	if defaults[1].Email != compute || defaults[1].DisplayName != "Compute Engine default service account" {
		t.Errorf("defaults[1] = %+v, want the Compute Engine default", defaults[1])
	}
	for _, d := range defaults {
		if len(d.UniqueID) != 21 || d.UniqueID[0] != '1' {
			t.Errorf("uniqueId %q must be a 21-digit numeric id starting with 1", d.UniqueID)
		}
		if again, _ := DefaultByEmail(project, d.Email); again.UniqueID != d.UniqueID {
			t.Errorf("uniqueId of %q churned between reads", d.Email)
		}
		if got, ok := DefaultByUniqueID(project, d.UniqueID); !ok || got.Email != d.Email {
			t.Errorf("DefaultByUniqueID(%q) = (%+v, %v), want %q", d.UniqueID, got, ok, d.Email)
		}
		if d.UniqueID == defaults[0].UniqueID && d.Email != defaults[0].Email {
			t.Errorf("%q reuses the same uniqueId as %q", d.Email, defaults[0].Email)
		}
	}

	for _, email := range []string{appspot, compute} {
		if !ImplicitDefault(project, email) {
			t.Errorf("ImplicitDefault(%q) = false, want true", email)
		}
	}
	for _, email := range []string{
		"missing@" + project + ".iam.gserviceaccount.com",
		"other@example.com",
		// The Google APIs service agent is not a default every project has:
		// a live project with many enabled APIs answers 404 for it (measured
		// 2026-11-09), so it must not count as implicitly live.
		number + "@cloudservices.gserviceaccount.com",
	} {
		if ImplicitDefault(project, email) {
			t.Errorf("ImplicitDefault(%q) = true, want false", email)
		}
	}

	// The wildcard resolver can recover the project from an appspot email
	// directly and the number from a compute email.
	if got := DefaultEmailProject(appspot); got != project {
		t.Errorf("DefaultEmailProject(%q) = %q, want %q", appspot, got, project)
	}
	if got := DefaultEmailNumber(compute); got != number {
		t.Errorf("DefaultEmailNumber(%q) = %q, want %q", compute, got, number)
	}
	if got := DefaultEmailNumber("abc-compute@developer.gserviceaccount.com"); got != "" {
		t.Errorf("DefaultEmailNumber of a non-numeric id = %q, want \"\"", got)
	}
	if got := DefaultEmailProject("sa@" + project + ".iam.gserviceaccount.com"); got != "" {
		t.Errorf("DefaultEmailProject of a created account = %q, want \"\"", got)
	}
	if Defaults("") != nil || Defaults("-") != nil {
		t.Error("Defaults must be empty for an unscoped project")
	}

	// A project addressed by number (Discovery's project parameter accepts
	// both) reports the same Compute default as the Compute surface: the number
	// is taken at face value, not hashed.
	const numeric = "123456789012"
	numericDefaults := Defaults(numeric)
	if got, want := numericDefaults[1].Email, numeric+"-compute@developer.gserviceaccount.com"; got != want {
		t.Errorf("numeric-project compute default = %q, want %q", got, want)
	}
	if got, want := resource.ProjectNumberFor(numeric), numeric; got != want {
		t.Errorf("ProjectNumberFor(%q) = %q, want the number itself", numeric, got)
	}
}

// TestExisterResolvesAccountsAndDefaults covers the adapter Eventarc uses:
// explicit records resolve, the implicit defaults resolve without a record, and
// an unknown account is (false, nil).
func TestExisterResolvesAccountsAndDefaults(t *testing.T) {
	ctx := context.Background()
	res := store.NewMemoryResourceStore()
	if err := res.Create(ctx, "proj", store.GlobalRegion, store.ResourceEntry{
		Type: AccountResourceType, ID: "sa@proj.iam.gserviceaccount.com", Data: []byte(`{"email":"sa@proj.iam.gserviceaccount.com"}`),
	}); err != nil {
		t.Fatalf("seed account: %v", err)
	}
	e := Exister{Resources: res}

	for _, email := range []string{
		"sa@proj.iam.gserviceaccount.com",
		"proj@appspot.gserviceaccount.com",
		resource.ProjectNumber("proj") + "-compute@developer.gserviceaccount.com",
	} {
		ok, err := e.ServiceAccountExists(ctx, "proj", email)
		if err != nil || !ok {
			t.Errorf("ServiceAccountExists(%q) = (%v, %v), want (true, nil)", email, ok, err)
		}
	}
	ok, err := e.ServiceAccountExists(ctx, "proj", "missing@proj.iam.gserviceaccount.com")
	if err != nil || ok {
		t.Errorf("unknown account = (%v, %v), want (false, nil)", ok, err)
	}
}
