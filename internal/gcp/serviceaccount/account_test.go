package serviceaccount

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/store"
)

// TestImplicitDefault covers the Google-managed service accounts every project
// has without a create: an Eventarc trigger that names one must resolve.
func TestImplicitDefault(t *testing.T) {
	const project = "proj"
	number := resource.ProjectNumber(project)
	for _, email := range []string{
		project + "@appspot.gserviceaccount.com",
		number + "-compute@developer.gserviceaccount.com",
		number + "@cloudservices.gserviceaccount.com",
	} {
		if !ImplicitDefault(project, email) {
			t.Errorf("ImplicitDefault(%q) = false, want true", email)
		}
	}
	for _, email := range []string{
		"missing@" + project + ".iam.gserviceaccount.com",
		"other@example.com",
	} {
		if ImplicitDefault(project, email) {
			t.Errorf("ImplicitDefault(%q) = true, want false", email)
		}
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
