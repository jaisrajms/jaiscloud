package workflows

import (
	"testing"

	"jaiscloud/internal/gcp/resource"
)

// TestDefaultServiceAccount covers the workflow default identity: it is the
// project's Compute Engine default service account, so it follows the same
// project-number rule as Compute and IAM (a numeric project reference is its own
// number, anything else is the synthesized number).
func TestDefaultServiceAccount(t *testing.T) {
	if got, want := DefaultServiceAccount("proj"),
		"projects/proj/serviceAccounts/"+resource.ProjectNumber("proj")+"-compute@developer.gserviceaccount.com"; got != want {
		t.Errorf("DefaultServiceAccount(proj) = %q, want %q", got, want)
	}
	if got, want := DefaultServiceAccount("123456"),
		"projects/123456/serviceAccounts/123456-compute@developer.gserviceaccount.com"; got != want {
		t.Errorf("DefaultServiceAccount(numeric) = %q, want %q", got, want)
	}
}
