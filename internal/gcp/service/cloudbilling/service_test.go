package cloudbilling

import (
	"context"
	"encoding/json"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// TestGetProjectBillingInfoAbsent locks the default answer: a project with no
// stored association is simply not billed, not an error.
func TestGetProjectBillingInfoAbsent(t *testing.T) {
	ctx := context.Background()
	s := NewService(store.NewMemoryResourceStore())

	info, err := s.GetProjectBillingInfo(ctx, "p")
	if err != nil {
		t.Fatalf("GetProjectBillingInfo: %v", err)
	}
	if info.ProjectID != "p" {
		t.Errorf("ProjectID = %q, want p", info.ProjectID)
	}
	if info.BillingAccountName != "" {
		t.Errorf("BillingAccountName = %q, want empty", info.BillingAccountName)
	}
	if got := BillingInfoName("p"); got != "projects/p/billingInfo" {
		t.Errorf("BillingInfoName(p) = %q, want projects/p/billingInfo", got)
	}
}

// TestGetProjectBillingInfoStored locks the stored-association read and its
// project scoping: the shared ResourceStore entry is keyed by the project id, so
// one project's account never leaks into another's read.
func TestGetProjectBillingInfoStored(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	s := NewService(resources)

	data, err := json.Marshal(BillingInfo{ProjectID: "p", BillingAccountName: "billingAccounts/0X0X0X-0X0X0X-0X0X0X"})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	if err := resources.Upsert(ctx, "p", store.GlobalRegion, store.ResourceEntry{
		Type: rtBillingInfo, ID: "p", Data: data,
	}); err != nil {
		t.Fatalf("seed record: %v", err)
	}

	info, err := s.GetProjectBillingInfo(ctx, "p")
	if err != nil {
		t.Fatalf("GetProjectBillingInfo: %v", err)
	}
	if info.BillingAccountName != "billingAccounts/0X0X0X-0X0X0X-0X0X0X" {
		t.Errorf("BillingAccountName = %q, want the stored account", info.BillingAccountName)
	}
	if info.ProjectID != "p" {
		t.Errorf("ProjectID = %q, want p", info.ProjectID)
	}

	other, err := s.GetProjectBillingInfo(ctx, "q")
	if err != nil {
		t.Fatalf("GetProjectBillingInfo(q): %v", err)
	}
	if other.BillingAccountName != "" {
		t.Errorf("project q inherited %q from p", other.BillingAccountName)
	}
}

// TestSetProjectBillingInfoRoundTrip locks the write path in the core: the full
// resource name is stored verbatim, a following read returns it, and an empty
// value clears the association so the read falls back to the zero association.
func TestSetProjectBillingInfoRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewService(store.NewMemoryResourceStore())
	const account = "billingAccounts/0X0X0X-0X0X0X-0X0X0X"

	set, err := s.SetProjectBillingInfo(ctx, "p", account)
	if err != nil {
		t.Fatalf("SetProjectBillingInfo: %v", err)
	}
	if set.ProjectID != "p" || set.BillingAccountName != account {
		t.Fatalf("SetProjectBillingInfo = %+v, want p/%s", set, account)
	}
	got, err := s.GetProjectBillingInfo(ctx, "p")
	if err != nil {
		t.Fatalf("GetProjectBillingInfo: %v", err)
	}
	if got.BillingAccountName != account {
		t.Fatalf("GetProjectBillingInfo = %q, want the stored account", got.BillingAccountName)
	}

	// Clearing stores the empty association: the read answers empty, not the
	// previous account.
	if _, err := s.SetProjectBillingInfo(ctx, "p", ""); err != nil {
		t.Fatalf("SetProjectBillingInfo(clear): %v", err)
	}
	got, err = s.GetProjectBillingInfo(ctx, "p")
	if err != nil {
		t.Fatalf("GetProjectBillingInfo after clear: %v", err)
	}
	if got.BillingAccountName != "" {
		t.Fatalf("GetProjectBillingInfo after clear = %q, want empty", got.BillingAccountName)
	}
	if got.ProjectID != "p" {
		t.Fatalf("ProjectID after clear = %q, want p", got.ProjectID)
	}
	// The write is project-scoped: another project is untouched.
	other, err := s.GetProjectBillingInfo(ctx, "q")
	if err != nil {
		t.Fatalf("GetProjectBillingInfo(q): %v", err)
	}
	if other.BillingAccountName != "" {
		t.Fatalf("project q inherited %q", other.BillingAccountName)
	}
}

// TestSetProjectBillingInfoValidation locks D4's form check: a non-empty value
// must be a billingAccounts/{account_id} resource name; anything else is
// InvalidArgument, and no record is written.
func TestSetProjectBillingInfoValidation(t *testing.T) {
	ctx := context.Background()
	s := NewService(store.NewMemoryResourceStore())
	for _, account := range []string{
		"projects/x",
		"0X0X0X-0X0X0X-0X0X0X",
		"billingAccounts/",
		"billingAccounts",
		" billingAccounts/0X0X0X",
		"billingAccounts/0X0X0X ",
	} {
		_, err := s.SetProjectBillingInfo(ctx, "p", account)
		if err == nil {
			t.Errorf("SetProjectBillingInfo(%q) succeeded, want InvalidArgument", account)
			continue
		}
		perr, ok := err.(*model.ProviderError)
		if !ok {
			t.Errorf("SetProjectBillingInfo(%q) error %v is not a *model.ProviderError", account, err)
			continue
		}
		if perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Errorf("SetProjectBillingInfo(%q) = %d/%q, want 400/InvalidArgument", account, perr.HTTPStatus, perr.Code)
		}
	}
	got, err := s.GetProjectBillingInfo(ctx, "p")
	if err != nil {
		t.Fatalf("GetProjectBillingInfo: %v", err)
	}
	if got.BillingAccountName != "" {
		t.Fatalf("a rejected write stored %q", got.BillingAccountName)
	}
}

// TestListBillingAccountsEmpty locks the catalog stance: no account lifecycle
// exists in the emulator, so the catalog is empty rather than fabricated (CB5).
func TestListBillingAccountsEmpty(t *testing.T) {
	s := NewService(store.NewMemoryResourceStore())
	if got := s.ListBillingAccounts(context.Background()); len(got) != 0 {
		t.Fatalf("ListBillingAccounts = %v, want empty", got)
	}
}
