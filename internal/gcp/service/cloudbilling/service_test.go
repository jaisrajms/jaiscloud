package cloudbilling

import (
	"context"
	"encoding/json"
	"testing"

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
