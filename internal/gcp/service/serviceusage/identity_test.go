package serviceusage

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// TestServiceAgentName covers the documented agent-domain derivation: the
// service DNS name with its .googleapis.com suffix dropped, and a fail-loud
// false for anything that is not a Google API service.
func TestServiceAgentName(t *testing.T) {
	for _, tc := range []struct {
		service string
		want    string
		ok      bool
	}{
		{"secretmanager.googleapis.com", "secretmanager", true},
		{"pubsub.googleapis.com", "pubsub", true},
		{"cloudresourcemanager.googleapis.com", "cloudresourcemanager", true},
		{"dataproc-metastore.googleapis.com", "dataproc-metastore", true},
		{"example.com", "", false},
		{"secretmanager", "", false},
		{".googleapis.com", "", false},
		{"proj/services/x.googleapis.com", "", false},
		{"UPPER.googleapis.com", "", false},
		{"-bad.googleapis.com", "", false},
	} {
		got, ok := ServiceAgentName(tc.service)
		if got != tc.want || ok != tc.ok {
			t.Errorf("ServiceAgentName(%q) = (%q, %v), want (%q, %v)", tc.service, got, ok, tc.want, tc.ok)
		}
	}
}

// TestGenerateServiceIdentity covers the wire contract: the documented email
// form, a 21-digit numeric uniqueId, an inline done operation carrying the
// identity as its response payload, and idempotence — a second call returns the
// same identity and does not create a second service-agent record.
func TestGenerateServiceIdentity(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	op, err := s.GenerateServiceIdentity(ctx, "proj", "secretmanager.googleapis.com")
	if err != nil {
		t.Fatalf("GenerateServiceIdentity: %v", err)
	}
	if !op.Done || op.Verb != "generateServiceIdentity" {
		t.Fatalf("operation = %+v, want a done generateServiceIdentity operation", op)
	}
	if op.Identity == nil {
		t.Fatal("operation carries no identity")
	}
	number := resource.ProjectNumber("proj")
	wantEmail := "service-" + number + "@gcp-sa-secretmanager.iam.gserviceaccount.com"
	if op.Identity.Email != wantEmail {
		t.Errorf("email = %q, want %q", op.Identity.Email, wantEmail)
	}
	if !regexp.MustCompile(`^1[0-9]{20}$`).MatchString(op.Identity.UniqueID) {
		t.Errorf("uniqueId = %q, want a 21-digit numeric id beginning with 1", op.Identity.UniqueID)
	}
	if len(op.ResourceNames) != 1 || op.ResourceNames[0] != "projects/proj/services/secretmanager.googleapis.com" {
		t.Errorf("resourceNames = %v", op.ResourceNames)
	}

	// Idempotent: repeated calls return the same identity (real GCP returns the
	// existing service agent, with no ALREADY_EXISTS).
	again, err := s.GenerateServiceIdentity(ctx, "proj", "secretmanager.googleapis.com")
	if err != nil {
		t.Fatalf("second GenerateServiceIdentity: %v", err)
	}
	if again.Identity == nil || *again.Identity != *op.Identity {
		t.Fatalf("second identity = %+v, want %+v", again.Identity, op.Identity)
	}
	if again.Name == op.Name {
		t.Errorf("each call must return its own operation, both named %q", op.Name)
	}

	// The identity is persisted per project and per service.
	other, err := s.GenerateServiceIdentity(ctx, "other", "secretmanager.googleapis.com")
	if err != nil {
		t.Fatalf("GenerateServiceIdentity(other): %v", err)
	}
	if other.Identity.Email == op.Identity.Email {
		t.Errorf("two projects share the email %q", other.Identity.Email)
	}
	pubsub, err := s.GenerateServiceIdentity(ctx, "proj", "pubsub.googleapis.com")
	if err != nil {
		t.Fatalf("GenerateServiceIdentity(pubsub): %v", err)
	}
	if pubsub.Identity.Email == op.Identity.Email {
		t.Errorf("two services share the email %q", pubsub.Identity.Email)
	}
}

// TestGenerateServiceIdentityPersistedAcrossInstances proves the identity is
// stored, not merely re-derived: a fresh core over the same store reads it back
// unchanged (the property that keeps an IAM binding's member stable across
// restarts).
func TestGenerateServiceIdentityPersistedAcrossInstances(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()

	first, err := NewService(resources).GenerateServiceIdentity(ctx, "proj", "kms.googleapis.com")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := NewService(resources).GenerateServiceIdentity(ctx, "proj", "kms.googleapis.com")
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if *first.Identity != *second.Identity {
		t.Fatalf("identity changed across cores: %+v -> %+v", first.Identity, second.Identity)
	}
}

// TestGenerateServiceIdentityUnsupportedService covers the error path: a name
// that is not a service under .googleapis.com has no Google-managed identity,
// so the call fails with a 400 FAILED_PRECONDITION instead of synthesizing an
// agent for an arbitrary string.
func TestGenerateServiceIdentityUnsupportedService(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	for _, service := range []string{"example.com", "not-a-service", "foo/bar.googleapis.com"} {
		_, err := s.GenerateServiceIdentity(ctx, "proj", service)
		var perr *model.ProviderError
		if !errors.As(err, &perr) {
			t.Fatalf("%q: err = %v, want a provider error", service, err)
		}
		if perr.Code != "FailedPrecondition" || perr.HTTPStatus != 400 {
			t.Errorf("%q: err = %v, want 400 FailedPrecondition", service, err)
		}
	}
	if _, err := s.GenerateServiceIdentity(ctx, "", "pubsub.googleapis.com"); err == nil {
		t.Error("empty project must be InvalidArgument")
	}
	if _, err := s.GenerateServiceIdentity(ctx, "proj", ""); err == nil {
		t.Error("empty service must be InvalidArgument")
	}
}

// identityNumberResolver maps a project number to a project ID, standing in for
// the Resource Manager registry.
type identityNumberResolver map[string]string

func (r identityNumberResolver) ProjectByNumber(_ context.Context, number string) (string, bool, error) {
	id, ok := r[number]
	return id, ok, nil
}

// TestGenerateServiceIdentityProjectNumber proves a numeric parent resolves to
// the project ID, so projects/{number}/services/... and
// projects/{id}/services/... share one identity (real clients — the Firebase
// CLI among them — address the parent by number).
func TestGenerateServiceIdentityProjectNumber(t *testing.T) {
	ctx := context.Background()
	number := resource.ProjectNumber("proj")
	s := NewService(store.NewMemoryResourceStore(), WithProjectResolver(identityNumberResolver{number: "proj"}))

	byNumber, err := s.GenerateServiceIdentity(ctx, number, "pubsub.googleapis.com")
	if err != nil {
		t.Fatalf("by number: %v", err)
	}
	byID, err := s.GenerateServiceIdentity(ctx, "proj", "pubsub.googleapis.com")
	if err != nil {
		t.Fatalf("by id: %v", err)
	}
	if *byNumber.Identity != *byID.Identity {
		t.Fatalf("number and id resolved different identities: %+v vs %+v", byNumber.Identity, byID.Identity)
	}

	// An unknown number is the 403 real Service Usage returns.
	if _, err := s.GenerateServiceIdentity(ctx, "00000000000", "pubsub.googleapis.com"); err == nil {
		t.Error("unknown project number must fail")
	}
}
