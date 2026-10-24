package metastore

import (
	"context"
	"errors"
	"testing"

	metastorestore "jaiscloud/internal/gcp/store/metastore"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newCoreWithResources() (*Service, store.ResourceStore) {
	res := store.NewMemoryResourceStore()
	return NewService(metastorestore.NewMemoryStore(), WithResources(res)), res
}

func providerCode(err error) string {
	var pe *model.ProviderError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func TestFederationCRUD(t *testing.T) {
	ctx := context.Background()
	s, _ := newCoreWithResources()

	if _, _, err := s.CreateFederation(ctx, "p", "us-central1", "fed", []byte(`{"version":"3.1.2","labels":{"env":"dev"}}`)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, _, err := s.CreateFederation(ctx, "p", "us-central1", "fed", nil); providerCode(err) != "AlreadyExists" {
		t.Fatalf("duplicate create = %v, want AlreadyExists", err)
	}

	got, err := s.GetFederation(ctx, "p", "us-central1", "fed")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != "ACTIVE" || got.Labels["env"] != "dev" {
		t.Fatalf("federation fields lost: %+v", got)
	}

	// No mask: the body replaces config wholesale.
	if _, _, err := s.UpdateFederation(ctx, "p", "us-central1", "fed", []byte(`{"version":"3.2.0"}`), ""); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ = s.GetFederation(ctx, "p", "us-central1", "fed")
	if out := FederationJSON(got, "p"); out["version"] != "3.2.0" {
		t.Fatalf("update version = %v", out["version"])
	}

	// An unknown snake_case mask root fails loud.
	if _, _, err := s.UpdateFederation(ctx, "p", "us-central1", "fed", []byte(`{}`), "bogus_field"); providerCode(err) != "InvalidArgument" {
		t.Fatalf("bad mask = %v, want InvalidArgument", err)
	}

	page, next, err := s.ListFederations(ctx, "p", "us-central1", 0, "")
	if err != nil || len(page) != 1 || next != "" {
		t.Fatalf("list = %v %d %q", err, len(page), next)
	}

	if _, err := s.DeleteFederation(ctx, "p", "us-central1", "fed"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetFederation(ctx, "p", "us-central1", "fed"); providerCode(err) != "NotFound" {
		t.Fatalf("get after delete = %v, want NotFound", err)
	}
}

func TestFederationJSONShape(t *testing.T) {
	ctx := context.Background()
	s, _ := newCoreWithResources()
	if _, _, err := s.CreateFederation(ctx, "p", "us-central1", "fed", []byte(`{"version":"3.1.2"}`)); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, _ := s.GetFederation(ctx, "p", "us-central1", "fed")
	out := FederationJSON(got, "p")
	if out["name"] != "projects/p/locations/us-central1/federations/fed" {
		t.Errorf("name = %v", out["name"])
	}
	if out["state"] != "ACTIVE" {
		t.Errorf("state = %v", out["state"])
	}
	if out["uid"] == "" || out["endpointUri"] == "" {
		t.Errorf("uid/endpointUri missing: %+v", out)
	}
	if out["version"] != "3.1.2" {
		t.Errorf("version echoed = %v", out["version"])
	}
}

func TestIAMPolicyLevels(t *testing.T) {
	ctx := context.Background()
	s, _ := newCoreWithResources()

	if _, _, err := s.CreateService(ctx, "p", "us-central1", "svc", []byte(`{"labels":{"env":"dev"}}`)); err != nil {
		t.Fatalf("create service: %v", err)
	}
	if _, _, err := s.CreateBackup(ctx, "p", "us-central1", "svc", "bk", nil); err != nil {
		t.Fatalf("create backup: %v", err)
	}
	if _, _, err := s.CreateFederation(ctx, "p", "us-central1", "fed", nil); err != nil {
		t.Fatalf("create federation: %v", err)
	}

	svcName := ServiceName("p", "us-central1", "svc")
	bkName := BackupName("p", "us-central1", "svc", "bk")
	fedName := FederationName("p", "us-central1", "fed")
	dbName := "projects/p/locations/us-central1/services/svc/databases/db"
	tblName := dbName + "/tables/tbl"

	// Every level starts as an empty default policy.
	for _, name := range []string{svcName, bkName, fedName, dbName, tblName} {
		pol, err := s.GetIamPolicy(ctx, name)
		if err != nil {
			t.Fatalf("get policy %s: %v", name, err)
		}
		if len(pol.Bindings) != 0 {
			t.Fatalf("policy %s not empty: %+v", name, pol)
		}
	}

	// Set a policy at the service level; a stale etag is rejected.
	body := map[string]any{"bindings": []any{map[string]any{"role": "roles/owner", "members": []any{"user:a@example.com"}}}}
	pol, err := s.SetIamPolicy(ctx, svcName, body)
	if err != nil {
		t.Fatalf("set policy: %v", err)
	}
	if pol.Etag == "" || len(pol.Bindings) != 1 {
		t.Fatalf("set policy = %+v", pol)
	}
	stale := map[string]any{
		"bindings": []any{map[string]any{"role": "roles/viewer", "members": []any{"user:b@example.com"}}},
		"etag":     "stale",
	}
	if _, err := s.SetIamPolicy(ctx, svcName, stale); providerCode(err) != "Aborted" {
		t.Fatalf("stale etag = %v, want Aborted", err)
	}

	// Policies are isolated per level: writing the service policy must not leak
	// into the backup/database/table names (same relative id, different type).
	backupPol, _ := s.GetIamPolicy(ctx, bkName)
	if len(backupPol.Bindings) != 0 {
		t.Fatalf("backup policy leaked: %+v", backupPol)
	}

	perms, err := s.TestIamPermissions(ctx, tblName, []string{"metastore.services.get"})
	if err != nil || len(perms) != 1 {
		t.Fatalf("test permissions = %v %v", perms, err)
	}

	// Service/backup/federation IAM requires the resource to exist; database and
	// table IAM is metadata-only (the control plane does not model them).
	if _, err := s.GetIamPolicy(ctx, ServiceName("p", "us-central1", "missing")); providerCode(err) != "NotFound" {
		t.Fatalf("missing service iam = %v, want NotFound", err)
	}
	if _, err := s.GetIamPolicy(ctx, "projects/p/locations/us-central1/services/svc/databases/gone"); err != nil {
		t.Fatalf("missing database iam should be metadata-only, got %v", err)
	}

	// Not a metastore resource name.
	if _, err := s.GetIamPolicy(ctx, "projects/p/topics/t"); providerCode(err) != "InvalidArgument" {
		t.Fatalf("invalid iam name = %v, want InvalidArgument", err)
	}
}

func TestParseIAMResource(t *testing.T) {
	cases := []struct {
		name  string
		level IAMLevel
	}{
		{"projects/p/locations/l/services/s", IAMService},
		{"projects/p/locations/l/services/s/backups/b", IAMBackup},
		{"projects/p/locations/l/services/s/databases/d", IAMDatabase},
		{"projects/p/locations/l/services/s/databases/d/tables/t", IAMTable},
		{"projects/p/locations/l/federations/f", IAMFederation},
	}
	for _, tc := range cases {
		r, ok := ParseIAMResource(tc.name)
		if !ok || r.Level != tc.level {
			t.Errorf("ParseIAMResource(%q) = %+v ok=%v, want level %v", tc.name, r, ok, tc.level)
		}
	}
	if _, ok := ParseIAMResource("projects/p/topics/t"); ok {
		t.Error("a non-metastore name should not parse")
	}
}
