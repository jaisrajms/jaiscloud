//go:build gcp_persistence

package iamcredentials

import (
	"context"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// TestPostgresExistenceAndDelegates runs the J67/J68 contract against the
// PostgreSQL backend: `--dsn` resolves the service account through the same
// registry and validates the same delegation chain as the memory store, so a
// restart does not resurrect the lazy signing path.
func TestPostgresExistenceAndDelegates(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres store test")
	}
	ctx := context.Background()
	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	res := store.ResourceStore(pg)
	res.Reset(ctx)
	defer res.Reset(ctx)

	s := New(res)
	seedAccount(t, s, testProject, testEmail)
	seedAccount(t, s, testProject, delegateEmail)

	token, _, err := s.GenerateAccessToken(ctx, "-", testEmail, []string{cloudScope}, 0, nil)
	if err != nil {
		t.Fatalf("GenerateAccessToken: %v", err)
	}
	if claims := jwtClaims(t, token); claims["email"] != testEmail {
		t.Errorf("access token email = %v, want %s", claims["email"], testEmail)
	}

	_, _, err = s.GenerateAccessToken(ctx, "-", "ghost@"+testProject+".iam.gserviceaccount.com", []string{cloudScope}, 0, nil)
	if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Errorf("missing account = %v, want 404 NotFound", err)
	}

	if _, _, err := s.SignBlob(ctx, "-", testEmail, []byte("hello"), []string{
		"projects/-/serviceAccounts/" + delegateEmail,
	}); err != nil {
		t.Fatalf("delegated SignBlob: %v", err)
	}
	if _, _, err := s.SignJWT(ctx, "-", testEmail, `{"iss":"test"}`, []string{delegateEmail}); err == nil {
		t.Error("malformed delegate chain: expected an error")
	}
}
