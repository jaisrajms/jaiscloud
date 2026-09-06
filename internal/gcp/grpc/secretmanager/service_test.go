package secretmanager

import (
	"context"
	"hash/crc32"
	"net"
	"testing"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

	"jaiscloud/internal/gcp/crypto"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// secretTestService dials a real in-process gRPC server backed by the memory
// stores and returns the Secret Manager and IAM clients.
func secretTestService(t *testing.T) (secretmanagerpb.SecretManagerServiceClient, iampb.IAMPolicyClient, func()) {
	t.Helper()
	secrets := secretmanagerstore.NewMemoryStore()
	resources := store.NewMemoryResourceStore()
	encryptor := crypto.NewEnvelopeEncryptor(kmsstore.NewMemoryStore())
	svc := NewService(secrets, resources, encryptor, "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	secretmanagerpb.RegisterSecretManagerServiceServer(srv, svc)
	iampb.RegisterIAMPolicyServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return secretmanagerpb.NewSecretManagerServiceClient(conn), iampb.NewIAMPolicyClient(conn), cleanup
}

func TestSecretManagerEndToEnd(t *testing.T) {
	client, _, cleanup := secretTestService(t)
	defer cleanup()
	ctx := context.Background()

	const secret = "projects/test/secrets/my-secret"

	// Create secret.
	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/test",
		SecretId: "my-secret",
		Secret:   &secretmanagerpb.Secret{Labels: map[string]string{"env": "dev"}},
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}
	if created.GetName() != secret {
		t.Fatalf("CreateSecret name = %q, want %q", created.GetName(), secret)
	}
	if created.GetEtag() == "" {
		t.Fatal("CreateSecret etag is empty")
	}

	// Duplicate → AlreadyExists.
	if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: "projects/test", SecretId: "my-secret",
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate CreateSecret err = %v, want AlreadyExists", err)
	}

	// Get returns a stable etag.
	got, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: secret})
	if err != nil {
		t.Fatalf("GetSecret: %v", err)
	}
	if got.GetEtag() != created.GetEtag() {
		t.Fatalf("GetSecret etag = %q, want %q", got.GetEtag(), created.GetEtag())
	}
	if got.GetLabels()["env"] != "dev" {
		t.Fatalf("GetSecret labels = %v, want env=dev", got.GetLabels())
	}

	// List secrets.
	listed, err := client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: "projects/test"})
	if err != nil {
		t.Fatalf("ListSecrets: %v", err)
	}
	if len(listed.GetSecrets()) != 1 || listed.GetSecrets()[0].GetName() != secret {
		t.Fatalf("ListSecrets = %v, want exactly [%s]", listed.GetSecrets(), secret)
	}

	// Add version.
	payload := []byte("hello world")
	addRes, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent:  secret,
		Payload: &secretmanagerpb.SecretPayload{Data: payload},
	})
	if err != nil {
		t.Fatalf("AddSecretVersion: %v", err)
	}
	if addRes.GetName() != secret+"/versions/1" {
		t.Fatalf("AddSecretVersion name = %q, want %q", addRes.GetName(), secret+"/versions/1")
	}
	if addRes.GetState() != secretmanagerpb.SecretVersion_ENABLED {
		t.Fatalf("AddSecretVersion state = %v, want ENABLED", addRes.GetState())
	}

	// Access decrypts the payload and returns a matching crc32c.
	access, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: secret + "/versions/1"})
	if err != nil {
		t.Fatalf("AccessSecretVersion: %v", err)
	}
	if string(access.GetPayload().GetData()) != string(payload) {
		t.Fatalf("AccessSecretVersion data = %q, want %q", string(access.GetPayload().GetData()), string(payload))
	}
	wantCRC := int64(crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)))
	if access.GetPayload().GetDataCrc32C() != wantCRC {
		t.Fatalf("AccessSecretVersion crc32c = %d, want %d", access.GetPayload().GetDataCrc32C(), wantCRC)
	}

	// List versions.
	versions, err := client.ListSecretVersions(ctx, &secretmanagerpb.ListSecretVersionsRequest{Parent: secret})
	if err != nil {
		t.Fatalf("ListSecretVersions: %v", err)
	}
	if len(versions.GetVersions()) != 1 || versions.GetVersions()[0].GetName() != secret+"/versions/1" {
		t.Fatalf("ListSecretVersions = %v, want exactly [versions/1]", versions.GetVersions())
	}

	// Disable → Enable → Destroy lifecycle.
	dis, err := client.DisableSecretVersion(ctx, &secretmanagerpb.DisableSecretVersionRequest{Name: secret + "/versions/1"})
	if err != nil {
		t.Fatalf("DisableSecretVersion: %v", err)
	}
	if dis.GetState() != secretmanagerpb.SecretVersion_DISABLED {
		t.Fatalf("DisableSecretVersion state = %v, want DISABLED", dis.GetState())
	}

	en, err := client.EnableSecretVersion(ctx, &secretmanagerpb.EnableSecretVersionRequest{Name: secret + "/versions/1"})
	if err != nil {
		t.Fatalf("EnableSecretVersion: %v", err)
	}
	if en.GetState() != secretmanagerpb.SecretVersion_ENABLED {
		t.Fatalf("EnableSecretVersion state = %v, want ENABLED", en.GetState())
	}

	des, err := client.DestroySecretVersion(ctx, &secretmanagerpb.DestroySecretVersionRequest{Name: secret + "/versions/1"})
	if err != nil {
		t.Fatalf("DestroySecretVersion: %v", err)
	}
	if des.GetState() != secretmanagerpb.SecretVersion_DESTROYED {
		t.Fatalf("DestroySecretVersion state = %v, want DESTROYED", des.GetState())
	}

	// Get a missing version → NotFound.
	if _, err := client.GetSecretVersion(ctx, &secretmanagerpb.GetSecretVersionRequest{Name: secret + "/versions/99"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetSecretVersion missing err = %v, want NotFound", err)
	}

	// Delete secret.
	if _, err := client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: secret}); err != nil {
		t.Fatalf("DeleteSecret: %v", err)
	}
	if _, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: secret}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetSecret after delete err = %v, want NotFound", err)
	}
}

func TestSecretManagerIamPolicy(t *testing.T) {
	client, iam, cleanup := secretTestService(t)
	defer cleanup()
	ctx := context.Background()

	const secret = "projects/test/secrets/iam-secret"

	if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: "projects/test", SecretId: "iam-secret",
	}); err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}

	// Empty policy by default.
	pol, err := iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: secret})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if len(pol.GetBindings()) != 0 {
		t.Fatalf("GetIamPolicy bindings = %v, want empty", pol.GetBindings())
	}

	// Set IAM policy.
	_, err = iam.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: secret,
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{{Role: "roles/secretmanager.secretAccessor", Members: []string{"allUsers"}}},
		},
	})
	if err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}

	// Read it back.
	pol, err = iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: secret})
	if err != nil {
		t.Fatalf("GetIamPolicy after set: %v", err)
	}
	if len(pol.GetBindings()) != 1 || pol.GetBindings()[0].GetRole() != "roles/secretmanager.secretAccessor" {
		t.Fatalf("GetIamPolicy bindings = %v, want roles/secretmanager.secretAccessor", pol.GetBindings())
	}

	// Test permissions echoes the request (no-authz posture).
	tp, err := iam.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource: secret, Permissions: []string{"secretmanager.secrets.get", "secretmanager.versions.access"},
	})
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(tp.GetPermissions()) != 2 {
		t.Fatalf("TestIamPermissions = %v, want 2 granted", tp.GetPermissions())
	}

	// IAM on a missing secret → NotFound.
	if _, err := iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: "projects/test/secrets/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetIamPolicy missing err = %v, want NotFound", err)
	}
}

func TestSecretManagerVersionAliases(t *testing.T) {
	client, _, cleanup := secretTestService(t)
	defer cleanup()
	ctx := context.Background()

	const secret = "projects/test/secrets/aliased"

	// Create with a "latest" version alias (stored metadata, matching REST).
	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/test",
		SecretId: "aliased",
		Secret: &secretmanagerpb.Secret{
			VersionAliases: map[string]int64{"latest": 1},
		},
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}
	if created.GetVersionAliases()["latest"] != 1 {
		t.Fatalf("CreateSecret versionAliases = %v, want latest=1", created.GetVersionAliases())
	}

	// Add a real version 1.
	if _, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent:  secret,
		Payload: &secretmanagerpb.SecretPayload{Data: []byte("hi")},
	}); err != nil {
		t.Fatalf("AddSecretVersion: %v", err)
	}

	// Update the alias to point at the new version.
	updated, err := client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret: &secretmanagerpb.Secret{
			Name:           secret,
			VersionAliases: map[string]int64{"latest": 2},
		},
	})
	if err != nil {
		t.Fatalf("UpdateSecret: %v", err)
	}
	if updated.GetVersionAliases()["latest"] != 2 {
		t.Fatalf("UpdateSecret versionAliases = %v, want latest=2", updated.GetVersionAliases())
	}
}

func TestSecretManagerLatestAlias(t *testing.T) {
	client, _, cleanup := secretTestService(t)
	defer cleanup()
	ctx := context.Background()

	const secret = "projects/test/secrets/latest-secret"

	if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: "projects/test", SecretId: "latest-secret",
	}); err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}

	// Add version 1.
	if _, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent: secret, Payload: &secretmanagerpb.SecretPayload{Data: []byte("v1")},
	}); err != nil {
		t.Fatalf("AddSecretVersion v1: %v", err)
	}

	// "latest" resolves to version 1.
	access, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: secret + "/versions/latest"})
	if err != nil {
		t.Fatalf("AccessSecretVersion latest (first): %v", err)
	}
	if string(access.GetPayload().GetData()) != "v1" {
		t.Fatalf("AccessSecretVersion latest data = %q, want v1", string(access.GetPayload().GetData()))
	}

	// Add version 2 → "latest" must now resolve to it.
	if _, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent: secret, Payload: &secretmanagerpb.SecretPayload{Data: []byte("v2")},
	}); err != nil {
		t.Fatalf("AddSecretVersion v2: %v", err)
	}

	access, err = client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: secret + "/versions/latest"})
	if err != nil {
		t.Fatalf("AccessSecretVersion latest (second): %v", err)
	}
	if string(access.GetPayload().GetData()) != "v2" {
		t.Fatalf("AccessSecretVersion latest data = %q, want v2", string(access.GetPayload().GetData()))
	}

	// GetSecretVersion with "latest" resolves the same way.
	ver, err := client.GetSecretVersion(ctx, &secretmanagerpb.GetSecretVersionRequest{Name: secret + "/versions/latest"})
	if err != nil {
		t.Fatalf("GetSecretVersion latest: %v", err)
	}
	if ver.GetName() != secret+"/versions/2" {
		t.Fatalf("GetSecretVersion latest name = %q, want %q", ver.GetName(), secret+"/versions/2")
	}
}
