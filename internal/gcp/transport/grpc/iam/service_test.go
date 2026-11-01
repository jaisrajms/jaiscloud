package iam

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	adminpb "cloud.google.com/go/iam/admin/apiv1/adminpb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	iamprovider "jaiscloud/internal/gcp/provider/iam"
	"jaiscloud/internal/store"
)

// testCertificatePEM wraps pub in a self-signed X.509 v3 certificate: the
// documented publicKeyData form for keys.upload.
func testCertificatePEM(t *testing.T, pub *rsa.PublicKey) []byte {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate certificate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "iam-grpc-key-upload-test"},
		NotBefore:             time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:              time.Date(2120, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	return NewService(iamprovider.New(store.NewMemoryResourceStore()))
}

// TestUploadServiceAccountKey drives the admin IAM gRPC surface through the
// same provider the REST codec uses: create the service account, upload a
// caller-supplied public key, read it back (get + list), then delete it. The
// uploaded key must round-trip as USER_MANAGED / USER_PROVIDED with no private
// material.
func TestUploadServiceAccountKey(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	parent := "projects/proj"

	sa, err := svc.CreateServiceAccount(ctx, &adminpb.CreateServiceAccountRequest{
		Name:      parent,
		AccountId: "sa",
		ServiceAccount: &adminpb.ServiceAccount{
			DisplayName: "gRPC SA",
		},
	})
	if err != nil {
		t.Fatalf("CreateServiceAccount: %v", err)
	}
	if sa.GetEmail() != "sa@proj.iam.gserviceaccount.com" {
		t.Fatalf("email = %q", sa.GetEmail())
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	cert := testCertificatePEM(t, &key.PublicKey)

	uploaded, err := svc.UploadServiceAccountKey(ctx, &adminpb.UploadServiceAccountKeyRequest{
		Name:          sa.GetName(),
		PublicKeyData: cert,
	})
	if err != nil {
		t.Fatalf("UploadServiceAccountKey: %v", err)
	}
	if uploaded.GetKeyType() != adminpb.ListServiceAccountKeysRequest_USER_MANAGED {
		t.Errorf("keyType = %v, want USER_MANAGED", uploaded.GetKeyType())
	}
	if uploaded.GetKeyOrigin() != adminpb.ServiceAccountKeyOrigin_USER_PROVIDED {
		t.Errorf("keyOrigin = %v, want USER_PROVIDED", uploaded.GetKeyOrigin())
	}
	if uploaded.GetKeyAlgorithm() != adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_2048 {
		t.Errorf("keyAlgorithm = %v, want KEY_ALG_RSA_2048", uploaded.GetKeyAlgorithm())
	}
	if len(uploaded.GetPrivateKeyData()) != 0 || len(uploaded.GetPublicKeyData()) != 0 {
		t.Errorf("upload response carried key material: private=%d public=%d",
			len(uploaded.GetPrivateKeyData()), len(uploaded.GetPublicKeyData()))
	}
	if got, want := uploaded.GetName(), sa.GetName()+"/keys/"; !strings.HasPrefix(got, want) {
		t.Errorf("name = %q, want prefix %q", got, want)
	}

	// keys.get carries publicKeyData; it must describe the uploaded key.
	got, err := svc.GetServiceAccountKey(ctx, &adminpb.GetServiceAccountKeyRequest{Name: uploaded.GetName()})
	if err != nil {
		t.Fatalf("GetServiceAccountKey: %v", err)
	}
	block, _ := pem.Decode(got.GetPublicKeyData())
	if block == nil || block.Type != "PUBLIC KEY" {
		t.Fatalf("publicKeyData is not a PUBLIC KEY PEM block: %q", got.GetPublicKeyData())
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse publicKeyData: %v", err)
	}
	if pub.(*rsa.PublicKey).N.Cmp(key.PublicKey.N) != 0 {
		t.Fatal("publicKeyData does not describe the uploaded key")
	}
	if len(got.GetPrivateKeyData()) != 0 {
		t.Error("keys.get returned privateKeyData")
	}

	// keys.list contains it.
	list, err := svc.ListServiceAccountKeys(ctx, &adminpb.ListServiceAccountKeysRequest{Name: sa.GetName()})
	if err != nil {
		t.Fatalf("ListServiceAccountKeys: %v", err)
	}
	found := false
	for _, k := range list.GetKeys() {
		if k.GetName() == uploaded.GetName() {
			found = true
			if k.GetKeyType() != adminpb.ListServiceAccountKeysRequest_USER_MANAGED {
				t.Errorf("listed keyType = %v, want USER_MANAGED", k.GetKeyType())
			}
		}
	}
	if !found {
		t.Fatalf("keys.list dropped the uploaded key %q", uploaded.GetName())
	}

	// A duplicate upload of the same public key is ALREADY_EXISTS.
	if _, err := svc.UploadServiceAccountKey(ctx, &adminpb.UploadServiceAccountKeyRequest{
		Name:          sa.GetName(),
		PublicKeyData: cert,
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate upload code = %v, want AlreadyExists", status.Code(err))
	}

	// disable/enable then delete it.
	if _, err := svc.DisableServiceAccountKey(ctx, &adminpb.DisableServiceAccountKeyRequest{Name: uploaded.GetName()}); err != nil {
		t.Fatalf("DisableServiceAccountKey: %v", err)
	}
	if got, err = svc.GetServiceAccountKey(ctx, &adminpb.GetServiceAccountKeyRequest{Name: uploaded.GetName()}); err != nil {
		t.Fatalf("get after disable: %v", err)
	}
	if !got.GetDisabled() {
		t.Error("uploaded key not disabled after DisableServiceAccountKey")
	}
	if _, err := svc.EnableServiceAccountKey(ctx, &adminpb.EnableServiceAccountKeyRequest{Name: uploaded.GetName()}); err != nil {
		t.Fatalf("EnableServiceAccountKey: %v", err)
	}
	if _, err := svc.DeleteServiceAccountKey(ctx, &adminpb.DeleteServiceAccountKeyRequest{Name: uploaded.GetName()}); err != nil {
		t.Fatalf("DeleteServiceAccountKey: %v", err)
	}
	if _, err := svc.GetServiceAccountKey(ctx, &adminpb.GetServiceAccountKeyRequest{Name: uploaded.GetName()}); status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete code = %v, want NotFound", status.Code(err))
	}
}

// TestUploadServiceAccountKeyRejects asserts malformed and unsupported keys
// surface the real gRPC status, never a mapped-away success.
func TestUploadServiceAccountKeyRejects(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	sa, err := svc.CreateServiceAccount(ctx, &adminpb.CreateServiceAccountRequest{
		Name: "projects/proj", AccountId: "sa",
	})
	if err != nil {
		t.Fatalf("CreateServiceAccount: %v", err)
	}

	if _, err := svc.UploadServiceAccountKey(ctx, &adminpb.UploadServiceAccountKeyRequest{Name: sa.GetName()}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("missing key code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := svc.UploadServiceAccountKey(ctx, &adminpb.UploadServiceAccountKeyRequest{
		Name: sa.GetName(), PublicKeyData: []byte("not a key"),
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("malformed key code = %v, want InvalidArgument", status.Code(err))
	}
	big, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		t.Fatalf("generate 4096: %v", err)
	}
	if _, err := svc.UploadServiceAccountKey(ctx, &adminpb.UploadServiceAccountKeyRequest{
		Name: sa.GetName(), PublicKeyData: testCertificatePEM(t, &big.PublicKey),
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("4096-bit key code = %v, want InvalidArgument", status.Code(err))
	}
	// A missing service account keeps the wildcard 403 / concrete 404 split.
	ok, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate 2048: %v", err)
	}
	if _, err := svc.UploadServiceAccountKey(ctx, &adminpb.UploadServiceAccountKeyRequest{
		Name: "projects/proj/serviceAccounts/missing@proj.iam.gserviceaccount.com", PublicKeyData: testCertificatePEM(t, &ok.PublicKey),
	}); status.Code(err) != codes.NotFound {
		t.Errorf("missing SA code = %v, want NotFound", status.Code(err))
	}
	if _, err := svc.UploadServiceAccountKey(ctx, &adminpb.UploadServiceAccountKeyRequest{
		Name: "projects/-/serviceAccounts/missing@proj.iam.gserviceaccount.com", PublicKeyData: testCertificatePEM(t, &ok.PublicKey),
	}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("missing SA via wildcard code = %v, want PermissionDenied", status.Code(err))
	}
}

// TestServiceAccountLifecycleOverGRPC covers the sibling admin RPCs the service
// maps onto the same provider: get/list/patch/disable/sign and the
// service-account IAM policy, so the gRPC admin surface is not upload-only.
func TestServiceAccountLifecycleOverGRPC(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	sa, err := svc.CreateServiceAccount(ctx, &adminpb.CreateServiceAccountRequest{
		Name: "projects/proj", AccountId: "sa",
		ServiceAccount: &adminpb.ServiceAccount{DisplayName: "before"},
	})
	if err != nil {
		t.Fatalf("CreateServiceAccount: %v", err)
	}

	got, err := svc.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: sa.GetName()})
	if err != nil {
		t.Fatalf("GetServiceAccount: %v", err)
	}
	if got.GetDisplayName() != "before" {
		t.Errorf("displayName = %q, want before", got.GetDisplayName())
	}

	patched, err := svc.PatchServiceAccount(ctx, &adminpb.PatchServiceAccountRequest{
		ServiceAccount: &adminpb.ServiceAccount{Name: sa.GetName(), DisplayName: "after"},
		UpdateMask:     &fieldmaskpb.FieldMask{Paths: []string{"displayName"}},
	})
	if err != nil {
		t.Fatalf("PatchServiceAccount: %v", err)
	}
	if patched.GetDisplayName() != "after" {
		t.Errorf("patched displayName = %q, want after", patched.GetDisplayName())
	}

	list, err := svc.ListServiceAccounts(ctx, &adminpb.ListServiceAccountsRequest{Name: "projects/proj"})
	if err != nil {
		t.Fatalf("ListServiceAccounts: %v", err)
	}
	if len(list.GetAccounts()) != 1 {
		t.Fatalf("ListServiceAccounts returned %d accounts, want 1", len(list.GetAccounts()))
	}

	if _, err := svc.DisableServiceAccount(ctx, &adminpb.DisableServiceAccountRequest{Name: sa.GetName()}); err != nil {
		t.Fatalf("DisableServiceAccount: %v", err)
	}
	got, err = svc.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: sa.GetName()})
	if err != nil {
		t.Fatalf("get after disable: %v", err)
	}
	if !got.GetDisabled() {
		t.Error("service account not disabled after DisableServiceAccount")
	}
	if _, err := svc.EnableServiceAccount(ctx, &adminpb.EnableServiceAccountRequest{Name: sa.GetName()}); err != nil {
		t.Fatalf("EnableServiceAccount: %v", err)
	}

	// signBlob still signs with the system-managed key.
	signed, err := svc.SignBlob(ctx, &adminpb.SignBlobRequest{Name: sa.GetName(), BytesToSign: []byte("hello")})
	if err != nil {
		t.Fatalf("SignBlob: %v", err)
	}
	if signed.GetKeyId() == "" || len(signed.GetSignature()) == 0 {
		t.Errorf("SignBlob returned keyId=%q signature=%d bytes", signed.GetKeyId(), len(signed.GetSignature()))
	}

	// Service-account IAM policy round-trips.
	const role = "roles/iam.serviceAccountUser"
	set, err := svc.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: sa.GetName(),
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: role, Members: []string{"allUsers"}}}},
	})
	if err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	if len(set.GetBindings()) != 1 || set.GetBindings()[0].GetRole() != role {
		t.Fatalf("SetIamPolicy bindings = %v", set.GetBindings())
	}
	pol, err := svc.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: sa.GetName()})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if len(pol.GetBindings()) != 1 || pol.GetBindings()[0].GetRole() != role {
		t.Fatalf("GetIamPolicy bindings = %v", pol.GetBindings())
	}
	perms, err := svc.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    sa.GetName(),
		Permissions: []string{"iam.serviceAccounts.get"},
	})
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(perms.GetPermissions()) != 1 || perms.GetPermissions()[0] != "iam.serviceAccounts.get" {
		t.Fatalf("TestIamPermissions = %v", perms.GetPermissions())
	}

	// Delete (and the undelete tombstone path) round-trip.
	if _, err := svc.DeleteServiceAccount(ctx, &adminpb.DeleteServiceAccountRequest{Name: sa.GetName()}); err != nil {
		t.Fatalf("DeleteServiceAccount: %v", err)
	}
	if _, err := svc.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: sa.GetName()}); status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete code = %v, want NotFound", status.Code(err))
	}
	restored, err := svc.UndeleteServiceAccount(ctx, &adminpb.UndeleteServiceAccountRequest{Name: sa.GetName()})
	if err != nil {
		t.Fatalf("UndeleteServiceAccount: %v", err)
	}
	if restored.GetRestoredAccount().GetEmail() != sa.GetEmail() {
		t.Errorf("restored email = %q, want %q", restored.GetRestoredAccount().GetEmail(), sa.GetEmail())
	}
}
