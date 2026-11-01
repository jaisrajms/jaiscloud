// iam_key_upload_test.go covers DC27 through the official apiary IAM client:
// projects.serviceAccounts.keys.upload takes the caller's public key (documented
// as an RSA public key wrapped in an X.509 v3 certificate, base64 on the wire)
// and returns a USER_MANAGED / USER_PROVIDED key that round-trips through
// keys.get and keys.list. Uploaded keys carry no private material, so the
// response must not contain privateKeyData and a duplicate upload of the same
// public key is ALREADY_EXISTS (the key id is derived from the key).
package sdkrest_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iam/v1"
)

// uploadCertificatePEM builds a self-signed X.509 v3 certificate around a fresh
// 2048-bit RSA public key: exactly the envelope keys.upload documents for
// publicKeyData. No private key leaves the test.
func uploadCertificatePEM(t *testing.T) (*rsa.PublicKey, string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "sdk-rest-key-upload"},
		NotBefore:             time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:              time.Date(2120, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return &priv.PublicKey, base64.StdEncoding.EncodeToString(pemBytes)
}

func TestSDKIAMKeyUpload(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId: unique("upload"),
	}).Do()
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = svc.Projects.ServiceAccounts.Delete(sa.Name).Do()
	})

	wantPub, publicKeyData := uploadCertificatePEM(t)

	uploaded, err := svc.Projects.ServiceAccounts.Keys.Upload(sa.Name, &iam.UploadServiceAccountKeyRequest{
		PublicKeyData: publicKeyData,
	}).Do()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(uploaded.Name, sa.Name+"/keys/"),
		"keys.upload name = %q, want the %q prefix", uploaded.Name, sa.Name+"/keys/")
	require.Equal(t, "USER_MANAGED", uploaded.KeyType)
	require.Equal(t, "USER_PROVIDED", uploaded.KeyOrigin)
	require.Equal(t, "KEY_ALG_RSA_2048", uploaded.KeyAlgorithm)
	// Discovery scopes the key material fields to create/get only.
	require.Empty(t, uploaded.PrivateKeyData, "upload response must not carry privateKeyData")
	require.Empty(t, uploaded.PrivateKeyType, "upload response must not carry privateKeyType")
	require.Empty(t, uploaded.PublicKeyData, "upload response must not carry publicKeyData (get-only)")

	// keys.get renders the uploaded public key.
	got, err := svc.Projects.ServiceAccounts.Keys.Get(uploaded.Name).Do()
	require.NoError(t, err)
	require.Equal(t, uploaded.Name, got.Name)
	require.Equal(t, "USER_MANAGED", got.KeyType)
	require.Equal(t, "USER_PROVIDED", got.KeyOrigin)
	require.NotEmpty(t, got.PublicKeyData)
	raw, err := base64.StdEncoding.DecodeString(got.PublicKeyData)
	require.NoError(t, err)
	block, _ := pem.Decode(raw)
	require.NotNil(t, block)
	require.Equal(t, "PUBLIC KEY", block.Type)
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	require.NoError(t, err)
	require.Equal(t, 0, pub.(*rsa.PublicKey).N.Cmp(wantPub.N), "publicKeyData must describe the uploaded key")

	// keys.list includes it, classified as user-managed.
	listed, err := svc.Projects.ServiceAccounts.Keys.List(sa.Name).Do()
	require.NoError(t, err)
	var found *iam.ServiceAccountKey
	for _, k := range listed.Keys {
		if k.Name == uploaded.Name {
			found = k
		}
	}
	require.NotNil(t, found, "keys.list dropped the uploaded key %q", uploaded.Name)
	require.Equal(t, "USER_MANAGED", found.KeyType)
	require.Equal(t, "USER_PROVIDED", found.KeyOrigin)

	// The lifecycle verbs work on an uploaded key.
	_, err = svc.Projects.ServiceAccounts.Keys.Disable(uploaded.Name, &iam.DisableServiceAccountKeyRequest{}).Do()
	require.NoError(t, err)
	got, err = svc.Projects.ServiceAccounts.Keys.Get(uploaded.Name).Do()
	require.NoError(t, err)
	require.True(t, got.Disabled)
	_, err = svc.Projects.ServiceAccounts.Keys.Enable(uploaded.Name, &iam.EnableServiceAccountKeyRequest{}).Do()
	require.NoError(t, err)
	got, err = svc.Projects.ServiceAccounts.Keys.Get(uploaded.Name).Do()
	require.NoError(t, err)
	require.False(t, got.Disabled)

	// The key id is derived from the public key: uploading it again conflicts.
	_, err = svc.Projects.ServiceAccounts.Keys.Upload(sa.Name, &iam.UploadServiceAccountKeyRequest{
		PublicKeyData: publicKeyData,
	}).Do()
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr, "duplicate upload")
	require.Equal(t, 409, apiErr.Code, "duplicate upload must be ALREADY_EXISTS")

	_, err = svc.Projects.ServiceAccounts.Keys.Delete(uploaded.Name).Do()
	require.NoError(t, err)
	_, err = svc.Projects.ServiceAccounts.Keys.Get(uploaded.Name).Do()
	require.ErrorAs(t, err, &apiErr, "get after delete")
	require.Equal(t, 404, apiErr.Code)
}
