// Package sdkrest_test — core security-service depth. The base suite covers the
// happy-path lifecycles; this file drives the AWS p2–p5 analogues through the
// official REST clients: Cloud KMS key import (the CKM_RSA_AES_KEY_WRAP flow),
// Secret Manager secret IAM policy, and IAM service-account disable/enable/
// undelete + service-account IAM policy.
package sdkrest_test

import (
	"context"
	"crypto/aes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"testing"

	"google.golang.org/api/cloudkms/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/secretmanager/v1"

	"github.com/stretchr/testify/require"
)

// TestSDKKMSImportJobAndImportedVersion drives the Cloud KMS import flow end to
// end: create an import job, fetch its RSA wrapping public key, wrap a 256-bit
// key with the CKM_RSA_AES_KEY_WRAP scheme (RSA-OAEP(SHA-256) over a one-time
// AES key-wrapping key, then AES-KWP of the material), import the version, and
// prove the imported material is live by encrypting/decrypting with it.
func TestSDKKMSImportJobAndImportedVersion(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudkms.NewService(ctx, opts()...)
	require.NoError(t, err)

	const method = "RSA_OAEP_3072_SHA256_AES_256"

	parent := "projects/proj/locations/global"
	krID := unique("kr")
	_, err = svc.Projects.Locations.KeyRings.Create(parent, &cloudkms.KeyRing{}).KeyRingId(krID).Do()
	require.NoError(t, err)
	krName := parent + "/keyRings/" + krID

	keyID := unique("imp")
	keyName, err := func() (string, error) {
		_, e := svc.Projects.Locations.KeyRings.CryptoKeys.Create(krName,
			&cloudkms.CryptoKey{Purpose: "ENCRYPT_DECRYPT"}).CryptoKeyId(keyID).Do()
		return krName + "/cryptoKeys/" + keyID, e
	}()
	require.NoError(t, err)

	job, err := svc.Projects.Locations.KeyRings.ImportJobs.Create(krName, &cloudkms.ImportJob{
		ImportMethod: method,
	}).ImportJobId(unique("job")).Do()
	require.NoError(t, err)
	require.Equal(t, method, job.ImportMethod)
	require.NotNil(t, job.PublicKey)
	require.NotEmpty(t, job.PublicKey.Pem, "an RSA import job must expose a PEM wrapping key")

	block, _ := pem.Decode([]byte(job.PublicKey.Pem))
	require.NotNil(t, block, "wrapping public key must be PEM")
	pubAny, err := x509.ParsePKIXPublicKey(block.Bytes)
	require.NoError(t, err)
	rpub, ok := pubAny.(*rsa.PublicKey)
	require.True(t, ok, "RSA import method must yield an RSA wrapping key")

	// CKM_RSA_AES_KEY_WRAP: a random 256-bit AES key-wrapping key is RSA-OAEP
	// encrypted, and the key material is AES-KWP wrapped by it.
	material := []byte("0123456789abcdef0123456789abcdef") // 32 bytes
	kwpKey := make([]byte, 32)
	_, err = rand.Read(kwpKey)
	require.NoError(t, err)
	encKwpKey, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, rpub, kwpKey, nil)
	require.NoError(t, err)
	wrappedKey := append(encKwpKey, aesKeyWrapWithPadding(t, kwpKey, material)...)

	ver, err := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.Import(
		keyName+"/cryptoKeyVersions", &cloudkms.ImportCryptoKeyVersionRequest{
			ImportJob:  job.Name,
			Algorithm:  "GOOGLE_SYMMETRIC_ENCRYPTION",
			WrappedKey: base64.StdEncoding.EncodeToString(wrappedKey),
		}).Do()
	require.NoError(t, err)
	require.Equal(t, "ENABLED", ver.State)
	require.NotEmpty(t, ver.ImportTime, "an imported version reports its import time")

	// The imported material is live: encrypt/decrypt round-trips.
	ct, err := svc.Projects.Locations.KeyRings.CryptoKeys.Encrypt(keyName, &cloudkms.EncryptRequest{
		Plaintext: b64("imported material is live"),
	}).Do()
	require.NoError(t, err)
	pt, err := svc.Projects.Locations.KeyRings.CryptoKeys.Decrypt(keyName, &cloudkms.DecryptRequest{
		Ciphertext: ct.Ciphertext,
	}).Do()
	require.NoError(t, err)
	require.Equal(t, b64("imported material is live"), pt.Plaintext)
}

// TestSDKSecretManagerIAMPolicy covers secret getIamPolicy/setIamPolicy
// round-trip and etag OCC (a stale-etag write is 409), plus testIamPermissions.
func TestSDKSecretManagerIAMPolicy(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	secret, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication: &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
	}).SecretId(unique("iam")).Do()
	require.NoError(t, err)

	pol, err := svc.Projects.Secrets.GetIamPolicy(secret.Name).Do()
	require.NoError(t, err)
	require.NotEmpty(t, pol.Etag, "getIamPolicy must return an etag for OCC")

	set, err := svc.Projects.Secrets.SetIamPolicy(secret.Name, &secretmanager.SetIamPolicyRequest{
		Policy: &secretmanager.Policy{
			Etag: pol.Etag,
			Bindings: []*secretmanager.Binding{{
				Role:    "roles/secretmanager.secretAccessor",
				Members: []string{"allUsers"},
			}},
		},
	}).Do()
	require.NoError(t, err)
	require.Len(t, set.Bindings, 1)
	require.Equal(t, "roles/secretmanager.secretAccessor", set.Bindings[0].Role)

	// Reusing the now-stale etag is rejected 409 (ABORTED surfaced on REST).
	_, err = svc.Projects.Secrets.SetIamPolicy(secret.Name, &secretmanager.SetIamPolicyRequest{
		Policy: &secretmanager.Policy{
			Etag:     pol.Etag,
			Bindings: []*secretmanager.Binding{{Role: "roles/secretmanager.admin", Members: []string{"allUsers"}}},
		},
	}).Do()
	var ae *googleapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, 409, ae.Code)

	perms, err := svc.Projects.Secrets.TestIamPermissions(secret.Name,
		&secretmanager.TestIamPermissionsRequest{Permissions: []string{"secretmanager.versions.access"}}).Do()
	require.NoError(t, err)
	require.Contains(t, perms.Permissions, "secretmanager.versions.access")
}

// TestSDKIAMServiceAccountState covers service-account disable/enable, delete
// and undelete through the official IAM client, plus service-account IAM policy.
func TestSDKIAMServiceAccountState(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId:      unique("state"),
		ServiceAccount: &iam.ServiceAccount{DisplayName: "State SA"},
	}).Do()
	require.NoError(t, err)

	// Disable → disabled.
	_, err = svc.Projects.ServiceAccounts.Disable(sa.Name, &iam.DisableServiceAccountRequest{}).Do()
	require.NoError(t, err)
	got, err := svc.Projects.ServiceAccounts.Get(sa.Name).Do()
	require.NoError(t, err)
	require.True(t, got.Disabled, "account must report disabled")

	// Enable → not disabled.
	_, err = svc.Projects.ServiceAccounts.Enable(sa.Name, &iam.EnableServiceAccountRequest{}).Do()
	require.NoError(t, err)
	got, err = svc.Projects.ServiceAccounts.Get(sa.Name).Do()
	require.NoError(t, err)
	require.False(t, got.Disabled)

	// Delete → get 404 → undelete → get succeeds, restoredAccount populated.
	_, err = svc.Projects.ServiceAccounts.Delete(sa.Name).Do()
	require.NoError(t, err)
	_, err = svc.Projects.ServiceAccounts.Get(sa.Name).Do()
	requireNotFound(t, err)

	restored, err := svc.Projects.ServiceAccounts.Undelete(sa.Name, &iam.UndeleteServiceAccountRequest{}).Do()
	require.NoError(t, err)
	require.NotNil(t, restored.RestoredAccount, "undelete must return {restoredAccount: {...}}")
	require.Equal(t, sa.Email, restored.RestoredAccount.Email)
	_, err = svc.Projects.ServiceAccounts.Get(sa.Name).Do()
	require.NoError(t, err)

	// Service-account IAM policy round-trips (with etag OCC).
	pol, err := svc.Projects.ServiceAccounts.GetIamPolicy(sa.Name).Do()
	require.NoError(t, err)
	require.NotEmpty(t, pol.Etag)
	set, err := svc.Projects.ServiceAccounts.SetIamPolicy(sa.Name, &iam.SetIamPolicyRequest{
		Policy: &iam.Policy{
			Etag: pol.Etag,
			Bindings: []*iam.Binding{{
				Role:    "roles/iam.serviceAccountUser",
				Members: []string{"allUsers"},
			}},
		},
	}).Do()
	require.NoError(t, err)
	require.Len(t, set.Bindings, 1)
	require.Equal(t, "roles/iam.serviceAccountUser", set.Bindings[0].Role)
}

// aesKeyWrapWithPadding implements RFC 5649 AES Key Wrap with Padding (the
// second half of CKM_RSA_AES_KEY_WRAP). It mirrors the emulator's
// kmsstore.AESKeyWrapWithPadding so the test builds the on-wire wrapped key the
// real client produces.
func aesKeyWrapWithPadding(t *testing.T, kek, plaintext []byte) []byte {
	t.Helper()
	block, err := aes.NewCipher(kek)
	require.NoError(t, err)

	n := (len(plaintext) + 7) / 8

	// AIV: 0xA65959A6 || MLI.
	a := make([]byte, 8)
	binary.BigEndian.PutUint32(a[:4], 0xA65959A6)
	binary.BigEndian.PutUint32(a[4:], uint32(len(plaintext)))

	r := make([][]byte, n)
	padded := make([]byte, n*8)
	copy(padded, plaintext)
	for i := 0; i < n; i++ {
		r[i] = padded[i*8 : i*8+8]
	}

	var buf [16]byte
	for j := 0; j < 6; j++ {
		for i := 0; i < n; i++ {
			copy(buf[:8], a)
			copy(buf[8:], r[i])
			block.Encrypt(buf[:], buf[:])
			copy(a, buf[:8])
			xorUint64(a, uint64(n*j+i+1))
			copy(r[i], buf[8:])
		}
	}

	out := make([]byte, 8*(n+1))
	copy(out, a)
	for i := 0; i < n; i++ {
		copy(out[8+i*8:], r[i])
	}
	return out
}

func xorUint64(b []byte, t uint64) {
	var x [8]byte
	binary.BigEndian.PutUint64(x[:], t)
	for i := range b {
		b[i] ^= x[i]
	}
}
