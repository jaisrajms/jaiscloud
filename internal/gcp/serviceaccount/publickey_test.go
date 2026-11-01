package serviceaccount

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"testing"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// testCertificate wraps pub in a self-signed X.509 v3 certificate, the form
// projects.serviceAccounts.keys.upload documents for publicKeyData.
func testCertificate(t *testing.T, pub *rsa.PublicKey) []byte {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate cert key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "serviceaccount-publickey-test"},
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

func publicKeyPEM(t *testing.T, pub *rsa.PublicKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("marshal PKIX: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

func pkcs1PublicKeyPEM(t *testing.T, pub *rsa.PublicKey) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(pub)})
}

func TestParseUploadedPublicKeyAcceptedForms(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	pkixDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("marshal PKIX: %v", err)
	}

	cert := testCertificate(t, &key.PublicKey)
	pubPEM := publicKeyPEM(t, &key.PublicKey)
	pkcs1PEM := pkcs1PublicKeyPEM(t, &key.PublicKey)

	forms := map[string][]byte{
		"certificate PEM": cert,
		"PUBLIC KEY PEM":  pubPEM,
		"RSA PUBLIC KEY":  pkcs1PEM,
		"PKIX DER":        pkixDER,
		"PKCS1 DER":       x509.MarshalPKCS1PublicKey(&key.PublicKey),
	}
	var first *UploadedPublicKey
	for name, data := range forms {
		parsed, err := ParseUploadedPublicKey(data)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if parsed.Algorithm != "KEY_ALG_RSA_2048" {
			t.Errorf("%s: algorithm = %q, want KEY_ALG_RSA_2048", name, parsed.Algorithm)
		}
		// Every accepted envelope must canonicalise to the same PKIX DER and
		// therefore the same stable key id.
		if string(parsed.PKIXDER) != string(pkixDER) {
			t.Errorf("%s: PKIX DER differs from x509.MarshalPKIXPublicKey", name)
		}
		if first == nil {
			first = parsed
			continue
		}
		if parsed.KeyID != first.KeyID {
			t.Errorf("%s: key id = %q, want %q (deterministic per key)", name, parsed.KeyID, first.KeyID)
		}
	}
	if len(first.KeyID) != 40 {
		t.Errorf("key id %q has length %d, want 40 (real service-account key id shape)", first.KeyID, len(first.KeyID))
	}
	// Same input, same id: determinism across calls.
	again, err := ParseUploadedPublicKey(cert)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	if again.KeyID != first.KeyID {
		t.Errorf("key id changed between parses: %q vs %q", again.KeyID, first.KeyID)
	}
}

func TestParseUploadedPublicKeyAlgorithms(t *testing.T) {
	for _, tc := range []struct {
		bits int
		want string
	}{
		{1024, "KEY_ALG_RSA_1024"},
		{2048, "KEY_ALG_RSA_2048"},
	} {
		key, err := rsa.GenerateKey(rand.Reader, tc.bits)
		if err != nil {
			t.Fatalf("generate %d: %v", tc.bits, err)
		}
		parsed, err := ParseUploadedPublicKey(publicKeyPEM(t, &key.PublicKey))
		if err != nil {
			t.Fatalf("%d-bit: %v", tc.bits, err)
		}
		if parsed.Algorithm != tc.want {
			t.Errorf("%d-bit: algorithm = %q, want %q", tc.bits, parsed.Algorithm, tc.want)
		}
	}
}

func TestParseUploadedPublicKeyRejects(t *testing.T) {
	invalidArgument := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: expected an error", name)
		}
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Fatalf("%s: error = %v, want 400 InvalidArgument", name, err)
		}
	}

	_, err := ParseUploadedPublicKey(nil)
	invalidArgument("empty", err)

	// 4096-bit RSA is not representable: ServiceAccountKey.keyAlgorithm has no
	// member for it (only KEY_ALG_RSA_1024 and KEY_ALG_RSA_2048).
	big, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		t.Fatalf("generate 4096: %v", err)
	}
	_, err = ParseUploadedPublicKey(publicKeyPEM(t, &big.PublicKey))
	invalidArgument("4096-bit RSA", err)

	// A non-RSA key is not a service-account key.
	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate EC: %v", err)
	}
	ecDER, err := x509.MarshalPKIXPublicKey(&ec.PublicKey)
	if err != nil {
		t.Fatalf("marshal EC: %v", err)
	}
	_, err = ParseUploadedPublicKey(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: ecDER}))
	invalidArgument("EC public key", err)

	_, err = ParseUploadedPublicKey([]byte("not a key"))
	invalidArgument("garbage", err)

	_, err = ParseUploadedPublicKey(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: ecDER}))
	invalidArgument("wrong PEM block type", err)
}

func TestUploadKeyStoresPublicMaterialOnly(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	cert := testCertificate(t, &key.PublicKey)

	uploaded, err := UploadKey(ctx, resources, "proj", "sa@proj.iam.gserviceaccount.com", cert)
	if err != nil {
		t.Fatalf("UploadKey: %v", err)
	}
	if uploaded.PrivateDER != "" {
		t.Errorf("uploaded key stored a private DER: %q", uploaded.PrivateDER)
	}
	if uploaded.PublicDER == "" {
		t.Fatal("uploaded key has no stored public DER")
	}
	if uploaded.Type() != KeyType || uploaded.Origin() != KeyOrigin {
		t.Errorf("uploaded key type/origin = %s/%s, want %s/%s", uploaded.Type(), uploaded.Origin(), KeyType, KeyOrigin)
	}

	// PrivDER fails loudly: an uploaded key has no private half, so signing with
	// it must never reach the signer with empty bytes.
	if _, err := uploaded.PrivDER(); !errors.Is(err, ErrNoPrivateKey) {
		t.Fatalf("PrivDER on uploaded key = %v, want ErrNoPrivateKey", err)
	}
	if _, err := SignBlob(nil, []byte("hello")); err == nil {
		t.Fatal("SignBlob with no private key material succeeded")
	}

	// PublicKeyData renders the caller's key as the get-only base64 PKIX PEM.
	encoded, ok := uploaded.PublicKeyData()
	if !ok {
		t.Fatal("uploaded key has no publicKeyData")
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("publicKeyData is not base64: %v", err)
	}
	block, _ := pem.Decode(raw)
	if block == nil || block.Type != "PUBLIC KEY" {
		t.Fatal("publicKeyData is not a PUBLIC KEY PEM block")
	}
	got, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse publicKeyData: %v", err)
	}
	if got.(*rsa.PublicKey).N.Cmp(key.PublicKey.N) != 0 {
		t.Fatal("publicKeyData does not describe the uploaded key")
	}

	// A reload from the store behaves identically (no in-memory state).
	loaded, err := LoadKey(ctx, resources, "proj", "sa@proj.iam.gserviceaccount.com", uploaded.KeyID)
	if err != nil {
		t.Fatalf("LoadKey: %v", err)
	}
	if _, err := loaded.PrivDER(); !errors.Is(err, ErrNoPrivateKey) {
		t.Fatalf("loaded uploaded key PrivDER = %v, want ErrNoPrivateKey", err)
	}
	if _, ok := loaded.PublicKeyData(); !ok {
		t.Fatal("loaded uploaded key has no publicKeyData")
	}

	// Same public key again: deterministic id, ALREADY_EXISTS.
	_, err = UploadKey(ctx, resources, "proj", "sa@proj.iam.gserviceaccount.com", cert)
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 409 || pe.Code != "AlreadyExists" {
		t.Fatalf("duplicate upload = %v, want 409 AlreadyExists", err)
	}
}

func TestCreateKeyWithAlgorithm(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()

	k, err := CreateKeyWithAlgorithm(ctx, resources, "proj", "sa@proj.iam.gserviceaccount.com", "KEY_ALG_RSA_1024")
	if err != nil {
		t.Fatalf("CreateKeyWithAlgorithm: %v", err)
	}
	if k.Algorithm != "KEY_ALG_RSA_1024" {
		t.Errorf("algorithm = %q, want KEY_ALG_RSA_1024", k.Algorithm)
	}
	der, err := k.PrivDER()
	if err != nil {
		t.Fatalf("PrivDER: %v", err)
	}
	priv, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		t.Fatalf("parse private key: %v", err)
	}
	if got := priv.(*rsa.PrivateKey).N.BitLen(); got != 1024 {
		t.Errorf("generated key size = %d bits, want 1024", got)
	}

	// The enum has no 4096-bit member, so it is not a valid request value.
	_, err = CreateKeyWithAlgorithm(ctx, resources, "proj", "sa@proj.iam.gserviceaccount.com", "KEY_ALG_RSA_4096")
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
		t.Fatalf("4096 algorithm = %v, want 400 InvalidArgument", err)
	}

	// Absent algorithm keeps the documented RSA-2048 default.
	k, err = CreateKeyWithAlgorithm(ctx, resources, "proj", "sa2@proj.iam.gserviceaccount.com", "")
	if err != nil {
		t.Fatalf("default algorithm: %v", err)
	}
	if k.Algorithm != "KEY_ALG_RSA_2048" {
		t.Errorf("default algorithm = %q, want KEY_ALG_RSA_2048", k.Algorithm)
	}
}
