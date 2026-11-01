package iam

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/gcp/serviceaccount"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// uploadTestPublicKeyData wraps pub in a self-signed X.509 v3 certificate and
// returns it base64-encoded, the shape projects.serviceAccounts.keys.upload
// documents for publicKeyData and the REST wire carries it as (format: byte).
func uploadTestPublicKeyData(t *testing.T, pub *rsa.PublicKey) string {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate certificate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "iam-key-upload-test"},
		NotBefore:             time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:              time.Date(2120, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// uploadRequest builds the REST shape the codec produces for
// POST …/serviceAccounts/{email}/keys:upload: the collection name plus a body.
func uploadRequest(email, publicKeyData string) *model.NormalizedRequest {
	return newNR(map[string]any{
		"name": "serviceAccounts/" + email + "/keys",
		"body": map[string]any{"publicKeyData": publicKeyData},
	})
}

// TestServiceAccountKeyUploadRoundTrip covers DC27 over the REST provider: the
// uploaded key stores public material only, renders as a USER_MANAGED /
// USER_PROVIDED key, is readable through get/list, and supports the same
// disable/enable/delete lifecycle as a generated key.
func TestServiceAccountKeyUploadRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa"}})); err != nil {
		t.Fatalf("create SA: %v", err)
	}
	email := "sa@proj.iam.gserviceaccount.com"

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	publicKeyData := uploadTestPublicKeyData(t, &key.PublicKey)

	resp, err := p.ServiceAccountKeyUpload(ctx, uploadRequest(email, publicKeyData))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	name, _ := resp.Data["name"].(string)
	if !strings.HasPrefix(name, "projects/proj/serviceAccounts/"+email+"/keys/") {
		t.Fatalf("upload name = %q, want a key under %s", name, email)
	}
	keyID := name[strings.LastIndex(name, "/")+1:]
	if resp.Data["keyType"] != "USER_MANAGED" {
		t.Errorf("keyType = %v, want USER_MANAGED", resp.Data["keyType"])
	}
	if resp.Data["keyOrigin"] != "USER_PROVIDED" {
		t.Errorf("keyOrigin = %v, want USER_PROVIDED", resp.Data["keyOrigin"])
	}
	if resp.Data["keyAlgorithm"] != "KEY_ALG_RSA_2048" {
		t.Errorf("keyAlgorithm = %v, want KEY_ALG_RSA_2048", resp.Data["keyAlgorithm"])
	}
	// Discovery scopes publicKeyData to keys.get and the private fields to
	// keys.create, so the upload response carries neither.
	if _, present := resp.Data["publicKeyData"]; present {
		t.Error("keys.upload must not emit publicKeyData (get-only per Discovery)")
	}
	if _, present := resp.Data["privateKeyData"]; present {
		t.Error("keys.upload must not emit privateKeyData (create-only per Discovery)")
	}
	if _, present := resp.Data["privateKeyType"]; present {
		t.Error("keys.upload must not emit privateKeyType (create-only per Discovery)")
	}

	// The stored record holds no private half: signing with it fails loudly
	// rather than reaching the signer with empty bytes.
	e, err := p.resources.Get(ctx, "proj", store.GlobalRegion, rtServiceAccountKey, keyID)
	if err != nil {
		t.Fatalf("load stored key: %v", err)
	}
	var stored serviceAccountKeyMeta
	if err := json.Unmarshal(e.Data, &stored); err != nil {
		t.Fatalf("unmarshal stored key: %v", err)
	}
	if stored.PrivateDER != "" {
		t.Errorf("uploaded key stored a private DER: %q", stored.PrivateDER)
	}
	if _, err := stored.PrivDER(); !errors.Is(err, serviceaccount.ErrNoPrivateKey) {
		t.Fatalf("PrivDER on uploaded key = %v, want ErrNoPrivateKey", err)
	}

	// keys.get renders the caller's public key.
	gr, err := p.ServiceAccountKeyGet(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	pubData, _ := gr.Data["publicKeyData"].(string)
	if pubData == "" {
		t.Fatal("keys.get dropped publicKeyData for an uploaded key")
	}
	pubPEM, err := base64.StdEncoding.DecodeString(pubData)
	if err != nil {
		t.Fatalf("publicKeyData is not base64: %v", err)
	}
	block, _ := pem.Decode(pubPEM)
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

	// list carries the key with the right classification (and no key material).
	lr, err := p.ServiceAccountKeyList(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys"}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	keys, _ := lr.Data["keys"].([]any)
	listed, ok := findKey(keys, name)
	if !ok {
		t.Fatalf("keys.list dropped the uploaded key %q: %v", name, keys)
	}
	if listed["keyType"] != "USER_MANAGED" || listed["keyOrigin"] != "USER_PROVIDED" {
		t.Errorf("listed as keyType=%v keyOrigin=%v, want USER_MANAGED/USER_PROVIDED",
			listed["keyType"], listed["keyOrigin"])
	}
	if _, present := listed["publicKeyData"]; present {
		t.Error("keys.list must not emit publicKeyData (get-only per Discovery)")
	}

	// disable/enable work on an uploaded key exactly as on a generated one.
	if _, err := p.ServiceAccountKeyDisable(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID})); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if gr, err = p.ServiceAccountKeyGet(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID})); err != nil {
		t.Fatalf("get after disable: %v", err)
	}
	if gr.Data["disabled"] != true {
		t.Errorf("disabled = %v after disable, want true", gr.Data["disabled"])
	}
	if _, err := p.ServiceAccountKeyEnable(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID})); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if gr, err = p.ServiceAccountKeyGet(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID})); err != nil {
		t.Fatalf("get after enable: %v", err)
	}
	if gr.Data["disabled"] != false {
		t.Errorf("disabled = %v after enable, want false", gr.Data["disabled"])
	}

	// A duplicate upload of the same public key is a conflict, not a second key.
	_, err = p.ServiceAccountKeyUpload(ctx, uploadRequest(email, publicKeyData))
	requireStatus(t, "duplicate upload", err, 409)

	// Delete removes it.
	if _, err := p.ServiceAccountKeyDelete(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID})); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := p.ServiceAccountKeyGet(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID})); err == nil {
		t.Fatal("expected 404 on deleted uploaded key")
	}
}

// TestServiceAccountKeyUploadValidation covers the documented failure modes:
// malformed or unsupported keys are INVALID_ARGUMENT, a missing account keeps
// the wildcard 403 / concrete 404 distinction.
func TestServiceAccountKeyUploadValidation(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa"}})); err != nil {
		t.Fatalf("create SA: %v", err)
	}
	email := "sa@proj.iam.gserviceaccount.com"
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	valid := uploadTestPublicKeyData(t, &key.PublicKey)

	_, err = p.ServiceAccountKeyUpload(ctx, uploadRequest(email, ""))
	requireStatus(t, "missing publicKeyData", err, 400)

	_, err = p.ServiceAccountKeyUpload(ctx, uploadRequest(email, "not base64!"))
	requireStatus(t, "non-base64 publicKeyData", err, 400)

	_, err = p.ServiceAccountKeyUpload(ctx, uploadRequest(email, base64.StdEncoding.EncodeToString([]byte("not a key"))))
	requireStatus(t, "malformed key", err, 400)

	// 4096-bit RSA has no ServiceAccountKey.keyAlgorithm member, so it is not
	// representable and must fail rather than be labelled RSA-2048.
	big, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		t.Fatalf("generate 4096: %v", err)
	}
	_, err = p.ServiceAccountKeyUpload(ctx, uploadRequest(email, uploadTestPublicKeyData(t, &big.PublicKey)))
	requireStatus(t, "4096-bit RSA", err, 400)

	// A concrete project addressing a missing account is 404 …
	_, err = p.ServiceAccountKeyUpload(ctx, uploadRequest("missing@proj.iam.gserviceaccount.com", valid))
	requireStatus(t, "missing SA", err, 404)

	// … while the documented "projects/-" wildcard answers 403.
	wild := &model.NormalizedRequest{
		AccountID:  "-",
		Params:     map[string]any{"name": "serviceAccounts/missing@proj.iam.gserviceaccount.com/keys", "body": map[string]any{"publicKeyData": valid}},
		ResourceID: resource.ResourceID("proj"),
	}
	_, err = p.ServiceAccountKeyUpload(ctx, wild)
	requireStatus(t, "missing SA via wildcard", err, 403)

	// The same wildcard resolves an existing account and renders the key under
	// the canonical project — never "projects/-".
	wild.Params["name"] = "serviceAccounts/" + email + "/keys"
	resp, err := p.ServiceAccountKeyUpload(ctx, wild)
	if err != nil {
		t.Fatalf("wildcard upload: %v", err)
	}
	name, _ := resp.Data["name"].(string)
	if !strings.HasPrefix(name, "projects/proj/serviceAccounts/"+email+"/keys/") {
		t.Fatalf("wildcard upload name = %q, want the canonical project path", name)
	}
}

// TestServiceAccountKeyCreateAlgorithm covers the documented keyAlgorithm
// field: 1024/2048 are representable, anything else is INVALID_ARGUMENT.
func TestServiceAccountKeyCreateAlgorithm(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa"}})); err != nil {
		t.Fatalf("create SA: %v", err)
	}
	email := "sa@proj.iam.gserviceaccount.com"

	resp, err := p.ServiceAccountKeyCreate(ctx, newNR(map[string]any{
		"name": "serviceAccounts/" + email + "/keys",
		"body": map[string]any{"keyAlgorithm": "KEY_ALG_RSA_1024"},
	}))
	if err != nil {
		t.Fatalf("create RSA-1024: %v", err)
	}
	if resp.Data["keyAlgorithm"] != "KEY_ALG_RSA_1024" {
		t.Errorf("keyAlgorithm = %v, want KEY_ALG_RSA_1024", resp.Data["keyAlgorithm"])
	}

	_, err = p.ServiceAccountKeyCreate(ctx, newNR(map[string]any{
		"name": "serviceAccounts/" + email + "/keys",
		"body": map[string]any{"keyAlgorithm": "KEY_ALG_RSA_4096"},
	}))
	requireStatus(t, "RSA-4096 create", err, 400)
}

// findKey returns the listed key whose name matches.
func findKey(keys []any, name string) (map[string]any, bool) {
	for _, item := range keys {
		listed, _ := item.(map[string]any)
		if listed["name"] == name {
			return listed, true
		}
	}
	return nil, false
}

// requireStatus asserts err is a ProviderError with the given HTTP status.
func requireStatus(t *testing.T, what string, err error, status int) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected an error", what)
	}
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != status {
		t.Fatalf("%s: error = %v, want %d ProviderError", what, err, status)
	}
}
