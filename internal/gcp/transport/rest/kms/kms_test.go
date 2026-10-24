package kms

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/resource"
	kmscore "jaiscloud/internal/gcp/service/kms"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

// newTestProvider returns a KMS provider backed by fresh in-memory stores.
func newTestProvider() *Provider {
	return New(kmscore.NewService(kmsstore.NewMemoryStore(), store.NewMemoryResourceStore(), "test"))
}

func TestKMSEncryptDecryptRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Create keyring.
	nr := newNR(map[string]any{"location": "global", "keyRingId": "my-kr"})
	if _, err := p.KeyRingCreate(ctx, nr); err != nil {
		t.Fatalf("keyring create: %v", err)
	}

	// Create crypto key.
	nr = newNR(map[string]any{"name": "locations/global/keyRings/my-kr", "cryptoKeyId": "my-key", "body": map[string]any{"purpose": "ENCRYPT_DECRYPT"}})
	if _, err := p.CryptoKeyCreate(ctx, nr); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}

	// Encrypt.
	nr = newNR(map[string]any{"name": "locations/global/keyRings/my-kr/cryptoKeys/my-key", "body": map[string]any{"plaintext": "aGVsbG8="}})
	resp, err := p.CryptoKeyEncrypt(ctx, nr)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	ciphertext, _ := resp.Data["ciphertext"].(string)
	if ciphertext == "" || ciphertext == "aGVsbG8=" {
		t.Fatalf("expected distinct ciphertext, got %q", ciphertext)
	}
	if name, _ := resp.Data["name"].(string); name != "projects/proj/locations/global/keyRings/my-kr/cryptoKeys/my-key/cryptoKeyVersions/1" {
		t.Errorf("encrypt name = %q, want full cryptoKeyVersion resource name", name)
	}

	// Decrypt.
	nr = newNR(map[string]any{"name": "locations/global/keyRings/my-kr/cryptoKeys/my-key", "body": map[string]any{"ciphertext": ciphertext}})
	resp, err = p.CryptoKeyDecrypt(ctx, nr)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if plaintext, _ := resp.Data["plaintext"].(string); plaintext != "aGVsbG8=" {
		t.Errorf("expected plaintext aGVsbG8=, got %q", plaintext)
	}
}

// TestCryptoKeyList verifies listing a key ring's crypto keys. The collection
// request names the key-ring parent (.../keyRings/{kr}/cryptoKeys), which is
// three path segments, so the handler must parse it as a key ring rather than
// with the five-segment crypto-key parser (which returned empty loc/ring and
// silently listed nothing).
func TestCryptoKeyList(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring create: %v", err)
	}
	for _, key := range []string{"key-a", "key-b"} {
		if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
			"name":        "locations/global/keyRings/kr",
			"cryptoKeyId": key,
			"body":        map[string]any{"purpose": "ENCRYPT_DECRYPT"},
		})); err != nil {
			t.Fatalf("cryptokey create %s: %v", key, err)
		}
	}

	resp, err := p.CryptoKeyList(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys"}))
	if err != nil {
		t.Fatalf("cryptokey list: %v", err)
	}
	keys, _ := resp.Data["cryptoKeys"].([]any)
	if len(keys) != 2 {
		t.Fatalf("expected 2 crypto keys, got %d (%v)", len(keys), resp.Data["cryptoKeys"])
	}
	if resp.Data["totalSize"] != 2 {
		t.Errorf("totalSize = %v, want 2", resp.Data["totalSize"])
	}
	names := map[string]bool{}
	for _, k := range keys {
		km, _ := k.(map[string]any)
		if n, _ := km["name"].(string); n != "" {
			names[n] = true
		}
	}
	for _, want := range []string{
		"projects/proj/locations/global/keyRings/kr/cryptoKeys/key-a",
		"projects/proj/locations/global/keyRings/kr/cryptoKeys/key-b",
	} {
		if !names[want] {
			t.Errorf("listed key names = %v, missing %q", names, want)
		}
	}
}

// TestCryptoKeyVersionListNumericOrder verifies crypto key versions are ordered
// and paginated numerically (1,2,...,12), not lexicographically
// (1,10,11,12,2,...). With pageSize=5 the pre-fix key skipped versions across
// page boundaries because the decimal-string cursor compared lexicographically.
func TestCryptoKeyVersionListNumericOrder(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring create: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "k",
		"body":        map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	})); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}
	// CryptoKeyCreate creates version 1; add 11 more for 12 total.
	for i := 0; i < 11; i++ {
		if _, err := p.CryptoKeyVersionCreate(ctx, newNR(map[string]any{
			"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions",
		})); err != nil {
			t.Fatalf("version create %d: %v", i, err)
		}
	}

	var got []int
	token := ""
	for page := 0; ; page++ {
		if page > 10 {
			t.Fatal("pagination did not terminate")
		}
		params := map[string]any{
			"name":     "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions",
			"pageSize": "5",
		}
		if token != "" {
			params["pageToken"] = token
		}
		resp, err := p.CryptoKeyVersionList(ctx, newNR(params))
		if err != nil {
			t.Fatalf("list page %d: %v", page, err)
		}
		items, _ := resp.Data["cryptoKeyVersions"].([]any)
		for _, it := range items {
			m := it.(map[string]any)
			name, _ := m["name"].(string)
			n, err := strconv.Atoi(name[strings.LastIndex(name, "/")+1:])
			if err != nil {
				t.Fatalf("bad version name %q: %v", name, err)
			}
			got = append(got, n)
		}
		next, _ := resp.Data["nextPageToken"].(string)
		if next == "" {
			break
		}
		token = next
	}

	if len(got) != 12 {
		t.Fatalf("expected 12 versions across pages, got %d (%v)", len(got), got)
	}
	for i, n := range got {
		if n != i+1 {
			t.Fatalf("version order = %v, want 1..12", got)
		}
	}
}

// TestCryptoKeyPrimaryAlgorithm verifies the version-template algorithm is
// surfaced and that Discovery's primary rule holds: only ENCRYPT_DECRYPT keys
// carry a primary version, every other purpose omits it.
func TestCryptoKeyPrimaryAlgorithm(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring create: %v", err)
	}

	// MAC defaults to HMAC_SHA256 and must not expose a primary version.
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "mac-key",
		"body":        map[string]any{"purpose": "MAC"},
	})); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}
	resp, err := p.CryptoKeyGet(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/mac-key"}))
	if err != nil {
		t.Fatalf("cryptokey get: %v", err)
	}
	if _, ok := resp.Data["primary"]; ok {
		t.Errorf("MAC key must omit primary, got %v", resp.Data["primary"])
	}
	vt, _ := resp.Data["versionTemplate"].(map[string]any)
	if alg, _ := vt["algorithm"].(string); alg != "HMAC_SHA256" {
		t.Errorf("versionTemplate.algorithm = %q, want HMAC_SHA256", alg)
	}
	if pl, _ := vt["protectionLevel"].(string); pl != "SOFTWARE" {
		t.Errorf("versionTemplate.protectionLevel = %q, want SOFTWARE", pl)
	}

	// An ENCRYPT_DECRYPT key carries its primary version.
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "sym-key",
		"body":        map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	})); err != nil {
		t.Fatalf("cryptokey create(sym): %v", err)
	}
	symResp, err := p.CryptoKeyGet(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/sym-key"}))
	if err != nil {
		t.Fatalf("cryptokey get(sym): %v", err)
	}
	primary, _ := symResp.Data["primary"].(map[string]any)
	if alg, _ := primary["algorithm"].(string); alg != "GOOGLE_SYMMETRIC_ENCRYPTION" {
		t.Errorf("primary.algorithm = %q, want GOOGLE_SYMMETRIC_ENCRYPTION", alg)
	}
	if state, _ := primary["state"].(string); state != "ENABLED" {
		t.Errorf("primary.state = %q, want ENABLED", state)
	}
}

// TestCryptoKeyLabelsRotationREST verifies the REST surface persists labels, a
// rotationPeriod and its required nextRotationTime on create and surfaces them
// on get.
func TestCryptoKeyLabelsRotationREST(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring create: %v", err)
	}
	nrt := t0.Add(24 * time.Hour)
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "rot",
		"body": map[string]any{
			"labels":           map[string]any{"env": "test"},
			"rotationPeriod":   "86400s",
			"nextRotationTime": nrt.Format(time.RFC3339Nano),
		},
	})); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}

	resp, err := p.CryptoKeyGet(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/rot"}))
	if err != nil {
		t.Fatalf("cryptokey get: %v", err)
	}
	labels, _ := resp.Data["labels"].(map[string]string)
	if labels["env"] != "test" {
		t.Errorf("labels = %v, want env=test", resp.Data["labels"])
	}
	if period, _ := resp.Data["rotationPeriod"].(string); period != "86400s" {
		t.Errorf("rotationPeriod = %q, want 86400s", period)
	}
	if next, _ := resp.Data["nextRotationTime"].(string); next != nrt.Format(time.RFC3339Nano) {
		t.Errorf("nextRotationTime = %q, want %q", next, nrt.Format(time.RFC3339Nano))
	}

	// The Discovery requires next_rotation_time whenever rotation_period is set.
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "orphan",
		"body":        map[string]any{"rotationPeriod": "86400s"},
	})); err == nil || errStatus(err) != 400 {
		t.Fatalf("rotationPeriod without nextRotationTime err = %v, want 400", err)
	}

	// rotation_period must be within [24h, 876000h].
	for _, bad := range []string{"-1h", "82800s", "3153600001s"} {
		if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
			"name":        "locations/global/keyRings/kr",
			"cryptoKeyId": "bad",
			"body":        map[string]any{"rotationPeriod": bad, "nextRotationTime": nrt.Format(time.RFC3339Nano)},
		})); err == nil || errStatus(err) != 400 {
			t.Fatalf("rotationPeriod %q err = %v, want 400", bad, err)
		}
	}

	// next_rotation_time must be in the future.
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "past",
		"body":        map[string]any{"rotationPeriod": "86400s", "nextRotationTime": t0.Add(-time.Hour).Format(time.RFC3339Nano)},
	})); err == nil || errStatus(err) != 400 {
		t.Fatalf("past nextRotationTime err = %v, want 400", err)
	}
}

// TestCryptoKeyRotationExecutesOnReadREST verifies a due rotation schedule is
// executed lazily: after the fixed clock advances past nextRotationTime, a
// CryptoKeyGet creates version 2, makes it primary, and advances the schedule
// by the period.
func TestCryptoKeyRotationExecutesOnReadREST(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring create: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "rot",
		"body":        map[string]any{"rotationPeriod": "86400s", "nextRotationTime": t0.Add(24 * time.Hour).Format(time.RFC3339Nano)},
	})); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}

	getName := "locations/global/keyRings/kr/cryptoKeys/rot"

	// Before the due time (nextRotationTime = t0+24h) nothing rotates.
	resp, err := p.CryptoKeyGet(ctx, newNR(map[string]any{"name": getName}))
	if err != nil {
		t.Fatalf("cryptokey get (not due): %v", err)
	}
	if n := primaryName(t, resp); n != "projects/proj/locations/global/keyRings/kr/cryptoKeys/rot/cryptoKeyVersions/1" {
		t.Fatalf("primary before rotation = %q, want version 1", n)
	}

	// Advance the clock past nextRotationTime and read again: rotation runs.
	due := t0.Add(25 * time.Hour)
	clock.SetGlobalClock(clock.FixedClock{T: due})
	resp, err = p.CryptoKeyGet(ctx, newNR(map[string]any{"name": getName}))
	if err != nil {
		t.Fatalf("cryptokey get (due): %v", err)
	}
	if n := primaryName(t, resp); n != "projects/proj/locations/global/keyRings/kr/cryptoKeys/rot/cryptoKeyVersions/2" {
		t.Fatalf("primary after rotation = %q, want version 2", n)
	}
	if next, _ := resp.Data["nextRotationTime"].(string); next != due.Add(24*time.Hour).UTC().Format(time.RFC3339Nano) {
		t.Fatalf("nextRotationTime = %q, want %q", next, due.Add(24*time.Hour).UTC().Format(time.RFC3339Nano))
	}

	// The rotation produced a second, ENABLED version.
	resp, err = p.CryptoKeyVersionList(ctx, newNR(map[string]any{"name": getName + "/cryptoKeyVersions"}))
	if err != nil {
		t.Fatalf("version list: %v", err)
	}
	versions, _ := resp.Data["cryptoKeyVersions"].([]any)
	if len(versions) != 2 {
		t.Fatalf("versions after rotation = %d, want 2", len(versions))
	}
}

// primaryName extracts the crypto key response's primary version name.
func primaryName(t *testing.T, resp *model.ProviderResponse) string {
	t.Helper()
	primary, _ := resp.Data["primary"].(map[string]any)
	n, _ := primary["name"].(string)
	return n
}

// TestCryptoKeyRotationOnEncrypt verifies Encrypt triggers a due rotation and
// that a ciphertext produced before rotation still decrypts afterwards: the
// versioned blob resolves its own version, independent of the primary.
func TestCryptoKeyRotationOnEncrypt(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring create: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "rot",
		"body":        map[string]any{"rotationPeriod": "86400s", "nextRotationTime": t0.Add(24 * time.Hour).Format(time.RFC3339Nano)},
	})); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}
	keyPath := "locations/global/keyRings/kr/cryptoKeys/rot"

	encrypt := func() *model.ProviderResponse {
		resp, err := p.CryptoKeyEncrypt(ctx, newNR(map[string]any{
			"name": keyPath,
			"body": map[string]any{"plaintext": "aGVsbG8="},
		}))
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		return resp
	}

	// Before the due time the primary is version 1.
	first := encrypt()
	if name, _ := first.Data["name"].(string); !strings.HasSuffix(name, "/cryptoKeyVersions/1") {
		t.Fatalf("first encrypt name = %q, want version 1", name)
	}
	firstCT, _ := first.Data["ciphertext"].(string)

	// Advance past nextRotationTime: Encrypt rotates and uses version 2.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(25 * time.Hour)})
	second := encrypt()
	if name, _ := second.Data["name"].(string); !strings.HasSuffix(name, "/cryptoKeyVersions/2") {
		t.Fatalf("second encrypt name = %q, want version 2", name)
	}

	// The pre-rotation ciphertext still decrypts (it names version 1).
	resp, err := p.CryptoKeyDecrypt(ctx, newNR(map[string]any{
		"name": keyPath,
		"body": map[string]any{"ciphertext": firstCT},
	}))
	if err != nil {
		t.Fatalf("decrypt old ciphertext: %v", err)
	}
	if pt, _ := resp.Data["plaintext"].(string); pt != "aGVsbG8=" {
		t.Fatalf("decrypted plaintext = %q, want aGVsbG8=", pt)
	}
}
