package kms

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	kmsstore "jaiscloud/internal/gcp/store/kms"
)

// TestCoreCryptoKeyRotationValidation covers the rotation-schedule invariants
// the core enforces on create: only ENCRYPT_DECRYPT keys rotate, rotation_period
// is bounded [24h, 876000h] and requires next_rotation_time, and
// next_rotation_time must be in the future.
func TestCoreCryptoKeyRotationValidation(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	future := t0.Add(24 * time.Hour)
	past := t0.Add(-time.Hour)

	negatives := []struct {
		name string
		id   string
		in   CryptoKeyInput
	}{
		{"periodWithoutNext", "p1", CryptoKeyInput{RotationPeriod: 24 * time.Hour}},
		{"periodBelowMin", "p2", CryptoKeyInput{RotationPeriod: 23 * time.Hour, NextRotationTime: &future}},
		{"periodAboveMax", "p3", CryptoKeyInput{RotationPeriod: 876001 * time.Hour, NextRotationTime: &future}},
		{"nextInPast", "p4", CryptoKeyInput{NextRotationTime: &past}},
		{"asymmetricRotation", "p5", CryptoKeyInput{Purpose: "ASYMMETRIC_SIGN", RotationPeriod: 24 * time.Hour, NextRotationTime: &future}},
		{"asymmetricNextOnly", "p6", CryptoKeyInput{Purpose: "ASYMMETRIC_SIGN", NextRotationTime: &future}},
	}
	for _, tc := range negatives {
		if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", tc.id, tc.in); err == nil {
			t.Fatalf("%s: CreateCryptoKey err = nil, want INVALID_ARGUMENT", tc.name)
		}
	}

	// A valid schedule round-trips; the core stores the caller's next rotation
	// time rather than deriving one.
	ck, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyInput{RotationPeriod: 24 * time.Hour, NextRotationTime: &future})
	if err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	if ck.RotationPeriod != 24*time.Hour || !ck.NextRotationTime.Equal(future) {
		t.Fatalf("created schedule = %v/%v, want 24h/%v", ck.RotationPeriod, ck.NextRotationTime, future)
	}

	// next_rotation_time alone (no period) is allowed.
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "nrt", CryptoKeyInput{NextRotationTime: &future}); err != nil {
		t.Fatalf("CreateCryptoKey next-only: %v", err)
	}
}

// TestCoreUpdateCryptoKeyMasks covers the mask contract on the shared core: the
// mask is required (an explicitly empty one is a no-op), only writable paths are
// accepted, only masked fields are validated/merged, and the rotation invariants
// hold on the merged result.
func TestCoreUpdateCryptoKeyMasks(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyInput{Labels: map[string]string{"env": "dev"}}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	key := func() kmsstore.CryptoKey {
		k, err := core.GetCryptoKey(ctx, "proj", "global", "kr", "sym")
		if err != nil {
			t.Fatalf("GetCryptoKey: %v", err)
		}
		return k
	}

	// A nil mask (field omitted) is required; an explicitly empty mask is a no-op.
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{Labels: map[string]string{"a": "b"}}); err == nil {
		t.Fatal("nil mask: err = nil, want INVALID_ARGUMENT")
	}
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{}, Labels: map[string]string{"a": "b"}}); err != nil {
		t.Fatalf("empty mask: %v", err)
	}
	if got := key().Labels["a"]; got != "" {
		t.Fatalf("empty mask applied labels: %v", key().Labels)
	}

	// A path outside the writable set is rejected, in either spelling.
	for _, path := range []string{"purpose", "rotationPeriodx"} {
		if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{path}}); err == nil {
			t.Fatalf("path %q: err = nil, want INVALID_ARGUMENT", path)
		}
	}

	// A labels mask replaces labels wholesale.
	updated, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{"labels"}, Labels: map[string]string{"env": "prod"}})
	if err != nil {
		t.Fatalf("labels mask: %v", err)
	}
	if updated.Labels["env"] != "prod" {
		t.Fatalf("labels = %v, want env=prod", updated.Labels)
	}

	// Setting rotation_period on a key with no next rotation time is rejected.
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{"rotationPeriod"}, RotationPeriod: durPtr(24 * time.Hour)}); err == nil {
		t.Fatal("period without next rotation time: err = nil, want INVALID_ARGUMENT")
	}

	// A camelCase period + next-rotation-time pair applies.
	later := t0.Add(24 * time.Hour)
	updated, err = core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{
		UpdateMask:       []string{"rotationPeriod", "nextRotationTime"},
		RotationPeriod:   durPtr(24 * time.Hour),
		NextRotationTime: &later,
	})
	if err != nil {
		t.Fatalf("rotation schedule mask: %v", err)
	}
	if updated.RotationPeriod != 24*time.Hour || !updated.NextRotationTime.Equal(later) {
		t.Fatalf("schedule = %v/%v, want 24h/%v", updated.RotationPeriod, updated.NextRotationTime, later)
	}

	// An out-of-bounds period is rejected on update too.
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{"rotation_period"}, RotationPeriod: durPtr(time.Hour)}); err == nil {
		t.Fatal("sub-24h update period: err = nil, want INVALID_ARGUMENT")
	}

	// Clearing next_rotation_time while a period is set is rejected; clearing the
	// period keeps next_rotation_time (the fields are independent).
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{"next_rotation_time"}}); err == nil {
		t.Fatal("clear next rotation time with a period: err = nil, want INVALID_ARGUMENT")
	}
	cleared, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{"rotation_period"}})
	if err != nil {
		t.Fatalf("clear period: %v", err)
	}
	if cleared.RotationPeriod != 0 || !cleared.NextRotationTime.Equal(later) {
		t.Fatalf("cleared = %v/%v, want no period and next %v", cleared.RotationPeriod, cleared.NextRotationTime, later)
	}

	// Unmasked body fields are ignored, not validated (AIP-134).
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{
		UpdateMask:       []string{"labels"},
		Labels:           map[string]string{"unmasked": "true"},
		RotationPeriod:   durPtr(time.Second),
		NextRotationTime: &t0,
	}); err != nil {
		t.Fatalf("unmasked invalid rotation fields: %v", err)
	}
}

func durPtr(d time.Duration) *time.Duration { return &d }

// TestCoreUpdateCryptoKeyVersionTemplateAlgorithm covers CryptoKey's
// version_template.algorithm update (TGM6): the provider sends the REST
// camelCase path when `key_algorithm` changes, the proto snake_case path must
// work too, the template's algorithm changes while existing versions keep
// theirs, a version created afterwards uses the new algorithm, the algorithm is
// validated against the key's immutable purpose, and an absent/empty algorithm
// under the mask resets the template to the purpose's default.
func TestCoreUpdateCryptoKeyVersionTemplateAlgorithm(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	mac, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "mac", CryptoKeyInput{Purpose: "MAC"})
	if err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	if mac.Algorithm != "HMAC_SHA256" {
		t.Fatalf("created algorithm = %q, want the MAC default HMAC_SHA256", mac.Algorithm)
	}

	// The provider's REST spelling: updateMask=versionTemplate.algorithm.
	hmac512 := "HMAC_SHA512"
	updated, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "mac", CryptoKeyUpdate{
		UpdateMask:               []string{"versionTemplate.algorithm"},
		VersionTemplateAlgorithm: &hmac512,
	})
	if err != nil {
		t.Fatalf("update versionTemplate.algorithm: %v", err)
	}
	if updated.Algorithm != "HMAC_SHA512" {
		t.Fatalf("template algorithm = %q, want HMAC_SHA512", updated.Algorithm)
	}
	// protection_level is ForceNew: the update must not touch it.
	if updated.ProtectionLevel != mac.ProtectionLevel {
		t.Fatalf("protection level = %q, want unchanged %q", updated.ProtectionLevel, mac.ProtectionLevel)
	}

	// Existing versions keep the algorithm they were created with ...
	v1, err := core.GetVersion(ctx, "proj", "global", "kr", "mac", "1")
	if err != nil {
		t.Fatalf("GetVersion(1): %v", err)
	}
	if v1.Algorithm != "HMAC_SHA256" {
		t.Fatalf("existing version algorithm = %q, want untouched HMAC_SHA256", v1.Algorithm)
	}
	// ... while a version created afterwards uses the new template.
	v2, err := core.CreateVersion(ctx, "proj", "global", "kr", "mac", CreateVersionInput{})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	if v2.Algorithm != "HMAC_SHA512" {
		t.Fatalf("new version algorithm = %q, want HMAC_SHA512", v2.Algorithm)
	}

	// The proto snake_case spelling is accepted too.
	hmac384 := "HMAC_SHA384"
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "mac", CryptoKeyUpdate{
		UpdateMask:               []string{"version_template.algorithm"},
		VersionTemplateAlgorithm: &hmac384,
	}); err != nil {
		t.Fatalf("update version_template.algorithm: %v", err)
	}

	// An absent algorithm under the mask resets the template to the purpose
	// default instead of leaving the stale HMAC_SHA384 behind.
	reset, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "mac", CryptoKeyUpdate{
		UpdateMask: []string{"version_template.algorithm"},
	})
	if err != nil {
		t.Fatalf("reset version template: %v", err)
	}
	if reset.Algorithm != "HMAC_SHA256" {
		t.Fatalf("reset algorithm = %q, want the MAC default HMAC_SHA256", reset.Algorithm)
	}
	// An explicitly empty algorithm is the same reset.
	empty := ""
	reset, err = core.UpdateCryptoKey(ctx, "proj", "global", "kr", "mac", CryptoKeyUpdate{
		UpdateMask:               []string{"versionTemplate.algorithm"},
		VersionTemplateAlgorithm: &empty,
	})
	if err != nil {
		t.Fatalf("empty algorithm reset: %v", err)
	}
	if reset.Algorithm != "HMAC_SHA256" {
		t.Fatalf("empty reset algorithm = %q, want HMAC_SHA256", reset.Algorithm)
	}

	// An algorithm outside the key's purpose is 400, and the stored template is
	// left untouched.
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyInput{}); err != nil {
		t.Fatalf("CreateCryptoKey(sym): %v", err)
	}
	_, err = core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{
		UpdateMask:               []string{"versionTemplate.algorithm"},
		VersionTemplateAlgorithm: &hmac512,
	})
	if err == nil {
		t.Fatal("HMAC_SHA512 on ENCRYPT_DECRYPT: err = nil, want INVALID_ARGUMENT")
	}
	if code, status := providerErr(t, err); status != 400 || code != "InvalidArgument" {
		t.Fatalf("HMAC_SHA512 on ENCRYPT_DECRYPT = %s/%d, want InvalidArgument/400", code, status)
	}
	sym, err := core.GetCryptoKey(ctx, "proj", "global", "kr", "sym")
	if err != nil {
		t.Fatalf("GetCryptoKey(sym): %v", err)
	}
	if sym.Algorithm != "GOOGLE_SYMMETRIC_ENCRYPTION" {
		t.Fatalf("rejected update changed the template: %q", sym.Algorithm)
	}

	// The other update paths still work in the same request.
	later := clock.Now().Add(24 * time.Hour)
	sym, err = core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{
		UpdateMask:       []string{"versionTemplate.algorithm", "labels", "rotationPeriod", "nextRotationTime"},
		Labels:           map[string]string{"team": "platform"},
		RotationPeriod:   durPtr(24 * time.Hour),
		NextRotationTime: &later,
	})
	if err != nil {
		t.Fatalf("combined mask: %v", err)
	}
	if sym.Labels["team"] != "platform" || sym.RotationPeriod != 24*time.Hour || !sym.NextRotationTime.Equal(later) {
		t.Fatalf("combined update dropped a masked field: %+v", sym)
	}

	// A path outside the writable set is still rejected.
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "mac", CryptoKeyUpdate{
		UpdateMask: []string{"versionTemplate.protectionLevel"},
	}); err == nil {
		t.Fatal("versionTemplate.protectionLevel: err = nil, want INVALID_ARGUMENT")
	}
}

// TestCoreCreateCryptoKeyAlgorithmPurposeValidation covers the create half of
// validateAlgorithmForPurpose: an algorithm that the immutable purpose cannot
// execute is 400, while both HMAC variants a MAC key legitimately uses (and the
// per-purpose defaults) create cleanly.
func TestCoreCreateCryptoKeyAlgorithmPurposeValidation(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}

	negatives := []struct {
		name string
		id   string
		in   CryptoKeyInput
	}{
		{"hmac on encrypt-decrypt", "bad-hmac-sym", CryptoKeyInput{Purpose: "ENCRYPT_DECRYPT", Algorithm: "HMAC_SHA512"}},
		{"aes-gcm on mac", "bad-gcm-mac", CryptoKeyInput{Purpose: "MAC", Algorithm: "AES_256_GCM"}},
		{"aes-gcm on aes-wrapping", "bad-gcm-wrap", CryptoKeyInput{Purpose: "AES_WRAPPING", Algorithm: "AES_256_GCM"}},
		{"unknown algorithm", "bad-unknown", CryptoKeyInput{Purpose: "MAC", Algorithm: "HMAC_MD5"}},
	}
	for _, tc := range negatives {
		_, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", tc.id, tc.in)
		if err == nil {
			t.Fatalf("%s: CreateCryptoKey err = nil, want INVALID_ARGUMENT", tc.name)
		}
		if code, status := providerErr(t, err); status != 400 || code != "InvalidArgument" {
			t.Fatalf("%s = %s/%d, want InvalidArgument/400", tc.name, code, status)
		}
	}

	positives := []struct {
		name string
		id   string
		in   CryptoKeyInput
		want string
	}{
		{"hmac sha256", "ok-hmac256", CryptoKeyInput{Purpose: "MAC", Algorithm: "HMAC_SHA256"}, "HMAC_SHA256"},
		{"hmac sha512", "ok-hmac512", CryptoKeyInput{Purpose: "MAC", Algorithm: "HMAC_SHA512"}, "HMAC_SHA512"},
		{"raw default", "ok-raw", CryptoKeyInput{Purpose: "RAW_ENCRYPT_DECRYPT"}, "AES_256_GCM"},
		{"wrapping default", "ok-wrap", CryptoKeyInput{Purpose: "AES_WRAPPING"}, "AES_256_KWP"},
	}
	for _, tc := range positives {
		ck, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", tc.id, tc.in)
		if err != nil {
			t.Fatalf("%s: CreateCryptoKey: %v", tc.name, err)
		}
		if ck.Algorithm != tc.want {
			t.Fatalf("%s algorithm = %q, want %q", tc.name, ck.Algorithm, tc.want)
		}
	}
}
