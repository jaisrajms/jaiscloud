package kms

import (
	"context"
	"strings"
	"testing"
)

// TestGenerateRandomBytesCore covers the shared core method (added so both
// transports share it) including the documented bounds and both accepted
// location-name forms.
func TestGenerateRandomBytesCore(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()

	for _, name := range []string{"locations/global", "projects/proj/locations/global"} {
		for _, n := range []int{8, 1024} {
			data, err := core.GenerateRandomBytes(ctx, name, n, "")
			if err != nil {
				t.Fatalf("GenerateRandomBytes(%q, %d): %v", name, n, err)
			}
			if len(data) != n {
				t.Fatalf("GenerateRandomBytes(%q, %d) len = %d", name, n, len(data))
			}
		}
	}
	for _, n := range []int{0, 7, 1025} {
		if _, err := core.GenerateRandomBytes(ctx, "locations/global", n, ""); err == nil {
			t.Errorf("GenerateRandomBytes(length=%d): want an error", n)
		}
	}
	for _, name := range []string{"", "global", "projects/proj/regions/us"} {
		if _, err := core.GenerateRandomBytes(ctx, name, 16, ""); err == nil {
			t.Errorf("GenerateRandomBytes(name=%q): want an error", name)
		}
	}
}

// TestImportJobIAMCore covers the import-job policy surface both transports now
// share, in the relative- and full-name forms.
func TestImportJobIAMCore(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if _, err := core.CreateImportJob(ctx, "proj", "global", "kr", "job", ImportJobInput{
		ImportMethod: "RSA_OAEP_3072_SHA256_AES_256",
	}); err != nil {
		t.Fatalf("CreateImportJob: %v", err)
	}
	rel := "locations/global/keyRings/kr/importJobs/job"
	full := "projects/proj/locations/global/keyRings/kr/importJobs/job"

	for _, name := range []string{rel, full} {
		pt, id, ok := IamPolicyInfo(name)
		if !ok || pt != rtImportJobPolicy || id != "global/kr/job" {
			t.Fatalf("IamPolicyInfo(%q) = (%q, %q, %v)", name, pt, id, ok)
		}
		if _, err := core.GetIamPolicy(ctx, "proj", name); err != nil {
			t.Fatalf("GetIamPolicy(%q): %v", name, err)
		}
		if _, err := core.SetIamPolicy(ctx, "proj", name, map[string]any{
			"bindings": []any{map[string]any{"role": "roles/viewer", "members": []any{"user:a@b.com"}}},
		}); err != nil {
			t.Fatalf("SetIamPolicy(%q): %v", name, err)
		}
		perms, err := core.TestIamPermissions(ctx, "proj", name, []string{"cloudkms.cryptoKeys.get"})
		if err != nil || len(perms) != 1 {
			t.Fatalf("TestIamPermissions(%q) = (%v, %v)", name, perms, err)
		}
	}

	// A missing import job is NotFound; a version name still has no IAM.
	if _, err := core.GetIamPolicy(ctx, "proj", "locations/global/keyRings/kr/importJobs/nope"); err == nil {
		t.Error("GetIamPolicy(missing job): want an error")
	}
	if _, _, ok := IamPolicyInfo("projects/proj/locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/1"); ok {
		t.Error("IamPolicyInfo(version) = ok, want false")
	}
	if _, err := core.GetIamPolicy(ctx, "proj", "projects/proj/locations/global/keyRings/kr/nonsense"); err == nil {
		t.Error("GetIamPolicy(nonsense): want an error")
	}
}

// TestImportJobRelParsing pins the relative-name parse directly.
func TestImportJobRelParsing(t *testing.T) {
	loc, kr, job, ok := importJobRel("locations/global/keyRings/kr/importJobs/job")
	if !ok || loc != "global" || kr != "kr" || job != "job" {
		t.Fatalf("importJobRel(relative) = (%q,%q,%q,%v)", loc, kr, job, ok)
	}
	loc, kr, job, ok = importJobRel("projects/p/locations/global/keyRings/kr/importJobs/job")
	if !ok || loc != "global" || kr != "kr" || job != "job" {
		t.Fatalf("importJobRel(full) = (%q,%q,%q,%v)", loc, kr, job, ok)
	}
	if _, _, _, ok := importJobRel("locations/global/keyRings/kr/cryptoKeys/k"); ok {
		t.Fatal("importJobRel(cryptoKey) = ok, want false")
	}
	if validLocationName(strings.Repeat("x", 3)) {
		t.Fatal("validLocationName(garbage) = true, want false")
	}
}
