package kms

import (
	"context"
	"testing"

	iampb "cloud.google.com/go/iam/apiv1/iampb"

	kmscore "jaiscloud/internal/gcp/service/kms"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"
)

// TestImportJobIAMGRPC pins the gRPC side of the import-job IAM surface: the
// IAM router's Owns must claim an import-job name (it rejected one before the
// core learned the resource), and the policy round-trips.
func TestImportJobIAMGRPC(t *testing.T) {
	ctx := context.Background()
	core := kmscore.NewService(kmsstore.NewMemoryStore(), store.NewMemoryResourceStore(), "proj")
	svc := NewService(core, "proj")

	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if _, err := core.CreateImportJob(ctx, "proj", "global", "kr", "job", kmscore.ImportJobInput{
		ImportMethod: "RSA_OAEP_3072_SHA256_AES_256",
	}); err != nil {
		t.Fatalf("CreateImportJob: %v", err)
	}

	job := "projects/proj/locations/global/keyRings/kr/importJobs/job"
	if !svc.Owns(job) {
		t.Fatalf("Owns(%q) = false, want true", job)
	}
	if _, err := svc.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: job}); err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	// A version-level name still has no IAM.
	if svc.Owns(job + "/cryptoKeyVersions/1") {
		t.Fatal("Owns(version) = true, want false")
	}
}
