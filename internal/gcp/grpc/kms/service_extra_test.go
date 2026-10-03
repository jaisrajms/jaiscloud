package kms

import (
	"context"
	"testing"
	"time"

	kmspb "cloud.google.com/go/kms/apiv1/kmspb"

	"jaiscloud/internal/clock"
	kmsstore "jaiscloud/internal/gcp/store/kms"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestKMSRawEncryptDecrypt(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	rawKey := keyRing + "/cryptoKeys/raw"
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      keyRing,
		CryptoKeyId: "raw",
		CryptoKey: &kmspb.CryptoKey{
			Purpose: kmspb.CryptoKey_RAW_ENCRYPT_DECRYPT,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
				Algorithm: kmspb.CryptoKeyVersion_AES_256_GCM,
			},
		},
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	version := rawKey + "/cryptoKeyVersions/1"

	plaintext := []byte("raw secret")
	aad := []byte("raw-aad")
	enc, err := client.RawEncrypt(ctx, &kmspb.RawEncryptRequest{
		Name:                        version,
		Plaintext:                   plaintext,
		AdditionalAuthenticatedData: aad,
	})
	if err != nil {
		t.Fatalf("RawEncrypt: %v", err)
	}
	if len(enc.GetCiphertext()) == 0 || len(enc.GetInitializationVector()) != 12 || enc.GetTagLength() != 16 {
		t.Fatalf("RawEncrypt = ct:%d iv:%d tag:%d, want non-empty/12/16",
			len(enc.GetCiphertext()), len(enc.GetInitializationVector()), enc.GetTagLength())
	}
	dec, err := client.RawDecrypt(ctx, &kmspb.RawDecryptRequest{
		Name:                        version,
		Ciphertext:                  enc.GetCiphertext(),
		InitializationVector:        enc.GetInitializationVector(),
		AdditionalAuthenticatedData: aad,
		TagLength:                   enc.GetTagLength(),
	})
	if err != nil {
		t.Fatalf("RawDecrypt: %v", err)
	}
	if string(dec.GetPlaintext()) != string(plaintext) {
		t.Fatalf("RawDecrypt plaintext = %q, want %q", dec.GetPlaintext(), plaintext)
	}
	if _, err := client.RawDecrypt(ctx, &kmspb.RawDecryptRequest{
		Name:                 version,
		Ciphertext:           enc.GetCiphertext(),
		InitializationVector: enc.GetInitializationVector(),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RawDecrypt wrong AAD err = %v, want InvalidArgument", err)
	}
}

func TestKMSDeleteAndRetiredResources(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	delKey := keyRing + "/cryptoKeys/del"
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: keyRing, CryptoKeyId: "del"}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	version := delKey + "/cryptoKeyVersions/1"

	// A live version cannot be deleted before it is destroyed.
	if _, err := client.DeleteCryptoKeyVersion(ctx, &kmspb.DeleteCryptoKeyVersionRequest{Name: version}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteCryptoKeyVersion(live) err = %v, want FailedPrecondition", err)
	}
	scheduled, err := client.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: version})
	if err != nil {
		t.Fatalf("DestroyCryptoKeyVersion: %v", err)
	}
	if scheduled.GetState() != kmspb.CryptoKeyVersion_DESTROY_SCHEDULED {
		t.Fatalf("DestroyCryptoKeyVersion state = %v, want DESTROY_SCHEDULED", scheduled.GetState())
	}
	// A scheduled version cannot be deleted either; it must first reach
	// DESTROYED, which the emulator promotes lazily once destroy_time passes.
	if _, err := client.DeleteCryptoKeyVersion(ctx, &kmspb.DeleteCryptoKeyVersionRequest{Name: version}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteCryptoKeyVersion(scheduled) err = %v, want FailedPrecondition", err)
	}
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(kmsstore.DefaultDestroyScheduledDuration + time.Second)})
	delVer, err := client.DeleteCryptoKeyVersion(ctx, &kmspb.DeleteCryptoKeyVersionRequest{Name: version})
	if err != nil {
		t.Fatalf("DeleteCryptoKeyVersion: %v", err)
	}
	if !delVer.GetDone() {
		t.Fatal("DeleteCryptoKeyVersion operation not done")
	}

	// A key with no versions can be deleted; the name is then retired.
	delKeyOp, err := client.DeleteCryptoKey(ctx, &kmspb.DeleteCryptoKeyRequest{Name: delKey})
	if err != nil {
		t.Fatalf("DeleteCryptoKey: %v", err)
	}
	if !delKeyOp.GetDone() {
		t.Fatal("DeleteCryptoKey operation not done")
	}
	if _, err := client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: delKey}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetCryptoKey after delete = %v, want NotFound", err)
	}
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: keyRing, CryptoKeyId: "del"}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateCryptoKey(reused retired name) err = %v, want AlreadyExists", err)
	}

	wantName := "projects/test/locations/global/retiredResources/del"
	rr, err := client.GetRetiredResource(ctx, &kmspb.GetRetiredResourceRequest{Name: wantName})
	if err != nil {
		t.Fatalf("GetRetiredResource: %v", err)
	}
	if rr.GetOriginalResource() != delKey || rr.GetDeleteTime() == nil {
		t.Fatalf("RetiredResource = %+v, want originalResource=%q with delete_time", rr, delKey)
	}
	list, err := client.ListRetiredResources(ctx, &kmspb.ListRetiredResourcesRequest{Parent: "projects/test/locations/global"})
	if err != nil {
		t.Fatalf("ListRetiredResources: %v", err)
	}
	if len(list.GetRetiredResources()) != 1 || list.GetRetiredResources()[0].GetName() != wantName {
		t.Fatalf("ListRetiredResources = %v, want [%s]", list.GetRetiredResources(), wantName)
	}
}

func TestKMSImportJobLifecycle(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")

	job, err := client.CreateImportJob(ctx, &kmspb.CreateImportJobRequest{
		Parent:      keyRing,
		ImportJobId: "job",
		ImportJob: &kmspb.ImportJob{
			ImportMethod:    kmspb.ImportJob_RSA_OAEP_3072_SHA256,
			ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
		},
	})
	if err != nil {
		t.Fatalf("CreateImportJob: %v", err)
	}
	wantName := keyRing + "/importJobs/job"
	if job.GetName() != wantName {
		t.Fatalf("CreateImportJob name = %q, want %q", job.GetName(), wantName)
	}
	if job.GetState() != kmspb.ImportJob_ACTIVE {
		t.Fatalf("CreateImportJob state = %v, want ACTIVE", job.GetState())
	}
	if job.GetPublicKey().GetPem() == "" || job.GetExpireTime() == nil {
		t.Fatal("CreateImportJob returned no public key / expire_time")
	}

	got, err := client.GetImportJob(ctx, &kmspb.GetImportJobRequest{Name: wantName})
	if err != nil {
		t.Fatalf("GetImportJob: %v", err)
	}
	if got.GetImportMethod() != kmspb.ImportJob_RSA_OAEP_3072_SHA256 {
		t.Fatalf("GetImportJob import_method = %v, want RSA_OAEP_3072_SHA256", got.GetImportMethod())
	}
	list, err := client.ListImportJobs(ctx, &kmspb.ListImportJobsRequest{Parent: keyRing})
	if err != nil {
		t.Fatalf("ListImportJobs: %v", err)
	}
	if len(list.GetImportJobs()) != 1 || list.GetImportJobs()[0].GetName() != wantName {
		t.Fatalf("ListImportJobs = %v, want [%s]", list.GetImportJobs(), wantName)
	}
}
