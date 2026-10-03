package kms

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"net"
	"testing"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"

	"jaiscloud/internal/clock"
	gcpcrypto "jaiscloud/internal/gcp/crypto"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// kmsTestService dials a real in-process gRPC server backed by the memory
// stores and returns the KMS and IAM clients.
func kmsTestService(t *testing.T) (kmspb.KeyManagementServiceClient, iampb.IAMPolicyClient, func()) {
	t.Helper()
	keys := kmsstore.NewMemoryStore()
	resources := store.NewMemoryResourceStore()
	svc := NewService(keys, resources, gcpcrypto.NewEnvelopeEncryptor(keys), "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	kmspb.RegisterKeyManagementServiceServer(srv, svc)
	iampb.RegisterIAMPolicyServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return kmspb.NewKeyManagementServiceClient(conn), iampb.NewIAMPolicyClient(conn), cleanup
}

const (
	keyRing = "projects/test/locations/global/keyRings/kr"
	symKey  = keyRing + "/cryptoKeys/sym"
	signKey = keyRing + "/cryptoKeys/sign"
	macKey  = keyRing + "/cryptoKeys/mac"
)

func createKeyRing(t *testing.T, client kmspb.KeyManagementServiceClient, name string) {
	t.Helper()
	if _, err := client.CreateKeyRing(context.Background(), &kmspb.CreateKeyRingRequest{
		Parent: "projects/test/locations/global", KeyRingId: name,
	}); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
}

func TestKMSKeyRingKeyVersionCRUD(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()

	// KeyRing create + get + list.
	createdKR, err := client.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
		Parent: "projects/test/locations/global", KeyRingId: "kr",
	})
	if err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if createdKR.GetName() != keyRing {
		t.Fatalf("CreateKeyRing name = %q, want %q", createdKR.GetName(), keyRing)
	}
	if _, err := client.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
		Parent: "projects/test/locations/global", KeyRingId: "kr",
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate CreateKeyRing err = %v, want AlreadyExists", err)
	}
	gotKR, err := client.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: keyRing})
	if err != nil {
		t.Fatalf("GetKeyRing: %v", err)
	}
	if gotKR.GetName() != keyRing {
		t.Fatalf("GetKeyRing name = %q, want %q", gotKR.GetName(), keyRing)
	}
	krs, err := client.ListKeyRings(ctx, &kmspb.ListKeyRingsRequest{Parent: "projects/test/locations/global"})
	if err != nil {
		t.Fatalf("ListKeyRings: %v", err)
	}
	if len(krs.GetKeyRings()) != 1 || krs.GetKeyRings()[0].GetName() != keyRing {
		t.Fatalf("ListKeyRings = %v, want exactly [%s]", krs.GetKeyRings(), keyRing)
	}

	// CryptoKey create + get + list (symmetric default).
	createdKey, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent: keyRing, CryptoKeyId: "sym",
	})
	if err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	if createdKey.GetName() != symKey {
		t.Fatalf("CreateCryptoKey name = %q, want %q", createdKey.GetName(), symKey)
	}
	if createdKey.GetPurpose() != kmspb.CryptoKey_ENCRYPT_DECRYPT {
		t.Fatalf("CreateCryptoKey purpose = %v, want ENCRYPT_DECRYPT", createdKey.GetPurpose())
	}
	if createdKey.GetPrimary().GetName() != symKey+"/cryptoKeyVersions/1" {
		t.Fatalf("CreateCryptoKey primary = %q, want version 1", createdKey.GetPrimary().GetName())
	}
	gotKey, err := client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: symKey})
	if err != nil {
		t.Fatalf("GetCryptoKey: %v", err)
	}
	if gotKey.GetName() != symKey {
		t.Fatalf("GetCryptoKey name = %q, want %q", gotKey.GetName(), symKey)
	}
	cks, err := client.ListCryptoKeys(ctx, &kmspb.ListCryptoKeysRequest{Parent: keyRing})
	if err != nil {
		t.Fatalf("ListCryptoKeys: %v", err)
	}
	if len(cks.GetCryptoKeys()) != 1 || cks.GetCryptoKeys()[0].GetName() != symKey {
		t.Fatalf("ListCryptoKeys = %v, want exactly [%s]", cks.GetCryptoKeys(), symKey)
	}

	// CryptoKeyVersion create + get + list.
	createdVer, err := client.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{
		Parent: symKey,
	})
	if err != nil {
		t.Fatalf("CreateCryptoKeyVersion: %v", err)
	}
	if createdVer.GetName() != symKey+"/cryptoKeyVersions/2" {
		t.Fatalf("CreateCryptoKeyVersion name = %q, want version 2", createdVer.GetName())
	}
	if createdVer.GetState() != kmspb.CryptoKeyVersion_ENABLED {
		t.Fatalf("CreateCryptoKeyVersion state = %v, want ENABLED", createdVer.GetState())
	}
	gotVer, err := client.GetCryptoKeyVersion(ctx, &kmspb.GetCryptoKeyVersionRequest{Name: symKey + "/cryptoKeyVersions/2"})
	if err != nil {
		t.Fatalf("GetCryptoKeyVersion: %v", err)
	}
	if gotVer.GetName() != symKey+"/cryptoKeyVersions/2" {
		t.Fatalf("GetCryptoKeyVersion name = %q, want version 2", gotVer.GetName())
	}
	vers, err := client.ListCryptoKeyVersions(ctx, &kmspb.ListCryptoKeyVersionsRequest{Parent: symKey})
	if err != nil {
		t.Fatalf("ListCryptoKeyVersions: %v", err)
	}
	if len(vers.GetCryptoKeyVersions()) != 2 {
		t.Fatalf("ListCryptoKeyVersions = %d versions, want 2", len(vers.GetCryptoKeyVersions()))
	}

	// Missing key → NotFound.
	if _, err := client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: keyRing + "/cryptoKeys/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetCryptoKey missing err = %v, want NotFound", err)
	}
}

func TestKMSSymmetricEncryptDecrypt(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: keyRing, CryptoKeyId: "sym"}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	plaintext := []byte("the quick brown fox")
	aad := []byte("context")

	enc, err := client.Encrypt(ctx, &kmspb.EncryptRequest{
		Name:                        symKey,
		Plaintext:                   plaintext,
		AdditionalAuthenticatedData: aad,
	})
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if len(enc.GetCiphertext()) == 0 {
		t.Fatal("Encrypt ciphertext is empty")
	}
	if enc.GetProtectionLevel() != kmspb.ProtectionLevel_SOFTWARE {
		t.Fatalf("Encrypt protectionLevel = %v, want SOFTWARE", enc.GetProtectionLevel())
	}

	// Decrypt with the matching AAD recovers the plaintext.
	dec, err := client.Decrypt(ctx, &kmspb.DecryptRequest{
		Name:                        symKey,
		Ciphertext:                  enc.GetCiphertext(),
		AdditionalAuthenticatedData: aad,
	})
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(dec.GetPlaintext()) != string(plaintext) {
		t.Fatalf("Decrypt plaintext = %q, want %q", dec.GetPlaintext(), plaintext)
	}
	if !dec.GetUsedPrimary() {
		t.Fatal("Decrypt usedPrimary = false, want true")
	}

	// Decrypt with the wrong AAD fails.
	if _, err := client.Decrypt(ctx, &kmspb.DecryptRequest{
		Name: symKey, Ciphertext: enc.GetCiphertext(),
		AdditionalAuthenticatedData: []byte("wrong"),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Decrypt wrong AAD err = %v, want InvalidArgument", err)
	}
}

func TestKMSAsymmetricSignAndPublicKey(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      keyRing,
		CryptoKeyId: "sign",
		CryptoKey: &kmspb.CryptoKey{
			Purpose: kmspb.CryptoKey_ASYMMETRIC_SIGN,
		},
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	verName := signKey + "/cryptoKeyVersions/1"
	msg := []byte("message to sign")
	digest := sha256.Sum256(msg)

	signResp, err := client.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{
		Name:   verName,
		Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}},
	})
	if err != nil {
		t.Fatalf("AsymmetricSign: %v", err)
	}
	if len(signResp.GetSignature()) == 0 {
		t.Fatal("AsymmetricSign signature is empty")
	}

	// GetPublicKey returns a verifiable PEM.
	pub, err := client.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: verName})
	if err != nil {
		t.Fatalf("GetPublicKey: %v", err)
	}
	if pub.GetProtectionLevel() != kmspb.ProtectionLevel_SOFTWARE {
		t.Fatalf("GetPublicKey protectionLevel = %v, want SOFTWARE", pub.GetProtectionLevel())
	}
	block, _ := pem.Decode([]byte(pub.GetPem()))
	if block == nil {
		t.Fatalf("GetPublicKey pem is not valid PEM: %q", pub.GetPem())
	}
	pubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	rsaPub, ok := pubKey.(*rsa.PublicKey)
	if !ok {
		t.Fatalf("public key type = %T, want *rsa.PublicKey", pubKey)
	}
	if err := rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, digest[:], signResp.GetSignature()); err != nil {
		t.Fatalf("signature verify failed: %v", err)
	}
}

func TestKMSMacSignVerify(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      keyRing,
		CryptoKeyId: "mac",
		CryptoKey: &kmspb.CryptoKey{
			Purpose: kmspb.CryptoKey_MAC,
		},
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	verName := macKey + "/cryptoKeyVersions/1"
	data := []byte("mac this data")

	signResp, err := client.MacSign(ctx, &kmspb.MacSignRequest{Name: verName, Data: data})
	if err != nil {
		t.Fatalf("MacSign: %v", err)
	}
	if len(signResp.GetMac()) == 0 {
		t.Fatal("MacSign mac is empty")
	}

	verifyResp, err := client.MacVerify(ctx, &kmspb.MacVerifyRequest{Name: verName, Data: data, Mac: signResp.GetMac()})
	if err != nil {
		t.Fatalf("MacVerify: %v", err)
	}
	if !verifyResp.GetSuccess() {
		t.Fatal("MacVerify success = false, want true")
	}

	// Tampered data fails verification.
	badVerify, err := client.MacVerify(ctx, &kmspb.MacVerifyRequest{Name: verName, Data: []byte("tampered"), Mac: signResp.GetMac()})
	if err != nil {
		t.Fatalf("MacVerify tampered: %v", err)
	}
	if badVerify.GetSuccess() {
		t.Fatal("MacVerify tampered success = true, want false")
	}
}

// TestKMSCryptoKeyLabelsAndRotation covers the stored labels/rotation schedule
// surface: create persists both and derives nextRotationTime; get returns them;
// UpdateCryptoKey honors labels/rotation_period masks; unsupported paths fail
// loud; a non-positive period is rejected.
func TestKMSCryptoKeyLabelsAndRotation(t *testing.T) {
	clock.SetGlobalClock(clock.FixedClock{T: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)})
	defer clock.SetGlobalClock(clock.RealClock{})

	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")

	const period = 24 * time.Hour
	created, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      keyRing,
		CryptoKeyId: "sym",
		CryptoKey: &kmspb.CryptoKey{
			Labels: map[string]string{"env": "test", "team": "kms"},
			RotationSchedule: &kmspb.CryptoKey_RotationPeriod{
				RotationPeriod: durationpb.New(period),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	if created.GetLabels()["env"] != "test" || created.GetLabels()["team"] != "kms" {
		t.Fatalf("CreateCryptoKey labels = %v, want env=test team=kms", created.GetLabels())
	}
	if created.GetRotationPeriod().AsDuration() != period {
		t.Fatalf("CreateCryptoKey rotationPeriod = %v, want %v", created.GetRotationPeriod(), period)
	}
	if got := created.GetNextRotationTime().AsTime().Sub(created.GetCreateTime().AsTime()); got != period {
		t.Fatalf("CreateCryptoKey nextRotationTime - createTime = %v, want %v", got, period)
	}
	wantNext := created.GetNextRotationTime().AsTime()

	// Get returns the persisted labels/rotation/derived next rotation time.
	got, err := client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: symKey})
	if err != nil {
		t.Fatalf("GetCryptoKey: %v", err)
	}
	if got.GetLabels()["env"] != "test" {
		t.Fatalf("GetCryptoKey labels = %v, want env=test", got.GetLabels())
	}
	if got.GetRotationPeriod().AsDuration() != period {
		t.Fatalf("GetCryptoKey rotationPeriod = %v, want %v", got.GetRotationPeriod(), period)
	}
	if !got.GetNextRotationTime().AsTime().Equal(wantNext) {
		t.Fatalf("GetCryptoKey nextRotationTime = %v, want %v", got.GetNextRotationTime().AsTime(), wantNext)
	}

	// Masked labels update preserves the rotation schedule.
	updated, err := client.UpdateCryptoKey(ctx, &kmspb.UpdateCryptoKeyRequest{
		CryptoKey:  &kmspb.CryptoKey{Name: symKey, Labels: map[string]string{"env": "prod"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateCryptoKey labels: %v", err)
	}
	if updated.GetLabels()["env"] != "prod" || updated.GetLabels()["team"] != "" {
		t.Fatalf("UpdateCryptoKey labels = %v, want exactly env=prod", updated.GetLabels())
	}
	if updated.GetRotationPeriod().AsDuration() != period {
		t.Fatalf("labels-only update disturbed rotationPeriod: %v", updated.GetRotationPeriod())
	}
	if !updated.GetNextRotationTime().AsTime().Equal(wantNext) {
		t.Fatalf("labels-only update disturbed nextRotationTime: %v", updated.GetNextRotationTime().AsTime())
	}

	// Setting a new period recomputes nextRotationTime from now.
	newPeriod := 48 * time.Hour
	reRotated, err := client.UpdateCryptoKey(ctx, &kmspb.UpdateCryptoKeyRequest{
		CryptoKey: &kmspb.CryptoKey{
			Name:             symKey,
			RotationSchedule: &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(newPeriod)},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rotation_period"}},
	})
	if err != nil {
		t.Fatalf("UpdateCryptoKey rotation_period: %v", err)
	}
	if reRotated.GetRotationPeriod().AsDuration() != newPeriod {
		t.Fatalf("updated rotationPeriod = %v, want %v", reRotated.GetRotationPeriod(), newPeriod)
	}
	wantRecomputed := clock.Now().Add(newPeriod)
	if !reRotated.GetNextRotationTime().AsTime().Equal(wantRecomputed) {
		t.Fatalf("updated nextRotationTime = %v, want %v", reRotated.GetNextRotationTime().AsTime(), wantRecomputed)
	}
	if reRotated.GetLabels()["env"] != "prod" {
		t.Fatalf("rotation-only update disturbed labels: %v", reRotated.GetLabels())
	}

	// Clearing the period clears nextRotationTime.
	cleared, err := client.UpdateCryptoKey(ctx, &kmspb.UpdateCryptoKeyRequest{
		CryptoKey:  &kmspb.CryptoKey{Name: symKey},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rotation_period"}},
	})
	if err != nil {
		t.Fatalf("UpdateCryptoKey clear rotation: %v", err)
	}
	if cleared.GetRotationPeriod() != nil {
		t.Fatalf("cleared rotationPeriod = %v, want nil", cleared.GetRotationPeriod())
	}
	if cleared.GetNextRotationTime() != nil {
		t.Fatalf("cleared nextRotationTime = %v, want nil", cleared.GetNextRotationTime())
	}
	if cleared.GetLabels()["env"] != "prod" {
		t.Fatalf("clear-rotation update disturbed labels: %v", cleared.GetLabels())
	}

	// Unsupported mask path fails loud with Unimplemented.
	if _, err := client.UpdateCryptoKey(ctx, &kmspb.UpdateCryptoKeyRequest{
		CryptoKey:  &kmspb.CryptoKey{Name: symKey},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"purpose"}},
	}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unsupported mask path err = %v, want Unimplemented", err)
	}

	// A non-positive rotation period is rejected on create and update.
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      keyRing,
		CryptoKeyId: "bad",
		CryptoKey: &kmspb.CryptoKey{
			RotationSchedule: &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(-time.Hour)},
		},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("negative rotation period err = %v, want InvalidArgument", err)
	}
	if _, err := client.UpdateCryptoKey(ctx, &kmspb.UpdateCryptoKeyRequest{
		CryptoKey: &kmspb.CryptoKey{
			Name:             symKey,
			RotationSchedule: &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(0)},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"rotation_period"}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("zero update rotation period err = %v, want InvalidArgument", err)
	}
}

// TestKMSCryptoKeyRotationExecutesOnRead verifies the gRPC surface executes a
// due rotation schedule lazily: once the clock advances past nextRotationTime,
// GetCryptoKey creates version 2, makes it primary, and advances the schedule.
func TestKMSCryptoKeyRotationExecutesOnRead(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")

	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      keyRing,
		CryptoKeyId: "sym",
		CryptoKey: &kmspb.CryptoKey{
			RotationSchedule: &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(time.Hour)},
		},
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	// Before the due time (nextRotationTime = t0+1h) nothing rotates.
	got, err := client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: symKey})
	if err != nil {
		t.Fatalf("GetCryptoKey (not due): %v", err)
	}
	if got.GetPrimary().GetName() != symKey+"/cryptoKeyVersions/1" {
		t.Fatalf("primary before rotation = %q, want version 1", got.GetPrimary().GetName())
	}

	// Advance past nextRotationTime and read again: rotation runs.
	due := t0.Add(2 * time.Hour)
	clock.SetGlobalClock(clock.FixedClock{T: due})
	got, err = client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: symKey})
	if err != nil {
		t.Fatalf("GetCryptoKey (due): %v", err)
	}
	if got.GetPrimary().GetName() != symKey+"/cryptoKeyVersions/2" {
		t.Fatalf("primary after rotation = %q, want version 2", got.GetPrimary().GetName())
	}
	if !got.GetNextRotationTime().AsTime().Equal(due.Add(time.Hour)) {
		t.Fatalf("nextRotationTime = %v, want %v", got.GetNextRotationTime().AsTime(), due.Add(time.Hour))
	}
	vers, err := client.ListCryptoKeyVersions(ctx, &kmspb.ListCryptoKeyVersionsRequest{Parent: symKey})
	if err != nil {
		t.Fatalf("ListCryptoKeyVersions: %v", err)
	}
	if len(vers.GetCryptoKeyVersions()) != 2 {
		t.Fatalf("versions after rotation = %d, want 2", len(vers.GetCryptoKeyVersions()))
	}
}

// TestKMSDestroyVersionLifecycle covers the faithful destruction timing:
// DestroyCryptoKeyVersion moves a version to DESTROY_SCHEDULED with a
// destroy_time 30 days out (idempotently), the scheduled version is unusable,
// RestoreCryptoKeyVersion reverses it before the window closes, and a read after
// destroy_time lazily promotes it to the terminal DESTROYED state.
func TestKMSDestroyVersionLifecycle(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: keyRing, CryptoKeyId: "sym"}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	verName := symKey + "/cryptoKeyVersions/1"
	wantDestroy := t0.Add(kmsstore.DefaultDestroyScheduledDuration)

	// Destroy schedules destruction rather than destroying eagerly.
	scheduled, err := client.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: verName})
	if err != nil {
		t.Fatalf("DestroyCryptoKeyVersion: %v", err)
	}
	if scheduled.GetState() != kmspb.CryptoKeyVersion_DESTROY_SCHEDULED {
		t.Fatalf("DestroyCryptoKeyVersion state = %v, want DESTROY_SCHEDULED", scheduled.GetState())
	}
	if scheduled.GetDestroyTime() == nil || !scheduled.GetDestroyTime().AsTime().Equal(wantDestroy) {
		t.Fatalf("destroy_time = %v, want %v", scheduled.GetDestroyTime().AsTime(), wantDestroy)
	}
	if scheduled.GetDestroyEventTime() != nil {
		t.Fatal("DESTROY_SCHEDULED version should not carry destroy_event_time")
	}

	// Destroying an already-scheduled version is idempotent.
	again, err := client.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: verName})
	if err != nil {
		t.Fatalf("DestroyCryptoKeyVersion (again): %v", err)
	}
	if again.GetState() != kmspb.CryptoKeyVersion_DESTROY_SCHEDULED ||
		again.GetDestroyTime() == nil || !again.GetDestroyTime().AsTime().Equal(wantDestroy) {
		t.Fatalf("second destroy = %v @ %v, want unchanged DESTROY_SCHEDULED @ %v",
			again.GetState(), again.GetDestroyTime().AsTime(), wantDestroy)
	}

	// A scheduled version cannot be used and the primary reports its state.
	if _, err := client.Encrypt(ctx, &kmspb.EncryptRequest{Name: symKey, Plaintext: []byte("hi")}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Encrypt scheduled err = %v, want FailedPrecondition", err)
	}
	got, err := client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: symKey})
	if err != nil {
		t.Fatalf("GetCryptoKey: %v", err)
	}
	if got.GetPrimary().GetState() != kmspb.CryptoKeyVersion_DESTROY_SCHEDULED {
		t.Fatalf("primary.state = %v, want DESTROY_SCHEDULED", got.GetPrimary().GetState())
	}

	// Restore before destroy_time returns the version to DISABLED and clears
	// destroy_time (GCP semantics).
	restored, err := client.RestoreCryptoKeyVersion(ctx, &kmspb.RestoreCryptoKeyVersionRequest{Name: verName})
	if err != nil {
		t.Fatalf("RestoreCryptoKeyVersion: %v", err)
	}
	if restored.GetState() != kmspb.CryptoKeyVersion_DISABLED {
		t.Fatalf("RestoreCryptoKeyVersion state = %v, want DISABLED", restored.GetState())
	}
	if restored.GetDestroyTime() != nil {
		t.Fatalf("restored version destroy_time = %v, want nil", restored.GetDestroyTime())
	}

	// UpdateCryptoKeyVersion moves it back to ENABLED.
	enabled, err := client.UpdateCryptoKeyVersion(ctx, &kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{Name: verName, State: kmspb.CryptoKeyVersion_ENABLED},
	})
	if err != nil {
		t.Fatalf("UpdateCryptoKeyVersion: %v", err)
	}
	if enabled.GetState() != kmspb.CryptoKeyVersion_ENABLED {
		t.Fatalf("UpdateCryptoKeyVersion state = %v, want ENABLED", enabled.GetState())
	}

	// Destroy again, then advance the clock past destroy_time. A read promotes
	// the version to DESTROYED, records destroy_event_time, and clears
	// destroy_time.
	if _, err := client.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: verName}); err != nil {
		t.Fatalf("DestroyCryptoKeyVersion (second schedule): %v", err)
	}
	clock.SetGlobalClock(clock.FixedClock{T: wantDestroy.Add(time.Second)})
	// Restoring after the window must fail even with no prior read: the service
	// promotes before restoring, so the version is already DESTROYED.
	if _, err := client.RestoreCryptoKeyVersion(ctx, &kmspb.RestoreCryptoKeyVersionRequest{Name: verName}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RestoreCryptoKeyVersion after window err = %v, want FailedPrecondition", err)
	}
	promoted, err := client.GetCryptoKeyVersion(ctx, &kmspb.GetCryptoKeyVersionRequest{Name: verName})
	if err != nil {
		t.Fatalf("GetCryptoKeyVersion after window: %v", err)
	}
	if promoted.GetState() != kmspb.CryptoKeyVersion_DESTROYED {
		t.Fatalf("promoted state = %v, want DESTROYED", promoted.GetState())
	}
	if promoted.GetDestroyTime() != nil {
		t.Fatalf("DESTROYED version destroy_time = %v, want nil", promoted.GetDestroyTime())
	}
	if promoted.GetDestroyEventTime() == nil || !promoted.GetDestroyEventTime().AsTime().Equal(wantDestroy) {
		t.Fatalf("destroy_event_time = %v, want %v", promoted.GetDestroyEventTime().AsTime(), wantDestroy)
	}

	// DESTROYED is terminal: destroy stays idempotent, restore and update fail.
	still, err := client.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: verName})
	if err != nil {
		t.Fatalf("DestroyCryptoKeyVersion (destroyed): %v", err)
	}
	if still.GetState() != kmspb.CryptoKeyVersion_DESTROYED {
		t.Fatalf("destroy on DESTROYED state = %v, want DESTROYED", still.GetState())
	}
	if _, err := client.RestoreCryptoKeyVersion(ctx, &kmspb.RestoreCryptoKeyVersionRequest{Name: verName}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RestoreCryptoKeyVersion on DESTROYED err = %v, want FailedPrecondition", err)
	}
	if _, err := client.UpdateCryptoKeyVersion(ctx, &kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{Name: verName, State: kmspb.CryptoKeyVersion_ENABLED},
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("UpdateCryptoKeyVersion on DESTROYED err = %v, want FailedPrecondition", err)
	}
}

func TestKMSKeyRingIamPolicy(t *testing.T) {
	client, iam, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")

	// Empty policy by default.
	pol, err := iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: keyRing})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if len(pol.GetBindings()) != 0 {
		t.Fatalf("GetIamPolicy bindings = %v, want empty", pol.GetBindings())
	}

	// Set IAM policy.
	if _, err := iam.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: keyRing,
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{{Role: "roles/cloudkms.cryptoKeyEncrypter", Members: []string{"allUsers"}}},
		},
	}); err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}

	pol, err = iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: keyRing})
	if err != nil {
		t.Fatalf("GetIamPolicy after set: %v", err)
	}
	if len(pol.GetBindings()) != 1 || pol.GetBindings()[0].GetRole() != "roles/cloudkms.cryptoKeyEncrypter" {
		t.Fatalf("GetIamPolicy bindings = %v, want roles/cloudkms.cryptoKeyEncrypter", pol.GetBindings())
	}

	// Test permissions echoes the request (no-authz posture).
	tp, err := iam.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource: keyRing, Permissions: []string{"cloudkms.cryptoKeys.get", "cloudkms.cryptoKeys.create"},
	})
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(tp.GetPermissions()) != 2 {
		t.Fatalf("TestIamPermissions = %v, want 2 granted", tp.GetPermissions())
	}

	// IAM on a missing keyring → NotFound.
	if _, err := iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
		Resource: "projects/test/locations/global/keyRings/missing",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetIamPolicy missing err = %v, want NotFound", err)
	}
}

// TestKMSVersionIamRejected pins that IAM is scoped to key rings and crypto
// keys only: real KMS has no version-level IAM, so a cryptoKeyVersions resource
// must be rejected rather than accepted (the pre-removal emulator served it).
func TestKMSVersionIamRejected(t *testing.T) {
	client, iam, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: keyRing, CryptoKeyId: "sym"}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	verName := symKey + "/cryptoKeyVersions/1"

	if _, err := iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: verName}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetIamPolicy(version) err = %v, want InvalidArgument", err)
	}
	if _, err := iam.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: verName,
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/viewer", Members: []string{"allUsers"}}}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SetIamPolicy(version) err = %v, want InvalidArgument", err)
	}
	if _, err := iam.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource: verName, Permissions: []string{"cloudkms.cryptoKeyVersions.get"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("TestIamPermissions(version) err = %v, want InvalidArgument", err)
	}

	// The crypto key itself still supports IAM.
	if _, err := iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: symKey}); err != nil {
		t.Fatalf("GetIamPolicy(cryptoKey): %v", err)
	}
}

func TestKMSGenerateRandomBytes(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := client.GenerateRandomBytes(ctx, &kmspb.GenerateRandomBytesRequest{
		Location:        "projects/test/locations/us-central1",
		LengthBytes:     32,
		ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
	})
	if err != nil {
		t.Fatalf("GenerateRandomBytes: %v", err)
	}
	if len(resp.GetData()) != 32 {
		t.Fatalf("GenerateRandomBytes data length = %d, want 32", len(resp.GetData()))
	}
	// Two calls must not produce identical bytes.
	resp2, err := client.GenerateRandomBytes(ctx, &kmspb.GenerateRandomBytesRequest{
		Location: "projects/test/locations/us-central1", LengthBytes: 32,
	})
	if err != nil {
		t.Fatalf("GenerateRandomBytes (2): %v", err)
	}
	if string(resp.GetData()) == string(resp2.GetData()) {
		t.Fatal("GenerateRandomBytes returned identical bytes across calls")
	}

	// Negative length is rejected.
	if _, err := client.GenerateRandomBytes(ctx, &kmspb.GenerateRandomBytesRequest{
		Location: "projects/test/locations/us-central1", LengthBytes: 0,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GenerateRandomBytes length 0 err = %v, want InvalidArgument", err)
	}
}

// createTestKey creates a crypto key of the given purpose and returns its
// primary version name.
func createTestKey(t *testing.T, client kmspb.KeyManagementServiceClient, id string, purpose kmspb.CryptoKey_CryptoKeyPurpose) string {
	t.Helper()
	if _, err := client.CreateCryptoKey(context.Background(), &kmspb.CreateCryptoKeyRequest{
		Parent:      keyRing,
		CryptoKeyId: id,
		CryptoKey:   &kmspb.CryptoKey{Purpose: purpose},
	}); err != nil {
		t.Fatalf("CreateCryptoKey %s: %v", id, err)
	}
	return keyRing + "/cryptoKeys/" + id + "/cryptoKeyVersions/1"
}

func TestKMSEncryptCRC32CVerification(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	createTestKey(t, client, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT)

	plaintext := []byte("the quick brown fox")
	aad := []byte("context")

	// Matching checksums verify and report so.
	enc, err := client.Encrypt(ctx, &kmspb.EncryptRequest{
		Name:                              symKey,
		Plaintext:                         plaintext,
		PlaintextCrc32C:                   wrapperspb.Int64(crc32cOf(plaintext)),
		AdditionalAuthenticatedData:       aad,
		AdditionalAuthenticatedDataCrc32C: wrapperspb.Int64(crc32cOf(aad)),
	})
	if err != nil {
		t.Fatalf("Encrypt matching checksums: %v", err)
	}
	if !enc.GetVerifiedPlaintextCrc32C() || !enc.GetVerifiedAdditionalAuthenticatedDataCrc32C() {
		t.Fatalf("Encrypt verified = (%v, %v), want (true, true)",
			enc.GetVerifiedPlaintextCrc32C(), enc.GetVerifiedAdditionalAuthenticatedDataCrc32C())
	}

	// Absent checksums succeed and are reported as not verified.
	enc, err = client.Encrypt(ctx, &kmspb.EncryptRequest{Name: symKey, Plaintext: plaintext, AdditionalAuthenticatedData: aad})
	if err != nil {
		t.Fatalf("Encrypt absent checksums: %v", err)
	}
	if enc.GetVerifiedPlaintextCrc32C() || enc.GetVerifiedAdditionalAuthenticatedDataCrc32C() {
		t.Fatalf("Encrypt absent verified = (%v, %v), want (false, false)",
			enc.GetVerifiedPlaintextCrc32C(), enc.GetVerifiedAdditionalAuthenticatedDataCrc32C())
	}

	// Mismatched plaintext checksum fails with InvalidArgument.
	if _, err := client.Encrypt(ctx, &kmspb.EncryptRequest{
		Name:            symKey,
		Plaintext:       plaintext,
		PlaintextCrc32C: wrapperspb.Int64(crc32cOf(plaintext) + 1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Encrypt plaintext mismatch err = %v, want InvalidArgument", err)
	}

	// Mismatched AAD checksum fails with InvalidArgument.
	if _, err := client.Encrypt(ctx, &kmspb.EncryptRequest{
		Name:                              symKey,
		Plaintext:                         plaintext,
		AdditionalAuthenticatedData:       aad,
		AdditionalAuthenticatedDataCrc32C: wrapperspb.Int64(crc32cOf(aad) + 1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Encrypt AAD mismatch err = %v, want InvalidArgument", err)
	}
}

func TestKMSAsymmetricSignCRC32CVerification(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	verName := createTestKey(t, client, "sign", kmspb.CryptoKey_ASYMMETRIC_SIGN)

	msg := []byte("message to sign")
	digest := sha256.Sum256(msg)

	// Matching digest checksum verifies and reports so.
	signResp, err := client.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{
		Name:         verName,
		Digest:       &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}},
		DigestCrc32C: wrapperspb.Int64(crc32cOf(digest[:])),
	})
	if err != nil {
		t.Fatalf("AsymmetricSign matching digest checksum: %v", err)
	}
	if !signResp.GetVerifiedDigestCrc32C() || signResp.GetVerifiedDataCrc32C() {
		t.Fatalf("AsymmetricSign verified = (%v, %v), want (true, false)",
			signResp.GetVerifiedDigestCrc32C(), signResp.GetVerifiedDataCrc32C())
	}

	// Absent checksum succeeds and is reported as not verified.
	signResp, err = client.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{
		Name:   verName,
		Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}},
	})
	if err != nil {
		t.Fatalf("AsymmetricSign absent checksum: %v", err)
	}
	if signResp.GetVerifiedDigestCrc32C() || signResp.GetVerifiedDataCrc32C() {
		t.Fatalf("AsymmetricSign absent verified = (%v, %v), want (false, false)",
			signResp.GetVerifiedDigestCrc32C(), signResp.GetVerifiedDataCrc32C())
	}

	// Mismatched digest checksum fails with InvalidArgument.
	if _, err := client.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{
		Name:         verName,
		Digest:       &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}},
		DigestCrc32C: wrapperspb.Int64(crc32cOf(digest[:]) + 1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("AsymmetricSign digest mismatch err = %v, want InvalidArgument", err)
	}

	// Matching raw-data checksum verifies and reports so.
	signResp, err = client.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{
		Name:       verName,
		Data:       msg,
		DataCrc32C: wrapperspb.Int64(crc32cOf(msg)),
	})
	if err != nil {
		t.Fatalf("AsymmetricSign matching data checksum: %v", err)
	}
	if !signResp.GetVerifiedDataCrc32C() || signResp.GetVerifiedDigestCrc32C() {
		t.Fatalf("AsymmetricSign data verified = (%v, %v), want (true, false)",
			signResp.GetVerifiedDataCrc32C(), signResp.GetVerifiedDigestCrc32C())
	}

	// Mismatched raw-data checksum fails with InvalidArgument.
	if _, err := client.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{
		Name:       verName,
		Data:       msg,
		DataCrc32C: wrapperspb.Int64(crc32cOf(msg) + 1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("AsymmetricSign data mismatch err = %v, want InvalidArgument", err)
	}
}

func TestKMSAsymmetricDecryptCRC32CVerification(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	verName := createTestKey(t, client, "decrypt", kmspb.CryptoKey_ASYMMETRIC_DECRYPT)

	plaintext := []byte("secret payload")

	pub, err := client.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: verName})
	if err != nil {
		t.Fatalf("GetPublicKey: %v", err)
	}
	block, _ := pem.Decode([]byte(pub.GetPem()))
	if block == nil {
		t.Fatalf("GetPublicKey pem is not valid PEM: %q", pub.GetPem())
	}
	pubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	rsaPub := pubKey.(*rsa.PublicKey)
	ct, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, rsaPub, plaintext, nil)
	if err != nil {
		t.Fatalf("RSA encrypt: %v", err)
	}

	// Matching ciphertext checksum verifies and reports so.
	dec, err := client.AsymmetricDecrypt(ctx, &kmspb.AsymmetricDecryptRequest{
		Name:             verName,
		Ciphertext:       ct,
		CiphertextCrc32C: wrapperspb.Int64(crc32cOf(ct)),
	})
	if err != nil {
		t.Fatalf("AsymmetricDecrypt matching checksum: %v", err)
	}
	if !dec.GetVerifiedCiphertextCrc32C() {
		t.Fatal("AsymmetricDecrypt verifiedCiphertextCrc32C = false, want true")
	}
	if string(dec.GetPlaintext()) != string(plaintext) {
		t.Fatalf("AsymmetricDecrypt plaintext = %q, want %q", dec.GetPlaintext(), plaintext)
	}

	// Absent checksum succeeds and is reported as not verified.
	dec, err = client.AsymmetricDecrypt(ctx, &kmspb.AsymmetricDecryptRequest{Name: verName, Ciphertext: ct})
	if err != nil {
		t.Fatalf("AsymmetricDecrypt absent checksum: %v", err)
	}
	if dec.GetVerifiedCiphertextCrc32C() {
		t.Fatal("AsymmetricDecrypt absent verifiedCiphertextCrc32C = true, want false")
	}

	// Mismatched ciphertext checksum fails with InvalidArgument.
	if _, err := client.AsymmetricDecrypt(ctx, &kmspb.AsymmetricDecryptRequest{
		Name:             verName,
		Ciphertext:       ct,
		CiphertextCrc32C: wrapperspb.Int64(crc32cOf(ct) + 1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("AsymmetricDecrypt mismatch err = %v, want InvalidArgument", err)
	}
}

func TestKMSMacSignCRC32CVerification(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	verName := createTestKey(t, client, "mac", kmspb.CryptoKey_MAC)

	data := []byte("mac this data")

	// Matching data checksum verifies and reports so.
	signResp, err := client.MacSign(ctx, &kmspb.MacSignRequest{
		Name:       verName,
		Data:       data,
		DataCrc32C: wrapperspb.Int64(crc32cOf(data)),
	})
	if err != nil {
		t.Fatalf("MacSign matching checksum: %v", err)
	}
	if !signResp.GetVerifiedDataCrc32C() {
		t.Fatal("MacSign verifiedDataCrc32C = false, want true")
	}

	// Absent checksum succeeds and is reported as not verified.
	signResp, err = client.MacSign(ctx, &kmspb.MacSignRequest{Name: verName, Data: data})
	if err != nil {
		t.Fatalf("MacSign absent checksum: %v", err)
	}
	if signResp.GetVerifiedDataCrc32C() {
		t.Fatal("MacSign absent verifiedDataCrc32C = true, want false")
	}

	// Mismatched data checksum fails with InvalidArgument.
	if _, err := client.MacSign(ctx, &kmspb.MacSignRequest{
		Name:       verName,
		Data:       data,
		DataCrc32C: wrapperspb.Int64(crc32cOf(data) + 1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("MacSign mismatch err = %v, want InvalidArgument", err)
	}
}

func TestKMSMacVerifyCRC32CVerification(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	verName := createTestKey(t, client, "mac", kmspb.CryptoKey_MAC)

	data := []byte("mac this data")
	signResp, err := client.MacSign(ctx, &kmspb.MacSignRequest{Name: verName, Data: data})
	if err != nil {
		t.Fatalf("MacSign: %v", err)
	}
	mac := signResp.GetMac()

	// Matching data + mac checksums verify and report so.
	verifyResp, err := client.MacVerify(ctx, &kmspb.MacVerifyRequest{
		Name:       verName,
		Data:       data,
		DataCrc32C: wrapperspb.Int64(crc32cOf(data)),
		Mac:        mac,
		MacCrc32C:  wrapperspb.Int64(crc32cOf(mac)),
	})
	if err != nil {
		t.Fatalf("MacVerify matching checksums: %v", err)
	}
	if !verifyResp.GetSuccess() || !verifyResp.GetVerifiedDataCrc32C() || !verifyResp.GetVerifiedMacCrc32C() {
		t.Fatalf("MacVerify = (success=%v, data=%v, mac=%v), want (true, true, true)",
			verifyResp.GetSuccess(), verifyResp.GetVerifiedDataCrc32C(), verifyResp.GetVerifiedMacCrc32C())
	}

	// Absent checksums succeed and are reported as not verified.
	verifyResp, err = client.MacVerify(ctx, &kmspb.MacVerifyRequest{Name: verName, Data: data, Mac: mac})
	if err != nil {
		t.Fatalf("MacVerify absent checksums: %v", err)
	}
	if !verifyResp.GetSuccess() || verifyResp.GetVerifiedDataCrc32C() || verifyResp.GetVerifiedMacCrc32C() {
		t.Fatalf("MacVerify absent = (success=%v, data=%v, mac=%v), want (true, false, false)",
			verifyResp.GetSuccess(), verifyResp.GetVerifiedDataCrc32C(), verifyResp.GetVerifiedMacCrc32C())
	}

	// Mismatched data checksum fails with InvalidArgument.
	if _, err := client.MacVerify(ctx, &kmspb.MacVerifyRequest{
		Name:       verName,
		Data:       data,
		DataCrc32C: wrapperspb.Int64(crc32cOf(data) + 1),
		Mac:        mac,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("MacVerify data mismatch err = %v, want InvalidArgument", err)
	}

	// Mismatched mac checksum fails with InvalidArgument.
	if _, err := client.MacVerify(ctx, &kmspb.MacVerifyRequest{
		Name:      verName,
		Data:      data,
		Mac:       mac,
		MacCrc32C: wrapperspb.Int64(crc32cOf(mac) + 1),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("MacVerify mac mismatch err = %v, want InvalidArgument", err)
	}
}
