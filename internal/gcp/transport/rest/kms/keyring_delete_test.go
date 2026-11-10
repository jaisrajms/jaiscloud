package kms

import (
	"context"
	"errors"
	"strings"
	"testing"

	"jaiscloud/internal/model"
)

// createRing creates a key ring through the REST handler (the surface under
// test), so the fixture uses the same code path a raw client does.
func createRing(t *testing.T, p *Provider, ring string) {
	t.Helper()
	if _, err := p.KeyRingCreate(context.Background(), newNR(map[string]any{
		"location": "global", "keyRingId": ring,
	})); err != nil {
		t.Fatalf("KeyRingCreate(%s): %v", ring, err)
	}
}

// restError returns the provider error behind err, failing on anything else.
func restError(t *testing.T, err error) *model.ProviderError {
	t.Helper()
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("err = %v (%T), want *model.ProviderError", err, err)
	}
	return perr
}

// TestKeyRingDeleteEmptyRing pins TF5-4: the Discovery documents
// projects.locations.keyRings.delete (DELETE v1/{+name}, an LRO) and real Cloud
// KMS permanently deletes an empty key ring. The response is the inline-done
// Operation cryptoKeys.delete returns, and the ring is gone afterwards — both a
// GET and a LIST answer 404/empty.
func TestKeyRingDeleteEmptyRing(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRing(t, p, "doomed")

	resp, err := p.KeyRingDelete(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/global/keyRings/doomed",
	}))
	if err != nil {
		t.Fatalf("KeyRingDelete: %v", err)
	}
	if done, _ := resp.Data["done"].(bool); !done {
		t.Fatalf("delete response done = %v, want true", resp.Data["done"])
	}
	if name, _ := resp.Data["name"].(string); name != "projects/proj/locations/global/keyRings/doomed/operations/delete" {
		t.Errorf("operation name = %q, want the key ring's delete operation", name)
	}
	if r, ok := resp.Data["response"].(map[string]any); !ok || len(r) != 0 {
		t.Errorf("operation response = %v, want an empty google.protobuf.Empty", resp.Data["response"])
	}

	// The deleted ring is gone: a get is NOT_FOUND and the location no longer
	// lists it.
	if _, err := p.KeyRingGet(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/global/keyRings/doomed",
	})); err == nil {
		t.Fatal("KeyRingGet on a deleted ring succeeded, want NOT_FOUND")
	} else if perr := restError(t, err); perr.Code != "NotFound" || perr.HTTPStatus != 404 {
		t.Fatalf("KeyRingGet after delete = %d %s, want 404 NotFound", perr.HTTPStatus, perr.Code)
	}
	list, err := p.KeyRingList(ctx, newNR(map[string]any{"location": "global"}))
	if err != nil {
		t.Fatalf("KeyRingList: %v", err)
	}
	if rings, _ := list.Data["keyRings"].([]any); len(rings) != 0 {
		t.Fatalf("KeyRingList after delete = %v, want none", list.Data["keyRings"])
	}

	// Deleting it again is NOT_FOUND, not a silent success.
	if _, err := p.KeyRingDelete(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/global/keyRings/doomed",
	})); err == nil {
		t.Fatal("second KeyRingDelete succeeded, want NOT_FOUND")
	}
}

// TestKeyRingDeleteRequiresEmptyRing pins the documented child precondition:
// "All child resources of the KeyRing must have been previously deleted using
// their corresponding Delete operations." A ring holding a crypto key is 400
// FAILED_PRECONDITION, the same shape cryptoKeys.delete returns while versions
// remain.
func TestKeyRingDeleteRequiresEmptyRing(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRing(t, p, "busy")
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/global/keyRings/busy", "cryptoKeyId": "k",
		"body": map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate: %v", err)
	}

	_, err := p.KeyRingDelete(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/global/keyRings/busy",
	}))
	if err == nil {
		t.Fatal("KeyRingDelete with a child crypto key succeeded, want FAILED_PRECONDITION")
	}
	perr := restError(t, err)
	if perr.Code != "FailedPrecondition" || perr.HTTPStatus != 400 {
		t.Fatalf("delete with children = %d %s, want 400 FailedPrecondition", perr.HTTPStatus, perr.Code)
	}
	if !strings.Contains(perr.Message, "CryptoKeys must be deleted before the KeyRing") {
		t.Fatalf("message = %q, want the documented child precondition", perr.Message)
	}

	// The ring and its key survive the rejected delete.
	if _, err := p.KeyRingGet(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/global/keyRings/busy",
	})); err != nil {
		t.Fatalf("key ring gone after the rejected delete: %v", err)
	}
	if _, err := p.CryptoKeyGet(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/global/keyRings/busy/cryptoKeys/k",
	})); err != nil {
		t.Fatalf("crypto key gone after the rejected delete: %v", err)
	}
}

// TestKeyRingDeleteKeepsRetiredKeyRecords pins the retired-name boundary: a
// deleted CryptoKey leaves a RetiredResource behind, which is a key-level record
// and not a child of the ring, so it does not block the ring's delete — but the
// key name stays unusable after the ring (and its name) is created again.
// Discovery models retirement for CryptoKeys only, so a deleted ring's own name
// is reusable.
func TestKeyRingDeleteKeepsRetiredKeyRecords(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createRing(t, p, "cycled")
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/global/keyRings/cycled", "cryptoKeyId": "k",
		"skipInitialVersionCreation": "true",
		"body":                       map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate: %v", err)
	}
	// Deleting the key (no versions) retires its name.
	if _, err := p.CryptoKeyDelete(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/global/keyRings/cycled/cryptoKeys/k",
	})); err != nil {
		t.Fatalf("CryptoKeyDelete: %v", err)
	}

	if _, err := p.KeyRingDelete(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/global/keyRings/cycled",
	})); err != nil {
		t.Fatalf("KeyRingDelete after every key was deleted = %v, want success (a retired key record is not a child)", err)
	}

	// The ring name is reusable (no KeyRing retirement record)...
	createRing(t, p, "cycled")
	// ...but the retired crypto-key name is not.
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/global/keyRings/cycled", "cryptoKeyId": "k",
		"skipInitialVersionCreation": "true",
		"body":                       map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	})); err == nil {
		t.Fatal("re-creating the retired crypto key name succeeded, want ALREADY_EXISTS")
	}
}
