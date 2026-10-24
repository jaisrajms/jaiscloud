package kms

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
)

// TestExpiredImportJobOmitsWrappingKey pins DC43: the ImportJob proto documents
// the wrapping public key as "Only returned if state is ACTIVE", but the REST
// renderer emitted it for every job — the state is computed lazily on read while
// the key material stays stored. An EXPIRED job now renders its format but no
// publicKey, on both get and list; an ACTIVE job still renders both.
func TestExpiredImportJobOmitsWrappingKey(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	ctx := context.Background()
	p := newTestProvider()
	createRings(t, p, "kr")
	for id, method := range map[string]string{
		"pem-job": "RSA_OAEP_3072_SHA256",
		"kem-job": "HPKE_KEM_XWING_HKDF_SHA256_AES_256_GCM",
	} {
		if _, err := p.ImportJobCreate(ctx, newNR(map[string]any{
			"name": "locations/global/keyRings/kr", "importJobId": id,
			"body": map[string]any{"importMethod": method},
		})); err != nil {
			t.Fatalf("create import job %s: %v", id, err)
		}
	}

	get := func(id string) map[string]any {
		t.Helper()
		// The JSON codec strips the projects/{p} prefix, so this is the name a
		// real client's GET produces.
		resp, err := p.ImportJobGet(ctx, newNR(map[string]any{
			"name": "locations/global/keyRings/kr/importJobs/" + id,
		}))
		if err != nil {
			t.Fatalf("GetImportJob %s: %v", id, err)
		}
		return resp.Data
	}

	// An import-job get also accepts the fully-qualified form.
	if _, err := p.ImportJobGet(ctx, newNR(map[string]any{
		"name": "projects/proj/locations/global/keyRings/kr/importJobs/pem-job",
	})); err != nil {
		t.Fatalf("GetImportJob with a projects/{p} prefix: %v", err)
	}

	// Inside the 72h window both jobs are ACTIVE and publish their wrapping key.
	active := get("pem-job")
	if active["state"] != "ACTIVE" {
		t.Fatalf("in-window state = %v, want ACTIVE", active["state"])
	}
	if _, ok := active["publicKey"].(map[string]any); !ok {
		t.Fatalf("ACTIVE job must publish its wrapping public key: %v", active)
	}
	if active["publicKeyFormat"] != "PEM" {
		t.Fatalf("ACTIVE job publicKeyFormat = %v, want PEM", active["publicKeyFormat"])
	}
	kem := get("kem-job")
	if kem["publicKeyFormat"] != "XWING_RAW_BYTES" {
		t.Fatalf("ACTIVE HPKE job publicKeyFormat = %v, want XWING_RAW_BYTES", kem["publicKeyFormat"])
	}
	if _, ok := kem["publicKey"]; !ok {
		t.Fatalf("ACTIVE HPKE job must publish its raw wrapping key: %v", kem)
	}

	// Past expire_time the same jobs render EXPIRED with no key material. The
	// format stays: the proto conditions only public_key on ACTIVE and the
	// format echoes what the caller asked for.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(73 * time.Hour)})
	for _, tc := range []struct{ id, format string }{
		{"pem-job", "PEM"},
		{"kem-job", "XWING_RAW_BYTES"},
	} {
		ij := get(tc.id)
		if ij["state"] != "EXPIRED" {
			t.Fatalf("%s state = %v, want EXPIRED", tc.id, ij["state"])
		}
		if key, ok := ij["publicKey"]; ok {
			t.Fatalf("%s EXPIRED job leaked its wrapping public key: %v", tc.id, key)
		}
		if ij["publicKeyFormat"] != tc.format {
			t.Fatalf("%s publicKeyFormat = %v, want %s", tc.id, ij["publicKeyFormat"], tc.format)
		}
	}

	// importJobs.list renders the same shape, so a client cannot recover the key
	// from the list either.
	list := func(filter string) []any {
		t.Helper()
		params := map[string]any{"name": "locations/global/keyRings/kr/importJobs"}
		if filter != "" {
			params["filter"] = filter
		}
		resp, err := p.ImportJobList(ctx, newNR(params))
		if err != nil {
			t.Fatalf("ImportJobList %q: %v", filter, err)
		}
		items, _ := resp.Data["importJobs"].([]any)
		return items
	}
	items := list("")
	if len(items) != 2 {
		t.Fatalf("importJobs.list returned %d jobs, want 2", len(items))
	}
	for _, it := range items {
		item, _ := it.(map[string]any)
		if item["state"] != "EXPIRED" {
			t.Fatalf("listed state = %v, want EXPIRED", item["state"])
		}
		if key, ok := item["publicKey"]; ok {
			t.Fatalf("EXPIRED list item leaked its wrapping public key: %v", key)
		}
	}
	// The publicKeyFormat filter still agrees with the body: the format is
	// rendered for an EXPIRED job, so the clause matches it (DC42).
	if got := list("publicKeyFormat=PEM"); len(got) != 1 {
		t.Fatalf("publicKeyFormat=PEM matched %d jobs, want the 1 rendered with that format", len(got))
	}
}
