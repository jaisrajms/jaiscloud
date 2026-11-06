package storage

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

// iamConfigOf reads the iamConfiguration object from a bucket response body.
func iamConfigOf(t *testing.T, data map[string]any) map[string]any {
	t.Helper()
	ic, ok := data["iamConfiguration"].(map[string]any)
	if !ok {
		t.Fatalf("response has no iamConfiguration object: %#v", data["iamConfiguration"])
	}
	return ic
}

// bucketIamFlags pulls the uniform bucket-level access flag (plus the
// deprecated bucketPolicyOnly mirror) and the public access prevention mode out
// of a bucket response.
func bucketIamFlags(t *testing.T, data map[string]any) (uniform, policyOnly bool, pap string) {
	t.Helper()
	ic := iamConfigOf(t, data)
	uba, _ := ic["uniformBucketLevelAccess"].(map[string]any)
	uniform, _ = uba["enabled"].(bool)
	bpo, _ := ic["bucketPolicyOnly"].(map[string]any)
	policyOnly, _ = bpo["enabled"].(bool)
	pap, _ = ic["publicAccessPrevention"].(string)
	return uniform, policyOnly, pap
}

func insertBucket(t *testing.T, p *Provider, body map[string]any) map[string]any {
	t.Helper()
	nr := bucketParams()
	nr.Params["body"] = body
	resp, err := p.BucketsInsert(context.Background(), nr)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	return resp.Data
}

func getBucket(t *testing.T, p *Provider, name string) map[string]any {
	t.Helper()
	nr := bucketParams()
	nr.Params["bucket"] = name
	resp, err := p.BucketsGet(context.Background(), nr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	return resp.Data
}

func patchBucket(t *testing.T, p *Provider, name string, body map[string]any) map[string]any {
	t.Helper()
	nr := bucketParams()
	nr.Params["bucket"] = name
	nr.Params["body"] = body
	resp, err := p.BucketsUpdate(context.Background(), nr)
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	return resp.Data
}

// TestBucketInsertRoundTripsIamConfiguration is the DGD10 fix: a bucket created
// with iamConfiguration.uniformBucketLevelAccess.enabled=true reads back enabled,
// with the deprecated bucketPolicyOnly alias mirroring it and the public access
// prevention mode preserved. Before this the read always reported false, which
// is what made google_storage_bucket's uniform_bucket_level_access permadiff.
func TestBucketInsertRoundTripsIamConfiguration(t *testing.T) {
	p := newTestProvider()
	insertBucket(t, p, map[string]any{
		"name": "uba-bucket",
		"iamConfiguration": map[string]any{
			"uniformBucketLevelAccess": map[string]any{"enabled": true},
			"publicAccessPrevention":   "enforced",
		},
	})

	for _, data := range []map[string]any{getBucket(t, p, "uba-bucket")} {
		uniform, policyOnly, pap := bucketIamFlags(t, data)
		if !uniform {
			t.Error("uniformBucketLevelAccess.enabled = false after create, want true")
		}
		if !policyOnly {
			t.Error("bucketPolicyOnly.enabled must mirror uniformBucketLevelAccess (true)")
		}
		if pap != "enforced" {
			t.Errorf("publicAccessPrevention = %q, want \"enforced\"", pap)
		}
	}

	// The list projection reports the same values.
	list, err := p.BucketsList(context.Background(), bucketParams())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := list.Data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("want 1 bucket in list, got %d", len(items))
	}
	if uniform, _, pap := bucketIamFlags(t, items[0].(map[string]any)); !uniform || pap != "enforced" {
		t.Errorf("list item iamConfiguration = %v, want uniform/enforced", items[0])
	}
}

// TestBucketIamConfigurationDefaults pins GCS's defaults for a bucket that never
// set the field: ACL-based access and public access prevention inherited, with
// the deprecated alias false. Existing tests, transcripts and differential
// goldens already assert this shape; it must not move.
func TestBucketIamConfigurationDefaults(t *testing.T) {
	p := newTestProvider()
	data := insertBucket(t, p, map[string]any{"name": "plain-bucket"})
	uniform, policyOnly, pap := bucketIamFlags(t, data)
	if uniform || policyOnly || pap != "inherited" {
		t.Fatalf("default iamConfiguration = uniform %v / policyOnly %v / pap %q, want false/false/inherited",
			uniform, policyOnly, pap)
	}
}

// TestBucketIamConfigurationBucketPolicyOnlyAlias verifies that the deprecated
// alias is accepted on create and drives the real flag, as real GCS accepts it
// for clients written before uniformBucketLevelAccess existed.
func TestBucketIamConfigurationBucketPolicyOnlyAlias(t *testing.T) {
	p := newTestProvider()
	data := insertBucket(t, p, map[string]any{
		"name": "alias-bucket",
		"iamConfiguration": map[string]any{
			"bucketPolicyOnly": map[string]any{"enabled": true},
		},
	})
	uniform, policyOnly, pap := bucketIamFlags(t, data)
	if !uniform || !policyOnly {
		t.Errorf("bucketPolicyOnly create = uniform %v / policyOnly %v, want both true", uniform, policyOnly)
	}
	if pap != "inherited" {
		t.Errorf("publicAccessPrevention = %q, want inherited", pap)
	}

	// The alias is derived: a PATCH carrying both the canonical flag and the
	// alias writes only the canonical member, and the alias keeps mirroring it
	// (FC3-6). A client must never see the two disagree.
	data = patchBucket(t, p, "alias-bucket", map[string]any{
		"iamConfiguration": map[string]any{
			"uniformBucketLevelAccess": map[string]any{"enabled": true},
			"bucketPolicyOnly":         map[string]any{"enabled": false},
		},
	})
	if uniform, policyOnly, _ := bucketIamFlags(t, data); !uniform || !policyOnly {
		t.Errorf("after a conflicting patch: uniform %v / policyOnly %v, want true/true (the alias is derived)", uniform, policyOnly)
	}
	data = patchBucket(t, p, "alias-bucket", map[string]any{
		"iamConfiguration": map[string]any{
			"uniformBucketLevelAccess": map[string]any{"enabled": false},
		},
	})
	if uniform, policyOnly, _ := bucketIamFlags(t, data); uniform || policyOnly {
		t.Errorf("after disabling the canonical flag: uniform %v / policyOnly %v, want false/false", uniform, policyOnly)
	}
}

// TestBucketsUpdateIamConfiguration covers the write path the hashicorp/google
// provider actually uses on update: a PATCH whose body carries only
// iamConfiguration. Each member is independent — toggling uniform bucket-level
// access must not reset public access prevention and vice versa.
func TestBucketsUpdateIamConfiguration(t *testing.T) {
	p := newTestProvider()
	insertBucket(t, p, map[string]any{"name": "patch-bucket"})

	data := patchBucket(t, p, "patch-bucket", map[string]any{
		"iamConfiguration": map[string]any{
			"uniformBucketLevelAccess": map[string]any{"enabled": true},
		},
	})
	if uniform, policyOnly, pap := bucketIamFlags(t, data); !uniform || !policyOnly || pap != "inherited" {
		t.Fatalf("after enabling: uniform %v / policyOnly %v / pap %q, want true/true/inherited",
			uniform, policyOnly, pap)
	}

	data = patchBucket(t, p, "patch-bucket", map[string]any{
		"iamConfiguration": map[string]any{"publicAccessPrevention": "enforced"},
	})
	if uniform, _, pap := bucketIamFlags(t, data); !uniform || pap != "enforced" {
		t.Fatalf("after enforcing PAP: uniform %v / pap %q, want true/enforced", uniform, pap)
	}

	data = patchBucket(t, p, "patch-bucket", map[string]any{
		"iamConfiguration": map[string]any{
			"uniformBucketLevelAccess": map[string]any{"enabled": false},
		},
	})
	if uniform, policyOnly, pap := bucketIamFlags(t, data); uniform || policyOnly || pap != "enforced" {
		t.Fatalf("after disabling: uniform %v / policyOnly %v / pap %q, want false/false/enforced",
			uniform, policyOnly, pap)
	}

	// A PATCH that touches neither member leaves both alone.
	data = patchBucket(t, p, "patch-bucket", map[string]any{"labels": map[string]any{"env": "test"}})
	if uniform, _, pap := bucketIamFlags(t, data); uniform || pap != "enforced" {
		t.Fatalf("labels patch changed iamConfiguration: uniform %v / pap %q", uniform, pap)
	}
}

// TestBucketIamConfigurationRejectsInvalidPublicAccessPrevention: real GCS
// rejects an unsupported mode rather than silently keeping the old one, so a
// typo in a client must not look like a successful write.
func TestBucketIamConfigurationRejectsInvalidPublicAccessPrevention(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{
		"name":             "bad-pap",
		"iamConfiguration": map[string]any{"publicAccessPrevention": "sometimes"},
	}
	_, err := p.BucketsInsert(ctx, nr)
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 {
		t.Fatalf("insert with invalid publicAccessPrevention err = %v, want 400 ProviderError", err)
	}

	insertBucket(t, p, map[string]any{"name": "bad-pap"})
	nr = bucketParams()
	nr.Params["bucket"] = "bad-pap"
	nr.Params["body"] = map[string]any{
		"iamConfiguration": map[string]any{"publicAccessPrevention": "sometimes"},
	}
	if _, err := p.BucketsUpdate(ctx, nr); err == nil {
		t.Fatal("patch with invalid publicAccessPrevention: want 400, got nil")
	}

	// The rejected patch left the stored value untouched.
	if _, _, pap := bucketIamFlags(t, getBucket(t, p, "bad-pap")); pap != "inherited" {
		t.Errorf("publicAccessPrevention = %q after rejected patch, want inherited", pap)
	}
}
