package storage

import (
	"context"
	"testing"
)

// TestBucketOutputOnlyPolicyMembersIgnored covers FC2-16 on the REST surface:
// retentionPolicy.isLocked/effectiveTime and softDeletePolicy.effectiveTime are
// server-owned, so a create/update carrying them neither stores nor echoes the
// caller's value.
func TestBucketOutputOnlyPolicyMembersIgnored(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{
		"name": "bkt-output-only",
		"retentionPolicy": map[string]any{
			"retentionPeriod": "3600",
			"isLocked":        true,
			"effectiveTime":   "2031-02-03T04:05:06Z",
		},
		"softDeletePolicy": map[string]any{
			"retentionDurationSeconds": "1209600",
			"effectiveTime":            "2031-02-03T04:05:06Z",
		},
	}
	resp, err := p.BucketsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	rp, _ := resp.Data["retentionPolicy"].(map[string]any)
	if locked, _ := rp["isLocked"].(bool); locked {
		t.Error("retentionPolicy.isLocked was accepted from the caller")
	}
	if et, _ := rp["effectiveTime"].(string); et == "2031-02-03T04:05:06Z" {
		t.Error("retentionPolicy.effectiveTime was copied from the caller")
	}
	if rp["effectiveTime"] == nil {
		t.Error("retentionPolicy.effectiveTime was not stamped by the server")
	}
	sd, _ := resp.Data["softDeletePolicy"].(map[string]any)
	if et, _ := sd["effectiveTime"].(string); et == "2031-02-03T04:05:06Z" {
		t.Error("softDeletePolicy.effectiveTime was copied from the caller")
	}

	// An update that supplies the server-owned members ignores them too.
	data := patchBucket(t, p, "bkt-output-only", map[string]any{
		"retentionPolicy": map[string]any{
			"retentionPeriod": "7200",
			"isLocked":        true,
			"effectiveTime":   "2031-02-03T04:05:06Z",
		},
		"softDeletePolicy": map[string]any{
			"retentionDurationSeconds": "604800",
			"effectiveTime":            "2031-02-03T04:05:06Z",
		},
	})
	rp, _ = data["retentionPolicy"].(map[string]any)
	if locked, _ := rp["isLocked"].(bool); locked {
		t.Error("an update accepted retentionPolicy.isLocked from the caller")
	}
	if rp["retentionPeriod"] != "7200" {
		t.Errorf("retentionPeriod = %v, want 7200", rp["retentionPeriod"])
	}
	sd, _ = data["softDeletePolicy"].(map[string]any)
	if et, _ := sd["effectiveTime"].(string); et == "2031-02-03T04:05:06Z" {
		t.Error("an update copied softDeletePolicy.effectiveTime from the caller")
	}
}
