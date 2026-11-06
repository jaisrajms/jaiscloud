package gcplifecycle

import (
	"fmt"
	"sort"
	"testing"
)

// TestLifecyclePilots is the FC5 table: one row per parent resource, each row
// carrying its ownership manifest. The runner drives both transports and reads
// state back through both, asserting against the stores rather than the
// responses.
func TestLifecyclePilots(t *testing.T) {
	cases := []struct {
		name string
		m    manifest
		new  func(t *testing.T) adapter
	}{
		{"firestore-database", firestoreManifest, newFirestoreAdapter},
		{"storage-bucket", storageManifest, newStorageAdapter},
		{"pubsub-topic", pubsubTopicManifest, newPubsubTopicAdapter},
		{"pubsub-subscription", pubsubSubscriptionManifest, newPubsubSubscriptionAdapter},
		{"kms-key-ring", kmsManifest, newKMSAdapter},
		{"dataproc-cluster", dataprocManifest, newDataprocAdapter},
	}
	seen := map[string]bool{}
	for _, c := range cases {
		if seen[c.name] {
			t.Fatalf("duplicate pilot %q", c.name)
		}
		seen[c.name] = true
		t.Run(c.name, func(t *testing.T) {
			runCase(t, c.m, c.new)
		})
	}
}

// TestManifestDeclaresEveryPolicy pins the manifest's contract: the pilots must
// together exercise every ownership policy the harness understands, so a future
// edit cannot silently drop one (which would make the "all policies covered"
// claim false while the suite stayed green).
func TestManifestDeclaresEveryPolicy(t *testing.T) {
	have := map[Policy]bool{}
	for _, m := range []manifest{firestoreManifest, storageManifest, pubsubTopicManifest, pubsubSubscriptionManifest, kmsManifest, dataprocManifest} {
		have[m.policy] = true
		for _, c := range m.children {
			have[c.policy] = true
		}
	}
	want := []Policy{Cascade, Retain, Detach, RejectIfNonEmpty, NoDelete}
	for _, p := range want {
		if !have[p] {
			t.Errorf("no pilot declares policy %q", p)
		}
	}
	var got []string
	for p := range have {
		got = append(got, string(p))
	}
	sort.Strings(got)
	t.Logf("ownership policies exercised: %s", fmt.Sprint(got))
}
