package ui

import "testing"

// The tier vocabulary is shared by every cloud's service catalog; keep it to
// exactly these values so the AWS/Azure and GCP consoles stay in lockstep.
func TestTierVocabulary(t *testing.T) {
	values := map[string]string{
		TierFull:     "TierFull",
		TierMetadata: "TierMetadata",
		TierShape:    "TierShape",
	}
	want := map[string]bool{"full": true, "metadata": true, "shape": true}
	if len(values) != len(want) {
		t.Fatalf("tier constants collide: %v", values)
	}
	for value, name := range values {
		if value == "" {
			t.Errorf("%s is empty", name)
		}
		if !want[value] {
			t.Errorf("%s = %q, not in the canonical vocabulary", name, value)
		}
	}
}
