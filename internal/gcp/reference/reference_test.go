package reference

import "testing"

func TestKMSLocationMatches(t *testing.T) {
	cases := []struct {
		resource, key string
		want          bool
	}{
		{"US", "us", true},
		{"EU", "europe", true},
		{"ASIA", "asia", true},
		{"us-central1", "US-CENTRAL1", true},
		{"us-central1", "europe-west1", false},
		{"US", "us-central1", false},
		{"", "europe-west1", true},
	}
	for _, c := range cases {
		if got := KMSLocationMatches(c.resource, c.key); got != c.want {
			t.Errorf("KMSLocationMatches(%q, %q) = %v, want %v", c.resource, c.key, got, c.want)
		}
	}
}
