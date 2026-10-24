package storage

import "testing"

// TestBucketEtagMatchesRealGCS pins the etag encoding to values taken from the
// live JSON API: the bucket etag is base64(proto{1: metageneration}).
func TestBucketEtagMatchesRealGCS(t *testing.T) {
	cases := map[string]string{
		"1":   "CAE=", // also the pre-existing constant
		"2":   "CAI=", //
		"11":  "CAs=", //
		"17":  "CBE=", // gcp-public-data-landsat
		"51":  "CDM=", // cloud-samples-data
		"":    "CAE=", // defensive: an unset metageneration behaves like 1
		"abc": "CAE=", // defensive: unparsable input
	}
	for meta, want := range cases {
		if got := BucketEtag(meta); got != want {
			t.Errorf("BucketEtag(%q) = %q, want %q", meta, got, want)
		}
	}
}

// TestObjectEtagMatchesRealGCS pins the object etag encoding: base64(proto{1:
// generation, 2: metageneration}), verified against gcp-public-data-landsat.
func TestObjectEtagMatchesRealGCS(t *testing.T) {
	cases := []struct{ gen, meta, want string }{
		{"1502391044760665", "1", "CNnIpeSqzdUCEAE="},
		{"1502391058568908", "1", "CMyt8OqqzdUCEAE="},
		{"1502391069301522", "1", "CJK2/++qzdUCEAE="},
		// A metadata update must change the etag: same generation, meta 2.
		{"1502391044760665", "2", "CNnIpeSqzdUCEAI="},
		{"", "1", "CAE="},  // no generation → identity-less fallback
		{"0", "1", "CAE="}, //
	}
	for _, c := range cases {
		if got := ObjectEtag(c.gen, c.meta); got != c.want {
			t.Errorf("ObjectEtag(%q, %q) = %q, want %q", c.gen, c.meta, got, c.want)
		}
	}
}

// TestObjectEtagChangesWithMetageneration guards the regression the fidelity
// work fixed: a constant etag hid metadata changes from conditional clients.
func TestObjectEtagChangesWithMetageneration(t *testing.T) {
	gen := "1792384325374308157"
	if ObjectEtag(gen, "1") == ObjectEtag(gen, "2") {
		t.Fatal("etag must change when the metageneration changes")
	}
}
