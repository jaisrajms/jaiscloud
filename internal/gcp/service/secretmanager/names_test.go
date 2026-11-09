package secretmanager

import "testing"

func TestParseSecretName(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		version string
	}{
		// Project-level (global) secrets, relative and full names.
		{"secrets/s", "s", ""},
		{"secrets/s/versions/1", "s", "1"},
		{"secrets/s/versions/latest", "s", "latest"},
		{"secrets/s/versions", "s", ""},
		{"projects/p/secrets/s", "s", ""},
		{"projects/p/secrets/s/versions/7", "s", "7"},
		// A secret literally named "versions" is not a versions collection.
		{"secrets/versions", "versions", ""},
		{"projects/p/secrets/versions", "versions", ""},
		// Regional secrets, relative and full names.
		{"locations/us-central1/secrets/s", "locations/us-central1/secrets/s", ""},
		{"locations/us-central1/secrets/s/versions/2", "locations/us-central1/secrets/s", "2"},
		{"locations/us-central1/secrets/s/versions", "locations/us-central1/secrets/s", ""},
		{"projects/p/locations/us-central1/secrets/s", "locations/us-central1/secrets/s", ""},
		{"projects/p/locations/us-central1/secrets/s/versions/2", "locations/us-central1/secrets/s", "2"},
		{"projects/p/locations/us-central1/secrets/versions", "locations/us-central1/secrets/versions", ""},
	}
	for _, tc := range cases {
		key, version := ParseSecretName(tc.name)
		if key != tc.key || version != tc.version {
			t.Errorf("ParseSecretName(%q) = (%q, %q), want (%q, %q)", tc.name, key, version, tc.key, tc.version)
		}
		if got := SecretKey(tc.name); got != tc.key {
			t.Errorf("SecretKey(%q) = %q, want %q", tc.name, got, tc.key)
		}
	}
}

func TestSecretName(t *testing.T) {
	cases := map[string]string{
		"s":                               "projects/p/secrets/s",
		"locations/us-central1/secrets/s": "projects/p/locations/us-central1/secrets/s",
	}
	for key, want := range cases {
		if got := SecretName("p", key); got != want {
			t.Errorf("SecretName(p, %q) = %q, want %q", key, got, want)
		}
	}
}

func TestSecretID(t *testing.T) {
	cases := map[string]string{
		"s":                               "s",
		"secrets/s":                       "s",
		"locations/us-central1/secrets/s": "s",
	}
	for key, want := range cases {
		if got := SecretID(key); got != want {
			t.Errorf("SecretID(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestParentLocation(t *testing.T) {
	cases := map[string]string{
		"projects/p":                       "",
		"p":                                "",
		"secrets":                          "",
		"locations/us-central1/secrets":    "us-central1",
		"locations/us-central1":            "us-central1",
		"projects/p/locations/us-central1": "us-central1",
		"projects/p/locations/us-central1/secrets":              "us-central1",
		"projects/p/locations/us-central1/secrets/s":            "us-central1",
		"projects/p/locations/us-central1/secrets/s/versions/1": "us-central1",
	}
	for parent, want := range cases {
		if got := ParentLocation(parent); got != want {
			t.Errorf("ParentLocation(%q) = %q, want %q", parent, got, want)
		}
	}
}

func TestParentKey(t *testing.T) {
	cases := []struct{ parent, id, want string }{
		{"projects/p", "s", "s"},
		{"secrets", "s", "s"},
		{"projects/p/locations/us-central1", "s", "locations/us-central1/secrets/s"},
		{"locations/us-central1/secrets", "s", "locations/us-central1/secrets/s"},
	}
	for _, tc := range cases {
		if got := ParentKey(tc.parent, tc.id); got != tc.want {
			t.Errorf("ParentKey(%q, %q) = %q, want %q", tc.parent, tc.id, got, tc.want)
		}
	}
}

func TestIsRegionalKey(t *testing.T) {
	if IsRegionalKey("s") || IsRegionalKey("secrets/s") {
		t.Error("a project-level key must not report regional")
	}
	if !IsRegionalKey("locations/us-central1/secrets/s") {
		t.Error("a location-qualified key must report regional")
	}
}
