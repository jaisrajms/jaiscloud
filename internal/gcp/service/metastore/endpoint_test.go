package metastore

import (
	"context"
	"testing"
)

// TestValidateMetastoreService covers the cluster-attachment validation: the
// canonical name and every accepted short form resolve to the service, while a
// malformed name fails InvalidArgument and an unknown service fails NotFound.
func TestValidateMetastoreService(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateService(ctx, "proj", "us-central1", "hms", nil); err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	ok := []struct {
		name string
		ref  string
	}{
		{"canonical", "projects/proj/locations/us-central1/services/hms"},
		{"location short form", "locations/us-central1/services/hms"},
		{"services short form", "services/hms"},
		{"bare id", "hms"},
		{"whitespace", "  services/hms  "},
	}
	for _, tc := range ok {
		if err := s.ValidateMetastoreService(ctx, tc.ref, "proj", "us-central1"); err != nil {
			t.Errorf("%s: ValidateMetastoreService(%q) = %v, want nil", tc.name, tc.ref, err)
		}
	}

	if err := s.ValidateMetastoreService(ctx, "services/missing", "proj", "us-central1"); !isCode(err, "NotFound") {
		t.Errorf("unknown service = %v, want NotFound", err)
	}

	// A service in a different region than the cluster is not attachable.
	if err := s.ValidateMetastoreService(ctx, "projects/proj/locations/europe-west1/services/hms", "proj", "us-central1"); !isCode(err, "InvalidArgument") {
		t.Errorf("cross-region ref = %v, want InvalidArgument", err)
	}

	bad := []string{
		"",
		"projects/proj",
		"projects/proj/locations/us-central1/services/hms/backups/b",
		"projects/proj/locations/us-central1/services/hms/metadataImports/m",
		"projects/proj/locations/us-central1/operations/op",
		"projects/proj/locations/us-central1/services",
	}
	for _, ref := range bad {
		if err := s.ValidateMetastoreService(ctx, ref, "proj", "us-central1"); !isCode(err, "InvalidArgument") {
			t.Errorf("ValidateMetastoreService(%q) = %v, want InvalidArgument", ref, err)
		}
	}

	// A short form with no default project/location cannot be resolved.
	if err := s.ValidateMetastoreService(ctx, "hms", "", ""); !isCode(err, "InvalidArgument") {
		t.Errorf("bare id without defaults = %v, want InvalidArgument", err)
	}
}

// TestMetastoreEndpoint covers the pure endpoint derivation, including the
// defaulted short forms and the synthesized URI shape.
func TestMetastoreEndpoint(t *testing.T) {
	s := newCore()
	want := "thrift://hms.us-central1.metastore.jaiscloud.local:9083"
	for _, ref := range []string{
		"projects/proj/locations/us-central1/services/hms",
		"locations/us-central1/services/hms",
		"services/hms",
		"hms",
	} {
		got, err := s.MetastoreEndpoint(ref, "proj", "us-central1")
		if err != nil {
			t.Errorf("MetastoreEndpoint(%q) error: %v", ref, err)
			continue
		}
		if got != want {
			t.Errorf("MetastoreEndpoint(%q) = %q, want %q", ref, got, want)
		}
	}

	if _, err := s.MetastoreEndpoint("projects/proj/locations/us-central1/services/hms/backups/b", "proj", "us-central1"); !isCode(err, "InvalidArgument") {
		t.Errorf("backup ref = %v, want InvalidArgument", err)
	}
}

// TestParseServiceRefDefaults verifies missing project/location segments are
// filled from the cluster defaults.
func TestParseServiceRefDefaults(t *testing.T) {
	project, location, service, err := parseServiceRef("services/hms", "cluster-proj", "cluster-region")
	if err != nil {
		t.Fatalf("parseServiceRef: %v", err)
	}
	if project != "cluster-proj" || location != "cluster-region" || service != "hms" {
		t.Fatalf("defaults not applied: %q %q %q", project, location, service)
	}

	// An explicit project/location in the reference wins over the defaults.
	project, location, service, err = parseServiceRef("projects/other/locations/europe-west1/services/hms2", "cluster-proj", "cluster-region")
	if err != nil {
		t.Fatalf("parseServiceRef: %v", err)
	}
	if project != "other" || location != "europe-west1" || service != "hms2" {
		t.Fatalf("explicit segments not honored: %q %q %q", project, location, service)
	}
}
