package functions

import (
	"context"
	"testing"

	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/store"
)

func TestParseTriggerHost(t *testing.T) {
	cases := []struct {
		name         string
		host         string
		wantProject  string
		wantLocation string
		wantOK       bool
	}{
		{"basic", "us-central1-proj.cloudfunctions.net", "proj", "us-central1", true},
		{"hyphenated project", "europe-west1-my-project.cloudfunctions.net", "my-project", "europe-west1", true},
		{"hyphenated region", "northamerica-northeast1-p.cloudfunctions.net", "p", "northamerica-northeast1", true},
		{"with port", "us-central1-proj.cloudfunctions.net:443", "proj", "us-central1", true},
		{"uppercase", "US-CENTRAL1-PROJ.CLOUDFUNCTIONS.NET", "proj", "us-central1", true},
		{"control plane", "cloudfunctions.googleapis.com", "", "", false},
		{"no region", "proj.cloudfunctions.net", "", "", false},
		{"empty project", "us-central1-.cloudfunctions.net", "", "", false},
		{"unknown region", "mars-central1-proj.cloudfunctions.net", "", "", false},
		{"other domain", "us-central1-proj.example.com", "", "", false},
		{"empty", "", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			project, location, ok := ParseTriggerHost(tc.host)
			if ok != tc.wantOK || project != tc.wantProject || location != tc.wantLocation {
				t.Errorf("ParseTriggerHost(%q) = (%q, %q, %v), want (%q, %q, %v)",
					tc.host, project, location, ok, tc.wantProject, tc.wantLocation, tc.wantOK)
			}
		})
	}
}

func TestIsTriggerHost(t *testing.T) {
	for _, host := range []string{
		"us-central1-proj.cloudfunctions.net",
		"us-central1-proj.cloudfunctions.net:443",
	} {
		if !IsTriggerHost(host) {
			t.Errorf("IsTriggerHost(%q) = false, want true", host)
		}
	}
	for _, host := range []string{
		"cloudfunctions.googleapis.com",
		"us-central1-proj.cloudfunctions.net.evil.com",
		"",
	} {
		if IsTriggerHost(host) {
			t.Errorf("IsTriggerHost(%q) = true, want false", host)
		}
	}
}

func TestTriggerHostLabel(t *testing.T) {
	cases := []struct {
		host      string
		wantLabel string
		wantOK    bool
	}{
		{"us-central1-proj.cloudfunctions.net", "us-central1-proj", true},
		{"US-CENTRAL1-PROJ.CLOUDFUNCTIONS.NET:443", "us-central1-proj", true},
		{"me-west1-my-proj.cloudfunctions.net", "me-west1-my-proj", true},
		{"cloudfunctions.googleapis.com", "", false},
		{"cloudfunctions.net", "", false},
		{"", "", false},
	}
	for _, tc := range cases {
		label, ok := TriggerHostLabel(tc.host)
		if ok != tc.wantOK || label != tc.wantLabel {
			t.Errorf("TriggerHostLabel(%q) = (%q, %v), want (%q, %v)", tc.host, label, ok, tc.wantLabel, tc.wantOK)
		}
	}
}

func TestResolveHTTPTriggerFunction(t *testing.T) {
	ctx := context.Background()
	fs := functionsstore.NewMemoryStore()
	s := NewService(fs, store.NewMemoryResourceStore())

	seed := func(project, location, id string, event bool) {
		t.Helper()
		f := functionsstore.Function{
			ID: id, Location: location, Runtime: "nodejs20", EntryPoint: "h", Status: "ACTIVE",
			HttpsTriggerURL: "https://" + location + "-" + project + ".cloudfunctions.net/" + id,
		}
		if event {
			f.HttpsTriggerURL = ""
			f.EventTrigger = &functionsstore.EventTrigger{EventType: "t", Resource: "r"}
		}
		if err := fs.CreateFunction(ctx, project, location, id, f); err != nil {
			t.Fatalf("seed %s/%s/%s: %v", project, location, id, err)
		}
	}
	seed("my-proj", "me-west1", "regional", false)
	seed("proj", "europe-west1", "eventfn", true)

	// An unlisted region resolves from the ambiguous label.
	project, f, err := s.ResolveHTTPTriggerFunction(ctx, "me-west1-my-proj", "regional")
	if err != nil || project != "my-proj" || f.ID != "regional" {
		t.Errorf("resolve unlisted = (%q, %+v, %v), want my-proj/regional", project, f, err)
	}

	// An event-only match resolves (the caller renders NotFound).
	project, f, err = s.ResolveHTTPTriggerFunction(ctx, "europe-west1-proj", "eventfn")
	if err != nil || project != "proj" || f.HttpsTriggerURL != "" {
		t.Errorf("resolve event-only = (%q, %+v, %v), want proj/event-only", project, f, err)
	}

	// No matching function is NotFound.
	if _, _, err := s.ResolveHTTPTriggerFunction(ctx, "me-west1-my-proj", "nope"); err == nil {
		t.Errorf("expected NotFound for an unknown function")
	}
	if _, _, err := s.ResolveHTTPTriggerFunction(ctx, "", "regional"); err == nil {
		t.Errorf("expected NotFound for an empty label")
	}
}
