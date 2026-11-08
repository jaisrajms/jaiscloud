package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"resources.google_kms_crypto_key.*.destroy_scheduled_duration", "resources.google_kms_crypto_key.primary.destroy_scheduled_duration", true},
		{"*.*.access[0].user_by_email", "resources.google_bigquery_dataset.cmek.access[0].user_by_email", true},
		{"resources.*.self_link", "plan.x.self_link", false},
		{"exact", "exact", true},
		{"exact", "exact2", false},
		{"a*b*c", "aXXbYYc", true},
	}
	for _, c := range cases {
		if got := globMatch(c.pattern, c.s); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}

func TestNormalizerFoldsRunSpecificValues(t *testing.T) {
	n := NewNormalizer("my-project", "123456789", "abc123", "http://localhost:8080")

	in := map[string]any{
		"id":                 "projects/my-project/locations/us/keyRings/ring-abc123",
		"project":            "123456789",
		"etag":               "BwXyz=",
		"time_created":       "2026-11-08T03:06:06.183Z",
		"number":             json.Number("987654321"),
		"self_link":          "http://localhost:8080/storage/v1/b/bucket-abc123",
		"updated":            "2026-11-08T03:09:30.760932773Z",
		"last_modified_time": json.Number("1794107300245"),
		"media_link":         "https://storage.googleapis.com/download/storage/v1/b/bucket-abc123/o/hello.txt?generation=123456&alt=media",
		"user_by_email":      "someone@gmail.com",
		"secret_data":        "hunter2",
		"plain":              "value",
	}
	sensitive := map[string]any{"secret_data": true}
	out, keep := n.Value("", in, sensitive)
	if !keep {
		t.Fatal("document dropped")
	}
	m := out.(map[string]any)

	checks := map[string]string{
		"id":                 "projects/<project>/locations/us/keyRings/ring-<suffix>",
		"project":            "<project>",
		"etag":               "<etag>",
		"time_created":       "<timestamp>",
		"number":             "<number>",
		"self_link":          "<origin>/storage/v1/b/bucket-<suffix>",
		"updated":            "<timestamp>",
		"last_modified_time": "<timestamp>",
		"media_link":         "<origin>/download/storage/v1/b/bucket-<suffix>/o/hello.txt?generation=<generation>&alt=media",
		"user_by_email":      "<user>",
		"plain":              "value",
	}
	for key, want := range checks {
		got, ok := m[key]
		if !ok {
			t.Errorf("%s missing from normalized document", key)
			continue
		}
		if got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}
	if _, ok := m["secret_data"]; ok {
		t.Error("sensitive value was not dropped")
	}
	if _, ok := m["timeouts"]; ok {
		t.Error("timeouts was not dropped")
	}
}

func TestParsePlanRecordsUnknownPathsOnly(t *testing.T) {
	raw := []byte(`{
	  "resource_changes": [
	    {"address": "google_storage_bucket.b", "change": {
	      "actions": ["create"],
	      "after": {"name": "secret-name"},
	      "after_unknown": {"id": true, "lifecycle_rule": [{"condition": [{"with_state": true}]}]}
	    }}
	  ]
	}`)
	got, err := ParsePlan(raw)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := got["google_storage_bucket.b"].(map[string]any)
	if !ok {
		t.Fatalf("missing plan entry: %#v", got)
	}
	unknown, _ := entry["unknown"].([]string)
	want := []string{"id", "lifecycle_rule[0].condition[0].with_state"}
	if strings.Join(unknown, ",") != strings.Join(want, ",") {
		t.Errorf("unknown = %v, want %v", unknown, want)
	}
	if _, ok := entry["after"]; ok {
		t.Error("plan values must not be recorded (only the diff set)")
	}
}

func TestParsePlanEmptyIsNotEmptyMapEntry(t *testing.T) {
	got, err := ParsePlan([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("empty plan produced %d entries", len(got))
	}
}

func TestParseStateFlattensModulesAndDropsSensitive(t *testing.T) {
	raw := []byte(`{
	  "values": {"root_module": {
	    "resources": [
	      {"address": "google_secret_manager_secret_version.v", "values": {"id": "projects/p/secrets/s/versions/1", "secret_data": "x"}, "sensitive_values": {"secret_data": true}}
	    ],
	    "child_modules": [
	      {"resources": [{"address": "module.m.google_pubsub_topic.t", "values": {"name": "topic"}}]}
	    ]
	  }}
	}`)
	n := NewNormalizer("p", "", "", "")
	got, err := ParseState(raw, n)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d resources, want 2: %#v", len(got), got)
	}
	sv := got["google_secret_manager_secret_version.v"].(map[string]any)
	if _, ok := sv["secret_data"]; ok {
		t.Error("sensitive state attribute was not dropped")
	}
	if _, ok := got["module.m.google_pubsub_topic.t"]; !ok {
		t.Error("child module resource was not flattened")
	}
}

func TestDiffDoc(t *testing.T) {
	base := func() Doc {
		return Doc{
			Resources: map[string]any{"bucket.b": map[string]any{"name": "b", "labels": map[string]any{"env": "x"}}},
			Plan:      map[string]any{"bucket.b": map[string]any{"actions": []string{"create"}}},
			Replan:    map[string]any{},
		}
	}
	if diffs := DiffDoc(base(), base()); len(diffs) != 0 {
		t.Fatalf("identical docs produced %d diffs: %#v", len(diffs), diffs)
	}
	emu := base()
	emu.Resources["bucket.b"].(map[string]any)["labels"].(map[string]any)["env"] = "y"
	diffs := DiffDoc(base(), emu)
	if len(diffs) != 1 || diffs[0].Path != "resources.bucket.b.labels.env" {
		t.Fatalf("unexpected diffs: %#v", diffs)
	}
	if diffs[0].Real != "x" || diffs[0].Emulator != "y" {
		t.Errorf("diff values = %#v", diffs[0])
	}
}

func TestDiffDocMissingAndLength(t *testing.T) {
	real := Doc{Resources: map[string]any{"b": map[string]any{"versioning": []any{}}}}
	emu := Doc{Resources: map[string]any{"b": map[string]any{"versioning": []any{map[string]any{"enabled": false}}}}}
	diffs := DiffDoc(real, emu)
	if len(diffs) != 1 || diffs[0].Path != "resources.b.versioning.length" {
		t.Fatalf("unexpected diffs: %#v", diffs)
	}

	real2 := Doc{Resources: map[string]any{"b": map[string]any{"self_link": "x"}}}
	emu2 := Doc{Resources: map[string]any{"b": map[string]any{}}}
	diffs = DiffDoc(real2, emu2)
	if len(diffs) != 1 || diffs[0].Path != "resources.b.self_link" {
		t.Fatalf("unexpected diffs: %#v", diffs)
	}
	if _, ok := diffs[0].Emulator.(absent); !ok {
		t.Errorf("missing side is not marked absent: %#v", diffs[0].Emulator)
	}
}

func TestClassifications(t *testing.T) {
	path := writeTemp(t, "classifications.yaml", `
rules:
  - case: "*"
    path: resources.b.*.id
    verdict: emulator-gap
    id: TF-X
    branch: fix/x
  - case: other
    path: resources.b.self_link
    verdict: accepted-risk
    reason: known
`)
	cls, err := LoadClassifications(path)
	if err != nil {
		t.Fatal(err)
	}
	diffs := []Diff{
		{Path: "resources.b.one.id"},
		{Path: "resources.b.self_link"}, // case other matches
		{Path: "resources.b.unknown"},
	}
	got, unclassified, err := cls.Classify("other", diffs)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("classified %d diffs, want 2: %#v", len(got), got)
	}
	if got[0].Verdict != verdictEmulatorGap || got[0].ID != "TF-X" {
		t.Errorf("first verdict = %#v", got[0])
	}
	if len(unclassified) != 1 || unclassified[0].Path != "resources.b.unknown" {
		t.Errorf("unclassified = %#v", unclassified)
	}
}

func TestClassificationsRejectsInvalidRows(t *testing.T) {
	for name, body := range map[string]string{
		"unknown verdict": "rules:\n  - {case: a, path: b, verdict: maybe, reason: r}\n",
		"gap without row": "rules:\n  - {case: a, path: b, verdict: emulator-gap, reason: r}\n",
		"no reason":       "rules:\n  - {case: a, path: b, verdict: accepted-risk}\n",
		"duplicate":       "rules:\n  - {case: a, path: b, verdict: accepted-risk, reason: r}\n  - {case: a, path: b, verdict: accepted-risk, reason: r}\n",
	} {
		if _, err := LoadClassifications(writeTemp(t, "c.yaml", body)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

// TestCommittedDifferential runs the offline hygiene check against the real
// corpus, goldens and manifest. It is what `make check-gcp-terraform-differential`
// and the preflight exercise: no terraform, no network.
func TestCommittedDifferential(t *testing.T) {
	root := filepath.Join("..", "..")
	corpusPath := filepath.Join(root, "tests", "integration", "gcp", "terraform", "differential", "corpus.yaml")
	corpus, err := LoadCorpus(corpusPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(corpus.Cases) < 2 {
		t.Fatalf("corpus has %d cases", len(corpus.Cases))
	}
	var o options
	o.goldenDir = filepath.Join(root, "tests", "integration", "gcp", "terraform", "differential", "golden")
	o.classPath = filepath.Join(root, "tests", "integration", "gcp", "terraform", "differential", "classifications.yaml")
	if err := check(o, corpus); err != nil {
		t.Fatal(err)
	}
	// Every classification row that schedules a fix must name a ledger row and
	// a branch so the parity loop can close it.
	cls, err := LoadClassifications(o.classPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range cls.Rules {
		if r.Verdict == verdictEmulatorGap && (r.ID == "" || r.Branch == "") {
			t.Errorf("rule %s %s schedules a fix without id/branch", r.Case, r.Path)
		}
	}
}

func TestReportRender(t *testing.T) {
	r := Report{
		Generated: mustTime(t),
		Corpus:    "corpus.yaml",
		GoldenDir: "golden",
		Endpoint:  "http://localhost:8080",
		Project:   "test-project",
		Cases: []caseResult{
			{Name: "foundation", Status: "compared", Diffs: 1},
			{Name: "bootstrap", Status: "not-recorded", Excluded: true, Reason: "creates a project"},
		},
		Differences: []ReportDiff{{
			CaseName:   "foundation",
			Classified: Classified{Diff: Diff{Path: "resources.b.x", Real: "a", Emulator: absent{}}, Verdict: verdictEmulatorGap, ID: "TF-1", Reason: "why"},
		}},
	}
	out := r.Render()
	for _, want := range []string{"| foundation | compared | 1 | 0 |", "resources.b.x", "<absent>", "TF-1", "not-recorded"} {
		if !strings.Contains(out, want) {
			t.Errorf("report is missing %q:\n%s", want, out)
		}
	}
}

func writeTemp(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func mustTime(t *testing.T) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, "2026-11-08T03:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	return ts
}
