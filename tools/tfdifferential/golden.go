package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Provenance is the metadata that makes a recorded golden trustworthy: which
// project, when, and with which tool/provider versions. It never contains a
// token or a resource name (those are folded in the document).
type Provenance struct {
	Project          string `json:"project"`
	Region           string `json:"region"`
	RecordedAt       string `json:"recorded_at"`
	TerraformVersion string `json:"terraform_version"`
	ProviderVersion  string `json:"provider_version"`
	Platform         string `json:"platform"`
	CaseDir          string `json:"case_dir"`
}

// Doc is the normalized, comparable part of a Terraform run.
type Doc struct {
	// Resources is the post-apply state, keyed by resource address.
	Resources map[string]any `json:"resources"`
	// Plan is the first plan's diff set: address -> {actions, unknown}.
	Plan map[string]any `json:"plan"`
	// Replan is the plan after apply (empty when Terraform reports no changes).
	Replan map[string]any `json:"replan"`
}

// Golden is one committed real-GCP recording.
type Golden struct {
	Case       string     `json:"case"`
	Provenance Provenance `json:"provenance"`
	Doc
}

// Manifest maps a golden filename to its sha256 so a hand edit fails loudly.
type Manifest struct {
	Files map[string]string `json:"files"`
}

// GoldenPath is the committed path for a case.
func GoldenPath(dir, name string) string { return filepath.Join(dir, name+".json") }

// WriteGolden writes a stable (sorted, indented) golden file.
func WriteGolden(path string, g Golden) error {
	raw, err := marshalStable(g)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

// ReadGolden loads a golden and verifies its document is well formed.
func ReadGolden(path string) (Golden, error) {
	var g Golden
	raw, err := os.ReadFile(path)
	if err != nil {
		return g, err
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&g); err != nil {
		return g, fmt.Errorf("parse %s: %w", path, err)
	}
	if g.Case == "" {
		return g, fmt.Errorf("%s: no case", path)
	}
	if g.Resources == nil {
		g.Resources = map[string]any{}
	}
	if g.Plan == nil {
		g.Plan = map[string]any{}
	}
	if g.Replan == nil {
		g.Replan = map[string]any{}
	}
	return g, nil
}

// WriteManifest hashes every *.json golden in dir (except the manifest itself)
// and writes manifest.json.
func WriteManifest(dir string) (Manifest, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return Manifest{}, err
	}
	m := Manifest{Files: map[string]string{}}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || e.Name() == "manifest.json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return Manifest{}, err
		}
		sum := sha256.Sum256(raw)
		m.Files[e.Name()] = hex.EncodeToString(sum[:])
	}
	raw, err := marshalStable(m)
	if err != nil {
		return Manifest{}, err
	}
	return m, os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644)
}

// ReadManifest loads golden/manifest.json.
func ReadManifest(dir string) (Manifest, error) {
	var m Manifest
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, fmt.Errorf("parse manifest: %w", err)
	}
	return m, nil
}

// VerifyManifest fails when a golden's bytes do not match its recorded hash, or
// when a golden in the directory is missing from / extra to the manifest.
func VerifyManifest(dir string) error {
	m, err := ReadManifest(dir)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	present := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || e.Name() == "manifest.json" {
			continue
		}
		present[e.Name()] = true
		want, ok := m.Files[e.Name()]
		if !ok {
			return fmt.Errorf("%s is not in golden/manifest.json (re-record it)", e.Name())
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return err
		}
		sum := sha256.Sum256(raw)
		if got := hex.EncodeToString(sum[:]); got != want {
			return fmt.Errorf("%s changed after recording (want sha256 %s, got %s); re-record it", e.Name(), want, got)
		}
	}
	extra := make([]string, 0)
	for name := range m.Files {
		if !present[name] {
			extra = append(extra, name)
		}
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		return fmt.Errorf("golden/manifest.json lists missing file(s): %s", strings.Join(extra, ", "))
	}
	return nil
}

// marshalStable renders JSON with sorted keys, two-space indent and a trailing
// newline (encoding/json sorts map keys).
func marshalStable(v any) ([]byte, error) {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}
