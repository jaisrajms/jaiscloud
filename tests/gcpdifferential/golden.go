//go:build gcp_differential

package gcpdifferential

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

// Exchange is one recorded request/response pair. The same type holds the
// golden (real GCP) and the replayed (emulator) exchange; the normalizer has
// already folded project/run-specific values before it is stored or compared.
type Exchange struct {
	Index    int             `json:"index"`
	Service  string          `json:"service"`
	Op       string          `json:"op"`
	Method   string          `json:"method"`
	Path     string          `json:"path"`
	Status   int             `json:"status"`
	Request  json.RawMessage `json:"request,omitempty"`
	Response json.RawMessage `json:"response,omitempty"`
}

// Manifest lists the golden files with a content hash so a replay can detect a
// stale or partially-committed golden set.
type Manifest struct {
	Version  int             `json:"version"`
	Project  string          `json:"project"`
	Ops      int             `json:"ops"`
	Services []string        `json:"services"`
	Files    []ManifestEntry `json:"files"`
}

// ManifestEntry is one golden file's metadata.
type ManifestEntry struct {
	File   string `json:"file"`
	Op     string `json:"op"`
	Hash   string `json:"sha256"`
	Status int    `json:"status"`
}

// GoldenFileName returns the stable filename for an exchange.
func GoldenFileName(index int, service, op string) string {
	return fmt.Sprintf("%03d-%s-%s.json", index, service, op)
}

// WriteGoldens writes one file per exchange plus manifest.json into dir. The
// manifest records the source project only as the normalized placeholder so
// committed goldens never carry a project identifier.
func WriteGoldens(dir string, exs []Exchange) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// Remove any stale golden files so a shrunk scenario set cannot leave an
	// orphan that a later replay would silently skip.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".json") {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}

	services := map[string]bool{}
	manifest := Manifest{Version: 1, Project: "<project>", Ops: len(exs)}
	for _, ex := range exs {
		name := GoldenFileName(ex.Index, ex.Service, ex.Op)
		data, err := marshalIndent(ex)
		if err != nil {
			return err
		}
		data = append(data, '\n')
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		manifest.Files = append(manifest.Files, ManifestEntry{
			File:   name,
			Op:     ex.Op,
			Hash:   hex.EncodeToString(sum[:]),
			Status: ex.Status,
		})
		services[ex.Service] = true
	}
	for svc := range services {
		manifest.Services = append(manifest.Services, svc)
	}
	sort.Strings(manifest.Services)

	mdata, err := marshalIndent(manifest)
	if err != nil {
		return err
	}
	mdata = append(mdata, '\n')
	return os.WriteFile(filepath.Join(dir, "manifest.json"), mdata, 0o644)
}

// ReadGoldens reads every golden file (sorted by filename) from dir. A missing
// or empty dir reports a non-nil error wrapping os.ErrNotExist so callers can
// skip cleanly.
func ReadGoldens(dir string) ([]Exchange, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || e.Name() == "manifest.json" {
			continue
		}
		names = append(names, e.Name())
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no goldens in %s: %w", dir, os.ErrNotExist)
	}
	sort.Strings(names)

	exs := make([]Exchange, 0, len(names))
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		var ex Exchange
		if err := json.Unmarshal(data, &ex); err != nil {
			return nil, fmt.Errorf("parse golden %s: %w", name, err)
		}
		exs = append(exs, ex)
	}
	return exs, nil
}

// GoldenFiles returns the golden JSON filenames in dir (excluding manifest.json).
func GoldenFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || e.Name() == "manifest.json" {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}
