package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Corpus is the committed differential corpus (differential/corpus.yaml).
type Corpus struct {
	Version int    `yaml:"version"`
	Cases   []Case `yaml:"cases"`
	dir     string
}

// Case is one root module in the corpus.
type Case struct {
	Name        string            `yaml:"name"`
	Dir         string            `yaml:"dir"`
	Description string            `yaml:"description"`
	RealGCP     string            `yaml:"real_gcp"` // safe | excluded
	Reason      string            `yaml:"reason"`
	Variables   map[string]string `yaml:"variables"`
}

// Real-GCP dispositions.
const (
	realSafe     = "safe"
	realExcluded = "excluded"
)

// LoadCorpus reads and validates the corpus. Paths in the corpus are relative
// to the corpus file.
func LoadCorpus(path string) (*Corpus, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read corpus: %w", err)
	}
	var c Corpus
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	c.dir = filepath.Dir(path)
	if c.Version != 1 {
		return nil, fmt.Errorf("%s: unsupported version %d (want 1)", path, c.Version)
	}
	if len(c.Cases) == 0 {
		return nil, fmt.Errorf("%s: no cases", path)
	}
	seen := map[string]bool{}
	for i := range c.Cases {
		cs := &c.Cases[i]
		if cs.Name == "" {
			return nil, fmt.Errorf("%s: case %d has no name", path, i+1)
		}
		if seen[cs.Name] {
			return nil, fmt.Errorf("%s: duplicate case %q", path, cs.Name)
		}
		seen[cs.Name] = true
		if cs.Dir == "" {
			return nil, fmt.Errorf("%s: case %q has no dir", path, cs.Name)
		}
		if _, err := os.Stat(c.dirFor(*cs)); err != nil {
			return nil, fmt.Errorf("%s: case %q: %w", path, cs.Name, err)
		}
		switch cs.RealGCP {
		case realSafe:
			if cs.Reason != "" {
				return nil, fmt.Errorf("%s: case %q is real_gcp: safe but carries a reason", path, cs.Name)
			}
		case realExcluded:
			if cs.Reason == "" {
				return nil, fmt.Errorf("%s: case %q is real_gcp: excluded without a reason", path, cs.Name)
			}
		default:
			return nil, fmt.Errorf("%s: case %q: real_gcp must be %q or %q", path, cs.Name, realSafe, realExcluded)
		}
	}
	return &c, nil
}

// Select returns the named cases, or every case when names is empty.
func (c *Corpus) Select(names []string) ([]Case, error) {
	if len(names) == 0 {
		return c.Cases, nil
	}
	want := map[string]bool{}
	for _, n := range names {
		if n != "" {
			want[n] = true
		}
	}
	var out []Case
	for _, cs := range c.Cases {
		if want[cs.Name] {
			out = append(out, cs)
			delete(want, cs.Name)
		}
	}
	if len(want) > 0 {
		missing := make([]string, 0, len(want))
		for n := range want {
			missing = append(missing, n)
		}
		sort.Strings(missing)
		return nil, fmt.Errorf("unknown case(s): %s", strings.Join(missing, ", "))
	}
	return out, nil
}

// Vars renders the case's variables with the run suffix substituted.
func (c *Case) Vars(suffix string) map[string]string {
	out := make(map[string]string, len(c.Variables))
	for k, v := range c.Variables {
		out[k] = strings.ReplaceAll(v, "{suffix}", suffix)
	}
	return out
}

// dirFor resolves a case directory against the corpus location.
func (c *Corpus) dirFor(cs Case) string {
	return filepath.Join(c.dir, cs.Dir)
}
