//go:build gcp_conformance

package gcpconformance

import (
	"sort"
	"testing"
)

func TestEnumerate(t *testing.T) {
	ops := Enumerate()
	if len(ops) == 0 {
		t.Fatal("no operations enumerated")
	}
	by := map[string]int{}
	for _, o := range ops {
		by[o.ProviderPrefix]++
	}
	prefixes := make([]string, 0, len(by))
	for p := range by {
		prefixes = append(prefixes, p)
	}
	sort.Strings(prefixes)
	t.Logf("total operations: %d across %d provider prefixes", len(ops), len(prefixes))
	for _, p := range prefixes {
		t.Logf("  %-20s %d", p, by[p])
	}
	for _, o := range ops {
		if o.Service == "" {
			t.Logf("unmapped provider prefix: %s", o.ProviderPrefix)
			break
		}
	}
}
