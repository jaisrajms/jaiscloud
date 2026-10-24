//go:build gcp_differential

package gcpdifferential

import (
	"sort"
	"strings"
)

// scenarioGolden pairs a recorded golden with the scenario that produced it.
type scenarioGolden struct {
	Scenario Scenario
	Golden   Exchange
}

// scenarioKey is the stable identity of a scenario/golden: its service and op.
// Golden filenames embed an index, which shifts whenever the curated list grows,
// so (Service, Op) — not position — is what matches a golden to a scenario.
func scenarioKey(service, op string) string { return service + "\x00" + op }

// displayKey renders a scenarioKey for a human-facing report (service/op).
func displayKey(k string) string {
	if i := strings.IndexByte(k, 0); i >= 0 {
		return k[:i] + "/" + k[i+1:]
	}
	return k
}

// matchGoldenKeys matches goldens to an ordered list of (Service, Op) keys by
// key, not position.
//
// It returns the golden for each key as a map, the keys with no committed golden
// ("pending recording"), the goldens with no key ("orphan": a stale file or a
// dropped scenario, always an error), and any duplicate keys. A pending key is
// expected while a new scenario awaits its first real-GCP recording.
func matchGoldenKeys(keys []string, goldens []Exchange) (byKey map[string]Exchange, pending, orphans, duplicates []string) {
	byKey = make(map[string]Exchange, len(goldens))
	dups := map[string]bool{}

	for _, ex := range goldens {
		k := scenarioKey(ex.Service, ex.Op)
		if _, ok := byKey[k]; ok {
			dups[k] = true
		}
		byKey[k] = ex
	}

	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		if keySet[k] {
			dups[k] = true
		}
		keySet[k] = true
	}

	for _, k := range keys {
		if _, ok := byKey[k]; !ok {
			pending = append(pending, displayKey(k))
		}
	}
	for _, ex := range goldens {
		k := scenarioKey(ex.Service, ex.Op)
		if !keySet[k] {
			orphans = append(orphans, displayKey(k))
		}
	}
	for k := range dups {
		duplicates = append(duplicates, displayKey(k))
	}
	sort.Strings(orphans)
	sort.Strings(duplicates)
	return byKey, pending, orphans, duplicates
}

// matchScenariosToGoldens matches goldens to the REST scenarios by (Service, Op).
//
// It returns the matched pairs in scenario order, the "service/op" keys of
// scenarios with no committed golden ("pending recording"), the "service/op"
// keys of goldens with no scenario ("orphan"), and any duplicate (Service, Op)
// keys. A pending scenario is expected while a new scenario awaits its first
// real-GCP recording; an orphan golden is a stale file that must be deleted (or
// a scenario that was dropped) and is always an error.
func matchScenariosToGoldens(scenarios []Scenario, goldens []Exchange) (matched []scenarioGolden, pending, orphans, duplicates []string) {
	// Setup scenarios produce no golden by design, so they are excluded from the
	// pending set (they would otherwise look like an unrecorded scenario).
	keys := make([]string, 0, len(scenarios))
	for _, sc := range scenarios {
		if sc.Setup {
			continue
		}
		keys = append(keys, scenarioKey(sc.Service, sc.Op))
	}
	byKey, pending, orphans, duplicates := matchGoldenKeys(keys, goldens)
	for _, sc := range scenarios {
		if sc.Setup {
			continue
		}
		if ex, ok := byKey[scenarioKey(sc.Service, sc.Op)]; ok {
			matched = append(matched, scenarioGolden{Scenario: sc, Golden: ex})
		}
	}
	return matched, pending, orphans, duplicates
}
