//go:build gcp_differential

package gcpdifferential

import "sort"

// scenarioGolden pairs a recorded golden with the scenario that produced it.
type scenarioGolden struct {
	Scenario Scenario
	Golden   Exchange
}

// scenarioKey is the stable identity of a scenario/golden: its service and op.
// Golden filenames embed an index, which shifts whenever the curated list grows,
// so (Service, Op) — not position — is what matches a golden to a scenario.
func scenarioKey(service, op string) string { return service + "\x00" + op }

// matchScenariosToGoldens matches goldens to scenarios by (Service, Op).
//
// It returns the matched pairs in scenario order, the "service/op" keys of
// scenarios with no committed golden ("pending recording"), the "service/op"
// keys of goldens with no scenario ("orphan"), and any duplicate (Service, Op)
// keys. A pending scenario is expected while a new scenario awaits its first
// real-GCP recording; an orphan golden is a stale file that must be deleted (or
// a scenario that was dropped) and is always an error.
func matchScenariosToGoldens(scenarios []Scenario, goldens []Exchange) (matched []scenarioGolden, pending, orphans, duplicates []string) {
	goldenByKey := make(map[string]Exchange, len(goldens))
	scenarioKeys := make(map[string]bool, len(scenarios))
	dups := map[string]bool{}

	for _, sc := range scenarios {
		k := scenarioKey(sc.Service, sc.Op)
		if scenarioKeys[k] {
			dups[sc.Service+"/"+sc.Op] = true
		}
		scenarioKeys[k] = true
	}
	for _, ex := range goldens {
		k := scenarioKey(ex.Service, ex.Op)
		if _, ok := goldenByKey[k]; ok {
			dups[ex.Service+"/"+ex.Op] = true
		}
		goldenByKey[k] = ex
	}

	for _, sc := range scenarios {
		if ex, ok := goldenByKey[scenarioKey(sc.Service, sc.Op)]; ok {
			matched = append(matched, scenarioGolden{Scenario: sc, Golden: ex})
			continue
		}
		pending = append(pending, sc.Service+"/"+sc.Op)
	}
	for _, ex := range goldens {
		if !scenarioKeys[scenarioKey(ex.Service, ex.Op)] {
			orphans = append(orphans, ex.Service+"/"+ex.Op)
		}
	}
	for k := range dups {
		duplicates = append(duplicates, k)
	}
	sort.Strings(duplicates)
	return matched, pending, orphans, duplicates
}
