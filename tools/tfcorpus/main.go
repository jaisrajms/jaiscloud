// Command tfcorpus builds and triages the Terraform web corpora (wave plan
// TF2).
//
//	# extract the candidate manifest from a pinned provider-docs checkout
//	tfcorpus -discover
//
//	# plan-only triage of the candidates against a running emulator
//	tfcorpus -run -max-per-service 5
//
//	# curated allowlist: apply -> assert -> plan No changes -> destroy
//	tfcorpus -apply
//
//	# the single endpoint map's consumers
//	tfcorpus -print-endpoints
//	tfcorpus -print-endpoint storage
//	tfcorpus -emit-tfvars /tmp/endpoints.auto.tfvars.json
//
// Corpora are never vendored or cloned by a gate: point TF_PROVIDER_DOCS and
// TF_SAMPLES_DIR at local checkouts and the tool reads them; without a checkout
// it reports what is missing (the Makefile target then SKIPs).
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
)

const (
	defaultManifest  = "tests/integration/gcp/terraform/corpus.yaml"
	defaultEndpoints = "tests/integration/gcp/terraform/terraform-endpoints.yaml"
	defaultCoverage  = "docs/fidelity/terraform-coverage.csv"
	defaultReport    = "tests/integration/gcp/terraform/corpus-report.md"
	defaultJSON      = "tests/integration/gcp/terraform/corpus-report.json"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "tfcorpus:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("tfcorpus", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	discoverFlag := fs.Bool("discover", false, "extract the corpus and rewrite the manifest (preserves hand edits)")
	runFlag := fs.Bool("run", false, "plan-only triage of the candidate cases")
	applyFlag := fs.Bool("apply", false, "apply/assert/re-plan/destroy the curated allowlist")

	manifestPath := fs.String("manifest", defaultManifest, "corpus manifest")
	endpointsPath := fs.String("endpoints", defaultEndpoints, "service -> provider custom-endpoint map")
	coveragePath := fs.String("coverage", defaultCoverage, "TF1 provider coverage inventory")
	reportPath := fs.String("report", defaultReport, "markdown report")
	reportJSON := fs.String("report-json", defaultJSON, "JSON report")

	providerDocs := fs.String("provider-docs", os.Getenv("TF_PROVIDER_DOCS"), "terraform-provider-google checkout (website/docs/r)")
	samplesDir := fs.String("samples", os.Getenv("TF_SAMPLES_DIR"), "terraform-docs-samples checkout")
	providerVersion := fs.String("provider-version", "", "hashicorp/google version to pin (default: manifest)")

	endpoint := fs.String("endpoint", firstNonEmpty(os.Getenv("TF_ENDPOINT"), ""), "emulator base URL")
	project := fs.String("project", firstNonEmpty(os.Getenv("TF_PROJECT"), "test-project"), "GCP project id")
	region := fs.String("region", firstNonEmpty(os.Getenv("TF_REGION"), "us-central1"), "default region")
	tfBin := fs.String("tf-bin", firstNonEmpty(os.Getenv("TF_BIN"), "terraform"), "terraform or tofu binary")

	maxPerService := fs.Int("max-per-service", 0, "cap planned cases per service (0 = all)")
	parallel := fs.Int("parallel", 4, "concurrent plan-only workers (apply is always sequential)")
	serviceFilter := fs.String("service", "", "comma-separated service filter")
	caseFilter := fs.String("case", "", "case name substring filter")
	keep := fs.Bool("keep", false, "keep the generated work directories")
	pluginDir := fs.String("plugin-dir", os.Getenv("TF_PLUGIN_DIR"), "flat provider plugin dir passed to terraform init")
	pluginCache := fs.String("plugin-cache", os.Getenv("TF_PLUGIN_CACHE_DIR"), "TF_PLUGIN_CACHE_DIR for terraform init")

	printEndpoints := fs.Bool("print-endpoints", false, "print the user-facing provider snippet")
	printEndpoint := fs.String("print-endpoint", "", "print the endpoint URL for one service")
	emitTfvars := fs.String("emit-tfvars", "", "write the fixture's endpoints tfvars JSON (path, or - for stdout)")
	selectApply := fs.String("select-apply", "", "comma-separated case names to mark mode: apply")
	selectPlan := fs.String("select-plan", "", "comma-separated case names to mark back to plan (and candidate)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *selectApply != "" || *selectPlan != "" {
		return selectModes(*manifestPath, *selectApply, *selectPlan)
	}

	ep, err := LoadEndpoints(*endpointsPath)
	if err != nil {
		return err
	}
	if *endpoint == "" {
		*endpoint = ep.DefaultEndpoint
	}

	switch {
	case *printEndpoints:
		snippet, err := ep.ProviderSnippet(*endpoint, *project, *region)
		if err != nil {
			return err
		}
		fmt.Print(snippet)
		return nil
	case *printEndpoint != "":
		u, err := ep.ServiceURL(*endpoint, *printEndpoint)
		if err != nil {
			return err
		}
		fmt.Println(u)
		return nil
	case *emitTfvars != "":
		return ep.WriteTfvars(*emitTfvars, *endpoint)
	}

	modes := 0
	for _, b := range []bool{*discoverFlag, *runFlag, *applyFlag} {
		if b {
			modes++
		}
	}
	if modes != 1 {
		return fmt.Errorf("choose exactly one of -discover, -run, -apply")
	}

	if *discoverFlag {
		return discover(*manifestPath, *coveragePath, *providerDocs, *samplesDir, *providerVersion)
	}

	manifest, err := LoadManifest(*manifestPath)
	if err != nil {
		return err
	}
	if *providerVersion != "" {
		manifest.ProviderVersion = *providerVersion
	}

	coverage, err := LoadCoverage(*coveragePath)
	if err != nil {
		return err
	}
	zeroExample := servicesWithoutCandidates(manifest, coverage)

	if *runFlag {
		if *providerDocs == "" {
			return fmt.Errorf("TF_PROVIDER_DOCS is not set; point it at a terraform-provider-google checkout")
		}
		cases := selectPlanCases(manifest, *maxPerService, *serviceFilter, *caseFilter)
		if len(cases) == 0 {
			return fmt.Errorf("no plan cases selected (manifest %s)", *manifestPath)
		}
		runner := NewRunner(*tfBin, *endpoint, *project, *region, manifest.ProviderVersion, ep)
		runner.ProviderDocs = *providerDocs
		runner.SamplesDir = *samplesDir
		runner.Keep = *keep
		runner.PluginDir = *pluginDir
		runner.PluginCache = *pluginCache
		results := make([]CaseResult, 0, len(cases))
		bad := 0
		if *parallel <= 1 {
			for _, c := range cases {
				res := runner.RunPlan(c)
				results = append(results, res)
				fmt.Printf("  [%-15s] %-6s %s\n", res.Class, res.Service, c.Name)
				if res.Class == "setup" || res.Class == "provider-panic" {
					bad++
				}
			}
		} else {
			// Plan-only cases never apply, so they cannot collide in the
			// emulator: one work directory and state per case, run in parallel.
			root, err := os.MkdirTemp("", "jc-tf-corpus-")
			if err != nil {
				return err
			}
			runner.WorkRoot = root
			out := make([]CaseResult, len(cases))
			sem := make(chan struct{}, *parallel)
			var wg sync.WaitGroup
			for i, c := range cases {
				wg.Add(1)
				go func(i int, c ManifestCase) {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()
					out[i] = runner.RunPlan(c)
				}(i, c)
			}
			wg.Wait()
			if !*keep {
				_ = os.RemoveAll(root)
			}
			for _, res := range out {
				results = append(results, res)
				fmt.Printf("  [%-15s] %-6s %s\n", res.Class, res.Service, res.Name)
				if res.Class == "setup" || res.Class == "provider-panic" {
					bad++
				}
			}
		}
		fmt.Printf("tfcorpus: planned %d cases (%d had setup/panic errors)\n", len(results), bad)
		if err := writeReport(manifest, runner, *reportPath, *reportJSON, *endpoint, *project, zeroExample, nil, results); err != nil {
			return err
		}
		if bad > 0 {
			return fmt.Errorf("%d case(s) could not run (setup or provider panic); see %s", bad, *reportPath)
		}
		return nil
	}

	// -apply
	cases := selectApplyCases(manifest, *serviceFilter, *caseFilter)
	if len(cases) == 0 {
		return fmt.Errorf("no apply cases selected (mark curated manifest cases mode: apply)")
	}
	runner := NewRunner(*tfBin, *endpoint, *project, *region, manifest.ProviderVersion, ep)
	runner.ProviderDocs = *providerDocs
	runner.SamplesDir = *samplesDir
	runner.Keep = *keep
	runner.PluginDir = *pluginDir
	runner.PluginCache = *pluginCache
	results := make([]CaseResult, 0, len(cases))
	failed := 0
	for _, c := range cases {
		res := runner.RunApply(c)
		results = append(results, res)
		fmt.Printf("  [%-15s] %-6s %s\n", res.Class, res.Service, c.Name)
		if res.Class != "apply-ok" {
			failed++
		}
	}
	fmt.Printf("tfcorpus: applied %d cases (%d failed)\n", len(results), failed)
	if err := writeReport(manifest, runner, *reportPath, *reportJSON, *endpoint, *project, zeroExample, results, nil); err != nil {
		return err
	}
	if failed > 0 {
		return fmt.Errorf("%d apply case(s) failed; see %s", failed, *reportPath)
	}
	return nil
}

func discover(manifestPath, coveragePath, providerDocs, samplesDir, providerVersion string) error {
	if providerDocs == "" {
		return fmt.Errorf("TF_PROVIDER_DOCS is not set; point it at a terraform-provider-google checkout")
	}
	coverage, err := LoadCoverage(coveragePath)
	if err != nil {
		return err
	}
	old, err := LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	docs, stats, err := DiscoverDocs(providerDocs, coverage)
	if err != nil {
		return err
	}
	corpora := []CorpusRef{corpusRef("provider-docs", "hashicorp/terraform-provider-google", providerDocs, "website/docs/r")}
	all := docs
	if samplesDir != "" {
		samples, err := DiscoverSamples(samplesDir, coverage)
		if err != nil {
			return err
		}
		corpora = append(corpora, corpusRef("docs-samples", "terraform-google-modules/terraform-docs-samples", samplesDir, "."))
		all = append(all, samples...)
	}
	m := &Manifest{
		Provider:        old.Provider,
		ProviderVersion: old.ProviderVersion,
		Corpora:         corpora,
		Cases:           MergeCases(old, all),
	}
	if providerVersion != "" {
		m.ProviderVersion = providerVersion
	}
	if err := SaveManifest(manifestPath, m); err != nil {
		return err
	}
	fmt.Printf("tfcorpus: discovered %d docs (%d fenced blocks, %d resource blocks: %d in scope, %d excluded)\n",
		stats.Docs, stats.FencedBlocks, stats.ResourceBlocks, stats.InScope, stats.Excluded)
	if samplesDir != "" {
		fmt.Printf("tfcorpus: %d sample scenarios recorded as deferred\n", len(all)-len(docs))
	}
	fmt.Printf("tfcorpus: wrote %s (%d cases)\n", manifestPath, len(m.Cases))
	return nil
}

// corpusRef pins the checkout: commit and commit date, never "today".
func corpusRef(name, repo, root, path string) CorpusRef {
	ref := CorpusRef{Name: name, Repo: repo, Path: path}
	if out, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output(); err == nil {
		ref.Commit = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("git", "-C", root, "log", "-1", "--format=%cs").Output(); err == nil {
		ref.Date = strings.TrimSpace(string(out))
	}
	if ref.Commit == "" {
		ref.Commit = "unknown"
	}
	if ref.Date == "" {
		ref.Date = "unknown"
	}
	return ref
}

// selectPlanCases picks candidate plan cases, filtered and capped per service.
// Within a service, non-IAM examples come first: an *_iam_* resource is a
// weaker example of the service's provisioning surface.
func selectPlanCases(m *Manifest, maxPerService int, serviceFilter, caseFilter string) []ManifestCase {
	want := csvSet(serviceFilter)
	var selected []ManifestCase
	for _, c := range m.Cases {
		if c.Status != StatusCandidate || c.Mode != ModePlan {
			continue
		}
		if len(want) > 0 && !want[c.Service] {
			continue
		}
		if caseFilter != "" && !strings.Contains(c.Name, caseFilter) {
			continue
		}
		selected = append(selected, c)
	}
	sort.SliceStable(selected, func(i, j int) bool {
		a, b := selected[i], selected[j]
		if a.Service != b.Service {
			return a.Service < b.Service
		}
		ai, bi := isIAMCase(a), isIAMCase(b)
		if ai != bi {
			return !ai
		}
		return a.Name < b.Name
	})
	if maxPerService > 0 {
		count := map[string]int{}
		kept := selected[:0]
		for _, c := range selected {
			if count[c.Service] >= maxPerService {
				continue
			}
			count[c.Service]++
			kept = append(kept, c)
		}
		selected = kept
	}
	return selected
}

func selectApplyCases(m *Manifest, serviceFilter, caseFilter string) []ManifestCase {
	want := csvSet(serviceFilter)
	var out []ManifestCase
	for _, c := range m.Cases {
		if c.Status != StatusCandidate || c.Mode != ModeApply {
			continue
		}
		if len(want) > 0 && !want[c.Service] {
			continue
		}
		if caseFilter != "" && !strings.Contains(c.Name, caseFilter) {
			continue
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func isIAMCase(c ManifestCase) bool {
	for _, t := range c.ResourceTypes {
		if strings.Contains(t, "_iam_") {
			return true
		}
	}
	return false
}

func csvSet(s string) map[string]bool {
	out := map[string]bool{}
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out[p] = true
		}
	}
	return out
}

// selectModes curates the manifest's apply allowlist without hand-editing the
// generated YAML. -discover preserves the choice afterwards.
func selectModes(manifestPath, applyNames, planNames string) error {
	m, err := LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	applySet, planSet := csvSet(applyNames), csvSet(planNames)
	changed := 0
	var missing []string
	for _, name := range append(keys(applySet), keys(planSet)...) {
		found := false
		for i := range m.Cases {
			if m.Cases[i].Name != name {
				continue
			}
			found = true
			if applySet[name] {
				m.Cases[i].Mode = ModeApply
				m.Cases[i].Status = StatusCandidate
				m.Cases[i].Reason = ""
			} else {
				m.Cases[i].Mode = ModePlan
			}
			changed++
		}
		if !found {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("unknown case(s): %s", strings.Join(missing, ", "))
	}
	if err := SaveManifest(manifestPath, m); err != nil {
		return err
	}
	fmt.Printf("tfcorpus: updated %d case(s) in %s\n", changed, manifestPath)
	return nil
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// servicesWithoutCandidates lists services the TF1 inventory serves that have
// no candidate case in the manifest. They are reported rather than silently
// absent, so "every in-scope service" is accounted for.
func servicesWithoutCandidates(m *Manifest, coverage map[string]CoverageRow) []string {
	served := map[string]bool{}
	for _, row := range coverage {
		if row.Classification == "served" {
			served[row.Service] = true
		}
	}
	have := map[string]bool{}
	for _, c := range m.Cases {
		if c.Status == StatusCandidate {
			have[c.Service] = true
		}
	}
	var out []string
	for svc := range served {
		if !have[svc] {
			out = append(out, svc)
		}
	}
	sort.Strings(out)
	return out
}

func writeReport(m *Manifest, r *Runner, mdPath, jsonPath, endpoint, project string, zeroExample []string, apply, plan []CaseResult) error {
	tfVersion := terraformVersion(r.TF)
	old := LoadReport(jsonPath)
	if old != nil {
		if plan == nil {
			plan = old.Plan
		}
		if apply == nil {
			apply = old.Apply
		}
	}
	rep := BuildReport(m, tfVersion, endpoint, project, plan, apply, zeroExample)
	if err := rep.WriteJSON(jsonPath); err != nil {
		return err
	}
	return rep.WriteMarkdown(mdPath)
}

func terraformVersion(tf string) string {
	out, err := exec.Command(tf, "version").Output()
	if err != nil {
		return tf
	}
	line := strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
	if line == "" {
		return tf
	}
	return line
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
