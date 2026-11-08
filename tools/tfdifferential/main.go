// Command tfdifferential records and replays a Terraform corpus against real
// GCP and the emulator (wave plan TF6).
//
//	# credentialed operator step; needs ADC and a real project (never CI)
//	tfdifferential -mode record -project parity-diff-jaiscloud
//
//	# replay the committed goldens against the emulator
//	tfdifferential -mode compare -endpoint http://localhost:8080
//
//	# offline hygiene check: corpus, classifications, golden manifest
//	tfdifferential -mode check
//
// A recording captures the first plan's diff set, the post-apply state and the
// re-plan, normalizes project ids/numbers, the run suffix and server-generated
// values, and writes differential/golden/<case>.json plus a content-hash
// manifest. A replay runs the same module against the emulator and diffs the
// normalized documents; every difference must be classified in
// differential/classifications.yaml or the run fails.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "tfdifferential:", err)
		os.Exit(1)
	}
}

type options struct {
	mode         string
	corpusPath   string
	providerFile string
	goldenDir    string
	classPath    string
	reportPath   string
	endpoint     string
	project      string
	projectNum   string
	region       string
	suffix       string
	tfBin        string
	endpoints    string
	logDir       string
	cases        string
	keep         bool
}

func run(args []string) error {
	var o options
	fs := flag.NewFlagSet("tfdifferential", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&o.mode, "mode", "", "record | compare | check")
	fs.StringVar(&o.corpusPath, "corpus", "tests/integration/gcp/terraform/differential/corpus.yaml", "differential corpus")
	fs.StringVar(&o.providerFile, "provider", "tests/integration/gcp/terraform/patterns/provider.tf", "shared provider block staged into every case")
	fs.StringVar(&o.goldenDir, "golden-dir", "tests/integration/gcp/terraform/differential/golden", "recorded goldens + manifest")
	fs.StringVar(&o.classPath, "classifications", "tests/integration/gcp/terraform/differential/classifications.yaml", "difference classifications")
	fs.StringVar(&o.reportPath, "report", "tests/integration/gcp/terraform/differential/report.md", "compare report (markdown)")
	fs.StringVar(&o.endpoint, "endpoint", "", "emulator base URL (compare; real GCP when empty)")
	fs.StringVar(&o.project, "project", "", "GCP project (record requires a real one; compare defaults to test-project)")
	fs.StringVar(&o.projectNum, "project-number", "", "project number folded by the normalizer (record: looked up with gcloud when empty)")
	fs.StringVar(&o.region, "region", "us-central1", "default region")
	fs.StringVar(&o.suffix, "suffix", "", "run suffix (default: random)")
	fs.StringVar(&o.tfBin, "tf-bin", "terraform", "terraform or tofu")
	fs.StringVar(&o.endpoints, "endpoints-tfvars", "", "generated endpoints tfvars for an emulator run (optional)")
	fs.StringVar(&o.logDir, "log-dir", "/tmp/jaiscloud-gcp-terraform-differential", "per-case terraform logs")
	fs.StringVar(&o.cases, "cases", "", "comma-separated subset of corpus cases (default: all)")
	fs.BoolVar(&o.keep, "keep", false, "keep each case's work directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if o.mode != "record" && o.mode != "compare" && o.mode != "check" {
		return fmt.Errorf("-mode must be record, compare or check")
	}

	corpus, err := LoadCorpus(o.corpusPath)
	if err != nil {
		return err
	}
	selected, err := corpus.Select(splitList(o.cases))
	if err != nil {
		return err
	}

	switch o.mode {
	case "record":
		return record(o, corpus, selected)
	case "compare":
		return compare(o, corpus, selected)
	default:
		return check(o, corpus)
	}
}

func splitList(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func record(o options, corpus *Corpus, selected []Case) error {
	if o.endpoint != "" {
		return errors.New("-mode record runs against real GCP; leave -endpoint empty")
	}
	if os.Getenv("GOOGLE_OAUTH_ACCESS_TOKEN") != "" {
		return errors.New("GOOGLE_OAUTH_ACCESS_TOKEN is set; unset it so the provider uses Application Default Credentials")
	}
	if o.project == "" || o.project == "test-project" {
		return errors.New("-project must name the real GCP project to record against")
	}
	if o.projectNum == "" {
		num, err := lookupProjectNumber(o.project)
		if err != nil {
			return fmt.Errorf("%w (or pass -project-number)", err)
		}
		o.projectNum = num
	}
	if o.suffix == "" {
		o.suffix = randomSuffix()
	}

	fmt.Printf("tfdifferential: recording %s against %s (project number %s, suffix %s)\n", o.corpusPath, o.project, o.projectNum, o.suffix)
	var goldens []Golden
	for _, cs := range selected {
		if cs.RealGCP != realSafe {
			fmt.Printf("  skip %-12s real_gcp: excluded (%s)\n", cs.Name, cs.Reason)
			continue
		}
		opts := runOptions{
			tfBin:         o.tfBin,
			workDir:       filepath.Join(os.TempDir(), "jc-tf-diff-"+cs.Name+"-"+o.suffix),
			logDir:        o.logDir,
			logName:       "record-" + cs.Name,
			providerFile:  o.providerFile,
			emulated:      false,
			project:       o.project,
			projectNumber: o.projectNum,
			region:        o.region,
			endpoint:      "",
			suffix:        o.suffix,
			vars:          cs.Vars(o.suffix),
			keep:          o.keep,
		}
		res, err := execute(opts, corpus.dirFor(cs))
		if err != nil {
			return fmt.Errorf("case %s: %w", cs.Name, err)
		}
		g := Golden{
			Case: cs.Name,
			Provenance: Provenance{
				Project:          o.project,
				Region:           o.region,
				RecordedAt:       time.Now().UTC().Format(time.RFC3339),
				TerraformVersion: res.TerraformVersion,
				ProviderVersion:  res.ProviderVersion,
				Platform:         res.Platform,
				CaseDir:          cs.Dir,
			},
			Doc: res.Doc,
		}
		if err := validateGolden(g); err != nil {
			return fmt.Errorf("case %s: %w", cs.Name, err)
		}
		goldens = append(goldens, g)
		replan := "no changes"
		if res.ReplanChanged {
			replan = "CHANGES"
		}
		fmt.Printf("  recorded %-12s %d resources, %d planned, re-plan: %s\n", cs.Name, len(g.Resources), len(g.Plan), replan)
	}
	if len(goldens) == 0 {
		return errors.New("no case qualified for recording")
	}
	for _, g := range goldens {
		if err := WriteGolden(GoldenPath(o.goldenDir, g.Case), g); err != nil {
			return err
		}
	}
	m, err := WriteManifest(o.goldenDir)
	if err != nil {
		return err
	}
	fmt.Printf("tfdifferential: wrote %d golden(s) + manifest under %s\n", len(m.Files), o.goldenDir)
	return nil
}

func compare(o options, corpus *Corpus, selected []Case) error {
	if o.project == "" {
		o.project = "test-project"
	}
	if o.endpoint == "" {
		o.endpoint = "http://localhost:8080"
	}
	if o.suffix == "" {
		o.suffix = randomSuffix()
	}
	if err := VerifyManifest(o.goldenDir); err != nil {
		return err
	}
	cls, err := LoadClassifications(o.classPath)
	if err != nil {
		return err
	}
	report := Report{
		Generated: time.Now().UTC(),
		Corpus:    o.corpusPath,
		GoldenDir: o.goldenDir,
		Endpoint:  o.endpoint,
		Project:   o.project,
	}
	var failures []string
	for _, cs := range selected {
		if _, err := os.Stat(GoldenPath(o.goldenDir, cs.Name)); err != nil {
			report.Cases = append(report.Cases, caseResult{
				Name:     cs.Name,
				Status:   "not-recorded",
				Excluded: cs.RealGCP == realExcluded,
				Reason:   cs.Reason,
			})
			if cs.RealGCP == realSafe {
				failures = append(failures, cs.Name+": missing golden (run -mode record)")
			}
			continue
		}
		g, err := ReadGolden(GoldenPath(o.goldenDir, cs.Name))
		if err != nil {
			return err
		}
		if g.Case != cs.Name {
			return fmt.Errorf("%s: golden case is %q, corpus case is %q", GoldenPath(o.goldenDir, cs.Name), g.Case, cs.Name)
		}
		opts := runOptions{
			tfBin:           o.tfBin,
			workDir:         filepath.Join(os.TempDir(), "jc-tf-diff-"+cs.Name+"-"+o.suffix),
			logDir:          o.logDir,
			logName:         "compare-" + cs.Name,
			providerFile:    o.providerFile,
			endpointsTfvars: o.endpoints,
			emulated:        true,
			project:         o.project,
			projectNumber:   o.projectNum,
			region:          o.region,
			endpoint:        o.endpoint,
			suffix:          o.suffix,
			vars:            cs.Vars(o.suffix),
			keep:            o.keep,
		}
		res, err := execute(opts, corpus.dirFor(cs))
		if err != nil {
			return fmt.Errorf("case %s: %w", cs.Name, err)
		}
		diffs := DiffDoc(g.Doc, res.Doc)
		classified, unclassified, err := cls.Classify(cs.Name, diffs)
		if err != nil {
			return err
		}
		for _, c := range classified {
			report.Differences = append(report.Differences, ReportDiff{CaseName: cs.Name, Classified: c})
		}
		report.Cases = append(report.Cases, caseResult{
			Name:         cs.Name,
			Status:       "compared",
			Diffs:        len(diffs),
			Unclassified: len(unclassified),
			ReplanChange: res.ReplanChanged,
		})
		fmt.Printf("  compared %-12s %d resource(s), %d difference(s), %d unclassified\n", cs.Name, len(res.Doc.Resources), len(diffs), len(unclassified))
		for _, u := range unclassified {
			report.Differences = append(report.Differences, ReportDiff{
				CaseName:   cs.Name,
				Classified: Classified{Diff: u, Verdict: "unclassified"},
			})
			failures = append(failures, fmt.Sprintf("%s: unclassified %s (real=%v emulator=%v)", cs.Name, u.Path, u.Real, u.Emulator))
		}
	}
	if err := report.WriteReport(o.reportPath); err != nil {
		return err
	}
	fmt.Printf("tfdifferential: report written to %s\n", o.reportPath)
	if len(failures) > 0 {
		return fmt.Errorf("%d differential failure(s):\n  %s", len(failures), strings.Join(failures, "\n  "))
	}
	return nil
}

func check(o options, corpus *Corpus) error {
	if err := VerifyManifest(o.goldenDir); err != nil {
		return err
	}
	if _, err := LoadClassifications(o.classPath); err != nil {
		return err
	}
	byName := map[string]Case{}
	for _, cs := range corpus.Cases {
		byName[cs.Name] = cs
	}
	covered := map[string]bool{}
	entries, err := os.ReadDir(o.goldenDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || e.Name() == "manifest.json" {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".json")
		cs, ok := byName[name]
		if !ok {
			return fmt.Errorf("%s: golden has no corpus case", e.Name())
		}
		if cs.RealGCP != realSafe {
			return fmt.Errorf("%s: golden exists for real_gcp: excluded case %q", e.Name(), name)
		}
		g, err := ReadGolden(filepath.Join(o.goldenDir, e.Name()))
		if err != nil {
			return err
		}
		if err := validateGolden(g); err != nil {
			return fmt.Errorf("%s: %w", e.Name(), err)
		}
		covered[name] = true
	}
	for _, cs := range corpus.Cases {
		if cs.RealGCP == realSafe && !covered[cs.Name] {
			return fmt.Errorf("safe case %q has no golden in %s", cs.Name, o.goldenDir)
		}
	}
	fmt.Printf("tfdifferential: %d golden(s) verified, %d case(s) in the corpus, classifications parse\n", len(covered), len(corpus.Cases))
	return nil
}

// validateGolden rejects a recording that would make the comparison vacuous.
func validateGolden(g Golden) error {
	if g.Case == "" {
		return errors.New("golden has no case name")
	}
	if g.Provenance.Project == "" || g.Provenance.RecordedAt == "" {
		return errors.New("golden has no provenance (project/recorded_at)")
	}
	if g.Provenance.TerraformVersion == "" || g.Provenance.ProviderVersion == "" {
		return errors.New("golden provenance is missing the Terraform or provider version")
	}
	if len(g.Resources) == 0 {
		return errors.New("golden recorded no resources")
	}
	return nil
}

// lookupProjectNumber asks gcloud for the project number the real API embeds in
// resource names.
func lookupProjectNumber(project string) (string, error) {
	if _, err := exec.LookPath("gcloud"); err != nil {
		return "", errors.New("gcloud not found, so the project number cannot be looked up")
	}
	out, err := exec.Command("gcloud", "projects", "describe", project, "--format=value(projectNumber)").Output()
	if err != nil {
		return "", fmt.Errorf("gcloud projects describe %s: %w", project, err)
	}
	num := strings.TrimSpace(string(out))
	if num == "" {
		return "", fmt.Errorf("gcloud returned no project number for %s", project)
	}
	return num, nil
}

func randomSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%08x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
