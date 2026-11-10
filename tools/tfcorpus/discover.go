package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// DocStats is what discovery measured in the provider-docs corpus.
type DocStats struct {
	Docs           int
	FencedBlocks   int
	ResourceBlocks int
	InScope        int
	Excluded       int
}

// fencedBlock is one ```hcl fenced block from a provider doc.
type fencedBlock struct {
	Lang string
	Body string
}

var (
	resourceHeadRe = regexp.MustCompile(`(?m)^[ \t]*resource[ \t]+"(google_[a-z0-9_]+)"[ \t]+"`)
	// allResourceHeadRe keeps non-google types so discovery can classify a
	// block that pulls a helper provider (random_id, time_sleep, ...).
	allResourceHeadRe = regexp.MustCompile(`(?m)^[ \t]*resource[ \t]+"([a-z0-9_]+)"[ \t]+"`)
	dataHeadRe        = regexp.MustCompile(`(?m)^[ \t]*data[ \t]+"(google_[a-z0-9_]+)"[ \t]+"`)
	projectAttrRe     = regexp.MustCompile(`(?m)^([ \t]*(?:project|billing_project)[ \t]*=[ \t]*)"[^"]*"`)
	// ellipsisRe drops a documentation placeholder line ("...") that is not HCL.
	ellipsisRe = regexp.MustCompile(`(?m)^[ \t]*(?:\.{3}|…)[ \t]*\n?`)
	// magicSuffixRe replaces the Magic Modules template placeholder the docs
	// generator leaves in a few examples ("%{random_suffix}").
	magicSuffixRe = regexp.MustCompile(`%\{random_suffix\}`)
	allDataHeadRe = regexp.MustCompile(`(?m)^[ \t]*data[ \t]+"([a-z0-9_]+)"[ \t]+"`)
)

// DiscoverDocs extracts every Example Usage block whose declared resource types
// are all served, and records the rest as excluded with the offending types.
func DiscoverDocs(root string, coverage map[string]CoverageRow) ([]ManifestCase, DocStats, error) {
	var stats DocStats
	files, err := filepath.Glob(filepath.Join(root, "website", "docs", "r", "*.html.markdown"))
	if err != nil {
		return nil, stats, err
	}
	if len(files) == 0 {
		return nil, stats, fmt.Errorf("no provider docs under %s/website/docs/r", root)
	}
	sort.Strings(files)
	var cases []ManifestCase
	for _, file := range files {
		stats.Docs++
		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, stats, err
		}
		base := strings.TrimSuffix(filepath.Base(file), ".html.markdown")
		ordinal := 0
		for _, blk := range extractFencedBlocks(string(raw)) {
			stats.FencedBlocks++
			types := resourceTypes(blk.Body)
			if len(types) == 0 {
				// A data-source-only snippet is not a provisioning case.
				continue
			}
			ordinal++
			stats.ResourceBlocks++
			c := ManifestCase{
				Name:          fmt.Sprintf("%s-example-%d", base, ordinal),
				Source:        "provider-docs",
				File:          filepath.ToSlash(mustRel(root, file)),
				Block:         ordinal,
				ResourceTypes: types,
				Mode:          ModePlan,
				Status:        StatusCandidate,
			}
			// The doc's own resource type is the service under test; a block
			// often also declares prerequisites (service accounts, buckets,
			// KMS keys) from other services. Fall back to the first declared
			// type for IAM docs and pages without a resource of their own.
			primary := "google_" + base
			if row, ok := coverage[primary]; ok {
				c.Service = row.Service
			} else if row, ok := coverage[types[0]]; ok {
				c.Service = row.Service
			}
			_, missing := outOfScope(types, coverage)
			helpers := helperProviders(blk.Body)
			switch {
			case strings.Contains(blk.Body, "google-beta"):
				c.Status = StatusExcluded
				c.Reason = "beta-provider: example is written for hashicorp/google-beta"
				stats.Excluded++
			case len(helpers) > 0:
				c.Status = StatusExcluded
				c.Reason = "helper-provider: " + strings.Join(helpers, ", ")
				stats.Excluded++
			case len(missing) > 0:
				c.Status = StatusExcluded
				c.Reason = "unsupported-service: " + strings.Join(missing, ", ")
				stats.Excluded++
			default:
				stats.InScope++
			}
			cases = append(cases, c)
		}
	}
	return cases, stats, nil
}

// DiscoverSamples turns the terraform-docs-samples scenarios into manifest
// cases. A scenario directory is runnable when every declared resource type is
// served, every data source it reads is served (or computed locally), it needs
// no helper provider and it does not demand a provider newer than the pinned
// one; those become candidate plan cases. The rest are recorded as excluded with
// the reason (TF2-9).
//
// A scenario's service is its *scenario group's* dominant service (the top
// directory, e.g. cloud_sql -> cloudsql) rather than its first declared type:
// samples commonly declare a prerequisite from another service first
// (google_project_service, google_compute_network), which would otherwise file
// a Cloud SQL scenario under serviceusage or compute and skew both the
// per-service cap and the triage report.
func DiscoverSamples(root string, coverage map[string]CoverageRow, pinnedProvider string) ([]ManifestCase, error) {
	dirs := map[string][]string{}
	typesByDir := map[string][]string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		switch d.Name() {
		case ".git", ".terraform", "modules", "test":
			if path != root {
				return filepath.SkipDir
			}
		}
		tf, _ := filepath.Glob(filepath.Join(path, "*.tf"))
		if len(tf) > 0 {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			dirs[filepath.ToSlash(rel)] = tf
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(dirs))
	for name := range dirs {
		names = append(names, name)
	}
	sort.Strings(names)

	// First pass: read every scenario once, keeping the body for the
	// classification below.
	bodies := make(map[string]string, len(names))
	for _, name := range names {
		var b strings.Builder
		var types []string
		for _, f := range dirs[name] {
			raw, err := os.ReadFile(f)
			if err != nil {
				return nil, err
			}
			b.Write(raw)
			b.WriteString("\n")
			types = append(types, resourceTypes(string(raw))...)
		}
		types = dedupe(types)
		if len(types) == 0 {
			continue
		}
		bodies[name] = b.String()
		typesByDir[name] = types
	}
	services := sampleServices(typesByDir, coverage)

	cases := make([]ManifestCase, 0, len(bodies))
	for _, name := range names {
		body, ok := bodies[name]
		if !ok {
			continue
		}
		types := typesByDir[name]
		c := ManifestCase{
			Name:          name,
			Source:        "docs-samples",
			File:          filepath.ToSlash(filepath.Join(name, "main.tf")),
			ResourceTypes: types,
			Service:       services[name],
			Mode:          ModePlan,
			Status:        StatusCandidate,
		}
		_, missing := outOfScope(types, coverage)
		missingData := unroutableDataSources(body, coverage)
		helpers := helperProviders(body)
		newer := futureProviderRequirement(body, pinnedProvider)
		switch {
		case newer != "":
			c.Status = StatusExcluded
			c.Reason = "provider-version: sample requires hashicorp/google " + newer + " (corpus pins " + pinnedProvider + ")"
		case strings.Contains(body, "google-beta"):
			c.Status = StatusExcluded
			c.Reason = "beta-provider: sample is written for hashicorp/google-beta"
		case len(helpers) > 0:
			c.Status = StatusExcluded
			c.Reason = "helper-provider: " + strings.Join(helpers, ", ")
		case len(missing) > 0:
			c.Status = StatusExcluded
			c.Reason = "unsupported-service: " + strings.Join(missing, ", ")
		case len(missingData) > 0:
			c.Status = StatusExcluded
			c.Reason = "unsupported-service (data source): " + strings.Join(missingData, ", ")
		}
		cases = append(cases, c)
	}
	return cases, nil
}

// sampleServices assigns each scenario the dominant service of its scenario
// group (the first path segment). Groups whose scenarios share no served
// resource keep the first declared type's service, which is what discovery
// used before TF2-9.
func sampleServices(typesByDir map[string][]string, coverage map[string]CoverageRow) map[string]string {
	groups := map[string]map[string]int{}
	for name, types := range typesByDir {
		group := sampleGroup(name)
		counts := groups[group]
		if counts == nil {
			counts = map[string]int{}
			groups[group] = counts
		}
		for _, t := range types {
			if row, ok := coverage[t]; ok && row.Service != "" {
				counts[row.Service]++
			}
		}
	}
	dominant := map[string]string{}
	for group, counts := range groups {
		best, bestCount := "", -1
		for svc, n := range counts {
			// Highest count wins; the name breaks a tie so a re-discovery
			// writes the same manifest.
			if n > bestCount || (n == bestCount && svc < best) {
				best, bestCount = svc, n
			}
		}
		dominant[group] = best
	}
	out := map[string]string{}
	for name, types := range typesByDir {
		out[name] = dominant[sampleGroup(name)]
		if out[name] == "" {
			for _, t := range types {
				if row, ok := coverage[t]; ok {
					out[name] = row.Service
					break
				}
			}
		}
	}
	return out
}

// sampleGroup is the top directory of a scenario ("gke" for
// "gke/standard/regional/loadbalancer").
func sampleGroup(name string) string {
	if i := strings.IndexByte(name, '/'); i >= 0 {
		return name[:i]
	}
	return name
}

// clientSideDataSources are computed by the provider from their arguments and
// never reach a Google API, so they are not a coverage gap.
var clientSideDataSources = map[string]bool{
	"google_iam_policy":             true,
	"google_client_config":          true,
	"google_client_openid_userinfo": true,
}

// unroutableDataSources lists the data sources a scenario reads that the
// coverage inventory does not serve. A read is a plan-time API call: an
// unserved one does not answer 501 like an unserved resource hook would (the
// provider falls back to its default host and talks to real GCP), so a
// scenario that reads it is not a case the emulator can run. Data sources are
// matched by the longest provider-resource prefix the inventory serves
// (google_storage_project_service_account -> google_storage_*).
func unroutableDataSources(body string, coverage map[string]CoverageRow) []string {
	served := servedResourcePrefixes(coverage)
	var missing []string
	for _, t := range dataSourceTypes(body) {
		if clientSideDataSources[t] {
			continue
		}
		if row, ok := coverage[t]; ok {
			if row.Classification == "served" {
				continue
			}
			missing = append(missing, t)
			continue
		}
		if servedByPrefix(t, served) {
			continue
		}
		missing = append(missing, t)
	}
	return missing
}

// servedResourcePrefixes is every underscore-boundary prefix of a served
// provider resource type, so a data source can be matched to its service family.
func servedResourcePrefixes(coverage map[string]CoverageRow) map[string]bool {
	out := map[string]bool{}
	for name, row := range coverage {
		if row.Classification != "served" {
			continue
		}
		parts := strings.Split(name, "_")
		for i := 2; i <= len(parts); i++ {
			out[strings.Join(parts[:i], "_")] = true
		}
	}
	return out
}

func servedByPrefix(t string, served map[string]bool) bool {
	parts := strings.Split(t, "_")
	for i := len(parts); i >= 2; i-- {
		if served[strings.Join(parts[:i], "_")] {
			return true
		}
	}
	return false
}

// futureProviderRequirement returns the sample's own minimum hashicorp/google
// version when it is above the pinned one ("" otherwise). A published scenario
// may be written for a newer provider whose schema the pinned version does not
// have; that is a corpus boundary, not an emulator gap.
func futureProviderRequirement(body, pinned string) string {
	if pinned == "" {
		return ""
	}
	pinnedV, ok := parseVersion(pinned)
	if !ok {
		return ""
	}
	for _, block := range requiredProviderBlocks(body) {
		for _, inner := range googleProviderBlocks(block) {
			if strings.Contains(inner, "source") && !strings.Contains(inner, `"hashicorp/google"`) {
				continue
			}
			for _, m := range versionRe.FindAllStringSubmatch(inner, -1) {
				for _, constraint := range strings.Split(m[1], ",") {
					constraint = strings.TrimSpace(constraint)
					min, ok := constraintMinimum(constraint)
					if !ok {
						continue
					}
					if compareVersions(min, pinnedV) > 0 {
						return constraint
					}
				}
			}
		}
	}
	return ""
}

// googleProviderBlocks returns the `google = { ... }` bodies inside a
// required_providers block (not google-beta, not another provider's version).
func googleProviderBlocks(block string) []string {
	var out []string
	for _, loc := range googleProviderAttrRe.FindAllStringIndex(block, -1) {
		open := strings.IndexByte(block[loc[0]:], '{')
		if open < 0 {
			continue
		}
		inner, _ := braceBlock(block, loc[0]+open)
		out = append(out, inner)
	}
	return out
}

// requiredProviderBlocks returns the bodies of every required_providers block,
// brace-matched so a nested provider block does not truncate it.
func requiredProviderBlocks(body string) []string {
	var out []string
	for i := 0; ; {
		j := strings.Index(body[i:], "required_providers")
		if j < 0 {
			return out
		}
		start := i + j
		open := strings.IndexByte(body[start:], '{')
		if open < 0 {
			return out
		}
		inner, end := braceBlock(body, start+open)
		out = append(out, inner)
		i = end
	}
}

// braceBlock returns the text inside the brace at open and the index just past
// its matching close.
func braceBlock(s string, open int) (string, int) {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return s[open+1 : i], i + 1
			}
		}
	}
	return s[open+1:], len(s)
}

var (
	versionRe            = regexp.MustCompile(`version\s*=\s*"([^"]+)"`)
	googleProviderAttrRe = regexp.MustCompile(`(?m)^[ \t]*google[ \t]*=[ \t]*\{`)
)

// constraintMinimum is the lower bound a version constraint implies.
func constraintMinimum(constraint string) ([3]int, bool) {
	var zero [3]int
	for _, op := range []string{">=", "=", "~>"} {
		if strings.HasPrefix(constraint, op) {
			return parseVersion(strings.TrimSpace(strings.TrimPrefix(constraint, op)))
		}
	}
	return zero, false
}

func parseVersion(s string) ([3]int, bool) {
	var v [3]int
	parts := strings.Split(strings.TrimSpace(s), ".")
	if len(parts) == 0 || len(parts) > 3 {
		return v, false
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return v, false
		}
		v[i] = n
	}
	return v, true
}

// compareVersions compares the numeric major.minor.patch triples.
func compareVersions(a, b [3]int) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// outOfScope returns the declared resource types the TF1 inventory does not
// classify as served.
func outOfScope(types []string, coverage map[string]CoverageRow) ([]string, []string) {
	var in, missing []string
	for _, t := range types {
		row, ok := coverage[t]
		if !ok || row.Classification != "served" {
			missing = append(missing, t)
			continue
		}
		in = append(in, t)
	}
	return in, missing
}

// helperProviders lists resource and data types from providers other than
// google, random and time. The corpus driver declares random/time (a test needs
// uniqueness and waits; both are local, not GCP surface), so those blocks run;
// anything else (a kubernetes_* resource or data source, ...) excludes the
// block instead of failing setup.
func helperProviders(body string) []string {
	var out []string
	seen := map[string]bool{}
	check := func(re *regexp.Regexp) {
		for _, m := range re.FindAllStringSubmatch(body, -1) {
			t := m[1]
			if strings.HasPrefix(t, "google_") || strings.HasPrefix(t, "random_") || strings.HasPrefix(t, "time_") || seen[t] {
				continue
			}
			seen[t] = true
			out = append(out, t)
		}
	}
	check(allResourceHeadRe)
	check(allDataHeadRe)
	return out
}

// extractFencedBlocks returns every fenced ```hcl/terraform/tf block.
func extractFencedBlocks(text string) []fencedBlock {
	var out []fencedBlock
	var cur *fencedBlock
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if cur == nil {
			if strings.HasPrefix(trimmed, "```") {
				lang := strings.TrimSpace(strings.TrimPrefix(trimmed, "```"))
				cur = &fencedBlock{Lang: lang}
			}
			continue
		}
		if trimmed == "```" {
			switch cur.Lang {
			case "hcl", "terraform", "tf":
				out = append(out, *cur)
			}
			cur = nil
			continue
		}
		cur.Body += line + "\n"
	}
	return out
}

// resourceTypes lists the resource types a block declares, in declaration
// order, deduplicated. Data sources are not counted: they are not the case's
// provisioning contract and do not appear in the TF1 inventory.
func resourceTypes(body string) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range resourceHeadRe.FindAllStringSubmatch(body, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, m[1])
		}
	}
	return out
}

// dataSourceTypes lists the data sources a block declares (report detail only).
func dataSourceTypes(body string) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range dataHeadRe.FindAllStringSubmatch(body, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, m[1])
		}
	}
	return out
}

// DocBlock returns the block body for one case (1-based ordinal among blocks
// that declare a resource), re-extracted from the pinned checkout.
func DocBlock(root, rel string, block int) (string, error) {
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		return "", err
	}
	ordinal := 0
	for _, blk := range extractFencedBlocks(string(raw)) {
		if len(resourceTypes(blk.Body)) == 0 {
			continue
		}
		ordinal++
		if ordinal == block {
			return blk.Body, nil
		}
	}
	return "", fmt.Errorf("%s has no resource block %d", rel, block)
}

// SampleFiles returns a sample directory's .tf files, sorted.
func SampleFiles(root, rel string) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(rel), "*.tf"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no .tf files under %s", rel)
	}
	return files, nil
}

// normalizeExample makes an extracted block runnable against the emulator:
// the provider/terraform blocks are removed (the tool emits its own), the
// project attribute is wired to the generated variable, and a `lifecycle
// { prevent_destroy = true }` block is dropped — a harness that must destroy
// cannot honour it, and it is a client-side meta-argument, not API surface.
func normalizeExample(body string) string {
	body = magicSuffixRe.ReplaceAllString(body, "jctest")
	body = ellipsisRe.ReplaceAllString(body, "")
	body = stripTopLevelBlocks(body, `provider "google"`, `provider "google-beta"`, "terraform {", "terraform{")
	body = stripPreventDestroy(body)
	body = projectAttrRe.ReplaceAllString(body, `${1}var.project`)
	return strings.TrimSpace(body) + "\n"
}

// stripPreventDestroy removes lifecycle blocks that carry prevent_destroy.
func stripPreventDestroy(body string) string {
	lines := strings.Split(body, "\n")
	var out []string
	for i := 0; i < len(lines); i++ {
		t := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(t, "lifecycle") || !strings.Contains(t, "{") {
			out = append(out, lines[i])
			continue
		}
		depth := strings.Count(lines[i], "{") - strings.Count(lines[i], "}")
		j := i
		block := []string{lines[i]}
		for depth > 0 && j+1 < len(lines) {
			j++
			depth += strings.Count(lines[j], "{") - strings.Count(lines[j], "}")
			block = append(block, lines[j])
		}
		if !strings.Contains(strings.Join(block, "\n"), "prevent_destroy") {
			out = append(out, block...)
		}
		i = j
	}
	return strings.Join(out, "\n")
}

// stripTopLevelBlocks removes top-level blocks whose first line starts with one
// of the heads. It tracks brace depth naively, which is enough for provider and
// terraform blocks.
func stripTopLevelBlocks(body string, heads ...string) string {
	var out []string
	depth := 0
	skipping := false
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if !skipping && depth == 0 {
			for _, h := range heads {
				if strings.HasPrefix(trimmed, h) {
					skipping = true
					break
				}
			}
		}
		if skipping {
			depth += strings.Count(line, "{") - strings.Count(line, "}")
			if depth <= 0 {
				skipping = false
				depth = 0
			}
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func mustRel(base, path string) string {
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return path
	}
	return rel
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
