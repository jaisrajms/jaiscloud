package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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

// DiscoverSamples records the scenario samples as classified-but-deferred
// cases (the follow-up row TF2-x owns running them).
func DiscoverSamples(root string, coverage map[string]CoverageRow) ([]ManifestCase, error) {
	dirs := map[string][]string{}
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
	cases := make([]ManifestCase, 0, len(names))
	for _, name := range names {
		var types []string
		for _, f := range dirs[name] {
			raw, err := os.ReadFile(f)
			if err != nil {
				return nil, err
			}
			types = append(types, resourceTypes(string(raw))...)
		}
		types = dedupe(types)
		if len(types) == 0 {
			continue
		}
		c := ManifestCase{
			Name:          name,
			Source:        "docs-samples",
			File:          filepath.ToSlash(filepath.Join(name, "main.tf")),
			ResourceTypes: types,
			Mode:          ModePlan,
			Status:        StatusExcluded,
		}
		if row, ok := coverage[types[0]]; ok {
			c.Service = row.Service
		}
		_, missing := outOfScope(types, coverage)
		if len(missing) > 0 {
			c.Reason = "unsupported-service: " + strings.Join(missing, ", ")
		} else {
			c.Reason = "sample scenario; plan-run deferred to a follow-up session (TF2-x)"
		}
		cases = append(cases, c)
	}
	return cases, nil
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
