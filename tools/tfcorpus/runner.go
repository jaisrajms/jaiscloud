package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Helper provider pins for examples that use hashicorp/random or
// hashicorp/time (client-side only; the same versions the suite cache holds).
const (
	helperRandomVersion = "3.9.1"
	helperTimeVersion   = "0.14.2"
)

// CaseResult is one executed case's outcome.
type CaseResult struct {
	Name          string   `json:"name"`
	Source        string   `json:"source"`
	File          string   `json:"file"`
	Block         int      `json:"block,omitempty"`
	Service       string   `json:"service"`
	ResourceTypes []string `json:"resource_types"`
	Mode          string   `json:"mode"`
	Class         string   `json:"class"`
	Summary       string   `json:"summary"`
	Detail        string   `json:"detail,omitempty"`
}

// Runner executes corpus cases against the emulator.
type Runner struct {
	TF           string
	Endpoint     string
	Project      string
	Region       string
	ProviderVers string
	Endpoints    *EndpointsFile
	WorkRoot     string
	PluginDir    string // optional flat -plugin-dir
	PluginCache  string // optional TF_PLUGIN_CACHE_DIR
	Timeout      time.Duration
	Keep         bool
	Credential   string
	ProviderDocs string
	SamplesDir   string
	HTTPClient   *http.Client
}

// NewRunner builds a runner with the endpoint values resolved.
func NewRunner(tf, endpoint, project, region, providerVersion string, ep *EndpointsFile) *Runner {
	cred := os.Getenv(ep.CredentialEnv)
	if cred == "" {
		cred = "fake-token-jaiscloud"
	}
	return &Runner{
		TF:           tf,
		Endpoint:     endpoint,
		Project:      project,
		Region:       region,
		ProviderVers: providerVersion,
		Endpoints:    ep,
		Timeout:      10 * time.Minute,
		Credential:   cred,
		HTTPClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

// caseBody re-extracts the case's HCL from the pinned corpus checkout.
func (r *Runner) caseBody(c ManifestCase) (string, error) {
	switch c.Source {
	case "provider-docs":
		return DocBlock(r.ProviderDocs, c.File, c.Block)
	case "docs-samples":
		dir := filepath.Dir(filepath.FromSlash(c.File))
		files, err := SampleFiles(r.SamplesDir, filepath.ToSlash(dir))
		if err != nil {
			return "", err
		}
		var b strings.Builder
		for _, f := range files {
			raw, err := os.ReadFile(f)
			if err != nil {
				return "", err
			}
			b.Write(raw)
			b.WriteString("\n")
		}
		return b.String(), nil
	default:
		return "", fmt.Errorf("unknown corpus source %q", c.Source)
	}
}

// module writes the generated root module for one case.
func (r *Runner) module(c ManifestCase) (string, error) {
	body, err := r.caseBody(c)
	if err != nil {
		return "", err
	}
	snippet, err := r.Endpoints.ProviderSnippet(r.Endpoint, r.Project, r.Region)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, `terraform {
  required_version = ">= 1.3"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "= %s"
    }
    # Client-side helpers some published examples use for name uniqueness and
    # waits. Neither talks to the emulator; they are pinned like the GCP
    # provider so the run is reproducible.
    random = {
      source  = "hashicorp/random"
      version = "= %s"
    }
    time = {
      source  = "hashicorp/time"
      version = "= %s"
    }
  }
}

variable "project" {
  type    = string
  default = %q
}

variable "region" {
  type    = string
  default = %q
}

`, r.ProviderVers, helperRandomVersion, helperTimeVersion, r.Project, r.Region)
	b.WriteString(snippet)
	b.WriteString("\n")
	b.WriteString(normalizeExample(body))
	return b.String(), nil
}

// RunPlan executes one case through init -> validate -> plan.
func (r *Runner) RunPlan(c ManifestCase) CaseResult {
	res := baseResult(c)
	dir, err := r.prepare(c)
	if err != nil {
		res.Class, res.Summary, res.Detail = "setup", "prepare case", err.Error()
		return res
	}
	defer r.cleanup(dir)

	if class, out := r.init(dir); class != "" {
		res.Class, res.Summary, res.Detail = class, "terraform init", tail(out, 600)
		return res
	}
	if class, summary, detail := r.validate(dir); class != "" {
		res.Class, res.Summary, res.Detail = class, summary, detail
		return res
	}
	class, summary, detail := r.plan(dir)
	res.Class, res.Summary, res.Detail = class, summary, detail
	return res
}

// RunApply executes one curated allowlist case through
// apply -> assert -> plan No changes -> destroy -> assert.
func (r *Runner) RunApply(c ManifestCase) CaseResult {
	res := baseResult(c)
	dir, err := r.prepare(c)
	if err != nil {
		res.Class, res.Summary, res.Detail = "setup", "prepare case", err.Error()
		return res
	}
	defer r.cleanup(dir)
	destroyed := false
	defer func() {
		if !destroyed {
			_, _, _ = r.exec(dir, "destroy", "-input=false", "-auto-approve", "-no-color")
		}
	}()

	if class, out := r.init(dir); class != "" {
		res.Class, res.Summary, res.Detail = class, "terraform init", tail(out, 600)
		return res
	}
	if class, summary, detail := r.validate(dir); class != "" {
		res.Class, res.Summary, res.Detail = class, summary, detail
		return res
	}
	if class, out := r.apply(dir); class != "" {
		res.Class, res.Summary, res.Detail = class, "terraform apply", tail(out, 600)
		return res
	}
	if class, summary, detail := r.asserts(c, "before"); class != "" {
		res.Class, res.Summary, res.Detail = class, summary, detail
		return res
	}
	if class, summary, detail := r.replan(dir); class != "" {
		res.Class, res.Summary, res.Detail = class, summary, detail
		return res
	}
	if class, out := r.destroy(dir); class != "" {
		res.Class, res.Summary, res.Detail = class, "terraform destroy", tail(out, 600)
		return res
	}
	destroyed = true
	if class, summary, detail := r.asserts(c, "after"); class != "" {
		res.Class, res.Summary, res.Detail = class, summary, detail
		return res
	}
	res.Class, res.Summary = "apply-ok", "apply, no changes, destroy"
	return res
}

func baseResult(c ManifestCase) CaseResult {
	return CaseResult{
		Name: c.Name, Source: c.Source, File: c.File, Block: c.Block,
		Service: c.Service, ResourceTypes: c.ResourceTypes, Mode: c.Mode,
	}
}

func (r *Runner) prepare(c ManifestCase) (string, error) {
	root := r.WorkRoot
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "jc-tf-corpus-")
		if err != nil {
			return "", err
		}
		r.WorkRoot = root
	}
	dir := filepath.Join(root, slug(c.Name))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	hcl, err := r.module(c)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(hcl), 0o644); err != nil {
		return "", err
	}
	return dir, nil
}

func (r *Runner) cleanup(dir string) {
	if r.Keep {
		return
	}
	_ = os.RemoveAll(dir)
}

func (r *Runner) env() []string {
	env := append([]string{}, os.Environ()...)
	appendEnv := func(k, v string) {
		if v != "" {
			env = append(env, k+"="+v)
		}
	}
	appendEnv("GOOGLE_OAUTH_ACCESS_TOKEN", r.Credential)
	appendEnv("GOOGLE_CLOUD_PROJECT", r.Project)
	appendEnv("TF_IN_AUTOMATION", "1")
	appendEnv("CHECKPOINT_DISABLE", "1")
	appendEnv("TF_PLUGIN_CACHE_DIR", r.PluginCache)
	return env
}

func (r *Runner) exec(dir string, args ...string) (string, string, int) {
	ctx, cancel := context.WithTimeout(context.Background(), r.Timeout)
	defer cancel()
	if r.PluginDir != "" && len(args) > 0 && args[0] == "init" {
		args = append(args, "-plugin-dir="+r.PluginDir)
	}
	cmd := exec.CommandContext(ctx, r.TF, append([]string{"-chdir=" + dir}, args...)...)
	cmd.Env = r.env()
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	rc := 0
	if err != nil {
		rc = 1
		if ee, ok := err.(*exec.ExitError); ok {
			rc = ee.ExitCode()
		}
		if ctx.Err() != nil {
			stderr.WriteString("\n" + ctx.Err().Error())
		}
	}
	return stdout.String(), stderr.String(), rc
}

func (r *Runner) init(dir string) (class, out string) {
	stdout, stderr, rc := r.exec(dir, "init", "-backend=false", "-input=false", "-no-color")
	if rc == 0 {
		return "", ""
	}
	out = stdout + stderr
	if providerMissingRe.MatchString(out) {
		return "setup", out
	}
	// Init also fails on invalid configuration (a bad heredoc, a placeholder
	// the docs generator left behind): that is a case finding, not a broken
	// harness.
	return "config", out
}

func (r *Runner) validate(dir string) (class, summary, detail string) {
	stdout, stderr, rc := r.exec(dir, "validate", "-no-color", "-json")
	if rc == 0 {
		return "", "", ""
	}
	return classifyDiagnostics(stdout, stderr)
}

func (r *Runner) plan(dir string) (class, summary, detail string) {
	stdout, stderr, rc := r.exec(dir, "plan", "-input=false", "-no-color", "-json", "-out=tfplan")
	if rc == 0 {
		return "plan-ok", "", ""
	}
	return classifyDiagnostics(stdout, stderr)
}

func (r *Runner) apply(dir string) (class, out string) {
	stdout, stderr, rc := r.exec(dir, "apply", "-input=false", "-auto-approve", "-no-color")
	return onError(rc, "apply-failed", stdout+stderr)
}

func (r *Runner) destroy(dir string) (class, out string) {
	stdout, stderr, rc := r.exec(dir, "destroy", "-input=false", "-auto-approve", "-no-color")
	return onError(rc, "destroy-failed", stdout+stderr)
}

func (r *Runner) replan(dir string) (class, summary, detail string) {
	stdout, stderr, rc := r.exec(dir, "plan", "-input=false", "-no-color", "-detailed-exitcode")
	switch rc {
	case 0:
		return "", "", ""
	case 2:
		return "permadiff", "re-plan is not empty", tail(stdout, 600)
	default:
		return classifyDiagnostics("", stderr+stdout)
	}
}

func onError(rc int, class, out string) (string, string) {
	if rc == 0 {
		return "", ""
	}
	return class, out
}

func (r *Runner) asserts(c ManifestCase, phase string) (class, summary, detail string) {
	for _, a := range c.Assert {
		if a.Phase != phase {
			continue
		}
		url := r.expand(a.URL)
		want := a.Status
		if want == 0 {
			want = http.StatusOK
		}
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return "assert-failed", "assert request", err.Error()
		}
		req.Header.Set("Authorization", "Bearer "+r.Credential)
		resp, err := r.HTTPClient.Do(req)
		if err != nil {
			return "assert-failed", "assert " + url, err.Error()
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			return "assert-failed", fmt.Sprintf("assert %s: HTTP %d, want %d", url, resp.StatusCode, want), tail(string(body), 400)
		}
		for _, wantBody := range a.Contains {
			if !strings.Contains(string(body), wantBody) {
				return "assert-failed", fmt.Sprintf("assert %s: missing %q", url, wantBody), tail(string(body), 400)
			}
		}
	}
	return "", "", ""
}

func (r *Runner) expand(s string) string {
	s = strings.ReplaceAll(s, "{{endpoint}}", strings.TrimSuffix(r.Endpoint, "/"))
	s = strings.ReplaceAll(s, "{{project}}", r.Project)
	s = strings.ReplaceAll(s, "{{region}}", r.Region)
	return s
}

// tfDiagnostic is one `terraform ... -json` diagnostic message.
type tfDiagnostic struct {
	Level      string `json:"@level"`
	Type       string `json:"type"`
	Diagnostic *struct {
		Severity string `json:"severity"`
		Summary  string `json:"summary"`
		Detail   string `json:"detail"`
	} `json:"diagnostic"`
}

var (
	panicRe           = regexp.MustCompile(`(?i)(panic:|runtime error:|goroutine \d+ \[running\])`)
	configRe          = regexp.MustCompile(`(?i)(missing required argument|unsupported argument|invalid or unknown key|no value for required variable|incorrect attribute value type|invalid value for variable|required field|invalid configuration|reference to undeclared|undeclared resource|has not been declared|insufficient .* blocks|blocks are required)`)
	transportRe       = regexp.MustCompile(`(?i)(connection refused|no such host|dial tcp|i/o timeout|tls:|certificate)`)
	unimplementedRe   = regexp.MustCompile(`(?i)(\b501\b|not implemented|unimplemented|unsupported operation|unsupported update_mask|does not support)`)
	unroutedRe        = regexp.MustCompile(`(?i)(\b404\b|not found|does not exist|unknown service|unrouted|no such service)`)
	validationRe      = regexp.MustCompile(`(?i)(\b400\b|invalid argument|invalid value|invalid field|bad request|failed validation)`)
	authRe            = regexp.MustCompile(`(?i)(\b401\b|\b403\b|permission denied|unauthorized)`)
	providerMissingRe = regexp.MustCompile(`(?i)(failed to query available provider packages|could not retrieve the list of available versions|could not download)`)
)

// classifyDiagnostics turns `terraform validate -json` (one object with a
// diagnostics array) or `terraform plan -json` (NDJSON messages) into a
// triage class.
func classifyDiagnostics(stdout, stderr string) (class, summary, detail string) {
	var msgs []string
	trimmed := strings.TrimSpace(stdout)
	if strings.HasPrefix(trimmed, "{") {
		var v struct {
			Diagnostics []struct {
				Severity string `json:"severity"`
				Summary  string `json:"summary"`
				Detail   string `json:"detail"`
			} `json:"diagnostics"`
		}
		if err := json.Unmarshal([]byte(trimmed), &v); err == nil && len(v.Diagnostics) > 0 {
			for _, d := range v.Diagnostics {
				if d.Severity == "error" {
					msgs = append(msgs, strings.TrimSpace(d.Summary+": "+d.Detail))
				}
			}
		}
	}
	if len(msgs) == 0 {
		for _, line := range strings.Split(stdout, "\n") {
			line = strings.TrimSpace(line)
			if line == "" || !strings.HasPrefix(line, "{") {
				continue
			}
			var d tfDiagnostic
			if err := json.Unmarshal([]byte(line), &d); err != nil {
				continue
			}
			if d.Diagnostic != nil && d.Diagnostic.Severity == "error" {
				msgs = append(msgs, strings.TrimSpace(d.Diagnostic.Summary+": "+d.Diagnostic.Detail))
			}
		}
	}
	text := strings.Join(msgs, "\n")
	if text == "" {
		text = strings.TrimSpace(stderr)
	}
	summary = firstLine(text)
	if summary == "" {
		summary = "terraform failed without diagnostics"
	}
	return classifyText(text), summary, tail(text, 600)
}

func classifyText(text string) string {
	switch {
	case panicRe.MatchString(text):
		return "provider-panic"
	case configRe.MatchString(text):
		return "config"
	case transportRe.MatchString(text):
		return "transport"
	case unimplementedRe.MatchString(text):
		return "unimplemented"
	case unroutedRe.MatchString(text):
		return "unrouted"
	case validationRe.MatchString(text):
		return "validation"
	case authRe.MatchString(text):
		return "auth"
	default:
		return "other"
	}
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return "…" + s[len(s)-n:]
}

// slug turns a case name into a filesystem-safe directory name.
func slug(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "case"
	}
	if len(out) > 120 {
		out = out[:120]
	}
	return out
}

// itoa is a tiny helper kept here so report sorting never depends on fmt.
func itoa(i int) string { return strconv.Itoa(i) }
