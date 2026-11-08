package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// caseRun is everything a differential comparison needs from one Terraform run.
type caseRun struct {
	Doc              Doc
	TerraformVersion string
	ProviderVersion  string
	Platform         string
	ReplanChanged    bool
}

// runOptions are the flags that shape one case run.
type runOptions struct {
	tfBin           string
	workDir         string
	logDir          string
	logName         string
	providerFile    string
	endpointsTfvars string
	emulated        bool
	project         string
	projectNumber   string
	region          string
	endpoint        string
	suffix          string
	vars            map[string]string
	keep            bool
}

// runner executes terraform in one staged work directory.
type runner struct {
	opts runOptions
	log  *os.File
}

func newRunner(opts runOptions) (*runner, error) {
	if err := os.MkdirAll(opts.workDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(opts.logDir, 0o755); err != nil {
		return nil, err
	}
	log, err := os.Create(filepath.Join(opts.logDir, opts.logName+".log"))
	if err != nil {
		return nil, err
	}
	return &runner{opts: opts, log: log}, nil
}

func (r *runner) close() {
	if r.log != nil {
		_ = r.log.Close()
	}
}

func (r *runner) env() []string {
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch name {
		case "GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_CLOUD_PROJECT", "TF_INPUT", "TF_IN_AUTOMATION", "CHECKPOINT_DISABLE":
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"TF_IN_AUTOMATION=1",
		"TF_INPUT=0",
		"CHECKPOINT_DISABLE=1",
		"GOOGLE_CLOUD_PROJECT="+r.opts.project,
	)
	if r.opts.emulated {
		// The emulator ignores auth, but the provider needs a credential
		// present. The fake token must never leak into a real-GCP run; the
		// caller refuses -mode record when this variable is set.
		env = append(env, "GOOGLE_OAUTH_ACCESS_TOKEN=fake-token-jaiscloud")
	}
	return env
}

// tf runs terraform and returns its combined output. A non-zero exit is an
// error.
func (r *runner) tf(args ...string) (string, error) {
	out, code, err := r.tfAllow(nil, args...)
	if err != nil {
		return out, err
	}
	if code != 0 {
		return out, fmt.Errorf("terraform %s: exit %d\n%s", strings.Join(args, " "), code, tail(out, 2500))
	}
	return out, nil
}

// tfAllow runs terraform and accepts any of allowed exit codes (used for
// `plan -detailed-exitcode`, where 2 means "changes" and is not a failure).
func (r *runner) tfAllow(allowed []int, args ...string) (string, int, error) {
	cmd := exec.Command(r.opts.tfBin, args...)
	cmd.Dir = r.opts.workDir
	cmd.Env = r.env()
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			fmt.Fprintf(r.log, "$ %s\n%s\nerror: %v\n\n", r.opts.tfBin+" "+strings.Join(args, " "), out, err)
			return string(out), code, fmt.Errorf("run terraform: %w", err)
		}
	}
	fmt.Fprintf(r.log, "$ %s\nexit=%d\n%s\n\n", r.opts.tfBin+" "+strings.Join(args, " "), code, out)
	if code != 0 && !containsInt(allowed, code) {
		return string(out), code, fmt.Errorf("terraform %s: exit %d\n%s", strings.Join(args, " "), code, tail(string(out), 2500))
	}
	return string(out), code, nil
}

func containsInt(list []int, v int) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "…" + s[len(s)-n:]
}

// stage copies the shared provider block and the case's root module into the
// work directory.
func stage(opts runOptions, caseDir string) error {
	provider, err := os.ReadFile(opts.providerFile)
	if err != nil {
		return fmt.Errorf("read shared provider block: %w", err)
	}
	if err := os.WriteFile(filepath.Join(opts.workDir, "provider.tf"), provider, 0o644); err != nil {
		return err
	}
	entries, err := os.ReadDir(caseDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".tf") {
			continue
		}
		if e.Name() == "provider.tf" {
			return fmt.Errorf("%s declares its own provider.tf; differential cases use the shared block", caseDir)
		}
		raw, err := os.ReadFile(filepath.Join(caseDir, e.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(opts.workDir, e.Name()), raw, 0o644); err != nil {
			return err
		}
	}
	if opts.endpointsTfvars != "" {
		raw, err := os.ReadFile(opts.endpointsTfvars)
		if err != nil {
			return fmt.Errorf("read endpoints tfvars: %w", err)
		}
		if err := os.WriteFile(filepath.Join(opts.workDir, "endpoints.auto.tfvars.json"), raw, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// varArgs renders the shared and case variables as terraform -var flags.
func varArgs(opts runOptions) []string {
	vars := map[string]string{
		"endpoint": opts.endpoint,
		"project":  opts.project,
		"region":   opts.region,
	}
	for k, v := range opts.vars {
		vars[k] = v
	}
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys)*2)
	for _, k := range keys {
		out = append(out, "-var", k+"="+vars[k])
	}
	return out
}

// execute runs the full contract for one case: init, plan, apply, state,
// re-plan, destroy. The destroy runs even when an earlier step fails.
func execute(opts runOptions, caseDir string) (res caseRun, err error) {
	r, err := newRunner(opts)
	if err != nil {
		return res, err
	}
	defer r.close()

	inited := false
	defer func() {
		if !inited {
			if !opts.keep {
				_ = os.RemoveAll(opts.workDir)
			}
			return
		}
		if _, derr := r.tf(append([]string{"destroy", "-input=false", "-no-color", "-auto-approve"}, varArgs(opts)...)...); derr != nil {
			if err == nil {
				err = fmt.Errorf("destroy: %w", derr)
			} else {
				fmt.Fprintf(os.Stderr, "tfdifferential: destroy after failure: %v\n", derr)
			}
		}
		if opts.keep {
			fmt.Printf("tfdifferential: kept work dir %s\n", opts.workDir)
		} else {
			_ = os.RemoveAll(opts.workDir)
		}
	}()

	if err := stage(opts, caseDir); err != nil {
		return res, err
	}
	if _, err := r.tf("init", "-input=false", "-no-color"); err != nil {
		return res, err
	}
	inited = true
	res.TerraformVersion, res.ProviderVersion, res.Platform = r.versions()

	plan1 := append([]string{"plan", "-input=false", "-no-color", "-out=tfplan1"}, varArgs(opts)...)
	if _, err := r.tf(plan1...); err != nil {
		return res, err
	}
	planRaw, err := r.tf("show", "-json", "tfplan1")
	if err != nil {
		return res, err
	}
	if res.Doc.Plan, err = ParsePlan([]byte(planRaw)); err != nil {
		return res, err
	}

	if _, err := r.tf("apply", "-input=false", "-no-color", "-auto-approve", "tfplan1"); err != nil {
		return res, err
	}

	stateRaw, err := r.tf("show", "-json")
	if err != nil {
		return res, err
	}
	n := NewNormalizer(opts.project, opts.projectNumber, opts.suffix, opts.endpoint)
	if res.Doc.Resources, err = ParseState([]byte(stateRaw), n); err != nil {
		return res, err
	}

	out, code, err := r.tfAllow([]int{0, 2}, append([]string{"plan", "-input=false", "-no-color", "-detailed-exitcode", "-out=tfplan2"}, varArgs(opts)...)...)
	if err != nil {
		return res, err
	}
	res.ReplanChanged = code == 2
	_ = out
	replanRaw, err := r.tf("show", "-json", "tfplan2")
	if err != nil {
		return res, err
	}
	if res.Doc.Replan, err = ParsePlan([]byte(replanRaw)); err != nil {
		return res, err
	}
	return res, nil
}

// versions reads `terraform version -json` after init so provider_selections is
// populated. A failure is non-fatal: provenance records what it can.
func (r *runner) versions() (tfVersion, providerVersion, platform string) {
	out, err := r.tf("version", "-json")
	if err != nil {
		return "", "", ""
	}
	var v struct {
		TerraformVersion   string            `json:"terraform_version"`
		Platform           string            `json:"platform"`
		ProviderSelections map[string]string `json:"provider_selections"`
	}
	if err := decodeJSON([]byte(out), &v); err != nil {
		return "", "", ""
	}
	for src, ver := range v.ProviderSelections {
		providerVersion = src + " " + ver
		if strings.Contains(src, "hashicorp/google") {
			providerVersion = "hashicorp/google " + ver
			break
		}
	}
	return v.TerraformVersion, providerVersion, v.Platform
}
