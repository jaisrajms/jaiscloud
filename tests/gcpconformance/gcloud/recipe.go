//go:build gcloud_conformance

// Package gcloud drives the real `gcloud` CLI against a running jaiscloud GCP
// emulator and records a per-command conformance matrix. It is a *reporting*
// suite: unsupported or known-incompatible commands are recorded, not fatal.
package gcloud

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"time"
)

// EmulatorEndpointDefault is the REST endpoint the ephemeral emulator listens on.
const EmulatorEndpointDefault = "http://localhost:8080"

// EmulatorProjectDefault matches the emulator's default project.
const EmulatorProjectDefault = "jaiscloud-project"

// endpointOverrideServices maps the gcloud `api_endpoint_overrides/<name>`
// property (uppercased into an environment variable) to every API surface the
// conformance table drives. jaiscloud serves all of them from one origin, so
// each override points at the same base; only the property name differs.
//
// BigQuery is deliberately absent: it is handled separately because the apiary
// client resolves method paths against the override and therefore needs the
// `/bigquery/v2` service path appended. The `bigquery` property is also not
// listed by `gcloud config list api_endpoint_overrides/ --all`, yet it is
// honored — both quirks are documented here because they are the crux of
// getting gcloud to talk to the emulator.
var endpointOverrideServices = []string{
	"STORAGE",
	"PUBSUB",
	"SECRETMANAGER",
	"CLOUDKMS",
	"DNS",
	"SQL",
	"COMPUTE",
	"CLOUDFUNCTIONS",
	"WORKFLOWS",
	"DATAPROC",
	// Cloud Functions' gen2 deploy checks that the Cloud Run/Cloud Build/
	// Artifact Registry APIs are enabled (Service Usage) and resolves the
	// project number (Resource Manager); both are emulated and must route here.
	"SERVICEUSAGE",
	"CLOUDRESOURCEMANAGER",
}

// BuildEnv returns the exact environment that makes gcloud talk to the emulator
// without touching the user's ~/.config/gcloud.
//
// Recipe (all environment, no credential files, no `gcloud config set`):
//
//	CLOUDSDK_CONFIG=<throwaway dir>              isolate config; never mutate the user's
//	CLOUDSDK_CORE_DISABLE_PROMPTS=1              never block on interactive prompts
//	CLOUDSDK_CORE_DISABLE_USAGE_REPORTING=true   no phone-home
//	CLOUDSDK_CORE_PROJECT=<project>              default project for unqualified commands
//	CLOUDSDK_AUTH_ACCESS_TOKEN=dummy             emulator accepts any bearer token; no login
//	CLOUDSDK_API_ENDPOINT_OVERRIDES_<SVC>=<ep>/  route every service at the emulator
//	CLOUDSDK_API_ENDPOINT_OVERRIDES_BIGQUERY=<ep>/bigquery/v2/
//
// The per-service override env var is the reliable mechanism. The global
// `--api-endpoint-overrides=<svc>=<url>` flag and `api_endpoint_overrides/<svc>`
// property are equivalent but require the property name to exist; the
// per-service env var works even for `bigquery`, which is not exposed as a
// configurable property.
func BuildEnv(endpoint, project, configDir, accessToken string) []string {
	base := strings.TrimRight(endpoint, "/")
	env := []string{
		"CLOUDSDK_CONFIG=" + configDir,
		"CLOUDSDK_CORE_DISABLE_PROMPTS=1",
		"CLOUDSDK_CORE_DISABLE_USAGE_REPORTING=true",
		"CLOUDSDK_CORE_PROJECT=" + project,
		"CLOUDSDK_AUTH_ACCESS_TOKEN=" + accessToken,
	}
	for _, svc := range endpointOverrideServices {
		env = append(env, "CLOUDSDK_API_ENDPOINT_OVERRIDES_"+svc+"="+base+"/")
	}
	env = append(env, "CLOUDSDK_API_ENDPOINT_OVERRIDES_BIGQUERY="+base+"/bigquery/v2/")
	return env
}

// Runner executes gcloud with a fixed environment and per-command timeout.
type Runner struct {
	Gcloud  string
	Env     []string
	Timeout time.Duration
}

// Result is the raw outcome of one gcloud invocation.
type Result struct {
	Args       []string
	ExitCode   int
	Stdout     string
	Stderr     string
	DurationMS int64
	Err        error
	TimedOut   bool
}

// Run executes one gcloud command (args excludes the binary itself). It never
// returns a nil Result; process-level failures are carried on Result.Err.
func (r *Runner) Run(args []string) Result {
	timeout := r.Timeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, r.Gcloud, args...)
	cmd.Env = r.Env

	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := time.Now()
	err := cmd.Run()
	res := Result{
		Args:       args,
		Stdout:     stdout.String(),
		Stderr:     stderr.String(),
		DurationMS: time.Since(start).Milliseconds(),
	}
	if err != nil {
		var exitErr *exec.ExitError
		switch {
		case errors.As(err, &exitErr):
			res.ExitCode = exitErr.ExitCode()
		default:
			res.ExitCode = -1
			res.Err = err
		}
	}
	if ctx.Err() == context.DeadlineExceeded {
		res.TimedOut = true
		res.Err = ctx.Err()
		if res.ExitCode == 0 {
			res.ExitCode = -1
		}
	}
	return res
}
