package jaiscloud

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// contractFixture is the subset of the contract the guide must agree with.
type contractFixture struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Topology   struct {
		Namespaces []struct {
			Name string `yaml:"name"`
		} `yaml:"namespaces"`
	} `yaml:"topology"`
	Images []struct {
		Workload string `yaml:"workload"`
		Image    string `yaml:"image"`
	} `yaml:"images"`
	Playbook map[string]any `yaml:"playbook"`
}

func parseContract(t *testing.T) contractFixture {
	t.Helper()
	var c contractFixture
	if err := yaml.Unmarshal(AgentGuideContractYAML(), &c); err != nil {
		t.Fatalf("embedded contract does not parse: %v", err)
	}
	return c
}

// TestAgentGuideContract pins the embedded contract to the schema the guide
// advertises, and its key facts.
func TestAgentGuideContract(t *testing.T) {
	c := parseContract(t)
	if c.APIVersion != AgentGuideContractAPIVersion {
		t.Fatalf("contract apiVersion = %q, guide advertises %q", c.APIVersion, AgentGuideContractAPIVersion)
	}
	if c.Kind != "IntegrationContract" {
		t.Fatalf("contract kind = %q", c.Kind)
	}
	namespaces := map[string]bool{}
	for _, n := range c.Topology.Namespaces {
		namespaces[n.Name] = true
	}
	for _, want := range []string{"jaiscloud", "jaiscloud-gcp", "jaiscloud-data"} {
		if !namespaces[want] {
			t.Errorf("contract is missing namespace %q", want)
		}
	}
	if len(c.Images) == 0 {
		t.Fatal("contract has no images")
	}
	if len(c.Playbook) == 0 {
		t.Fatal("contract has no playbook")
	}
	for _, key := range []string{"scenarios", "failure_taxonomy", "destructive", "tooling"} {
		if _, ok := c.Playbook[key]; !ok {
			t.Errorf("playbook is missing %q", key)
		}
	}
}

// TestAgentGuideTextMatchesContract makes the short brief and the contract
// agree on the facts an agent will copy (image, ports, namespaces).
func TestAgentGuideTextMatchesContract(t *testing.T) {
	c := parseContract(t)
	images := map[string]bool{}
	for _, i := range c.Images {
		images[i.Image] = true
	}

	gcp := AgentGuideText("gcp", "v1.2.3", "abc1234", "2026-01-01")
	for _, want := range []string{
		"JaisCloud GCP", "jaiscloud-gcp", "jaiscloud_gcp",
		"http://127.0.0.1:8080/_jaiscloud/health",
		"agent-guide --emit", "agent-guide --json",
		"deploy/deploy.sh --check --json", "deploy/deploy.sh --migrate-store",
		"never rewrites :authority",
	} {
		if !strings.Contains(gcp, want) {
			t.Errorf("GCP guide is missing %q", want)
		}
	}
	if !images["jaisraj/jaiscloud-gcp:latest"] {
		t.Errorf("contract does not carry the GCP image the guide prints")
	}
	if !strings.Contains(gcp, "jaisraj/jaiscloud-gcp:latest") {
		t.Errorf("GCP guide does not print the published image")
	}

	aws := AgentGuideText("aws", "v1.2.3", "abc1234", "2026-01-01")
	for _, want := range []string{
		"JaisCloud AWS", "http://127.0.0.1:4566/_jaiscloud/health",
		"AWS_ENDPOINT_URL", "jaisraj/jaiscloud-aws:latest",
	} {
		if !strings.Contains(aws, want) {
			t.Errorf("AWS guide is missing %q", want)
		}
	}
	if !images["jaisraj/jaiscloud-aws:latest"] {
		t.Errorf("contract does not carry the AWS image the guide prints")
	}
}

// TestAgentGuideContractJSON ensures JSON-only consumers can read the contract.
func TestAgentGuideContractJSON(t *testing.T) {
	b, err := AgentGuideContractJSON()
	if err != nil {
		t.Fatalf("ContractJSON: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if m["kind"] != "IntegrationContract" {
		t.Fatalf("kind = %v", m["kind"])
	}
	if _, ok := m["topology"].(map[string]any); !ok {
		t.Fatalf("topology missing from JSON")
	}
}

// TestAgentGuideFiles checks the kit is complete enough for a binary-only user:
// the deploy script, the preflight it shells out to, the manifests, the Istio
// assets, the CI assets and the docs.
func TestAgentGuideFiles(t *testing.T) {
	files := AgentGuideFiles()
	have := map[string]bool{}
	for _, f := range files {
		have[f] = true
	}
	for _, want := range []string{
		"llms.txt",
		"README.md",
		"README-GCP.md",
		"DEVELOPER_GUIDE.md",
		"deploy/deploy.sh",
		"deploy/k8s/namespace-aws.yaml",
		"deploy/k8s/namespace-gcp.yaml",
		"deploy/k8s/namespace-data.yaml",
		"deploy/k8s/postgres.yaml",
		"deploy/k8s/rbac-shared.yaml",
		"deploy/k8s/rbac-aws.yaml",
		"deploy/k8s/rbac-gcp.yaml",
		"deploy/k8s/rbac-admission-policy.yaml",
		"deploy/k8s/jaiscloud.yaml",
		"deploy/k8s/jaiscloud-gcp.yaml",
		"deploy/k8s/istio/gateway.yaml",
		"deploy/k8s/istio/virtualservices.yaml",
		"deploy/k8s/istio/destinationrule.yaml",
		"deploy/k8s/istio/certs/gen.sh",
		"deploy/ci/docker-compose.ci.yml",
		"deploy/ci/ci-tls/gen.sh",
		"scripts/integration-preflight.sh",
		"scripts/rbac-isolation-gate.sh",
		"docs/gcp/integration/AGENTS.md",
		"docs/gcp/integration/contract.yaml",
		"docs/gcp/integration/topology.md",
		"docs/gcp/ci.md",
	} {
		if !have[want] {
			t.Errorf("kit is missing %q", want)
		}
	}
}

// TestAgentGuideEmit writes the kit, checks executable bits and the non-empty
// directory guard, then re-emits with force.
func TestAgentGuideEmit(t *testing.T) {
	dir := t.TempDir()
	written, err := AgentGuideEmit(dir, false)
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if len(written) != len(AgentGuideFiles()) {
		t.Errorf("wrote %d files, want %d", len(written), len(AgentGuideFiles()))
	}
	for _, rel := range []string{"deploy/deploy.sh", "scripts/integration-preflight.sh", "deploy/k8s/istio/certs/gen.sh"} {
		info, err := os.Stat(filepath.Join(dir, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("%s not written: %v", rel, err)
		}
		if info.Mode()&0o111 == 0 {
			t.Errorf("%s is not executable (mode %v)", rel, info.Mode())
		}
	}
	if _, err := AgentGuideEmit(dir, false); err == nil {
		t.Fatal("Emit into a non-empty directory succeeded without --force")
	}
	if _, err := AgentGuideEmit(dir, true); err != nil {
		t.Fatalf("Emit --force: %v", err)
	}
}

// TestAgentGuideCommand exercises the CLI surface an agent uses.
func TestAgentGuideCommand(t *testing.T) {
	cmd := AgentGuideCommand("gcp", "v1.2.3", "abc1234", "2026-01-01")
	cmd.SetArgs([]string{"--json"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("agent-guide --json: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(out.Bytes(), &m); err != nil {
		t.Fatalf("--json did not emit JSON: %v", err)
	}

	text := AgentGuideCommand("gcp", "v1.2.3", "abc1234", "2026-01-01")
	var textOut bytes.Buffer
	text.SetOut(&textOut)
	if err := text.Execute(); err != nil {
		t.Fatalf("agent-guide: %v", err)
	}
	if !strings.Contains(textOut.String(), "agent integration guide") {
		t.Fatalf("default output is not the guide: %q", textOut.String()[:80])
	}
}
