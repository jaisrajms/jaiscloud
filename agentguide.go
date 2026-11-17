package jaiscloud

// This file embeds the integration pack into the emulator binaries so a user who
// only has a container image or a release binary can still hand an LLM (or a
// script) accurate, versioned setup instructions and the deployment kit itself —
// no source tree required. The content is compiled in from the repository's
// canonical files, so it cannot drift from the release:
// `make check-integration-contract` keeps the contract fresh, and the tests in
// agentguide_test.go assert the guide matches it.

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
)

// AgentGuideContractAPIVersion is the schema version of the embedded contract.
const AgentGuideContractAPIVersion = "jaiscloud.dev/integration/v1"

//go:embed llms.txt
//go:embed README.md
//go:embed README-GCP.md
//go:embed DEVELOPER_GUIDE.md
//go:embed docs/gcp/ci.md
//go:embed all:docs/gcp/integration
//go:embed all:deploy/k8s
//go:embed all:deploy/ci
//go:embed deploy/deploy.sh
//go:embed scripts/integration-preflight.sh
var assets embed.FS

// Text is the short, self-contained brief printed by `agent-guide`. It is
// deliberately opinionated: the first thing an agent reads must be the safe path.
func AgentGuideText(cloud, version, commit, date string) string {
	if cloud == "" {
		cloud = "gcp"
	}
	isGCP := cloud == "gcp"
	label := "AWS"
	if isGCP {
		label = "GCP"
	}
	image := "jaisraj/jaiscloud-" + cloud + ":latest"
	runPorts := "-p 4566:4566"
	runArgs := "start --port 4566 --ephemeral"
	clientEnv := "  export AWS_ENDPOINT_URL=http://127.0.0.1:4566\n" +
		"  # or per-service --endpoint-url http://127.0.0.1:4566"
	database := "jaiscloud"
	if isGCP {
		runPorts = "-p 8080:8080 -p 8081:8081"
		runArgs = "start --port 8080 --grpc-port 8081 --ephemeral"
		clientEnv = "  export STORAGE_EMULATOR_HOST=http://127.0.0.1:8080\n" +
			"  export FIRESTORE_EMULATOR_HOST=127.0.0.1:8081\n" +
			"  export PUBSUB_EMULATOR_HOST=127.0.0.1:8081"
		database = "jaiscloud_gcp"
	}
	binary := "jaiscloud-" + cloud
	workload := "jaiscloud"
	if isGCP {
		workload = "jaiscloud-gcp"
	}
	install := ""
	if isGCP {
		install += "  ./deploy/deploy.sh --gcp --yes            # GCP substrate + shared store (public image)\n"
	}
	install += "  ./deploy/deploy.sh --skip-build --yes     # AWS substrate (kit is deployment-only: use a preloaded image)\n"
	install += "  ./deploy/deploy.sh --check --json         # READ-ONLY state + next safe step\n"
	install += "  ./deploy/deploy.sh --plan <mode>          # preview any mode; --skip-build for a preloaded image\n"
	install += "  ./deploy/deploy.sh --migrate-store --yes  # pre-split upgrade (destructive: confirm first)\n"
	install += "  mesh (optional): deploy/k8s/istio/README.md + deploy/k8s/istio/certs/gen.sh\n"
	install += "  (the kit has no source tree: --skip-build deploys an image you already have; --gcp never builds)\n"
	return fmt.Sprintf(`JaisCloud %s — agent integration guide
  binary   %s %s (commit %s, built %s)
  contract %s   (full facts: --json)

You do not need this source tree. This binary can emit the deployment kit:

  %s agent-guide --emit ./jaiscloud-kit    # manifests, scripts, docs, contract
  %s agent-guide --json                    # machine-readable facts + playbook
  # inside a cluster: kubectl -n <ns> exec deploy/%s -- /jaiscloud agent-guide

A. No Kubernetes: run the emulator
  docker run -d --name %s %s %s %s
  until curl -sf http://127.0.0.1:%s/_jaiscloud/health; do sleep 1; done
  clients:
%s
  isolation: POST /_jaiscloud/reset   seeding: GET /_jaiscloud/export + POST /_jaiscloud/import
  deterministic: --deterministic --time 2026-01-01T00:00:00Z --seed 1
  persistence: --dsn postgres://...:5432/%s --blob-dir /tmp/blobs (one database per cloud)

B. Existing Kubernetes cluster
  %s agent-guide --emit ./kit && cd kit
%s

Topology (exact objects/ports/env/images in --json)
  jaiscloud       AWS substrate   :4566 (LoadBalancer), instance aws-substrate-1
  jaiscloud-gcp   GCP substrate   REST :8080, gRPC :8081 (h2c), HMS :9083-9093, instance gcp-substrate-1
  jaiscloud-data  PostgreSQL      databases jaiscloud (AWS) + jaiscloud_gcp (GCP)
  istio-system    optional front door: TLS for *.amazonaws.com, *.googleapis.com,
                  *.run.app, *.cloudfunctions.net (HTTP(S) only; never rewrites :authority)

Do not
  - rewrite :authority/Host in the mesh (GKE vs Managed Kafka, Cloud Run, S3 virtual-hosted)
  - delete namespace jaiscloud-data or pvc postgres-pvc (shared store)
  - run --reset/--gcp-reset on state you care about; test-dual-substrate-k8s needs ALLOW_RESET=1
  - expose 443 on the GCP Service by default; TLS terminates at the front door
  - apply deploy/k8s/istio/*.yaml (the glob includes object-less dns.yaml)

The kit has the full docs: docs/gcp/integration/{AGENTS.md,topology.md,contract.yaml},
docs/gcp/ci.md, llms.txt. Read AGENTS.md first; act on deploy.sh --check --json, not on guesses.
`, label, binary, version, commit, date, AgentGuideContractAPIVersion,
		binary, binary, workload,
		workload, runPorts, image, runArgs, firstPort(isGCP),
		clientEnv, database,
		binary, install)
}

func firstPort(isGCP bool) string {
	if isGCP {
		return "8080"
	}
	return "4566"
}

// ContractYAML returns the embedded generated contract (facts + playbook).
func AgentGuideContractYAML() []byte {
	b, err := assets.ReadFile("docs/gcp/integration/contract.yaml")
	if err != nil {
		// Embedded at build time; a missing file is a programming error.
		panic("agent-guide: embedded contract missing: " + err.Error())
	}
	return b
}

// ContractJSON converts the embedded contract YAML to JSON (string keys, so
// agents that only speak JSON can consume it).
func AgentGuideContractJSON() ([]byte, error) {
	out, err := yaml.YAMLToJSON(AgentGuideContractYAML())
	if err != nil {
		return nil, fmt.Errorf("agent-guide: contract to JSON: %w", err)
	}
	var pretty any
	if err := json.Unmarshal(out, &pretty); err != nil {
		return nil, err
	}
	return json.MarshalIndent(pretty, "", "  ")
}

// Files lists the embedded kit paths, sorted.
func AgentGuideFiles() []string {
	var paths []string
	err := fs.WalkDir(assets, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		panic("agent-guide: walk assets: " + err.Error())
	}
	sort.Strings(paths)
	return paths
}

// Emit writes the embedded kit under dir, preserving the repository layout so
// the emitted deploy/deploy.sh and scripts/integration-preflight.sh work as-is.
// It refuses to write into a non-empty directory unless force is set.
func AgentGuideEmit(dir string, force bool) ([]string, error) {
	if dir == "" {
		return nil, fmt.Errorf("agent-guide: emit directory is required")
	}
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 && !force {
		return nil, fmt.Errorf("agent-guide: %s is not empty (use --force to overwrite)", dir)
	}
	var written []string
	for _, path := range AgentGuideFiles() {
		data, err := assets.ReadFile(path)
		if err != nil {
			return written, err
		}
		dst := filepath.Join(dir, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return written, err
		}
		mode := os.FileMode(0o644)
		if strings.HasSuffix(path, ".sh") {
			mode = 0o755
		}
		if err := os.WriteFile(dst, data, mode); err != nil {
			return written, err
		}
		written = append(written, dst)
	}
	sort.Strings(written)
	return written, nil
}

// Command returns the `agent-guide` cobra command for a binary.
func AgentGuideCommand(cloud, version, commit, date string) *cobra.Command {
	var (
		asJSON bool
		emit   string
		force  bool
	)
	cmd := &cobra.Command{
		Use:     "agent-guide",
		Aliases: []string{"help-agent"},
		Short:   "Print integration instructions for an LLM agent; --emit writes the deployment kit",
		Long: `Print a self-contained integration brief for an LLM agent or automation.

The brief covers running the emulator with no cluster and installing it into an
existing Kubernetes cluster, and points at the machine-readable contract.

  --json     print the full contract (namespaces, objects, images, env, ports,
             upgrade detection, failure taxonomy, destructive operations)
  --emit DIR write the deployment kit (manifests, deploy.sh, preflight script,
             Istio assets, CI assets, docs) so no source tree is needed;
             use --force to write into a non-empty directory

Inside a cluster without a shell in the image:
  kubectl -n <namespace> exec deploy/<workload> -- /jaiscloud agent-guide`,
		RunE: func(cmd *cobra.Command, args []string) error {
			switch {
			case asJSON && emit != "":
				return fmt.Errorf("--json and --emit are mutually exclusive")
			case asJSON:
				b, err := AgentGuideContractJSON()
				if err != nil {
					return err
				}
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s\n", b)
				return err
			case emit != "":
				written, err := AgentGuideEmit(emit, force)
				if err != nil {
					return err
				}
				out := cmd.OutOrStdout()
				fmt.Fprintf(out, "wrote %d files to %s\n\n", len(written), emit)
				fmt.Fprintf(out, "next:\n")
				fmt.Fprintf(out, "  cd %s\n", emit)
				fmt.Fprintf(out, "  ./deploy/deploy.sh --gcp --yes     # GCP substrate + shared store\n")
				fmt.Fprintf(out, "  ./deploy/deploy.sh --check --json  # read-only state + next step\n")
				fmt.Fprintf(out, "  cat docs/gcp/integration/AGENTS.md # the runbook\n")
				return nil
			default:
				_, err := fmt.Fprint(cmd.OutOrStdout(), AgentGuideText(cloud, version, commit, date))
				return err
			}
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the machine-readable integration contract")
	cmd.Flags().StringVar(&emit, "emit", "", "write the deployment kit to this directory")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite a non-empty --emit directory")
	return cmd
}
