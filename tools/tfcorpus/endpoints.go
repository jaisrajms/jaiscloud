// Package tfcorpus turns the provider-docs / Google-samples corpora into
// plan-only Terraform cases and triages the diagnostics (wave plan TF2).
//
// It also owns tests/integration/gcp/terraform/terraform-endpoints.yaml, the
// single service -> provider custom-endpoint map consumed by the fixture, the
// module driver and the corpus generator.
package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// EndpointField is one provider `*_custom_endpoint` attribute.
type EndpointField struct {
	// Field is the provider argument name, e.g. "storage_custom_endpoint".
	Field string `yaml:"field"`
	// Path is appended to the emulator origin, e.g. "/storage/v1/".
	Path string `yaml:"path"`
	// HostPrefix rewrites the endpoint host's first DNS label. The provider
	// keeps only scheme+host for these services, so the host carries the
	// service token the emulator routes by (container).
	HostPrefix string `yaml:"host_prefix,omitempty"`
}

// ServiceEndpoints is the endpoint set belonging to one emulated service.
type ServiceEndpoints struct {
	Endpoints []EndpointField `yaml:"endpoints"`
}

// EndpointsFile is tests/integration/gcp/terraform/terraform-endpoints.yaml.
type EndpointsFile struct {
	Provider        string                      `yaml:"provider"`
	CredentialEnv   string                      `yaml:"credential_env"`
	DefaultEndpoint string                      `yaml:"default_endpoint"`
	Services        map[string]ServiceEndpoints `yaml:"services"`
}

// LoadEndpoints reads and validates the endpoint map.
func LoadEndpoints(path string) (*EndpointsFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read endpoint map (%s): %w", path, err)
	}
	var e EndpointsFile
	if err := yaml.Unmarshal(raw, &e); err != nil {
		return nil, fmt.Errorf("parse endpoint map (%s): %w", path, err)
	}
	if err := e.validate(); err != nil {
		return nil, fmt.Errorf("endpoint map (%s): %w", path, err)
	}
	return &e, nil
}

func (e *EndpointsFile) validate() error {
	if strings.TrimSpace(e.Provider) == "" {
		return fmt.Errorf("provider is empty")
	}
	if len(e.Services) == 0 {
		return fmt.Errorf("no services")
	}
	seen := map[string]EndpointField{}
	for svc, s := range e.Services {
		if len(s.Endpoints) == 0 {
			return fmt.Errorf("service %q has no endpoints", svc)
		}
		for _, f := range s.Endpoints {
			if !strings.HasSuffix(f.Field, "_custom_endpoint") {
				return fmt.Errorf("service %q field %q is not a provider custom-endpoint attribute", svc, f.Field)
			}
			if !strings.HasPrefix(f.Path, "/") || !strings.HasSuffix(f.Path, "/") {
				return fmt.Errorf("service %q field %q path %q must start and end with /", svc, f.Field, f.Path)
			}
			if prev, ok := seen[f.Field]; ok {
				// firestore and firestoreadmin share one attribute; that is
				// fine only when they resolve to the same endpoint.
				if prev.Path != f.Path || prev.HostPrefix != f.HostPrefix {
					return fmt.Errorf("field %q is declared twice with different paths (%q, %q)", f.Field, prev.Path, f.Path)
				}
			}
			seen[f.Field] = f
		}
	}
	return nil
}

// Services returns the sorted service keys.
func (e *EndpointsFile) ServicesList() []string {
	out := make([]string, 0, len(e.Services))
	for svc := range e.Services {
		out = append(out, svc)
	}
	sort.Strings(out)
	return out
}

// fieldURL builds one endpoint value from the origin.
func fieldURL(base string, f EndpointField) (string, error) {
	u, err := url.Parse(strings.TrimSpace(base))
	if err != nil {
		return "", fmt.Errorf("endpoint %q: %w", base, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("endpoint %q must be an absolute URL", base)
	}
	if f.HostPrefix != "" {
		if !strings.HasPrefix(u.Host, f.HostPrefix+".") {
			u.Host = f.HostPrefix + "." + u.Host
		}
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + f.Path
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

// Values returns every provider endpoint attribute -> URL, for the generated
// provider block and tfvars. Duplicate fields (firestore/firestoreadmin) must
// agree.
func (e *EndpointsFile) Values(base string) (map[string]string, error) {
	out := map[string]string{}
	for _, svc := range e.ServicesList() {
		for _, f := range e.Services[svc].Endpoints {
			v, err := fieldURL(base, f)
			if err != nil {
				return nil, fmt.Errorf("%s/%s: %w", svc, f.Field, err)
			}
			if prev, ok := out[f.Field]; ok && prev != v {
				return nil, fmt.Errorf("field %q resolves to both %q and %q", f.Field, prev, v)
			}
			out[f.Field] = v
		}
	}
	return out, nil
}

// ServiceURL is the endpoint the driver helper prints for one service. The
// first field of a service is its canonical endpoint.
func (e *EndpointsFile) ServiceURL(base, service string) (string, error) {
	s, ok := e.Services[service]
	if !ok {
		return "", fmt.Errorf("unknown service %q (known: %s)", service, strings.Join(e.ServicesList(), ", "))
	}
	return fieldURL(base, s.Endpoints[0])
}

// ProviderSnippet renders the user-facing provider block: one block that points
// every emulated service at one endpoint.
func (e *EndpointsFile) ProviderSnippet(base, project, region string) (string, error) {
	values, err := e.Values(base)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# Generated from tests/integration/gcp/terraform/terraform-endpoints.yaml\n")
	fmt.Fprintf(&b, "# Endpoint: %s\n", base)
	fmt.Fprintf(&b, "provider \"google\" {\n")
	fmt.Fprintf(&b, "  project = %q\n", project)
	fmt.Fprintf(&b, "  region  = %q\n\n", region)
	fmt.Fprintf(&b, "  user_project_override = false\n\n")
	printed := map[string]bool{}
	for _, svc := range e.ServicesList() {
		for _, f := range e.Services[svc].Endpoints {
			if printed[f.Field] {
				// firestore and firestoreadmin share one attribute.
				continue
			}
			printed[f.Field] = true
			fmt.Fprintf(&b, "  %-36s = %q\n", f.Field, values[f.Field])
		}
	}
	fmt.Fprintf(&b, "}\n")
	return b.String(), nil
}

// WriteTfvars writes the `endpoints` variable map the fixture consumes.
func (e *EndpointsFile) WriteTfvars(path, base string) error {
	values, err := e.Values(base)
	if err != nil {
		return err
	}
	// Keep the JSON stable so a diff only shows a real endpoint change.
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make(map[string]string, len(values))
	for _, k := range keys {
		ordered[k] = values[k]
	}
	doc := map[string]any{"endpoints": ordered}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if path == "-" {
		_, err = os.Stdout.Write(raw)
		return err
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
