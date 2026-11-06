//go:build gcp_conformance

package main

import (
	providerschema "jaiscloud/tools/providerschema"
)

// FC6 hook — Terraform provider-schema input.
//
// The third authoritative source for field coverage is the Terraform provider's
// own schema (`terraform providers schema -json`): a provider attribute the
// module sends but the emulator does not serve is the TGM15/TGM1-3 class seen
// from the client side. FC6 (tools/tfcapture) joins captured request bodies to
// the provider schema and to docs/fidelity/field-coverage.csv, failing on a
// sent-but-unsupported attribute.
//
// The parser lives in tools/providerschema so the field inventory and the
// capture gate read one implementation. ProviderSchema is aliased here so the
// FC1 entry point (main.go) keeps its API.
type ProviderSchema = providerschema.ProviderSchema

// LoadProviderSchema reads a `terraform providers schema -json` dump.
func LoadProviderSchema(path string) (*ProviderSchema, error) {
	return providerschema.Load(path)
}
