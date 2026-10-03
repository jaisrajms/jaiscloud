// Package paritygrpc holds the gRPC-only GCP Postgres persistence-parity suite.
//
// The probes live behind the gcp_persistence build tag, mirroring the REST
// parity suite. This file deliberately has no build constraint so the module
// still exposes a package to `go vet ./...` / `go build ./...` when the tag is
// not set.
package paritygrpc
