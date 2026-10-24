//go:build gcp_parity

// Package gcpparity is the REST↔gRPC cross-transport parity harness for the
// jaiscloud GCP emulator (audit workstream AUD3).
//
// Structural dual-protocol parity is already closed: every service that is dual
// in real GCP shares one transport-neutral core and exposes both a REST and a
// gRPC adapter. What no existing gate proved is that the *same logical
// operation* returns equivalent data over both transports. A field that the
// gRPC adapter drops while transcoding (or that the REST adapter invents) passes
// every existing gate, because each transport's conformance check only asserts a
// key field.
//
// This harness drives a canonical create → get → list → mutate → delete flow
// per dual service, reads the resource back over BOTH transports at each read
// step, normalizes the two responses to logical fields, and diffs them. It also
// cross-diffs each mutation's response (AUD3-12): a mutation-parity step drives
// the same create/update over each transport on its own twin resource and diffs
// the two response bodies head to head (Create against Create, Update against
// Update), so a field one adapter drops or invents in a mutation response is
// caught exactly as it would be on a read. The per-service scenarios live in
// cases_*.go; the runner, normalizer and diff rules live in case.go, normalize.go
// and differ.go.
//
// The suite runs against a live emulator (REST :8080, gRPC :8081), like the REST
// wire-conformance and gRPC message-conformance harnesses, and is wired into
// `make test-gcp-rest-grpc-parity` / `make ga-check` and CI. Run it with
// `-tags gcp_parity,gcp_differential` (it reuses the differential normalizer/diff
// engine) plus `gcp_conformance` for the coverage gate, which reads the
// emulator's registries; the Makefile passes all three.
//
// Coverage boundary: reads cover Get/List bodies and mutations cover the
// resource-returning Create/Update responses. A server-streaming read
// (Firestore's runQuery/batchGet, which REST answers as newline-delimited JSON
// and gRPC as an iterator) is cross-diffed by a StreamParity step, which
// aggregates each transport's frames into one logical array. Delete has no
// response body to diff (gRPC returns google.protobuf.Empty, REST an empty
// body). Service Usage's
// EnableService returns an Operation whose response is the Service, so its
// parity step compares the settled Service; KMS key rings and crypto keys cannot
// be deleted by the real API, so their twins are intentionally left in place.
package gcpparity
