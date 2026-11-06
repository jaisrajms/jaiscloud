//go:build gcp_conformance

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeScope(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scope.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadScopeValid(t *testing.T) {
	path := writeScope(t, `
sources:
  - service: storage
    resource: Bucket
    transports:
      grpc:
        message: google.storage.v2.Bucket
        depth: 2
`)
	scope, err := LoadScope(path)
	if err != nil {
		t.Fatalf("LoadScope: %v", err)
	}
	if len(scope.Sources) != 1 || scope.Sources[0].Service != "storage" {
		t.Fatalf("scope = %+v", scope)
	}
}

func TestLoadScopeRejectsBadSources(t *testing.T) {
	cases := map[string]string{
		"no sources": `
sources: []
`,
		"missing service": `
sources:
  - resource: Bucket
    transports:
      rest: {discovery: storage, schema: Bucket, depth: 1}
`,
		"unknown transport": `
sources:
  - service: storage
    resource: Bucket
    transports:
      http: {message: google.storage.v2.Bucket, depth: 1}
`,
		"no definition": `
sources:
  - service: storage
    resource: Bucket
    transports:
      rest: {depth: 1}
`,
		"discovery without schema": `
sources:
  - service: storage
    resource: Bucket
    transports:
      rest: {discovery: storage, depth: 1}
`,
		"schema without discovery": `
sources:
  - service: storage
    resource: Bucket
    transports:
      rest: {schema: Bucket, depth: 1}
`,
		"depth zero": `
sources:
  - service: storage
    resource: Bucket
    transports:
      rest: {discovery: storage, schema: Bucket, depth: 0}
`,
		"unknown transport twice": `
sources:
  - service: storage
    resource: Bucket
    transports:
      rest: {discovery: storage, schema: Bucket, depth: 1}
      rest2: {discovery: storage, schema: Bucket, depth: 1}
`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadScope(writeScope(t, body)); err == nil {
				t.Fatalf("LoadScope(%s) = nil error", name)
			}
		})
	}
}

func TestLoadScopeMissingFile(t *testing.T) {
	if _, err := LoadScope(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("LoadScope(missing) = nil error")
	}
}

func TestBuildUniverseRejectsDuplicateField(t *testing.T) {
	scope := &Scope{Sources: []ScopeSource{{
		Service:  "storage",
		Resource: "Bucket",
		Transports: map[string]ScopeTransport{
			"grpc": {Message: "google.storage.v2.Bucket", Depth: 1},
		},
	}}}
	if _, err := BuildUniverse(scope, nil); err != nil {
		t.Fatalf("BuildUniverse(valid): %v", err)
	}

	dup := &Scope{Sources: []ScopeSource{
		{Service: "svc", Resource: "Thing", Transports: map[string]ScopeTransport{"grpc": {Message: "google.storage.v2.Bucket", Depth: 1}}},
		{Service: "svc", Resource: "Thing", Transports: map[string]ScopeTransport{"grpc": {Message: "google.storage.v2.Bucket", Depth: 1}}},
	}}
	if _, err := BuildUniverse(dup, nil); err == nil {
		t.Fatal("BuildUniverse(duplicate) = nil error")
	}
}
