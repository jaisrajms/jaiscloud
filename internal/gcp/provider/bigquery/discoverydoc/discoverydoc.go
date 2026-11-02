// Package discoverydoc serves the BigQuery v2 Discovery document.
//
// The bq CLI (google-cloud-sdk >= 469) fetches
// {--api}/$discovery/rest?version=v2 and builds its whole client from the
// response, so a BigQuery emulator that does not serve a document is
// unreachable for the standalone CLI; older SDKs probe the legacy
// /discovery/v1/apis/bigquery/v2/rest path instead. Both are answered from the
// real v2 snapshot the conformance harness vendors under
// tests/gcpconformance/discovery/bigquery.json.gz.
//
// The snapshot is embedded here as a copy because Go embed patterns cannot
// reach ../tests/..., and a test pins the two files byte-identical so the copy
// cannot drift from the vendored original.
//
// Clients resolve method paths against rootUrl+servicePath (googleapiclient's
// build_from_document), so the document keeps its servicePath and rewrites only
// the origin: rootUrl becomes the endpoint the client called and baseUrl the
// same origin plus the servicePath, exactly the shape real BigQuery serves.
package discoverydoc

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// Name and Version identify the served document.
const (
	Name    = "bigquery"
	Version = "v2"
)

// ServicePath is the Discovery servicePath the document declares. Method paths
// are resolved against it, so it must stay the real "bigquery/v2/".
const ServicePath = "bigquery/v2/"

//go:embed bigquery_v2.json.gz
var compressed []byte

var (
	loadOnce sync.Once
	loaded   map[string]any
	loadErr  error
)

// decode once: the gzip and JSON parse cost is paid on the first discovery
// request (or CLI invocation), not at process start.
func decode() (map[string]any, error) {
	loadOnce.Do(func() {
		zr, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			loadErr = fmt.Errorf("open embedded bigquery discovery document: %w", err)
			return
		}
		defer zr.Close()
		var doc map[string]any
		if err := json.NewDecoder(zr).Decode(&doc); err != nil {
			loadErr = fmt.Errorf("decode embedded bigquery discovery document: %w", err)
			return
		}
		loaded = doc
	})
	return loaded, loadErr
}

// Document returns a deep copy of the discovery document with rootUrl and
// baseUrl rewritten to origin ("scheme://host", without a trailing slash).
// Each request gets its own copy because clients (and tests) mutate the map
// they receive.
func Document(origin string) (map[string]any, error) {
	doc, err := decode()
	if err != nil {
		return nil, err
	}
	origin = strings.TrimSuffix(origin, "/")
	copied, ok := deepCopy(doc).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("embedded bigquery discovery document is not an object")
	}
	copied["rootUrl"] = origin + "/"
	copied["baseUrl"] = origin + "/" + ServicePath
	return copied, nil
}

// deepCopy copies the JSON-decoded value tree (map[string]any, []any and
// scalars) so a caller's mutation cannot leak into the shared decoded document.
func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = deepCopy(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = deepCopy(val)
		}
		return out
	default:
		return v
	}
}
