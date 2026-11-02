package bigquery

import (
	"context"
	"errors"
	"testing"

	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// discoveryNR is the normalized request the codec produces for a discovery
// probe: the default version and the origin the client called.
func discoveryNR(origin string) *model.NormalizedRequest {
	nr := newNR(map[string]any{"version": "v2"})
	if origin != "" {
		nr.Params[wire.BaseURLKey] = origin
	}
	nr.Action = "GetDiscovery"
	return nr
}

// TestGetDiscoveryDocumentShape pins the document the bq CLI builds its client
// from: it must be the real BigQuery v2 document with the origin rewritten and
// every $ref resolvable, or googleapiclient rejects it.
func TestGetDiscoveryDocumentShape(t *testing.T) {
	p := New(bqstore.NewMemoryStore())
	resp, err := p.GetDiscovery(context.Background(), discoveryNR("http://bq.local:8080"))
	if err != nil {
		t.Fatalf("GetDiscovery: %v", err)
	}
	if resp.HTTPStatus != 200 {
		t.Fatalf("status = %d, want 200", resp.HTTPStatus)
	}
	doc := resp.Data

	if got := doc["name"]; got != "bigquery" {
		t.Errorf("name = %v, want bigquery", got)
	}
	if got := doc["version"]; got != "v2" {
		t.Errorf("version = %v, want v2", got)
	}
	if got := doc["kind"]; got != "discovery#restDescription" {
		t.Errorf("kind = %v", got)
	}
	if got := doc["rootUrl"]; got != "http://bq.local:8080/" {
		t.Errorf("rootUrl = %v, want http://bq.local:8080/", got)
	}
	if got := doc["baseUrl"]; got != "http://bq.local:8080/bigquery/v2/" {
		t.Errorf("baseUrl = %v, want http://bq.local:8080/bigquery/v2/", got)
	}
	// Clients resolve method paths against rootUrl+servicePath, so it must stay
	// the real servicePath.
	if got := doc["servicePath"]; got != "bigquery/v2/" {
		t.Errorf("servicePath = %v, want bigquery/v2/", got)
	}

	schemas, _ := doc["schemas"].(map[string]any)
	if len(schemas) == 0 {
		t.Fatal("document has no schemas")
	}
	resources, _ := doc["resources"].(map[string]any)
	if len(resources) == 0 {
		t.Fatal("document has no resources")
	}

	// Every $ref anywhere in the document must resolve against schemas, or the
	// client's schema resolvers (googleapiclient, the Java/Python SDKs) reject
	// the document. Collect the targets of resources.*.methods.*.request/response
	// explicitly too, since those are the ones a client resolves eagerly.
	var refs []string
	collectRefs(doc, &refs)
	for _, ref := range refs {
		if _, ok := schemas[ref]; !ok {
			t.Errorf("$ref %q does not resolve in schemas", ref)
		}
	}
	methods := 0
	for resName, resAny := range resources {
		res, _ := resAny.(map[string]any)
		ms, _ := res["methods"].(map[string]any)
		for methodName, mAny := range ms {
			m, _ := mAny.(map[string]any)
			methods++
			for _, key := range []string{"request", "response"} {
				body, _ := m[key].(map[string]any)
				ref, _ := body["$ref"].(string)
				if ref == "" {
					continue
				}
				if _, ok := schemas[ref]; !ok {
					t.Errorf("%s.%s %s $ref %q does not resolve", resName, methodName, key, ref)
				}
			}
		}
	}
	if methods == 0 {
		t.Fatal("document declares no methods")
	}
}

func collectRefs(v any, out *[]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if k == "$ref" {
				if s, ok := val.(string); ok {
					*out = append(*out, s)
				}
				continue
			}
			collectRefs(val, out)
		}
	case []any:
		for _, val := range t {
			collectRefs(val, out)
		}
	}
}

// TestGetDiscoveryRejectsUnknownVersion covers the supported-version contract:
// only v2 is served (the emulator implements no other BigQuery API version).
func TestGetDiscoveryRejectsUnknownVersion(t *testing.T) {
	p := New(bqstore.NewMemoryStore())
	_, err := p.GetDiscovery(context.Background(), newNR(map[string]any{"version": "v1"}))
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("want ProviderError, got %v", err)
	}
	if pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
		t.Fatalf("got %d/%s, want 400/InvalidArgument", pe.HTTPStatus, pe.Code)
	}
}

// TestGetDiscoveryReturnsIndependentCopies guards the shared decoded snapshot:
// a caller (or a future test) mutating the document must not leak into the next
// response.
func TestGetDiscoveryReturnsIndependentCopies(t *testing.T) {
	p := New(bqstore.NewMemoryStore())
	first, err := p.GetDiscovery(context.Background(), discoveryNR("http://one.local"))
	if err != nil {
		t.Fatalf("GetDiscovery: %v", err)
	}
	firstDoc := first.Data
	firstDoc["rootUrl"] = "http://mutated/"
	schemas, _ := firstDoc["schemas"].(map[string]any)
	if len(schemas) > 0 {
		for k := range schemas {
			delete(schemas, k)
			break
		}
	}

	second, err := p.GetDiscovery(context.Background(), discoveryNR("http://two.local"))
	if err != nil {
		t.Fatalf("GetDiscovery: %v", err)
	}
	if got := second.Data["rootUrl"]; got != "http://two.local/" {
		t.Errorf("rootUrl = %v, want http://two.local/ (copy leaked)", got)
	}
	secondSchemas, _ := second.Data["schemas"].(map[string]any)
	if len(schemas)+1 != len(secondSchemas) {
		t.Errorf("schemas len = %d, want %d (copy leaked)", len(secondSchemas), len(schemas)+1)
	}
}
