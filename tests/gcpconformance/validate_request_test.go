//go:build gcp_conformance

package gcpconformance

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestValidateRequestValue proves the request-side path: present-field types and
// unknown fields are checked, while `required` is deliberately not enforced.
func TestValidateRequestValue(t *testing.T) {
	doc := &DiscoveryDoc{Schemas: map[string]*Schema{
		"Req": {
			Type:     "object",
			Required: []string{"name"},
			Properties: map[string]*Schema{
				"name":  {Type: "string"},
				"count": {Type: "integer"},
			},
		},
	}}
	ref := &Schema{Ref: "Req"}

	// Requests do NOT enforce `required` (partial/PATCH bodies are legitimate).
	if d := ValidateRequestValue(doc, ref, map[string]any{"count": float64(1)}, "Req"); len(d) != 0 {
		t.Fatalf("missing required must be allowed for requests: %+v", d)
	}
	// ...but present-field types and unknown fields still are checked.
	if d := ValidateRequestValue(doc, ref, map[string]any{"count": "nope"}, "Req"); len(d) == 0 {
		t.Fatal("want a wrong_type finding for count")
	}
	if d := ValidateRequestValue(doc, ref, map[string]any{"bogus": true}, "Req"); len(d) == 0 {
		t.Fatal("want an unknown_field finding for bogus")
	}
	// Responses DO enforce required — the contrast.
	if d := ValidateValue(doc, ref, map[string]any{"count": float64(1)}, "Req"); len(d) == 0 {
		t.Fatal("response validation should flag the missing required field")
	}
}

// TestValidateNullValueEnumAcceptsJSONNull proves the google.protobuf.NullValue
// carve-out: real GCP's REST transcoder (protojson) emits JSON null for the
// single-value NullValue enum the Discovery schema documents as "NULL_VALUE"
// (AUD3-13), so a nil value is conformant for exactly that schema — but not for
// an ordinary string field, and a genuinely wrong enum name is still flagged.
func TestValidateNullValueEnumAcceptsJSONNull(t *testing.T) {
	doc := &DiscoveryDoc{Schemas: map[string]*Schema{
		"Value": {
			Type: "object",
			Properties: map[string]*Schema{
				"nullValue":   {Type: "string", Enum: []string{"NULL_VALUE"}},
				"stringValue": {Type: "string"},
			},
		},
	}}
	ref := &Schema{Ref: "Value"}

	if d := ValidateValue(doc, ref, map[string]any{"nullValue": nil}, "Value"); len(d) != 0 {
		t.Fatalf("JSON null must be conformant for the NullValue enum: %+v", d)
	}
	if d := ValidateValue(doc, ref, map[string]any{"nullValue": "NOT_A_VALUE"}, "Value"); len(d) == 0 {
		t.Fatal("a wrong enum name must still be flagged")
	}
	if d := ValidateValue(doc, ref, map[string]any{"stringValue": nil}, "Value"); len(d) == 0 {
		t.Fatal("JSON null must not be accepted for an ordinary string field")
	}
}

// TestValidateErrorEnvelopeStatusForHTTP400 proves the 400 map accepts every
// google.rpc status that maps to HTTP 400, not only INVALID_ARGUMENT, so a
// FAILED_PRECONDITION body is not reported as error.status divergence.
func TestValidateErrorEnvelopeStatusForHTTP400(t *testing.T) {
	body := []byte(`{"error":{"code":400,"message":"offset out of range","status":"FAILED_PRECONDITION"}}`)
	for _, d := range ValidateErrorEnvelope(400, body) {
		if d.Path == "error.status" && (d.Severity == "high" || d.Severity == "medium") {
			t.Fatalf("FAILED_PRECONDITION must be accepted for HTTP 400: %+v", d)
		}
	}
}

// TestValidateAnyPayload proves a google.protobuf.Any (the Discovery
// object-with-any-additionalProperties shape, e.g. Operation.metadata) is
// resolved through its "@type" URL and the payload validated against that
// message schema rather than treated as opaque bytes.
func TestValidateAnyPayload(t *testing.T) {
	doc := &DiscoveryDoc{Schemas: map[string]*Schema{
		"Operation": {
			Type: "object",
			Properties: map[string]*Schema{
				"metadata": {Type: "object", AdditionalProperties: []byte(`{"type":"any"}`)},
			},
		},
		"OperationMetadataV1": {
			Type: "object",
			Properties: map[string]*Schema{
				"target": {Type: "string"},
				"type":   {Type: "string", Enum: []string{"OPERATION_UNSPECIFIED", "CREATE_FUNCTION"}},
			},
		},
	}}
	ref := &Schema{Ref: "Operation"}
	metadata := func(typ string) map[string]any {
		return map[string]any{
			"metadata": map[string]any{
				"@type":  "type.googleapis.com/google.cloud.functions.v1.OperationMetadataV1",
				"target": "projects/p/locations/l/functions/f",
				"type":   typ,
			},
		}
	}

	if d := ValidateValue(doc, ref, metadata("CREATE_FUNCTION"), "Operation"); len(d) != 0 {
		t.Fatalf("valid Any payload flagged: %+v", d)
	}
	if d := ValidateValue(doc, ref, metadata(""), "Operation"); len(d) == 0 {
		t.Fatal("a zero enum name inside an Any payload must be flagged (protojson omits it)")
	}
	// A message the snapshot does not describe stays opaque rather than failing.
	if d := ValidateValue(doc, ref, map[string]any{"metadata": map[string]any{
		"@type": "type.googleapis.com/unknown.Service.Unknown", "anything": 1,
	}}, "Operation"); len(d) != 0 {
		t.Fatalf("unknown Any @type must stay unvalidated: %+v", d)
	}
}

// TestJSONStream proves a transcoded server-streaming response (newline-
// delimited JSON, e.g. Firestore documents:runQuery) is captured as elements
// instead of being dropped because the whole body is not one JSON document.
func TestJSONStream(t *testing.T) {
	stream, ok := jsonStream([]byte("{\"document\":{\"name\":\"a\"},\"readTime\":\"t\"}\n{\"done\":true}\n"))
	if !ok || len(stream) != 2 {
		t.Fatalf("jsonStream = %d elements, ok=%t; want 2, true", len(stream), ok)
	}
	if _, ok := jsonStream([]byte("<html>not json</html>")); ok {
		t.Fatal("a non-JSON body must not be captured as a stream")
	}
	if _, ok := jsonStream([]byte("{\"a\":1}\nnot json\n")); ok {
		t.Fatal("a stream with an invalid line must be rejected")
	}
	if _, ok := jsonStream([]byte("   \n")); ok {
		t.Fatal("an empty body is not a stream")
	}
}

// TestValidateStreamEntry proves every element of a captured stream is validated
// against the method's response schema, with its index in the report path.
func TestValidateStreamEntry(t *testing.T) {
	doc := &DiscoveryDoc{
		Name:    "firestore",
		Version: "v1",
		Resources: map[string]*Resource{
			"documents": {Methods: map[string]*Method{
				"runQuery": {
					ID: "firestore.documents.runQuery", Path: "v1/{+parent}:runQuery",
					HTTPMethod: "POST", Response: &Ref{Ref: "RunQueryResponse"},
				},
			}},
		},
		Schemas: map[string]*Schema{
			"RunQueryResponse": {
				Type: "object",
				Properties: map[string]*Schema{
					"done":     {Type: "boolean"},
					"readTime": {Type: "string"},
				},
			},
		},
	}
	docs := map[string]*DiscoveryDoc{"firestore": doc}

	good := Transcript{Entries: []Entry{{
		Service: "firestore", Method: "POST", Path: "/v1/projects/p/databases/d/documents:runQuery", Status: 200,
		Stream: []json.RawMessage{
			json.RawMessage(`{"readTime":"t"}`),
			json.RawMessage(`{"done":true,"readTime":"t"}`),
		},
	}}}
	if d := ValidateTranscripts(docs, good); len(d) != 0 {
		t.Fatalf("valid stream flagged: %+v", d)
	}

	bad := Transcript{Entries: []Entry{{
		Service: "firestore", Method: "POST", Path: "/v1/projects/p/databases/d/documents:runQuery", Status: 200,
		Stream: []json.RawMessage{
			json.RawMessage(`{"done":"yes"}`),
		},
	}}}
	divs := ValidateTranscripts(docs, bad)
	if len(divs) == 0 {
		t.Fatal("a wrong-typed stream element must be flagged")
	}
	for _, d := range divs {
		if !strings.Contains(d.Path, "stream[0]") {
			t.Errorf("divergence path = %q, want it to name stream[0]", d.Path)
		}
	}
}
