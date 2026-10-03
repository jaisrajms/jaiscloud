package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	"jaiscloud/internal/model"

	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

// decodeProto marshals msg and decodes it through the codec as a protobuf POST.
func decodeProto(t *testing.T, c *Codec, project, verb string, msg proto.Message) (*model.NormalizedRequest, error) {
	t.Helper()
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal proto: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, "/v1/projects/"+project+":"+verb, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	return c.Decode(req, raw)
}

// dispatchProto decodes a protobuf request, dispatches it, and asserts decoding
// produced the protobuf response flag.
func dispatchProto(t *testing.T, c *Codec, p *Provider, project, verb string, msg proto.Message) (*model.NormalizedRequest, *model.ProviderResponse) {
	t.Helper()
	nr, err := decodeProto(t, c, project, verb, msg)
	if err != nil {
		t.Fatalf("decode %s: %v", verb, err)
	}
	if !protoResponseWanted(nr) {
		t.Fatalf("decode %s: protobuf response flag not set", verb)
	}
	resp, err := p.Routes()["Datastore."+verbActions[verb]](context.Background(), nr)
	if err != nil {
		t.Fatalf("dispatch %s: %v", verb, err)
	}
	return nr, resp
}

// encodeProtoResponseTo encodes resp and unmarshals the protobuf body into out.
func encodeProtoResponseTo(t *testing.T, c *Codec, nr *model.NormalizedRequest, resp *model.ProviderResponse, out proto.Message) {
	t.Helper()
	status, headers, body := c.Encode(nr, resp)
	if status != http.StatusOK {
		t.Fatalf("encode status = %d, want 200", status)
	}
	if got := headers.Get("Content-Type"); got != "application/x-protobuf" {
		t.Fatalf("Content-Type = %q, want application/x-protobuf", got)
	}
	if err := proto.Unmarshal(body, out); err != nil {
		t.Fatalf("unmarshal proto response: %v", err)
	}
}

func protoNameKey(kind, name string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: "test"},
		Path: []*datastorepb.Key_PathElement{
			{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: name}},
		},
	}
}

func protoIncompleteKey(kind string) *datastorepb.Key {
	return &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: kind}}}
}

func protoIntValue(n int64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n}}
}

func upsertMutation(kind, name string, n int64) *datastorepb.Mutation {
	return &datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: &datastorepb.Entity{
		Key:        protoNameKey(kind, name),
		Properties: map[string]*datastorepb.Value{"n": protoIntValue(n)},
	}}}
}

// ─── tests ────────────────────────────────────────────────────────────────────

func TestProtoCommitLookupRunQueryRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)

	commit := &datastorepb.CommitRequest{
		Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{
			upsertMutation("Task", "a", 1),
			upsertMutation("Task", "b", 2),
		},
	}
	nr, resp := dispatchProto(t, c, p, "test", "commit", commit)
	out := &datastorepb.CommitResponse{}
	encodeProtoResponseTo(t, c, nr, resp, out)
	if got := len(out.GetMutationResults()); got != 2 {
		t.Fatalf("mutationResults = %d, want 2", got)
	}

	lookup := &datastorepb.LookupRequest{Keys: []*datastorepb.Key{protoNameKey("Task", "a")}}
	nr, resp = dispatchProto(t, c, p, "test", "lookup", lookup)
	lout := &datastorepb.LookupResponse{}
	encodeProtoResponseTo(t, c, nr, resp, lout)
	if got := len(lout.GetFound()); got != 1 {
		t.Fatalf("found = %d, want 1", got)
	}
	if got := lout.GetFound()[0].GetEntity().GetProperties()["n"].GetIntegerValue(); got != 1 {
		t.Fatalf("found n = %d, want 1", got)
	}

	query := &datastorepb.RunQueryRequest{QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
		Kind: []*datastorepb.KindExpression{{Name: "Task"}},
		Filter: &datastorepb.Filter{FilterType: &datastorepb.Filter_PropertyFilter{
			PropertyFilter: &datastorepb.PropertyFilter{
				Property: &datastorepb.PropertyReference{Name: "n"},
				Op:       datastorepb.PropertyFilter_EQUAL,
				Value:    protoIntValue(2),
			},
		}},
	}}}
	nr, resp = dispatchProto(t, c, p, "test", "runQuery", query)
	qout := &datastorepb.RunQueryResponse{}
	encodeProtoResponseTo(t, c, nr, resp, qout)
	if got := len(qout.GetBatch().GetEntityResults()); got != 1 {
		t.Fatalf("entityResults = %d, want 1", got)
	}
	if got := qout.GetBatch().GetEntityResults()[0].GetEntity().GetProperties()["n"].GetIntegerValue(); got != 2 {
		t.Fatalf("query n = %d, want 2", got)
	}
}

func TestProtoCommitDeleteMutation(t *testing.T) {
	c, p := newTestProvider(t)

	seed := &datastorepb.CommitRequest{
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{upsertMutation("Task", "gone", 7)},
	}
	dispatchProto(t, c, p, "test", "commit", seed)

	del := &datastorepb.CommitRequest{
		Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{
			Delete: protoNameKey("Task", "gone"),
		}}},
	}
	dispatchProto(t, c, p, "test", "commit", del)

	lookup := &datastorepb.LookupRequest{Keys: []*datastorepb.Key{protoNameKey("Task", "gone")}}
	nr, resp := dispatchProto(t, c, p, "test", "lookup", lookup)
	lout := &datastorepb.LookupResponse{}
	encodeProtoResponseTo(t, c, nr, resp, lout)
	if got := len(lout.GetMissing()); got != 1 {
		t.Fatalf("missing = %d, want 1 (found=%d)", got, len(lout.GetFound()))
	}
}

func TestProtoTransactionAndKeys(t *testing.T) {
	c, p := newTestProvider(t)

	nr, resp := dispatchProto(t, c, p, "test", "beginTransaction", &datastorepb.BeginTransactionRequest{})
	bout := &datastorepb.BeginTransactionResponse{}
	encodeProtoResponseTo(t, c, nr, resp, bout)
	txn := bout.GetTransaction()
	if len(txn) == 0 {
		t.Fatal("beginTransaction returned an empty transaction")
	}

	nr, resp = dispatchProto(t, c, p, "test", "rollback", &datastorepb.RollbackRequest{Transaction: txn})
	rout := &datastorepb.RollbackResponse{}
	encodeProtoResponseTo(t, c, nr, resp, rout)

	nr, resp = dispatchProto(t, c, p, "test", "allocateIds", &datastorepb.AllocateIdsRequest{
		Keys: []*datastorepb.Key{protoIncompleteKey("Task")},
	})
	aout := &datastorepb.AllocateIdsResponse{}
	encodeProtoResponseTo(t, c, nr, resp, aout)
	if got := len(aout.GetKeys()); got != 1 {
		t.Fatalf("allocated keys = %d, want 1", got)
	}
	if got := aout.GetKeys()[0].GetPath()[0].GetId(); got == 0 {
		t.Fatal("allocated key id = 0, want generated id")
	}

	nr, resp = dispatchProto(t, c, p, "test", "reserveIds", &datastorepb.ReserveIdsRequest{
		Keys: []*datastorepb.Key{protoNameKey("Task", "reserved")},
	})
	resout := &datastorepb.ReserveIdsResponse{}
	encodeProtoResponseTo(t, c, nr, resp, resout)
}

func TestProtoRunAggregationQuery(t *testing.T) {
	c, p := newTestProvider(t)

	seed := &datastorepb.CommitRequest{
		Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{
			upsertMutation("Task", "a", 1),
			upsertMutation("Task", "b", 2),
		},
	}
	dispatchProto(t, c, p, "test", "commit", seed)

	req := &datastorepb.RunAggregationQueryRequest{
		QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
			AggregationQuery: &datastorepb.AggregationQuery{
				QueryType: &datastorepb.AggregationQuery_NestedQuery{
					NestedQuery: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: "Task"}}},
				},
				Aggregations: []*datastorepb.AggregationQuery_Aggregation{{
					Alias: "total",
					Operator: &datastorepb.AggregationQuery_Aggregation_Count_{
						Count: &datastorepb.AggregationQuery_Aggregation_Count{},
					},
				}},
			},
		},
	}
	nr, resp := dispatchProto(t, c, p, "test", "runAggregationQuery", req)
	out := &datastorepb.RunAggregationQueryResponse{}
	encodeProtoResponseTo(t, c, nr, resp, out)
	results := out.GetBatch().GetAggregationResults()
	if len(results) != 1 {
		t.Fatalf("aggregationResults = %d, want 1", len(results))
	}
	if got := results[0].GetAggregateProperties()["total"].GetIntegerValue(); got != 2 {
		t.Fatalf("count total = %d, want 2", got)
	}
}

func TestEncodeJSONUnchangedAndAltProto(t *testing.T) {
	c, p := newTestProvider(t)

	raw, _ := json.Marshal(map[string]any{"keys": []any{nameKey("Task", "a")}})
	req, err := http.NewRequest(http.MethodPost, "/v1/projects/test:lookup", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	nr, err := c.Decode(req, raw)
	if err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if protoResponseWanted(nr) {
		t.Fatal("JSON request unexpectedly selected protobuf response")
	}
	resp, err := p.Routes()["Datastore.Lookup"](context.Background(), nr)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	_, headers, body := c.Encode(nr, resp)
	if got := headers.Get("Content-Type"); got != "application/json; charset=UTF-8" {
		t.Fatalf("JSON Content-Type = %q", got)
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("JSON body: %v", err)
	}

	// A JSON body with alt=proto must still return protobuf.
	req2, err := http.NewRequest(http.MethodPost, "/v1/projects/test:lookup?alt=proto", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req2.Header.Set("Content-Type", "application/json")
	nr2, err := c.Decode(req2, raw)
	if err != nil {
		t.Fatalf("decode alt=proto: %v", err)
	}
	if !protoResponseWanted(nr2) {
		t.Fatal("alt=proto did not select protobuf response")
	}
	resp2, err := p.Routes()["Datastore.Lookup"](context.Background(), nr2)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	_, headers2, body2 := c.Encode(nr2, resp2)
	if got := headers2.Get("Content-Type"); got != "application/x-protobuf" {
		t.Fatalf("alt=proto Content-Type = %q", got)
	}
	var lp datastorepb.LookupResponse
	if err := proto.Unmarshal(body2, &lp); err != nil {
		t.Fatalf("alt=proto body: %v", err)
	}
}

func TestProtoErrorEnvelope(t *testing.T) {
	c, _ := newTestProvider(t)

	nr, err := decodeProto(t, c, "test", "commit", &datastorepb.CommitRequest{})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	perr := model.NewProviderError("InvalidArgument", "bad mutation", 400)
	status, headers, body := c.EncodeError(nr, perr)
	if status != http.StatusBadRequest {
		t.Fatalf("error status = %d, want 400", status)
	}
	if got := headers.Get("Content-Type"); got != "application/x-protobuf" {
		t.Fatalf("error Content-Type = %q, want application/x-protobuf", got)
	}
	var sp statuspb.Status
	if err := proto.Unmarshal(body, &sp); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if sp.GetCode() != int32(codes.InvalidArgument) {
		t.Fatalf("status code = %d, want %d", sp.GetCode(), int32(codes.InvalidArgument))
	}
	if sp.GetMessage() != "bad mutation" {
		t.Fatalf("status message = %q", sp.GetMessage())
	}

	// A JSON request keeps the JSON envelope.
	raw := []byte(`{}`)
	req, _ := http.NewRequest(http.MethodPost, "/v1/projects/test:commit", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	jnr, err := c.Decode(req, raw)
	if err != nil {
		t.Fatalf("decode json: %v", err)
	}
	_, jheaders, jbody := c.EncodeError(jnr, perr)
	if got := jheaders.Get("Content-Type"); got != "application/json; charset=UTF-8" {
		t.Fatalf("JSON error Content-Type = %q", got)
	}
	var env map[string]any
	if err := json.Unmarshal(jbody, &env); err != nil {
		t.Fatalf("JSON error body: %v", err)
	}
}

func TestMalformedProtoBody(t *testing.T) {
	c := NewCodec()
	raw := []byte{0xff, 0x00, 0x01} // field 31 with invalid wire type 7
	req, err := http.NewRequest(http.MethodPost, "/v1/projects/test:commit", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if _, err := c.Decode(req, raw); err == nil {
		t.Fatal("expected an error for a malformed protobuf body")
	}
}

func TestProtoResponseNegotiation(t *testing.T) {
	cases := []struct {
		name   string
		ct     string
		accept string
		url    string
		want   bool
	}{
		{"json default", "application/json", "", "/v1/projects/test:lookup", false},
		{"proto content type with params", "application/x-protobuf; charset=binary", "", "/v1/projects/test:lookup", true},
		{"protobuf content type alias", "application/protobuf", "", "/v1/projects/test:lookup", true},
		{"alt proto", "application/json", "", "/v1/projects/test:lookup?alt=proto", true},
		{"accept proto", "application/json", "application/x-protobuf", "/v1/projects/test:lookup", true},
		{"accept json listed first", "application/json", "application/json, application/x-protobuf", "/v1/projects/test:lookup", false},
		{"accept q prefers proto", "application/json", "application/json;q=0.2, application/x-protobuf;q=0.9", "/v1/projects/test:lookup", true},
		{"accept vendor +json is json", "application/json", "application/x-protobuf+json", "/v1/projects/test:lookup", false},
		{"accept irrelevant", "application/json", "text/plain", "/v1/projects/test:lookup", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, tc.url, nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			if tc.ct != "" {
				req.Header.Set("Content-Type", tc.ct)
			}
			if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			if got := protoResponseRequested(req); got != tc.want {
				t.Fatalf("protoResponseRequested = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestProtoNonFiniteDouble guards the protojson encode bridge against
// non-finite doubles, which encoding/json cannot represent.
func TestProtoNonFiniteDouble(t *testing.T) {
	c, p := newTestProvider(t)

	commit := &datastorepb.CommitRequest{
		Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{
			Operation: &datastorepb.Mutation_Upsert{Upsert: &datastorepb.Entity{
				Key: protoNameKey("Reading", "nan"),
				Properties: map[string]*datastorepb.Value{
					"d": {ValueType: &datastorepb.Value_DoubleValue{DoubleValue: math.NaN()}},
				},
			}},
		}},
	}
	dispatchProto(t, c, p, "test", "commit", commit)

	lookup := &datastorepb.LookupRequest{Keys: []*datastorepb.Key{protoNameKey("Reading", "nan")}}
	nr, resp := dispatchProto(t, c, p, "test", "lookup", lookup)
	out := &datastorepb.LookupResponse{}
	encodeProtoResponseTo(t, c, nr, resp, out)
	if got := len(out.GetFound()); got != 1 {
		t.Fatalf("found = %d, want 1", got)
	}
	if got := out.GetFound()[0].GetEntity().GetProperties()["d"].GetDoubleValue(); !math.IsNaN(got) {
		t.Fatalf("doubleValue = %v, want NaN", got)
	}
}
