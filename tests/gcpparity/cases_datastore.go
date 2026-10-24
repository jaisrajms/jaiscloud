//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	latlng "google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// datastoreScenario compares the Cloud Datastore v1 data plane over REST and
// gRPC. Datastore has no create/get/list/delete resource lifecycle — a Commit
// writes an entity addressed by a Key and a Lookup/RunQuery reads it back — so
// the canonical flow is Commit(upsert) → Lookup(found) → Lookup(missing) →
// RunQuery → RunAggregationQuery → Commit delete, with Commit upsert and update
// mutation-parity steps for the mutation-response gate (AUD3-12). No projection
// is needed: the REST Entity JSON and the gRPC proto value union are the same
// logical data encoded two ways, and every member — including
// google.protobuf.NullValue, which both now render as JSON null — coincides
// (AUD3-13).
func datastoreScenario() Scenario {
	kind := func(e *Env) string { return "ParityEntity" + e.Cfg.Suffix }
	refKind := func(e *Env) string { return "ParityRef" + e.Cfg.Suffix }
	entityName := func(e *Env) string { return e.Resource("ds-entity") }
	project := func(e *Env) string { return e.Cfg.Project }

	// dsKey is the Key for a single-element name path in the scenario's kind.
	dsKey := func(e *Env, name string) *datastorepb.Key {
		return &datastorepb.Key{
			PartitionId: &datastorepb.PartitionId{ProjectId: project(e)},
			Path: []*datastorepb.Key_PathElement{
				{Kind: kind(e), IdType: &datastorepb.Key_PathElement_Name{Name: name}},
			},
		}
	}
	// dsUpsert is the Commit mutation that writes entity for name.
	dsUpsert := func(e *Env, name, marker string) *datastorepb.CommitRequest {
		return &datastorepb.CommitRequest{
			ProjectId: project(e),
			Mutations: []*datastorepb.Mutation{{
				Operation: &datastorepb.Mutation_Upsert{Upsert: datastoreEntity(e, dsKey(e, name), refKind(e), marker)},
			}},
		}
	}
	dsDeleteReq := func(e *Env, name string) *datastorepb.CommitRequest {
		return &datastorepb.CommitRequest{
			ProjectId: project(e),
			Mutations: []*datastorepb.Mutation{{
				Operation: &datastorepb.Mutation_Delete{Delete: dsKey(e, name)},
			}},
		}
	}
	// dsMarshal renders a request proto through protojson, so the REST body is
	// generated from the exact logical request the gRPC side sends.
	dsMarshal := func(m protoMessage) (string, error) {
		b, err := protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}.Marshal(m)
		return string(b), err
	}
	commitGRPC := func(ctx context.Context, e *Env, req *datastorepb.CommitRequest) (protoMessage, error) {
		var out *datastorepb.CommitResponse
		err := dsDial(ctx, e, func(c datastorepb.DatastoreClient) error {
			var cerr error
			out, cerr = c.Commit(ctx, req)
			return cerr
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	commitREST := func(ctx context.Context, e *Env, req *datastorepb.CommitRequest) (json.RawMessage, error) {
		body, err := dsMarshal(req)
		if err != nil {
			return nil, err
		}
		return e.Rest(ctx, "POST", "/v1/projects/"+project(e)+":commit", body)
	}

	// Mutation-parity Update: stage the twin (the create half, discarded), then
	// commit the changed entity and return that response, so both transports
	// diff an update rather than a second create.
	updateGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		if _, err := commitGRPC(ctx, e, dsUpsert(e, entityName(e), "v1")); err != nil {
			return nil, err
		}
		return commitGRPC(ctx, e, dsUpsert(e, entityName(e), "v2"))
	}
	updateREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		if _, err := commitREST(ctx, e, dsUpsert(e, entityName(e), "v1")); err != nil {
			return nil, err
		}
		return commitREST(ctx, e, dsUpsert(e, entityName(e), "v2"))
	}
	// Cleanup deletes the twin through REST; the emulator is one shared origin,
	// so either transport removes the same key.
	cleanup := func(ctx context.Context, e *Env) error {
		_, err := commitREST(ctx, e, dsDeleteReq(e, entityName(e)))
		return err
	}

	return Scenario{Service: "datastore", Steps: []Step{
		// Establish the canonical entity once (gRPC side) so both transports
		// read the identical state.
		{
			Op: "Commit(upsert)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := commitGRPC(ctx, e, dsUpsert(e, entityName(e), "v1"))
				return err
			},
		},
		{
			Op: "Lookup",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				var out *datastorepb.LookupResponse
				err := dsDial(ctx, e, func(c datastorepb.DatastoreClient) error {
					var cerr error
					out, cerr = c.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: project(e), Keys: []*datastorepb.Key{dsKey(e, entityName(e))}})
					return cerr
				})
				if err != nil {
					return nil, err
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				body, err := dsMarshal(&datastorepb.LookupRequest{ProjectId: project(e), Keys: []*datastorepb.Key{dsKey(e, entityName(e))}})
				if err != nil {
					return nil, err
				}
				return e.Rest(ctx, "POST", "/v1/projects/"+project(e)+":lookup", body)
			},
		},
		{
			Op: "Lookup(missing)",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				var out *datastorepb.LookupResponse
				err := dsDial(ctx, e, func(c datastorepb.DatastoreClient) error {
					var cerr error
					out, cerr = c.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: project(e), Keys: []*datastorepb.Key{dsKey(e, e.Resource("ds-absent"))}})
					return cerr
				})
				if err != nil {
					return nil, err
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				body, err := dsMarshal(&datastorepb.LookupRequest{ProjectId: project(e), Keys: []*datastorepb.Key{dsKey(e, e.Resource("ds-absent"))}})
				if err != nil {
					return nil, err
				}
				return e.Rest(ctx, "POST", "/v1/projects/"+project(e)+":lookup", body)
			},
		},
		{
			Op: "RunQuery",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				var out *datastorepb.RunQueryResponse
				err := dsDial(ctx, e, func(c datastorepb.DatastoreClient) error {
					var cerr error
					out, cerr = c.RunQuery(ctx, &datastorepb.RunQueryRequest{
						ProjectId: project(e),
						QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind(e)}}}},
					})
					return cerr
				})
				if err != nil {
					return nil, err
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				body, err := dsMarshal(&datastorepb.RunQueryRequest{
					ProjectId: project(e),
					QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{Kind: []*datastorepb.KindExpression{{Name: kind(e)}}}},
				})
				if err != nil {
					return nil, err
				}
				return e.Rest(ctx, "POST", "/v1/projects/"+project(e)+":runQuery", body)
			},
		},
		{
			// An aggregation response is a Value union too (aggregateProperties),
			// so it is a second surface the two transports must encode alike.
			Op: "RunAggregationQuery",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				var out *datastorepb.RunAggregationQueryResponse
				err := dsDial(ctx, e, func(c datastorepb.DatastoreClient) error {
					var cerr error
					out, cerr = c.RunAggregationQuery(ctx, &datastorepb.RunAggregationQueryRequest{
						ProjectId: project(e),
						QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: dsAggregation(kind(e))},
					})
					return cerr
				})
				if err != nil {
					return nil, err
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				body, err := dsMarshal(&datastorepb.RunAggregationQueryRequest{
					ProjectId: project(e),
					QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{AggregationQuery: dsAggregation(kind(e))},
				})
				if err != nil {
					return nil, err
				}
				return e.Rest(ctx, "POST", "/v1/projects/"+project(e)+":runAggregationQuery", body)
			},
		},
		{
			Op: "Commit(upsert) (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					return commitGRPC(ctx, e, dsUpsert(e, entityName(e), "v1"))
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					return commitREST(ctx, e, dsUpsert(e, entityName(e), "v1"))
				},
				Cleanup: cleanup,
			},
		},
		{
			Op: "Commit(update) (parity)",
			Mutation: &MutationParity{
				GRPC:    updateGRPC,
				REST:    updateREST,
				Cleanup: cleanup,
			},
		},
		// Remove the canonical entity.
		{
			Op: "Commit(delete)",
			Mutate: func(ctx context.Context, e *Env) error {
				_, err := commitREST(ctx, e, dsDeleteReq(e, entityName(e)))
				return err
			},
		},
	}}
}

// dsDial opens a one-shot Datastore gRPC connection and runs fn with the
// generated stub client, mirroring the gRPC error probes in errors_test.go (the
// generated pb package exports the stub client, not a high-level option-taking
// client).
func dsDial(ctx context.Context, e *Env, fn func(datastorepb.DatastoreClient) error) error {
	conn, err := grpc.NewClient(e.Cfg.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(datastorepb.NewDatastoreClient(conn))
}

// dsAggregation is a COUNT(*) aggregate over kind, so the aggregation response
// carries a Value (the count) the projection must reconcile.
func dsAggregation(kind string) *datastorepb.AggregationQuery {
	return &datastorepb.AggregationQuery{
		QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: &datastorepb.Query{
			Kind: []*datastorepb.KindExpression{{Name: kind}},
		}},
		Aggregations: []*datastorepb.AggregationQuery_Aggregation{{
			Alias:    "total",
			Operator: &datastorepb.AggregationQuery_Aggregation_Count_{Count: &datastorepb.AggregationQuery_Aggregation_Count{}},
		}},
	}
}

// datastoreEntity builds an entity that exercises every member of the Value
// union (and a nested entity, an array, a null, and a key-valued property) so a
// per-member encoding divergence cannot hide.
func datastoreEntity(e *Env, key *datastorepb.Key, refKind, marker string) *datastorepb.Entity {
	ref := &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: e.Cfg.Project},
		Path: []*datastorepb.Key_PathElement{
			{Kind: refKind, IdType: &datastorepb.Key_PathElement_Name{Name: "ref-" + marker}},
		},
	}
	return &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{
		"str":  {ValueType: &datastorepb.Value_StringValue{StringValue: "hello-" + marker}},
		"int":  {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 42}},
		"neg":  {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: -7}},
		"zero": {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 0}},
		"dbl":  {ValueType: &datastorepb.Value_DoubleValue{DoubleValue: 3.5}},
		"bool": {ValueType: &datastorepb.Value_BooleanValue{BooleanValue: true}},
		"fals": {ValueType: &datastorepb.Value_BooleanValue{BooleanValue: false}},
		"nul":  {ValueType: &datastorepb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}},
		"ts":   {ValueType: &datastorepb.Value_TimestampValue{TimestampValue: timestamppb.New(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC))}},
		"blob": {ValueType: &datastorepb.Value_BlobValue{BlobValue: []byte("bytes")}},
		"geo":  {ValueType: &datastorepb.Value_GeoPointValue{GeoPointValue: &latlng.LatLng{Latitude: 1.5, Longitude: 2.5}}},
		"arr": {ValueType: &datastorepb.Value_ArrayValue{ArrayValue: &datastorepb.ArrayValue{Values: []*datastorepb.Value{
			{ValueType: &datastorepb.Value_StringValue{StringValue: "a"}},
			{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 7}},
		}}}},
		"nested": {ValueType: &datastorepb.Value_EntityValue{EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{
			"inner": {ValueType: &datastorepb.Value_StringValue{StringValue: "in-" + marker}},
		}}}},
		// excludeFromIndexes is not echoed by either transport today; it is
		// included so a future one-sided echo surfaces in the diff.
		"exi": {ValueType: &datastorepb.Value_StringValue{StringValue: "x"}, ExcludeFromIndexes: true},
		"ref": {ValueType: &datastorepb.Value_KeyValue{KeyValue: ref}},
	}}
}
