//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"io"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// firestoreScenario compares the Cloud Firestore v1 document data plane over
// REST and gRPC. Firestore documents are the same google.protobuf.Value union
// as Datastore, and the REST surface addresses a document through the recursive
// "{+parent=…/documents/**}" binding — a nested subcollection is a document
// path, not a separate resource — so the scenario creates a top-level document
// and a nested subcollection document, reads both back over each transport,
// lists each collection, and drives CreateDocument/UpdateDocument
// mutation-parity steps. No projection is needed: the REST JSON and gRPC
// protojson encodings of the Value union coincide, including
// google.protobuf.NullValue, which both now render as JSON null (AUD3-13).
// It also cross-diffs the streaming query surface (AUD3-14):
// runQuery/batchGet return newline-delimited JSON over REST and an iterator over
// gRPC, so a StreamParity step aggregates each side's frames into one logical
// array and diffs them, proving the query data plane agrees across transports.
func firestoreScenario() Scenario {
	const database = "(default)"
	collection := func(e *Env) string { return "ParityCollection" + e.Cfg.Suffix }
	subcollection := func(e *Env) string { return "ParitySub" + e.Cfg.Suffix }
	project := func(e *Env) string { return e.Cfg.Project }
	// root is the database's documents root; both transports address it by the
	// same "projects/{p}/databases/{db}/documents" resource name.
	root := func(e *Env) string {
		return "projects/" + project(e) + "/databases/" + database + "/documents"
	}
	// docPath and subPath are the document paths relative to the documents root
	// (what a REST URL carries and a gRPC name appends to root).
	docPath := func(e *Env) string { return collection(e) + "/" + e.Resource("fs-doc") }
	subPath := func(e *Env) string {
		return docPath(e) + "/" + subcollection(e) + "/" + e.Resource("fs-subdoc")
	}
	// fsREST is the v1 REST URL for a collection or document path.
	fsREST := func(e *Env, path string) string {
		return "/v1/projects/" + project(e) + "/databases/" + database + "/documents/" + path
	}
	// fsVerb is the v1 REST URL for a custom method (:runQuery, :batchGet) on
	// the documents collection (empty path) or a document path within it.
	fsVerb := func(e *Env, path, verb string) string {
		url := "/v1/projects/" + project(e) + "/databases/" + database + "/documents"
		if path != "" {
			url += "/" + path
		}
		return url + ":" + verb
	}
	// structuredQuery selects the run's top-level collection, so the query
	// returns exactly the one canonical document both transports created.
	structuredQuery := func(e *Env) *firestorepb.StructuredQuery {
		return &firestorepb.StructuredQuery{
			From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: collection(e)}},
		}
	}
	// pipeline is an ExecutePipeline request that reads the same collection
	// through the pipeline DSL (a collection source stage), so the two
	// transports execute the same logical pipeline.
	pipeline := func(e *Env) *firestorepb.StructuredPipeline {
		return &firestorepb.StructuredPipeline{
			Pipeline: &firestorepb.Pipeline{Stages: []*firestorepb.Pipeline_Stage{{
				Name: "collection",
				Args: []*firestorepb.Value{{
					ValueType: &firestorepb.Value_ReferenceValue{ReferenceValue: "/" + collection(e)},
				}},
			}}},
		}
	}
	// batchDocs exercises all three batchGet result shapes: a found top-level
	// document, a found subcollection document (the recursive path) and a
	// missing document.
	batchDocs := func(e *Env) []string {
		return []string{
			root(e) + "/" + docPath(e),
			root(e) + "/" + subPath(e),
			root(e) + "/" + collection(e) + "/" + e.Resource("fs-missing"),
		}
	}

	// createGRPC creates a document whose fields exercise the whole Value union.
	createGRPC := func(ctx context.Context, e *Env, parent, coll, docID string, fields map[string]*firestorepb.Value) (protoMessage, error) {
		var out *firestorepb.Document
		err := fsDial(ctx, e, func(c firestorepb.FirestoreClient) error {
			var cerr error
			out, cerr = c.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
				Parent:       parent,
				CollectionId: coll,
				DocumentId:   docID,
				Document:     &firestorepb.Document{Fields: fields},
			})
			return cerr
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	// createREST creates the same logical document over REST. The body is
	// generated from the proto through protojson, so both transports send an
	// identical logical input (including the null variant, which protojson
	// renders as JSON null).
	createREST := func(ctx context.Context, e *Env, path, docID string, fields map[string]*firestorepb.Value) (json.RawMessage, error) {
		body, err := fsMarshal(&firestorepb.Document{Fields: fields})
		if err != nil {
			return nil, err
		}
		return e.Rest(ctx, "POST", fsREST(e, path)+"?documentId="+docID, body)
	}

	return Scenario{Service: "firestore", Steps: []Step{
		// Establish the canonical state once (gRPC side) so both transports
		// read identical documents: a top-level document and a document in a
		// subcollection that hangs off it (the recursive {+parent} binding).
		{
			Op: "CreateDocument",
			Mutate: func(ctx context.Context, e *Env) error {
				if _, err := createGRPC(ctx, e, root(e), collection(e), e.Resource("fs-doc"), firestoreFields(e, collection(e), "v1")); err != nil {
					return err
				}
				_, err := createGRPC(ctx, e, root(e)+"/"+docPath(e), subcollection(e), e.Resource("fs-subdoc"), firestoreFields(e, subcollection(e), "v1"))
				return err
			},
		},
		{
			Op: "GetDocument",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				var out *firestorepb.Document
				err := fsDial(ctx, e, func(c firestorepb.FirestoreClient) error {
					var cerr error
					out, cerr = c.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: root(e) + "/" + docPath(e)})
					return cerr
				})
				if err != nil {
					return nil, err
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", fsREST(e, docPath(e)), "")
			},
		},
		{
			// A document in a subcollection exercises the recursive
			// "{+parent}/documents/**" REST binding on the read path.
			Op: "GetDocument(nested)",
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				var out *firestorepb.Document
				err := fsDial(ctx, e, func(c firestorepb.FirestoreClient) error {
					var cerr error
					out, cerr = c.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: root(e) + "/" + subPath(e)})
					return cerr
				})
				if err != nil {
					return nil, err
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", fsREST(e, subPath(e)), "")
			},
		},
		{
			Op:    "ListDocuments",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				var out *firestorepb.ListDocumentsResponse
				err := fsDial(ctx, e, func(c firestorepb.FirestoreClient) error {
					var cerr error
					out, cerr = c.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{
						Parent:       root(e),
						CollectionId: collection(e),
					})
					return cerr
				})
				if err != nil {
					return nil, err
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", fsREST(e, collection(e)), "")
			},
		},
		{
			Op:    "ListDocuments(nested)",
			Scope: true,
			GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
				var out *firestorepb.ListDocumentsResponse
				err := fsDial(ctx, e, func(c firestorepb.FirestoreClient) error {
					var cerr error
					out, cerr = c.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{
						Parent:       root(e) + "/" + docPath(e),
						CollectionId: subcollection(e),
					})
					return cerr
				})
				if err != nil {
					return nil, err
				}
				return out, nil
			},
			REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, "GET", fsREST(e, docPath(e)+"/"+subcollection(e)), "")
			},
		},
		// The query surface streams (AUD3-14): REST answers newline-delimited
		// JSON, gRPC an iterator. Each side's frames are aggregated into one
		// logical array and diffed, so the streaming data plane is compared
		// frame-for-frame rather than being excluded for lack of a single body.
		{
			Op: "RunQuery",
			Stream: &StreamParity{
				GRPC: func(ctx context.Context, e *Env) ([]protoMessage, error) {
					var frames []protoMessage
					err := fsDial(ctx, e, func(c firestorepb.FirestoreClient) error {
						stream, cerr := c.RunQuery(ctx, &firestorepb.RunQueryRequest{
							Parent:    root(e),
							QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: structuredQuery(e)},
						})
						if cerr != nil {
							return cerr
						}
						for {
							msg, rerr := stream.Recv()
							if rerr == io.EOF {
								return nil
							}
							if rerr != nil {
								return rerr
							}
							frames = append(frames, msg)
						}
					})
					if err != nil {
						return nil, err
					}
					return frames, nil
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					body, err := fsMarshal(&firestorepb.RunQueryRequest{
						QueryType: &firestorepb.RunQueryRequest_StructuredQuery{StructuredQuery: structuredQuery(e)},
					})
					if err != nil {
						return nil, err
					}
					return e.Rest(ctx, "POST", fsVerb(e, "", "runQuery"), body)
				},
			},
		},
		{
			Op: "BatchGet",
			Stream: &StreamParity{
				GRPC: func(ctx context.Context, e *Env) ([]protoMessage, error) {
					var frames []protoMessage
					err := fsDial(ctx, e, func(c firestorepb.FirestoreClient) error {
						stream, cerr := c.BatchGetDocuments(ctx, &firestorepb.BatchGetDocumentsRequest{
							Database:  "projects/" + project(e) + "/databases/" + database,
							Documents: batchDocs(e),
						})
						if cerr != nil {
							return cerr
						}
						for {
							msg, rerr := stream.Recv()
							if rerr == io.EOF {
								return nil
							}
							if rerr != nil {
								return rerr
							}
							frames = append(frames, msg)
						}
					})
					if err != nil {
						return nil, err
					}
					return frames, nil
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					body, err := fsMarshal(&firestorepb.BatchGetDocumentsRequest{Documents: batchDocs(e)})
					if err != nil {
						return nil, err
					}
					return e.Rest(ctx, "POST", fsVerb(e, "", "batchGet"), body)
				},
			},
		},
		{
			Op: "ExecutePipeline",
			Stream: &StreamParity{
				GRPC: func(ctx context.Context, e *Env) ([]protoMessage, error) {
					var frames []protoMessage
					err := fsDial(ctx, e, func(c firestorepb.FirestoreClient) error {
						stream, cerr := c.ExecutePipeline(ctx, &firestorepb.ExecutePipelineRequest{
							Database: "projects/" + project(e) + "/databases/" + database,
							PipelineType: &firestorepb.ExecutePipelineRequest_StructuredPipeline{
								StructuredPipeline: pipeline(e),
							},
						})
						if cerr != nil {
							return cerr
						}
						for {
							msg, rerr := stream.Recv()
							if rerr == io.EOF {
								return nil
							}
							if rerr != nil {
								return rerr
							}
							frames = append(frames, msg)
						}
					})
					if err != nil {
						return nil, err
					}
					return frames, nil
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					body, err := fsMarshal(&firestorepb.ExecutePipelineRequest{
						PipelineType: &firestorepb.ExecutePipelineRequest_StructuredPipeline{
							StructuredPipeline: pipeline(e),
						},
					})
					if err != nil {
						return nil, err
					}
					return e.Rest(ctx, "POST", fsVerb(e, "", "executePipeline"), body)
				},
			},
		},
		{
			Op: "CreateDocument (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					return createGRPC(ctx, e, root(e), collection(e), e.Resource("fs-doc"), firestoreFields(e, collection(e), "v1"))
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					return createREST(ctx, e, collection(e), e.Resource("fs-doc"), firestoreFields(e, collection(e), "v1"))
				},
				Cleanup: func(ctx context.Context, e *Env) error {
					return e.RestDelete(ctx, fsREST(e, docPath(e)))
				},
			},
		},
		{
			Op: "UpdateDocument (parity)",
			Mutation: &MutationParity{
				GRPC: func(ctx context.Context, e *Env) (protoMessage, error) {
					// Stage the twin, then update it, so both transports diff an
					// update response rather than a second create.
					if _, err := createGRPC(ctx, e, root(e), collection(e), e.Resource("fs-doc"), firestoreFields(e, collection(e), "v1")); err != nil {
						return nil, err
					}
					var out *firestorepb.Document
					err := fsDial(ctx, e, func(c firestorepb.FirestoreClient) error {
						var cerr error
						out, cerr = c.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
							Document: &firestorepb.Document{
								Name:   root(e) + "/" + docPath(e),
								Fields: firestoreUpdateFields(),
							},
							UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"str"}},
						})
						return cerr
					})
					if err != nil {
						return nil, err
					}
					return out, nil
				},
				REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
					if _, err := createREST(ctx, e, collection(e), e.Resource("fs-doc"), firestoreFields(e, collection(e), "v1")); err != nil {
						return nil, err
					}
					body, err := fsMarshal(&firestorepb.Document{Fields: firestoreUpdateFields()})
					if err != nil {
						return nil, err
					}
					return e.Rest(ctx, "PATCH", fsREST(e, docPath(e))+"?updateMask.fieldPaths=str", body)
				},
				Cleanup: func(ctx context.Context, e *Env) error {
					return e.RestDelete(ctx, fsREST(e, docPath(e)))
				},
			},
		},
		// Remove the canonical documents.
		{
			Op: "DeleteDocument",
			Mutate: func(ctx context.Context, e *Env) error {
				if err := e.RestDelete(ctx, fsREST(e, subPath(e))); err != nil {
					return err
				}
				return e.RestDelete(ctx, fsREST(e, docPath(e)))
			},
		},
	}}
}

// fsDial opens a one-shot Firestore gRPC connection and runs fn with the
// generated stub client, mirroring dsDial (the generated pb package exports the
// stub client, not a high-level option-taking client).
func fsDial(ctx context.Context, e *Env, fn func(firestorepb.FirestoreClient) error) error {
	conn, err := grpc.NewClient(e.Cfg.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(firestorepb.NewFirestoreClient(conn))
}

// fsMarshal renders a request proto through protojson, so a REST body is
// generated from the exact logical request the gRPC side sends.
func fsMarshal(m protoMessage) (string, error) {
	b, err := protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}.Marshal(m)
	return string(b), err
}

// firestoreUpdateFields is the update mutation's field set: a single changed
// string under the mask, so the response is an update rather than a second
// create and the stored Value members are preserved.
func firestoreUpdateFields() map[string]*firestorepb.Value {
	return map[string]*firestorepb.Value{
		"str": {ValueType: &firestorepb.Value_StringValue{StringValue: "updated"}},
	}
}

// firestoreFields builds a document that exercises every member of the Value
// union (string, int, negative int, zero, double, true/false bool, null,
// timestamp, bytes, geoPoint, reference, array with a nested null, map with a
// nested null), so a per-member encoding divergence cannot hide. collection is
// the collection the document lives in, used to build the referenceValue.
func firestoreFields(e *Env, collection, marker string) map[string]*firestorepb.Value {
	ref := "projects/" + e.Cfg.Project + "/databases/(default)/documents/" + collection + "/ref-" + marker
	return map[string]*firestorepb.Value{
		"str":  {ValueType: &firestorepb.Value_StringValue{StringValue: "hello-" + marker}},
		"int":  {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 42}},
		"neg":  {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: -7}},
		"zero": {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 0}},
		"dbl":  {ValueType: &firestorepb.Value_DoubleValue{DoubleValue: 3.5}},
		"bool": {ValueType: &firestorepb.Value_BooleanValue{BooleanValue: true}},
		"fals": {ValueType: &firestorepb.Value_BooleanValue{BooleanValue: false}},
		"nul":  {ValueType: &firestorepb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}},
		"ts":   {ValueType: &firestorepb.Value_TimestampValue{TimestampValue: timestamppb.New(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC))}},
		"blob": {ValueType: &firestorepb.Value_BytesValue{BytesValue: []byte("bytes")}},
		"geo":  {ValueType: &firestorepb.Value_GeoPointValue{GeoPointValue: &latlng.LatLng{Latitude: 1.5, Longitude: 2.5}}},
		"arr": {ValueType: &firestorepb.Value_ArrayValue{ArrayValue: &firestorepb.ArrayValue{Values: []*firestorepb.Value{
			{ValueType: &firestorepb.Value_StringValue{StringValue: "a"}},
			{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 7}},
			{ValueType: &firestorepb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}},
		}}}},
		"map": {ValueType: &firestorepb.Value_MapValue{MapValue: &firestorepb.MapValue{Fields: map[string]*firestorepb.Value{
			"inner": {ValueType: &firestorepb.Value_StringValue{StringValue: "in-" + marker}},
			"nul":   {ValueType: &firestorepb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}},
		}}}},
		"ref": {ValueType: &firestorepb.Value_ReferenceValue{ReferenceValue: ref}},
	}
}
