//go:build gcp_differential

package gcpdifferential

import (
	"context"
	"time"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// GRPCScenarios returns the curated gRPC request list (AUD6-1) for the given
// project and run suffix. It targets the surfaces the REST differential
// declared out of scope because they are gRPC-first: Cloud Datastore,
// Firestore, Cloud Logging and Cloud Monitoring, plus (AUD6-4)
// google.longrunning.Operations reached on the Dataproc endpoint. Each
// service's flow is read-mostly, self-cleaning and scoped to
// run-suffixed names, so a recording leaves nothing behind and the committed
// golden carries no project- or run-specific string.
//
// As on the REST side, a scenario with no committed golden is "pending
// recording" and skipped by TestReplayGRPC, so the list may grow ahead of the
// next real-GCP capture.
func GRPCScenarios(project, suffix string) []GRPCScenario {
	n := Names(suffix)

	var sc []GRPCScenario

	// ─── Cloud Datastore v1 (data plane, Value union) ─────────────────────────
	// Commit an entity exercising the Value projection, read it back, miss it,
	// query it, aggregate it, then delete it. The kind and entity are
	// run-suffixed, so a real-GCP RunQuery sees only this run's entity.
	dsKind := n.DSKind
	dsEntity := n.DSEntity
	dsKey := func(name string) *datastorepb.Key {
		return &datastorepb.Key{
			PartitionId: &datastorepb.PartitionId{ProjectId: project},
			Path:        []*datastorepb.Key_PathElement{{Kind: dsKind, IdType: &datastorepb.Key_PathElement_Name{Name: name}}},
		}
	}
	dsEntityProto := func(name string) *datastorepb.Entity {
		return &datastorepb.Entity{
			Key: dsKey(name),
			Properties: map[string]*datastorepb.Value{
				"greeting": {ValueType: &datastorepb.Value_StringValue{StringValue: "hello jaiscloud"}},
				"count":    {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 7}},
			},
		}
	}
	dsUpsert := func(name string) *datastorepb.CommitRequest {
		return &datastorepb.CommitRequest{
			ProjectId: project,
			Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
			Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: dsEntityProto(name)}}},
		}
	}
	dsDelete := func(name string) *datastorepb.CommitRequest {
		return &datastorepb.CommitRequest{
			ProjectId: project,
			Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
			Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: dsKey(name)}}},
		}
	}

	sc = append(sc,
		GRPCScenario{
			Service: "datastore", Op: "ds_commit_upsert",
			Method: "google.datastore.v1.Datastore/Commit", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := dsUpsert(dsEntity)
				resp, err := dsCommit(ctx, t, req)
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "datastore", Op: "ds_lookup_found",
			Method: "google.datastore.v1.Datastore/Lookup", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &datastorepb.LookupRequest{ProjectId: project, Keys: []*datastorepb.Key{dsKey(dsEntity)}}
				var resp *datastorepb.LookupResponse
				err := withDatastore(ctx, t, func(c datastorepb.DatastoreClient) error {
					var cerr error
					resp, cerr = c.Lookup(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "datastore", Op: "ds_lookup_missing",
			Method: "google.datastore.v1.Datastore/Lookup", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &datastorepb.LookupRequest{ProjectId: project, Keys: []*datastorepb.Key{dsKey("missing-" + suffix)}}
				var resp *datastorepb.LookupResponse
				err := withDatastore(ctx, t, func(c datastorepb.DatastoreClient) error {
					var cerr error
					resp, cerr = c.Lookup(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "datastore", Op: "ds_run_query",
			Method: "google.datastore.v1.Datastore/RunQuery", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &datastorepb.RunQueryRequest{
					ProjectId: project,
					QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
						Kind: []*datastorepb.KindExpression{{Name: dsKind}},
					}},
				}
				var resp *datastorepb.RunQueryResponse
				err := withDatastore(ctx, t, func(c datastorepb.DatastoreClient) error {
					var cerr error
					resp, cerr = c.RunQuery(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "datastore", Op: "ds_run_aggregation_query",
			Method: "google.datastore.v1.Datastore/RunAggregationQuery", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &datastorepb.RunAggregationQueryRequest{
					ProjectId: project,
					QueryType: &datastorepb.RunAggregationQueryRequest_AggregationQuery{
						AggregationQuery: &datastorepb.AggregationQuery{
							QueryType: &datastorepb.AggregationQuery_NestedQuery{NestedQuery: &datastorepb.Query{
								Kind: []*datastorepb.KindExpression{{Name: dsKind}},
							}},
							Aggregations: []*datastorepb.AggregationQuery_Aggregation{{
								Alias:    "total",
								Operator: &datastorepb.AggregationQuery_Aggregation_Count_{Count: &datastorepb.AggregationQuery_Aggregation_Count{}},
							}},
						},
					},
				}
				var resp *datastorepb.RunAggregationQueryResponse
				err := withDatastore(ctx, t, func(c datastorepb.DatastoreClient) error {
					var cerr error
					resp, cerr = c.RunAggregationQuery(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "datastore", Op: "ds_commit_delete",
			Method: "google.datastore.v1.Datastore/Commit", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := dsDelete(dsEntity)
				resp, err := dsCommit(ctx, t, req)
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
	)

	// ─── Firestore (documents data plane) ──────────────────────────────────────
	// Create a document with ?documentId, get/list/miss it, then delete it. The
	// collection and document id are run-suffixed.
	fsRoot := "projects/" + project + "/databases/(default)/documents"
	fsParent := fsRoot + "/" + n.FSCollection
	fsDocName := fsParent + "/" + n.FSDoc

	sc = append(sc,
		GRPCScenario{
			Service: "firestore", Op: "fs_doc_create",
			Method: "google.firestore.v1.Firestore/CreateDocument", Path: fsParent,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &firestorepb.CreateDocumentRequest{
					Parent:       fsRoot,
					CollectionId: n.FSCollection,
					DocumentId:   n.FSDoc,
					Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{
						"greeting": {ValueType: &firestorepb.Value_StringValue{StringValue: "hello"}},
						"count":    {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 1}},
					}},
				}
				var resp *firestorepb.Document
				err := withFirestore(ctx, t, func(c firestorepb.FirestoreClient) error {
					var cerr error
					resp, cerr = c.CreateDocument(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "firestore", Op: "fs_doc_get",
			Method: "google.firestore.v1.Firestore/GetDocument", Path: fsDocName,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &firestorepb.GetDocumentRequest{Name: fsDocName}
				var resp *firestorepb.Document
				err := withFirestore(ctx, t, func(c firestorepb.FirestoreClient) error {
					var cerr error
					resp, cerr = c.GetDocument(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "firestore", Op: "fs_docs_list",
			Method: "google.firestore.v1.Firestore/ListDocuments", Path: fsParent,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &firestorepb.ListDocumentsRequest{Parent: fsRoot, CollectionId: n.FSCollection}
				var resp *firestorepb.ListDocumentsResponse
				err := withFirestore(ctx, t, func(c firestorepb.FirestoreClient) error {
					var cerr error
					resp, cerr = c.ListDocuments(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "firestore", Op: "fs_doc_get_missing",
			Method: "google.firestore.v1.Firestore/GetDocument", Path: fsParent + "/missing-" + suffix,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &firestorepb.GetDocumentRequest{Name: fsParent + "/missing-" + suffix}
				var resp *firestorepb.Document
				err := withFirestore(ctx, t, func(c firestorepb.FirestoreClient) error {
					var cerr error
					resp, cerr = c.GetDocument(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "firestore", Op: "fs_doc_delete",
			Method: "google.firestore.v1.Firestore/DeleteDocument", Path: fsDocName,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &firestorepb.DeleteDocumentRequest{Name: fsDocName}
				var resp *emptypb.Empty
				err := withFirestore(ctx, t, func(c firestorepb.FirestoreClient) error {
					var cerr error
					resp, cerr = c.DeleteDocument(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
	)

	// ─── Cloud Logging v2 (data plane) ─────────────────────────────────────────
	// Write one entry to a run-suffixed custom log, then read it back with a
	// ListLogEntries filter scoped to that log. Real GCP's log index is
	// eventually consistent, so the read polls until the entry appears.
	logName := "projects/" + project + "/logs/" + n.LogName
	sc = append(sc,
		GRPCScenario{
			Service: "logging", Op: "log_write_entries",
			Method: "google.logging.v2.LoggingServiceV2/WriteLogEntries", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &loggingpb.WriteLogEntriesRequest{
					LogName: logName,
					Resource: &monitoredres.MonitoredResource{
						Type:   "global",
						Labels: map[string]string{"project_id": project},
					},
					Entries: []*loggingpb.LogEntry{{
						LogName:  logName,
						Severity: ltype.LogSeverity_INFO,
						Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: "hello jaiscloud"},
					}},
				}
				var resp *loggingpb.WriteLogEntriesResponse
				err := withLogging(ctx, t, func(c loggingpb.LoggingServiceV2Client) error {
					var cerr error
					resp, cerr = c.WriteLogEntries(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "logging", Op: "log_list_entries",
			Method: "google.logging.v2.LoggingServiceV2/ListLogEntries", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &loggingpb.ListLogEntriesRequest{
					ResourceNames: []string{"projects/" + project},
					Filter:        "logName=\"" + logName + "\"",
					PageSize:      10,
				}
				var resp *loggingpb.ListLogEntriesResponse
				err := withLogging(ctx, t, func(c loggingpb.LoggingServiceV2Client) error {
					var cerr error
					resp, cerr = c.ListLogEntries(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
			Wait: &GRPCWait{
				Until: func(m proto.Message) bool {
					r, ok := m.(*loggingpb.ListLogEntriesResponse)
					return ok && len(r.GetEntries()) > 0
				},
				Interval: time.Second,
				Timeout:  60 * time.Second,
			},
		},
	)

	// ─── Cloud Monitoring v3 (descriptor read) ─────────────────────────────────
	// A descriptor list filtered to a nonexistent custom metric type (so the
	// golden is not polluted by the project's built-in descriptors), plus a 404
	// descriptor get matching the REST scenario.
	sc = append(sc,
		GRPCScenario{
			Service: "monitoring", Op: "mon_list_metric_descriptors",
			Method: "google.monitoring.v3.MetricService/ListMetricDescriptors", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &monitoringpb.ListMetricDescriptorsRequest{
					Name:   "projects/" + project,
					Filter: "metric.type = \"" + n.MetricType + "\"",
				}
				var resp *monitoringpb.ListMetricDescriptorsResponse
				err := withMonitoring(ctx, t, func(c monitoringpb.MetricServiceClient) error {
					var cerr error
					resp, cerr = c.ListMetricDescriptors(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "monitoring", Op: "mon_get_metric_descriptor_missing",
			Method: "google.monitoring.v3.MetricService/GetMetricDescriptor", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &monitoringpb.GetMetricDescriptorRequest{Name: "projects/" + project + "/metricDescriptors/" + n.MetricType}
				var resp *metricpb.MetricDescriptor
				err := withMonitoring(ctx, t, func(c monitoringpb.MetricServiceClient) error {
					var cerr error
					resp, cerr = c.GetMetricDescriptor(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
	)

	// ─── google.longrunning.Operations (Dataproc endpoint, AUD6-4) ──────────────
	// Real GCP serves google.longrunning.Operations per service endpoint, and
	// regional Dataproc operations only on the region-scoped host
	// ({region}-dataproc.googleapis.com); the global host rejects the region and
	// the other reachable endpoints deny the parity principal. Dataproc is the
	// emulator's genuinely asynchronous resolver (done=false, settles on poll).
	// The emulator's single listener serves the service under the "dataproc"
	// :authority token, which GRPCTarget.dial sets in emulator mode. ListOperations
	// on the empty collection matches on both sides; GetOperation on a missing name
	// is NOT_FOUND on both sides (only the human-readable message differs, accepted
	// by the shared .message rule).
	dpOpsCollection := "projects/" + project + "/regions/us-central1/operations"
	sc = append(sc,
		GRPCScenario{
			Service: "dataproc", Op: "operations_list",
			Method: "google.longrunning.Operations/ListOperations", Path: dpOpsCollection,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &longrunningpb.ListOperationsRequest{Name: dpOpsCollection}
				var resp *longrunningpb.ListOperationsResponse
				err := withOperations(ctx, t, "dataproc", func(c longrunningpb.OperationsClient) error {
					var cerr error
					resp, cerr = c.ListOperations(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
		GRPCScenario{
			Service: "dataproc", Op: "operation_get_missing",
			Method: "google.longrunning.Operations/GetOperation", Path: dpOpsCollection + "/missing-" + suffix,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				req := &longrunningpb.GetOperationRequest{Name: dpOpsCollection + "/missing-" + suffix}
				var resp *longrunningpb.Operation
				err := withOperations(ctx, t, "dataproc", func(c longrunningpb.OperationsClient) error {
					var cerr error
					resp, cerr = c.GetOperation(ctx, req)
					return cerr
				})
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		},
	)

	// ─── AUD6-6: Datastore cursor paging ───────────────────────────────────────
	// A one-result first page is taken (unrecorded) and its end cursor is fed
	// back on the recorded request — as start_cursor for the next page and as
	// end_cursor for the bounded range. The golden proves both that a real
	// request carries the cursor and that the emulator resumes/limits at the
	// same boundary. The kind holds three equal-length names so real-GCP key
	// order and the emulator's canonical-key order agree.
	dsPageKind := n.DSPageKind
	dsPageNames := []string{n.DSPagePrefix + "a", n.DSPagePrefix + "b", n.DSPagePrefix + "c"}
	seedPages := func(ctx context.Context, t *GRPCTarget) error {
		for _, name := range dsPageNames {
			req := &datastorepb.CommitRequest{
				ProjectId: project,
				Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
				Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: &datastorepb.Entity{
					Key: &datastorepb.Key{
						PartitionId: &datastorepb.PartitionId{ProjectId: project},
						Path:        []*datastorepb.Key_PathElement{{Kind: dsPageKind, IdType: &datastorepb.Key_PathElement_Name{Name: name}}},
					},
					Properties: map[string]*datastorepb.Value{
						"greeting": {ValueType: &datastorepb.Value_StringValue{StringValue: "hello jaiscloud"}},
						"count":    {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 7}},
					},
				}}}},
			}
			if _, err := dsCommit(ctx, t, req); err != nil {
				return err
			}
		}
		return nil
	}
	firstPageCursor := func(ctx context.Context, t *GRPCTarget) ([]byte, error) {
		resp, err := dsRunQuery(ctx, t, &datastorepb.RunQueryRequest{
			ProjectId: project,
			QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
				Kind:  []*datastorepb.KindExpression{{Name: dsPageKind}},
				Limit: wrapperspb.Int32(1),
			}},
		})
		if err != nil {
			return nil, err
		}
		return resp.GetBatch().GetEndCursor(), nil
	}
	lastCursor := func(ctx context.Context, t *GRPCTarget) ([]byte, error) {
		resp, err := dsRunQuery(ctx, t, &datastorepb.RunQueryRequest{
			ProjectId: project,
			QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
				Kind: []*datastorepb.KindExpression{{Name: dsPageKind}},
			}},
		})
		if err != nil {
			return nil, err
		}
		return resp.GetBatch().GetEndCursor(), nil
	}
	pageQuery := func(start, end []byte) *datastorepb.Query {
		return &datastorepb.Query{
			Kind:        []*datastorepb.KindExpression{{Name: dsPageKind}},
			StartCursor: start,
			EndCursor:   end,
		}
	}
	pageScenario := func(op string, cursorAt func(context.Context, *GRPCTarget) ([]byte, error), build func([]byte) *datastorepb.Query) GRPCScenario {
		return GRPCScenario{
			Service: "datastore", Op: op,
			Method: "google.datastore.v1.Datastore/RunQuery", Path: "projects/" + project,
			Call: func(ctx context.Context, t *GRPCTarget) (proto.Message, proto.Message, error) {
				if err := seedPages(ctx, t); err != nil {
					return nil, nil, err
				}
				cursor, err := cursorAt(ctx, t)
				if err != nil {
					return nil, nil, err
				}
				req := &datastorepb.RunQueryRequest{
					ProjectId: project,
					QueryType: &datastorepb.RunQueryRequest_Query{Query: build(cursor)},
				}
				resp, err := dsRunQuery(ctx, t, req)
				if err != nil {
					return req, nil, err
				}
				return req, resp, nil
			},
		}
	}
	sc = append(sc,
		pageScenario("ds_run_query_start_cursor", firstPageCursor, func(cursor []byte) *datastorepb.Query {
			return pageQuery(cursor, nil)
		}),
		pageScenario("ds_run_query_end_cursor", firstPageCursor, func(cursor []byte) *datastorepb.Query {
			return pageQuery(nil, cursor)
		}),
		pageScenario("ds_run_query_end_cursor_last", lastCursor, func(cursor []byte) *datastorepb.Query {
			return pageQuery(nil, cursor)
		}),
	)

	// ─── Pub/Sub messaging semantics (PSM1) ───────────────────────────────────
	// Appended last so the golden indices above stay stable. Backs the gRPC-only
	// Acknowledge/ModifyAckDeadline/Pull/Seek/StreamingPull cells with a
	// real-GCP transcript.
	sc = append(sc, pubsubMessagingGRPCScenarios(project, n)...)

	return sc
}

// withDatastore dials the target's Datastore endpoint and runs fn with the
// generated stub, closing the connection afterwards.
func withDatastore(ctx context.Context, t *GRPCTarget, fn func(datastorepb.DatastoreClient) error) error {
	conn, err := t.dial(ctx, "datastore")
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(datastorepb.NewDatastoreClient(conn))
}

// dsCommit issues a Commit through withDatastore.
func dsCommit(ctx context.Context, t *GRPCTarget, req *datastorepb.CommitRequest) (*datastorepb.CommitResponse, error) {
	var resp *datastorepb.CommitResponse
	err := withDatastore(ctx, t, func(c datastorepb.DatastoreClient) error {
		var cerr error
		resp, cerr = c.Commit(ctx, req)
		return cerr
	})
	return resp, err
}

// dsRunQuery issues a RunQuery through withDatastore.
func dsRunQuery(ctx context.Context, t *GRPCTarget, req *datastorepb.RunQueryRequest) (*datastorepb.RunQueryResponse, error) {
	var resp *datastorepb.RunQueryResponse
	err := withDatastore(ctx, t, func(c datastorepb.DatastoreClient) error {
		var cerr error
		resp, cerr = c.RunQuery(ctx, req)
		return cerr
	})
	return resp, err
}

// withFirestore dials the target's Firestore endpoint and runs fn with the
// generated stub, closing the connection afterwards.
func withFirestore(ctx context.Context, t *GRPCTarget, fn func(firestorepb.FirestoreClient) error) error {
	conn, err := t.dial(ctx, "firestore")
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(firestorepb.NewFirestoreClient(conn))
}

// withLogging dials the target's Logging endpoint and runs fn with the generated
// LoggingServiceV2 stub, closing the connection afterwards.
func withLogging(ctx context.Context, t *GRPCTarget, fn func(loggingpb.LoggingServiceV2Client) error) error {
	conn, err := t.dial(ctx, "logging")
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(loggingpb.NewLoggingServiceV2Client(conn))
}

// withMonitoring dials the target's Monitoring endpoint and runs fn with the
// generated MetricService stub, closing the connection afterwards.
func withMonitoring(ctx context.Context, t *GRPCTarget, fn func(monitoringpb.MetricServiceClient) error) error {
	conn, err := t.dial(ctx, "monitoring")
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(monitoringpb.NewMetricServiceClient(conn))
}

// withOperations dials a service endpoint and runs fn with the
// google.longrunning.Operations stub, closing the connection afterwards. service
// names the owning host (real GCP serves Operations per service); in emulator
// mode GRPCTarget.dial sets the matching :authority token so the single listener
// answers as that service.
func withOperations(ctx context.Context, t *GRPCTarget, service string, fn func(longrunningpb.OperationsClient) error) error {
	conn, err := t.dial(ctx, service)
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(longrunningpb.NewOperationsClient(conn))
}

// CleanupGRPC deletes every resource the gRPC scenario set creates, so a
// capture leaves nothing behind. It is best-effort and idempotent: it runs
// after both success and failure, and deleting an absent Datastore entity or
// Firestore document is a no-op. Log entries expire on their own and the
// Monitoring/Operations probes create nothing.
func (t *GRPCTarget) CleanupGRPC(ctx context.Context) []string {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	n := t.Names
	var log []string

	dsReq := &datastorepb.CommitRequest{
		ProjectId: t.Project,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: &datastorepb.Key{
			PartitionId: &datastorepb.PartitionId{ProjectId: t.Project},
			Path:        []*datastorepb.Key_PathElement{{Kind: n.DSKind, IdType: &datastorepb.Key_PathElement_Name{Name: n.DSEntity}}},
		}}}},
	}
	if _, err := dsCommit(ctx, t, dsReq); err != nil {
		log = append(log, "cleanup datastore entity: "+err.Error())
	} else {
		log = append(log, "cleanup datastore entity: ok")
	}

	// AUD6-6 paging entities (three equal-length names in a dedicated kind).
	pageCleanupOK := true
	for _, letter := range []string{"a", "b", "c"} {
		name := n.DSPagePrefix + letter
		req := &datastorepb.CommitRequest{
			ProjectId: t.Project,
			Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
			Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: &datastorepb.Key{
				PartitionId: &datastorepb.PartitionId{ProjectId: t.Project},
				Path:        []*datastorepb.Key_PathElement{{Kind: n.DSPageKind, IdType: &datastorepb.Key_PathElement_Name{Name: name}}},
			}}}},
		}
		if _, err := dsCommit(ctx, t, req); err != nil {
			log = append(log, "cleanup datastore page entity "+letter+": "+err.Error())
			pageCleanupOK = false
		}
	}
	if pageCleanupOK {
		log = append(log, "cleanup datastore page entities: ok")
	}

	fsDocName := "projects/" + t.Project + "/databases/(default)/documents/" + n.FSCollection + "/" + n.FSDoc
	err := withFirestore(ctx, t, func(c firestorepb.FirestoreClient) error {
		_, cerr := c.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: fsDocName})
		return cerr
	})
	if err != nil {
		log = append(log, "cleanup firestore document: "+err.Error())
	} else {
		log = append(log, "cleanup firestore document: ok")
	}

	// PSM1 Pub/Sub messaging resources.
	log = append(log, t.CleanupPubSubGRPC(ctx)...)
	return log
}
