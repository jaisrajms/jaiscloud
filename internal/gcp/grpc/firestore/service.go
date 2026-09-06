package firestore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"jaiscloud/internal/clock"
	grpcutil "jaiscloud/internal/gcp/grpc"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements firestorepb.FirestoreServer over the shared, transport-
// agnostic provider Service, so the REST adapter and the gRPC transport share
// one transaction read-set registry.
type Service struct {
	firestorepb.UnimplementedFirestoreServer
	svc         *firestoreprovider.Service
	defaultProj string
}

// NewService returns a Firestore gRPC service wrapping the provider Service.
// defaultProj is the config-default project used when a request carries none.
func NewService(svc *firestoreprovider.Service, defaultProj string) *Service {
	return &Service{svc: svc, defaultProj: defaultProj}
}

// Reset clears the shared transaction read-set registry (delegating to the
// provider Service). Registering this with the admin handler keeps the gRPC
// path self-contained if the REST provider were ever removed.
func (s *Service) Reset(ctx context.Context) { s.svc.Reset(ctx) }

// ─── identity (project over gRPC metadata) ───────────────────────────────────

// resolveProject derives the project for an RPC via the shared gRPC metadata
// resolver, falling back to the configured default. The Firestore unary request
// messages themselves carry full resource names, so the message is
// authoritative; this is the fallback.
func (s *Service) resolveProject(ctx context.Context) string {
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// mapError converts a provider/service error into a gRPC status error via the
// shared grpc.GRPCStatus mapping.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// ─── resource-name parsing ────────────────────────────────────────────────────

// splitParent parses a parent resource name
// "projects/{p}/databases/{db}/documents[/{path}]" into project, database, and
// the relative document path (after "documents").
func splitParent(parent string) (project, database, rel string, ok bool) {
	parts := strings.Split(parent, "/")
	if len(parts) < 4 || parts[0] != "projects" || parts[2] != "databases" {
		return "", "", "", false
	}
	project, database = parts[1], parts[3]
	if len(parts) > 4 {
		if parts[4] != "documents" {
			return "", "", "", false
		}
		rel = strings.Join(parts[5:], "/")
	}
	return project, database, rel, true
}

// collectionPath parses a CreateDocument parent and appends the collection ID
// to form the collection path the Service expects.
func (s *Service) collectionPath(ctx context.Context, parent, collectionID string) (project, database, path string, err error) {
	project, database, rel, ok := splitParent(parent)
	if !ok {
		return "", "", "", model.NewProviderError("InvalidArgument", "invalid parent resource name", 400)
	}
	if collectionID == "" {
		return "", "", "", model.NewProviderError("InvalidArgument", "collectionId is required", 400)
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	if rel == "" {
		path = collectionID
	} else {
		path = rel + "/" + collectionID
	}
	return project, database, path, nil
}

// ─── unary handlers ───────────────────────────────────────────────────────────

func (s *Service) GetDocument(ctx context.Context, req *firestorepb.GetDocumentRequest) (*firestorepb.Document, error) {
	var mask []string
	if req.GetMask() != nil {
		mask = req.GetMask().GetFieldPaths()
	}
	doc, err := s.svc.GetDocument(ctx, req.GetName(), req.GetTransaction(), mask)
	if err != nil {
		return nil, mapError(err)
	}
	return encodeDocument(doc), nil
}

func (s *Service) CreateDocument(ctx context.Context, req *firestorepb.CreateDocumentRequest) (*firestorepb.Document, error) {
	project, database, collPath, err := s.collectionPath(ctx, req.GetParent(), req.GetCollectionId())
	if err != nil {
		return nil, mapError(err)
	}
	fields, err := decodeFields(req.GetDocument())
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", err.Error(), 400))
	}
	doc, err := s.svc.CreateDocument(ctx, project, database, collPath, req.GetDocumentId(), fields)
	if err != nil {
		return nil, mapError(err)
	}
	return encodeDocument(doc), nil
}

func (s *Service) UpdateDocument(ctx context.Context, req *firestorepb.UpdateDocumentRequest) (*firestorepb.Document, error) {
	doc := req.GetDocument()
	if doc == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "document is required", 400))
	}
	project, database, path, ok := firestorestore.ParseDocumentName(doc.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid document name", 400))
	}
	fields, err := decodeFields(doc)
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", err.Error(), 400))
	}
	var mask []string
	if req.GetUpdateMask() != nil {
		mask = req.GetUpdateMask().GetFieldPaths()
	}
	pre, err := decodeStorePrecondition(req.GetCurrentDocument())
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", err.Error(), 400))
	}
	out, err := s.svc.PatchDocument(ctx, project, database, path, fields, mask, pre)
	if err != nil {
		return nil, mapError(err)
	}
	return encodeDocument(out), nil
}

func (s *Service) DeleteDocument(ctx context.Context, req *firestorepb.DeleteDocumentRequest) (*emptypb.Empty, error) {
	project, database, path, ok := firestorestore.ParseDocumentName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid document name", 400))
	}
	pre, err := decodeStorePrecondition(req.GetCurrentDocument())
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", err.Error(), 400))
	}
	if err := s.svc.DeleteDocument(ctx, project, database, path, pre); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) ListDocuments(ctx context.Context, req *firestorepb.ListDocumentsRequest) (*firestorepb.ListDocumentsResponse, error) {
	project, database, rel, ok := splitParent(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	path := rel
	if id := req.GetCollectionId(); id != "" {
		if path == "" {
			path = id
		} else {
			path = path + "/" + id
		}
	}
	var mask []string
	if req.GetMask() != nil {
		mask = req.GetMask().GetFieldPaths()
	}
	page := firestoreprovider.NewPageParams(int(req.GetPageSize()), req.GetPageToken())
	docs, nextToken, err := s.svc.ListDocuments(ctx, project, database, path, req.GetTransaction(), mask, page)
	if err != nil {
		return nil, mapError(err)
	}
	return &firestorepb.ListDocumentsResponse{
		Documents:     encodeDocuments(docs),
		NextPageToken: nextToken,
	}, nil
}

func (s *Service) ListCollectionIds(ctx context.Context, req *firestorepb.ListCollectionIdsRequest) (*firestorepb.ListCollectionIdsResponse, error) {
	project, database, rel, ok := splitParent(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	page := firestoreprovider.NewPageParams(int(req.GetPageSize()), req.GetPageToken())
	ids, nextToken, err := s.svc.ListCollectionIds(ctx, project, database, rel, page)
	if err != nil {
		return nil, mapError(err)
	}
	return &firestorepb.ListCollectionIdsResponse{
		CollectionIds: ids,
		NextPageToken: nextToken,
	}, nil
}

func (s *Service) BeginTransaction(ctx context.Context, req *firestorepb.BeginTransactionRequest) (*firestorepb.BeginTransactionResponse, error) {
	txn, err := s.svc.BeginTransaction(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	return &firestorepb.BeginTransactionResponse{Transaction: txn}, nil
}

func (s *Service) Commit(ctx context.Context, req *firestorepb.CommitRequest) (*firestorepb.CommitResponse, error) {
	writes, err := decodeWrites(req.GetWrites())
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", err.Error(), 400))
	}
	commitTime, results, err := s.svc.Commit(ctx, req.GetTransaction(), writes)
	if err != nil {
		return nil, mapError(err)
	}
	wr, err := commitResultsToProto(results)
	if err != nil {
		return nil, mapError(err)
	}
	return &firestorepb.CommitResponse{
		CommitTime:   timestamppb.New(commitTime),
		WriteResults: wr,
	}, nil
}

func (s *Service) Rollback(ctx context.Context, req *firestorepb.RollbackRequest) (*emptypb.Empty, error) {
	if err := s.svc.Rollback(ctx, req.GetTransaction()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) BatchWrite(ctx context.Context, req *firestorepb.BatchWriteRequest) (*firestorepb.BatchWriteResponse, error) {
	writes, err := decodeWrites(req.GetWrites())
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", err.Error(), 400))
	}
	statuses, writeResults, err := s.svc.BatchWrite(ctx, writes)
	if err != nil {
		return nil, mapError(err)
	}
	wr := make([]*firestorepb.WriteResult, 0, len(writeResults))
	for _, r := range writeResults {
		m, _ := r.(map[string]any)
		item, err := writeResultToProto(m)
		if err != nil {
			return nil, mapError(err)
		}
		wr = append(wr, item)
	}
	return &firestorepb.BatchWriteResponse{
		WriteResults: wr,
		Status:       batchStatusesToProto(statuses),
	}, nil
}

// ─── server-streaming handlers ────────────────────────────────────────────────

// RunQuery executes a StructuredQuery and streams one response per matching
// document, ending with a done frame. The whole stream shares a single
// readTime (the internal path applies offset/limit itself, so no
// skipped_results are reported).
func (s *Service) RunQuery(req *firestorepb.RunQueryRequest, stream firestorepb.Firestore_RunQueryServer) error {
	ctx := stream.Context()
	q, err := decodeStructuredQuery(req.GetStructuredQuery())
	if err != nil {
		return mapError(model.NewProviderError("InvalidArgument", err.Error(), 400))
	}
	project, database, rel, ok := splitParent(req.GetParent())
	if !ok {
		return mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	docs, err := s.svc.RunQuery(ctx, project, database, rel, q, req.GetTransaction())
	if err != nil {
		return mapError(err)
	}
	readTime := timestamppb.New(clock.Now())
	for _, d := range docs {
		if err := stream.Send(&firestorepb.RunQueryResponse{
			Document: encodeDocument(d),
			ReadTime: readTime,
		}); err != nil {
			return err
		}
	}
	return stream.Send(&firestorepb.RunQueryResponse{
		ContinuationSelector: &firestorepb.RunQueryResponse_Done{Done: true},
	})
}

// BatchGetDocuments streams one response per requested document: Found for
// existing documents and Missing for absent ones. The stream ends after the
// last result (no done frame). The internal path does not support field-mask
// projection, so req.Mask is ignored.
func (s *Service) BatchGetDocuments(req *firestorepb.BatchGetDocumentsRequest, stream firestorepb.Firestore_BatchGetDocumentsServer) error {
	ctx := stream.Context()
	items, err := s.svc.BatchGet(ctx, req.GetDocuments(), req.GetTransaction())
	if err != nil {
		return mapError(err)
	}
	readTime := timestamppb.New(clock.Now())
	for _, item := range items {
		resp := &firestorepb.BatchGetDocumentsResponse{ReadTime: readTime}
		if name, ok := item["missing"].(string); ok {
			resp.Result = &firestorepb.BatchGetDocumentsResponse_Missing{Missing: name}
		} else if found, ok := item["found"].(map[string]any); ok {
			doc, err := foundDocument(found)
			if err != nil {
				return mapError(model.NewProviderError("Internal", err.Error(), 500))
			}
			resp.Result = &firestorepb.BatchGetDocumentsResponse_Found{Found: doc}
		} else {
			return mapError(model.NewProviderError("Internal", "unknown batchGet result", 500))
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

// foundDocument decodes a BatchGet "found" wire item (a provider documentMap)
// back into a protobuf Document via a JSON round-trip through the store
// Document type.
func foundDocument(m map[string]any) (*firestorepb.Document, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal found document: %w", err)
	}
	var doc firestorestore.Document
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("unmarshal found document: %w", err)
	}
	return encodeDocument(doc), nil
}

// ─── write-result shims (thin extraction from the Service's map results) ─────

// commitResultsToProto converts Commit's []map[string]any write results.
func commitResultsToProto(results []map[string]any) ([]*firestorepb.WriteResult, error) {
	out := make([]*firestorepb.WriteResult, 0, len(results))
	for _, r := range results {
		wr, err := writeResultToProto(r)
		if err != nil {
			return nil, err
		}
		out = append(out, wr)
	}
	return out, nil
}

// writeResultToProto extracts updateTime → google.protobuf.Timestamp and
// transformResults → repeated firestorepb.Value from a Service write-result map.
func writeResultToProto(m map[string]any) (*firestorepb.WriteResult, error) {
	wr := &firestorepb.WriteResult{}
	if m == nil {
		return wr, nil
	}
	if u, ok := m["updateTime"].(string); ok && u != "" {
		t, err := time.Parse(time.RFC3339Nano, u)
		if err != nil {
			return nil, err
		}
		wr.UpdateTime = timestamppb.New(t)
	}
	if tr, ok := m["transformResults"].([]any); ok {
		for _, v := range tr {
			b, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			var fv firestorestore.Value
			if err := json.Unmarshal(b, &fv); err != nil {
				return nil, err
			}
			wr.TransformResults = append(wr.TransformResults, encodeValue(&fv))
		}
	}
	return wr, nil
}

// batchStatusesToProto converts BatchWrite's []any status maps to []*Status.
func batchStatusesToProto(statuses []any) []*status.Status {
	out := make([]*status.Status, 0, len(statuses))
	for _, s := range statuses {
		m, ok := s.(map[string]any)
		if !ok {
			out = append(out, &status.Status{Code: 0})
			continue
		}
		st := &status.Status{}
		switch c := m["code"].(type) {
		case int64:
			st.Code = int32(c)
		case float64:
			st.Code = int32(c)
		}
		st.Message, _ = m["message"].(string)
		out = append(out, st)
	}
	return out
}
