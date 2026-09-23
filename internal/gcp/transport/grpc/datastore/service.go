// Package datastore is the gRPC transport for the Cloud Datastore v1 service
// (google.datastore.v1.Datastore). It is a thin proto adapter over the
// transport-neutral core in internal/gcp/service/datastore: it transcodes
// between the generated protobuf messages and the core's typed API, and maps
// core errors to gRPC status codes. It owns no business logic and no state
// beyond its default project.
package datastore

import (
	"context"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/datastore"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements datastorepb.DatastoreServer over the shared core.
type Service struct {
	datastorepb.UnimplementedDatastoreServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Datastore gRPC service wrapping the core. defaultProj is
// the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

// project resolves the owning project: the request's project_id when set, else
// the gRPC metadata routing header, else the configured default.
func (s *Service) project(ctx context.Context, reqProject string) string {
	if reqProject != "" {
		return reqProject
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// mapError translates a core ProviderError into a gRPC status error.
func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// ─── Datastore service ────────────────────────────────────────────────────────

func (s *Service) Commit(ctx context.Context, req *datastorepb.CommitRequest) (*datastorepb.CommitResponse, error) {
	project := s.project(ctx, req.GetProjectId())

	mutations := make([]core.Mutation, 0, len(req.GetMutations()))
	for _, m := range req.GetMutations() {
		cm, err := mutationFromProto(m)
		if err != nil {
			return nil, mapError(err)
		}
		mutations = append(mutations, cm)
	}

	mode := core.CommitModeUnspecified
	switch req.GetMode() {
	case datastorepb.CommitRequest_TRANSACTIONAL:
		mode = core.CommitModeTransactional
	case datastorepb.CommitRequest_NON_TRANSACTIONAL:
		mode = core.CommitModeNonTransactional
	}

	resp, err := s.core.Commit(ctx, project, &core.CommitRequest{
		Mode:        mode,
		Transaction: req.GetTransaction(),
		Mutations:   mutations,
	})
	if err != nil {
		return nil, mapError(err)
	}

	out := &datastorepb.CommitResponse{MutationResults: make([]*datastorepb.MutationResult, 0, len(resp.Results))}
	for _, r := range resp.Results {
		mr := &datastorepb.MutationResult{Version: r.Version, ConflictDetected: r.ConflictDetected}
		if r.Key != nil {
			mr.Key = keyToProto(*r.Key, project)
		}
		out.MutationResults = append(out.MutationResults, mr)
	}
	// CommitTime is set only for transactional commits (see the core doc).
	if !resp.CommitTime.IsZero() {
		out.CommitTime = timestamppb.New(resp.CommitTime)
	}
	return out, nil
}

func (s *Service) Lookup(ctx context.Context, req *datastorepb.LookupRequest) (*datastorepb.LookupResponse, error) {
	txn := req.GetReadOptions().GetTransaction()
	project := s.project(ctx, req.GetProjectId())

	keys := make([]core.Key, 0, len(req.GetKeys()))
	for _, k := range req.GetKeys() {
		ck, err := keyFromProto(k)
		if err != nil {
			return nil, mapError(err)
		}
		keys = append(keys, ck)
	}

	resp, err := s.core.Lookup(ctx, project, keys, txn)
	if err != nil {
		return nil, mapError(err)
	}

	out := &datastorepb.LookupResponse{}
	for _, r := range resp.Found {
		out.Found = append(out.Found, &datastorepb.EntityResult{
			Entity:  entityToProto(r.Entity, project),
			Version: r.Version,
		})
	}
	for _, k := range resp.Missing {
		out.Missing = append(out.Missing, &datastorepb.EntityResult{
			Entity:  &datastorepb.Entity{Key: keyToProto(k, project)},
			Version: 1,
		})
	}
	return out, nil
}

func (s *Service) RunQuery(ctx context.Context, req *datastorepb.RunQueryRequest) (*datastorepb.RunQueryResponse, error) {
	txn := req.GetReadOptions().GetTransaction()
	project := s.project(ctx, req.GetProjectId())

	var q *core.Query
	switch qt := req.GetQueryType().(type) {
	case *datastorepb.RunQueryRequest_Query:
		nq, err := queryFromProto(qt.Query)
		if err != nil {
			return nil, mapError(err)
		}
		q = nq
	case *datastorepb.RunQueryRequest_GqlQuery:
		return nil, mapError(invalidArgument("GQL queries are not supported"))
	default:
		return nil, mapError(invalidArgument("run query request has no query"))
	}

	resp, err := s.core.RunQuery(ctx, project, q, txn)
	if err != nil {
		return nil, mapError(err)
	}

	batch := &datastorepb.QueryResultBatch{
		EntityResultType: datastorepb.EntityResult_FULL,
		MoreResults:      datastorepb.QueryResultBatch_NO_MORE_RESULTS,
		SkippedResults:   int32(resp.Skipped),
	}
	if resp.MoreResults == core.MoreResultsAfterLimit {
		batch.MoreResults = datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT
	}
	for _, r := range resp.Entities {
		batch.EntityResults = append(batch.EntityResults, &datastorepb.EntityResult{
			Entity:  entityToProto(r.Entity, project),
			Version: r.Version,
		})
	}
	return &datastorepb.RunQueryResponse{Batch: batch}, nil
}

func (s *Service) RunAggregationQuery(ctx context.Context, req *datastorepb.RunAggregationQueryRequest) (*datastorepb.RunAggregationQueryResponse, error) {
	if err := rejectDatabaseID(req.GetDatabaseId()); err != nil {
		return nil, mapError(err)
	}
	txn := req.GetReadOptions().GetTransaction()
	project := s.project(ctx, req.GetProjectId())

	var aq *core.AggregationQuery
	switch qt := req.GetQueryType().(type) {
	case *datastorepb.RunAggregationQueryRequest_AggregationQuery:
		naq, err := aggregationQueryFromProto(qt.AggregationQuery)
		if err != nil {
			return nil, mapError(err)
		}
		aq = naq
	case *datastorepb.RunAggregationQueryRequest_GqlQuery:
		return nil, mapError(invalidArgument("GQL aggregation queries are not supported"))
	default:
		return nil, mapError(invalidArgument("run aggregation query request has no query"))
	}

	resp, err := s.core.RunAggregationQuery(ctx, project, aq, txn)
	if err != nil {
		return nil, mapError(err)
	}

	props := make(map[string]*datastorepb.Value, len(resp.Aggregates))
	for alias, v := range resp.Aggregates {
		props[alias] = valueToProto(v, project)
	}
	return &datastorepb.RunAggregationQueryResponse{
		Batch: &datastorepb.AggregationResultBatch{
			AggregationResults: []*datastorepb.AggregationResult{{AggregateProperties: props}},
			MoreResults:        datastorepb.QueryResultBatch_NO_MORE_RESULTS,
			ReadTime:           timestamppb.New(resp.ReadTime),
		},
	}, nil
}

func (s *Service) BeginTransaction(ctx context.Context, _ *datastorepb.BeginTransactionRequest) (*datastorepb.BeginTransactionResponse, error) {
	txn, err := s.core.BeginTransaction(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	return &datastorepb.BeginTransactionResponse{Transaction: txn}, nil
}

func (s *Service) Rollback(ctx context.Context, req *datastorepb.RollbackRequest) (*datastorepb.RollbackResponse, error) {
	// Idempotent for an unknown/expired transaction, matching real Datastore.
	s.core.Rollback(ctx, req.GetTransaction())
	return &datastorepb.RollbackResponse{}, nil
}

func (s *Service) AllocateIds(ctx context.Context, req *datastorepb.AllocateIdsRequest) (*datastorepb.AllocateIdsResponse, error) {
	project := s.project(ctx, req.GetProjectId())

	keys := make([]core.Key, 0, len(req.GetKeys()))
	for _, k := range req.GetKeys() {
		ck, err := keyFromProto(k)
		if err != nil {
			return nil, mapError(err)
		}
		keys = append(keys, ck)
	}

	out, err := s.core.AllocateIDs(ctx, project, keys)
	if err != nil {
		return nil, mapError(err)
	}
	resp := &datastorepb.AllocateIdsResponse{Keys: make([]*datastorepb.Key, 0, len(out))}
	for _, k := range out {
		resp.Keys = append(resp.Keys, keyToProto(k, project))
	}
	return resp, nil
}

func (s *Service) ReserveIds(ctx context.Context, req *datastorepb.ReserveIdsRequest) (*datastorepb.ReserveIdsResponse, error) {
	if err := rejectDatabaseID(req.GetDatabaseId()); err != nil {
		return nil, mapError(err)
	}
	project := s.project(ctx, req.GetProjectId())

	keys := make([]core.Key, 0, len(req.GetKeys()))
	for _, k := range req.GetKeys() {
		ck, err := keyFromProto(k)
		if err != nil {
			return nil, mapError(err)
		}
		keys = append(keys, ck)
	}

	if err := s.core.ReserveIDs(ctx, project, keys); err != nil {
		return nil, mapError(err)
	}
	return &datastorepb.ReserveIdsResponse{}, nil
}

// compile-time assertion that Service implements the generated server.
var _ datastorepb.DatastoreServer = (*Service)(nil)
