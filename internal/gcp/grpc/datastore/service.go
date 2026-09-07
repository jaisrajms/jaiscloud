// Package datastore implements the Cloud Datastore (v1) gRPC service
// (google.datastore.v1.Datastore) over the shared datastorestore.Store, so
// entities written via the Go SDK are stored and queried consistently.
//
// The emulator is intentionally non-transactional: it does not implement
// optimistic-concurrency transactions. BeginTransaction mints an opaque id so
// SDK init paths that poll the transaction surface do not error, and
// Rollback is a no-op, but a TRANSACTIONAL Commit (or a Commit carrying a
// transaction selector) is rejected with Unimplemented rather than being
// silently committed as a non-transactional write.
package datastore

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	datastorestore "jaiscloud/internal/gcp/store/datastore"
	"jaiscloud/internal/model"

	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements datastorepb.DatastoreServer over the shared store.
type Service struct {
	datastorepb.UnimplementedDatastoreServer

	store       datastorestore.Store
	defaultProj string
}

// NewService returns a Datastore gRPC service backed by the shared store.
// defaultProj is the config-default project used when a request carries none.
func NewService(store datastorestore.Store, defaultProj string) *Service {
	return &Service{store: store, defaultProj: defaultProj}
}

// mapError translates a store/service error into a gRPC status error. The
// store's ErrInvalidKey sentinel is not a ProviderError, so it is mapped to
// InvalidArgument here rather than falling through to Internal.
func mapError(err error) error {
	if errors.Is(err, datastorestore.ErrInvalidKey) {
		return grpcutil.GRPCStatus(model.NewProviderError("InvalidArgument", "invalid key", 400))
	}
	return grpcutil.GRPCStatus(err)
}

// txnSeq mints opaque transaction ids for BeginTransaction (non-transactional
// operations do not need full OCC; a unique id suffices so SDK init paths that
// poll the txn surface do not error).
var txnSeq int64

func (s *Service) project(ctx context.Context, reqProject string) string {
	if reqProject != "" {
		return reqProject
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// ─── Datastore service ────────────────────────────────────────────────────────

func (s *Service) Commit(ctx context.Context, req *datastorepb.CommitRequest) (*datastorepb.CommitResponse, error) {
	// The emulator is non-transactional: a TRANSACTIONAL commit (or one carrying
	// a transaction selector) is rejected rather than silently applied as a
	// non-transactional write. See the package doc.
	if req.GetMode() == datastorepb.CommitRequest_TRANSACTIONAL || len(req.GetTransaction()) > 0 {
		return nil, mapError(model.NewProviderError("UnsupportedOperation", "transactions are not supported by this emulator", 501))
	}

	project := s.project(ctx, req.GetProjectId())
	results := make([]*datastorepb.MutationResult, 0, len(req.GetMutations()))
	for _, m := range req.GetMutations() {
		pre := mutationPrecondition(m)
		mr := &datastorepb.MutationResult{Version: 1}
		switch op := m.GetOperation().(type) {
		case *datastorepb.Mutation_Insert:
			e, allocated, err := s.resolveEntity(ctx, project, op.Insert)
			if err != nil {
				return nil, mapError(err)
			}
			applied, err := s.store.ApplyMutation(ctx, project, datastorestore.MutationInsert, e, pre)
			switch {
			case errors.Is(err, datastorestore.ErrConflict):
				mr.ConflictDetected = true
				mr.Version = applied.Version
			case errors.Is(err, datastorestore.ErrEntityExists):
				return nil, mapError(model.NewProviderError("AlreadyExists", "entity already exists", 409))
			case err != nil:
				return nil, mapError(err)
			default:
				mr.Version = applied.Version
				if allocated {
					mr.Key = keyProto(e.Key, project)
				}
			}
		case *datastorepb.Mutation_Upsert:
			e, allocated, err := s.resolveEntity(ctx, project, op.Upsert)
			if err != nil {
				return nil, mapError(err)
			}
			applied, err := s.store.ApplyMutation(ctx, project, datastorestore.MutationUpsert, e, pre)
			switch {
			case errors.Is(err, datastorestore.ErrConflict):
				mr.ConflictDetected = true
				mr.Version = applied.Version
			case err != nil:
				return nil, mapError(err)
			default:
				mr.Version = applied.Version
				if allocated {
					mr.Key = keyProto(e.Key, project)
				}
			}
		case *datastorepb.Mutation_Update:
			e, err := entityFromProto(op.Update)
			if err != nil {
				return nil, mapError(err)
			}
			if e.Key == "" {
				return nil, mapError(model.NewProviderError("InvalidArgument", "update key is incomplete", 400))
			}
			applied, err := s.store.ApplyMutation(ctx, project, datastorestore.MutationUpdate, e, pre)
			switch {
			case errors.Is(err, datastorestore.ErrConflict):
				mr.ConflictDetected = true
				mr.Version = applied.Version
			case errors.Is(err, datastorestore.ErrEntityNotFound):
				return nil, mapError(model.NewProviderError("FailedPrecondition", "entity not found", 404))
			case err != nil:
				return nil, mapError(err)
			default:
				mr.Version = applied.Version
			}
		case *datastorepb.Mutation_Delete:
			key, err := deleteKey(op.Delete)
			if err != nil {
				return nil, mapError(err)
			}
			if err := s.store.DeleteConflictChecked(ctx, project, key, pre); err != nil {
				if errors.Is(err, datastorestore.ErrConflict) {
					mr.ConflictDetected = true
				} else {
					return nil, mapError(err)
				}
			}
		default:
			return nil, mapError(model.NewProviderError("InvalidArgument", "mutation has no operation", 400))
		}
		results = append(results, mr)
	}
	return &datastorepb.CommitResponse{
		MutationResults: results,
		// CommitTime is not set for non-transactional commits (see the proto).
	}, nil
}

// mutationPrecondition translates a Mutation's conflict_detection_strategy
// oneof (base_version or update_time — real Datastore's per-mutation
// optimistic-concurrency precondition) into a store Precondition. Returns nil
// when the mutation carries neither (the common case — no precondition).
func mutationPrecondition(m *datastorepb.Mutation) *datastorestore.Precondition {
	switch v := m.GetConflictDetectionStrategy().(type) {
	case *datastorepb.Mutation_BaseVersion:
		bv := v.BaseVersion
		return &datastorestore.Precondition{BaseVersion: &bv}
	case *datastorepb.Mutation_UpdateTime:
		ut := v.UpdateTime.AsTime()
		return &datastorestore.Precondition{UpdateTime: &ut}
	default:
		return nil
	}
}

func (s *Service) Lookup(ctx context.Context, req *datastorepb.LookupRequest) (*datastorepb.LookupResponse, error) {
	project := s.project(ctx, req.GetProjectId())
	resp := &datastorepb.LookupResponse{}
	for _, k := range req.GetKeys() {
		key, kind, complete, err := canonicalKey(k)
		if err != nil {
			return nil, mapError(err)
		}
		if !complete {
			return nil, mapError(model.NewProviderError("InvalidArgument", "lookup key is incomplete", 400))
		}
		e, err := s.store.Get(ctx, project, key)
		switch {
		case errors.Is(err, datastorestore.ErrEntityNotFound):
			resp.Missing = append(resp.Missing, &datastorepb.EntityResult{
				Entity:  &datastorepb.Entity{Key: keyProto(key, project)},
				Version: 1,
			})
		case err != nil:
			return nil, mapError(err)
		default:
			_ = kind
			resp.Found = append(resp.Found, &datastorepb.EntityResult{
				Entity: entityToProto(e, project),
				// Real, per-entity version — clients read this and pass it
				// back as a Mutation's base_version for a conditional write;
				// a hardcoded 1 would make that OCC contract meaningless.
				Version: e.Version,
			})
		}
	}
	return resp, nil
}

func (s *Service) RunQuery(ctx context.Context, req *datastorepb.RunQueryRequest) (*datastorepb.RunQueryResponse, error) {
	project := s.project(ctx, req.GetProjectId())
	batch := &datastorepb.QueryResultBatch{
		EntityResultType: datastorepb.EntityResult_FULL,
		MoreResults:      datastorepb.QueryResultBatch_NO_MORE_RESULTS,
	}

	var q *datastorepb.Query
	switch qt := req.GetQueryType().(type) {
	case *datastorepb.RunQueryRequest_Query:
		q = qt.Query
	case *datastorepb.RunQueryRequest_GqlQuery:
		return nil, mapError(model.NewProviderError("InvalidArgument", "GQL queries are not supported", 400))
	default:
		return nil, mapError(model.NewProviderError("InvalidArgument", "run query request has no query", 400))
	}

	kind := ""
	if q != nil && len(q.GetKind()) > 0 {
		kind = q.GetKind()[0].GetName()
	}
	entities, err := s.store.ListKind(ctx, project, kind)
	if err != nil {
		return nil, mapError(err)
	}
	for _, e := range entities {
		if q != nil {
			match, err := matchesFilter(e, q.GetFilter())
			if err != nil {
				return nil, mapError(err)
			}
			if !match {
				continue
			}
		}
		batch.EntityResults = append(batch.EntityResults, &datastorepb.EntityResult{
			Entity:  entityToProto(e, project),
			Version: e.Version,
		})
	}
	return &datastorepb.RunQueryResponse{Batch: batch}, nil
}

func (s *Service) BeginTransaction(ctx context.Context, req *datastorepb.BeginTransactionRequest) (*datastorepb.BeginTransactionResponse, error) {
	return &datastorepb.BeginTransactionResponse{
		Transaction: []byte("txn-" + strconv.FormatInt(atomic.AddInt64(&txnSeq, 1), 10)),
	}, nil
}

func (s *Service) Rollback(ctx context.Context, req *datastorepb.RollbackRequest) (*datastorepb.RollbackResponse, error) {
	return &datastorepb.RollbackResponse{}, nil
}

func (s *Service) AllocateIds(ctx context.Context, req *datastorepb.AllocateIdsRequest) (*datastorepb.AllocateIdsResponse, error) {
	project := s.project(ctx, req.GetProjectId())
	// AllocateIds only accepts incomplete keys; a complete key is rejected.
	for _, k := range req.GetKeys() {
		_, _, complete, err := canonicalKey(k)
		if err != nil {
			return nil, mapError(err)
		}
		if complete {
			return nil, mapError(model.NewProviderError("InvalidArgument", "allocate ids requires incomplete keys", 400))
		}
	}
	ids, err := s.store.AllocateIDs(ctx, project, len(req.GetKeys()))
	if err != nil {
		return nil, mapError(err)
	}
	resp := &datastorepb.AllocateIdsResponse{}
	for i, k := range req.GetKeys() {
		completed, err := completeKey(k, project, ids[i])
		if err != nil {
			return nil, mapError(err)
		}
		resp.Keys = append(resp.Keys, completed)
	}
	return resp, nil
}

// resolveEntity transcodes a mutation entity and, when its key path is
// incomplete, allocates a numeric ID. The bool reports whether an ID was
// allocated (so Commit can echo the resolved key back in MutationResult.Key).
// For an explicitly-keyed entity, the per-project ID allocator is advanced past
// the explicit numeric ID so AllocateIds never reissues an ID already in use.
func (s *Service) resolveEntity(ctx context.Context, project string, p *datastorepb.Entity) (datastorestore.Entity, bool, error) {
	e, err := entityFromProto(p)
	if err != nil {
		return e, false, err
	}
	if e.Key == "" {
		ids, err := s.store.AllocateIDs(ctx, project, 1)
		if err != nil {
			return e, false, err
		}
		e.Key = datastorestore.KeyOfID(e.Kind, ids[0])
		return e, true, nil
	}
	if err := s.advanceAllocator(ctx, project, e.Key); err != nil {
		return e, false, err
	}
	return e, false, nil
}

// advanceAllocator advances the project's ID allocator past an explicitly-used
// numeric ID (name keys are ignored; they never collide with allocated IDs).
func (s *Service) advanceAllocator(ctx context.Context, project, key string) error {
	_, idOrName, ok := datastorestore.SplitKey(key)
	if !ok {
		return nil
	}
	id, _, isID := datastorestore.ParseIDOrName(idOrName)
	if !isID {
		return nil
	}
	return s.store.AdvanceIDs(ctx, project, id)
}

// deleteKey requires a complete key and returns its canonical string.
func deleteKey(k *datastorepb.Key) (string, error) {
	key, _, complete, err := canonicalKey(k)
	if err != nil {
		return "", err
	}
	if !complete {
		return "", model.NewProviderError("InvalidArgument", "delete key is incomplete", 400)
	}
	return key, nil
}

// ─── key transcoding ──────────────────────────────────────────────────────────

// canonicalKey converts a proto Key to the store's canonical key string
// "kind/id-or-name" using the (single, last) path element's kind and
// id-or-name. complete is false for an incomplete key path. Ancestor
// (multi-element) paths and non-default namespace/database scopes are not
// supported and return InvalidArgument rather than silently collapsing to the
// last path element.
func canonicalKey(k *datastorepb.Key) (key, kind string, complete bool, err error) {
	if k == nil {
		return "", "", false, nil
	}
	if pid := k.GetPartitionId(); pid != nil {
		if pid.GetNamespaceId() != "" {
			return "", "", false, model.NewProviderError("InvalidArgument", "namespaced keys are not supported", 400)
		}
		if pid.GetDatabaseId() != "" {
			return "", "", false, model.NewProviderError("InvalidArgument", "database-scoped keys are not supported", 400)
		}
	}
	path := k.GetPath()
	if len(path) == 0 {
		return "", "", false, nil
	}
	if len(path) > 1 {
		return "", "", false, model.NewProviderError("InvalidArgument", "ancestor keys are not supported", 400)
	}
	el := path[0]
	kind = el.GetKind()
	if id, ok := el.GetIdType().(*datastorepb.Key_PathElement_Id); ok {
		return datastorestore.KeyOfID(kind, id.Id), kind, true, nil
	}
	if name, ok := el.GetIdType().(*datastorepb.Key_PathElement_Name); ok {
		return datastorestore.KeyOfName(kind, name.Name), kind, true, nil
	}
	return "", kind, false, nil
}

// keyProto reconstructs a single-element proto Key from the canonical key
// string, tagging the partition with the owning project.
func keyProto(key, project string) *datastorepb.Key {
	kind, idOrName, ok := datastorestore.SplitKey(key)
	if !ok {
		return nil
	}
	el := &datastorepb.Key_PathElement{Kind: kind}
	id, name, isID := datastorestore.ParseIDOrName(idOrName)
	if isID {
		el.IdType = &datastorepb.Key_PathElement_Id{Id: id}
	} else {
		el.IdType = &datastorepb.Key_PathElement_Name{Name: name}
	}
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: project},
		Path:        []*datastorepb.Key_PathElement{el},
	}
}

// completeKey fills the last path element of an incomplete key with the given
// numeric ID, preserving partition and ancestor path elements.
func completeKey(k *datastorepb.Key, project string, id int64) (*datastorepb.Key, error) {
	if k == nil || len(k.GetPath()) == 0 {
		return nil, model.NewProviderError("InvalidArgument", "cannot allocate an id for an empty key path", 400)
	}
	out := &datastorepb.Key{
		PartitionId: k.GetPartitionId(),
		Path:        make([]*datastorepb.Key_PathElement, len(k.GetPath())),
	}
	for i, el := range k.GetPath() {
		out.Path[i] = &datastorepb.Key_PathElement{Kind: el.GetKind()}
		if i < len(k.GetPath())-1 {
			out.Path[i].IdType = el.GetIdType()
		} else {
			out.Path[i].IdType = &datastorepb.Key_PathElement_Id{Id: id}
		}
	}
	if out.PartitionId == nil {
		out.PartitionId = &datastorepb.PartitionId{ProjectId: project}
	} else if out.PartitionId.GetProjectId() == "" {
		out.PartitionId.ProjectId = project
	}
	return out, nil
}

// ─── entity transcoding ───────────────────────────────────────────────────────

func entityFromProto(p *datastorepb.Entity) (datastorestore.Entity, error) {
	e := datastorestore.Entity{Properties: map[string]datastorestore.Value{}}
	if p == nil {
		return e, nil
	}
	if k := p.GetKey(); k != nil {
		key, kind, complete, err := canonicalKey(k)
		if err != nil {
			return e, err
		}
		e.Kind = kind
		if complete {
			e.Key = key
		}
	}
	for name, v := range p.GetProperties() {
		sv, err := valueFromProto(v)
		if err != nil {
			return e, err
		}
		e.Properties[name] = sv
	}
	return e, nil
}

func entityToProto(e datastorestore.Entity, project string) *datastorepb.Entity {
	props := make(map[string]*datastorepb.Value, len(e.Properties))
	for name, v := range e.Properties {
		props[name] = valueToProto(v, project)
	}
	out := &datastorepb.Entity{Properties: props}
	if e.Key != "" {
		out.Key = keyProto(e.Key, project)
	}
	return out
}

// ─── value transcoding ────────────────────────────────────────────────────────

func valueFromProto(p *datastorepb.Value) (datastorestore.Value, error) {
	if p == nil {
		return datastorestore.Value{}, nil
	}
	switch v := p.GetValueType().(type) {
	case *datastorepb.Value_NullValue:
		s := "NULL_VALUE"
		return datastorestore.Value{NullValue: &s}, nil
	case *datastorepb.Value_BooleanValue:
		b := v.BooleanValue
		return datastorestore.Value{BooleanValue: &b}, nil
	case *datastorepb.Value_IntegerValue:
		n := v.IntegerValue
		return datastorestore.Value{IntegerValue: &n}, nil
	case *datastorepb.Value_DoubleValue:
		f := v.DoubleValue
		return datastorestore.Value{DoubleValue: &f}, nil
	case *datastorepb.Value_TimestampValue:
		s := v.TimestampValue.AsTime().UTC().Format(time.RFC3339Nano)
		return datastorestore.Value{TimestampValue: &s}, nil
	case *datastorepb.Value_KeyValue:
		key, _, complete, err := canonicalKey(v.KeyValue)
		if err != nil {
			return datastorestore.Value{}, err
		}
		if !complete {
			return datastorestore.Value{}, model.NewProviderError("InvalidArgument", "key value is incomplete", 400)
		}
		return datastorestore.Value{KeyValue: &key}, nil
	case *datastorepb.Value_StringValue:
		s := v.StringValue
		return datastorestore.Value{StringValue: &s}, nil
	case *datastorepb.Value_BlobValue:
		return datastorestore.Value{BlobValue: v.BlobValue}, nil
	case *datastorepb.Value_GeoPointValue:
		g := datastorestore.GeoPoint{
			Latitude:  v.GeoPointValue.GetLatitude(),
			Longitude: v.GeoPointValue.GetLongitude(),
		}
		return datastorestore.Value{GeoPointValue: &g}, nil
	case *datastorepb.Value_EntityValue:
		e, err := entityFromProto(v.EntityValue)
		if err != nil {
			return datastorestore.Value{}, err
		}
		return datastorestore.Value{EntityValue: &e}, nil
	case *datastorepb.Value_ArrayValue:
		arr := datastorestore.ArrayValue{Values: make([]datastorestore.Value, 0, len(v.ArrayValue.GetValues()))}
		for _, x := range v.ArrayValue.GetValues() {
			xv, err := valueFromProto(x)
			if err != nil {
				return datastorestore.Value{}, err
			}
			arr.Values = append(arr.Values, xv)
		}
		return datastorestore.Value{ArrayValue: &arr}, nil
	}
	return datastorestore.Value{}, nil
}

func valueToProto(v datastorestore.Value, project string) *datastorepb.Value {
	switch {
	case v.NullValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
	case v.BooleanValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_BooleanValue{BooleanValue: *v.BooleanValue}}
	case v.IntegerValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: *v.IntegerValue}}
	case v.DoubleValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: *v.DoubleValue}}
	case v.TimestampValue != nil:
		t, err := time.Parse(time.RFC3339Nano, *v.TimestampValue)
		if err != nil {
			t = time.Time{}
		}
		return &datastorepb.Value{ValueType: &datastorepb.Value_TimestampValue{TimestampValue: timestamppb.New(t)}}
	case v.KeyValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: keyProto(*v.KeyValue, project)}}
	case v.StringValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: *v.StringValue}}
	case v.BlobValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_BlobValue{BlobValue: v.BlobValue}}
	case v.GeoPointValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_GeoPointValue{GeoPointValue: &latlng.LatLng{
			Latitude:  v.GeoPointValue.Latitude,
			Longitude: v.GeoPointValue.Longitude,
		}}}
	case v.EntityValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{EntityValue: entityToProto(*v.EntityValue, project)}}
	case v.ArrayValue != nil:
		arr := &datastorepb.ArrayValue{Values: make([]*datastorepb.Value, 0, len(v.ArrayValue.Values))}
		for _, x := range v.ArrayValue.Values {
			arr.Values = append(arr.Values, valueToProto(x, project))
		}
		return &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: arr}}
	}
	return &datastorepb.Value{}
}

// ─── query filter evaluation ──────────────────────────────────────────────────

// matchesFilter evaluates a Query.Filter against an entity. It supports
// PropertyFilter EQUAL, NOT_EQUAL, IN, and the four comparison operators
// (LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL), and
// CompositeFilter AND over those property filters. Unsupported operators
// (HAS_ANCESTOR, NOT_IN, unspecified/unknown) and CompositeFilter OR return an
// InvalidArgument error rather than matching everything.
func matchesFilter(e datastorestore.Entity, f *datastorepb.Filter) (bool, error) {
	if f == nil {
		return true, nil
	}
	switch ft := f.GetFilterType().(type) {
	case *datastorepb.Filter_PropertyFilter:
		return matchesPropertyFilter(e, ft.PropertyFilter)
	case *datastorepb.Filter_CompositeFilter:
		cf := ft.CompositeFilter
		if cf == nil {
			return true, nil
		}
		switch cf.GetOp() {
		case datastorepb.CompositeFilter_AND:
			for _, sub := range cf.GetFilters() {
				match, err := matchesFilter(e, sub)
				if err != nil {
					return false, err
				}
				if !match {
					return false, nil
				}
			}
			return true, nil
		default:
			return false, unsupportedQueryOperator(cf.GetOp().String())
		}
	default:
		return false, unsupportedQueryOperator("unknown filter")
	}
}

// unsupportedQueryOperator returns an InvalidArgument error for a filter
// operator the emulator does not implement (fail closed, never match-all).
func unsupportedQueryOperator(op string) error {
	return model.NewProviderError("InvalidArgument", "unsupported query operator: "+op, 400)
}

func matchesPropertyFilter(e datastorestore.Entity, pf *datastorepb.PropertyFilter) (bool, error) {
	if pf == nil || pf.GetProperty() == nil {
		return false, unsupportedQueryOperator("missing property filter")
	}
	prop := pf.GetProperty().GetName()
	val, ok := e.Properties[prop]

	filterVal, err := valueFromProto(pf.GetValue())
	if err != nil {
		return false, err
	}

	switch pf.GetOp() {
	case datastorepb.PropertyFilter_EQUAL:
		return ok && valueEqual(val, filterVal), nil
	case datastorepb.PropertyFilter_NOT_EQUAL:
		return ok && !valueEqual(val, filterVal), nil
	case datastorepb.PropertyFilter_IN:
		if !ok || filterVal.ArrayValue == nil {
			return false, nil
		}
		for _, el := range filterVal.ArrayValue.Values {
			if valueEqual(val, el) {
				return true, nil
			}
		}
		return false, nil
	case datastorepb.PropertyFilter_LESS_THAN,
		datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL,
		datastorepb.PropertyFilter_GREATER_THAN,
		datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
		if !ok {
			return false, nil
		}
		c, comparable := valueCompare(val, filterVal)
		if !comparable {
			return false, nil
		}
		switch pf.GetOp() {
		case datastorepb.PropertyFilter_LESS_THAN:
			return c < 0, nil
		case datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL:
			return c <= 0, nil
		case datastorepb.PropertyFilter_GREATER_THAN:
			return c > 0, nil
		default:
			return c >= 0, nil
		}
	default:
		return false, unsupportedQueryOperator(pf.GetOp().String())
	}
}

// numericValue reports the numeric value of v (integer or double) as a float64.
func numericValue(v datastorestore.Value) (float64, bool) {
	if v.IntegerValue != nil {
		return float64(*v.IntegerValue), true
	}
	if v.DoubleValue != nil {
		return *v.DoubleValue, true
	}
	return 0, false
}

// valueCompare compares two values for ordering, returning -1, 0, or 1. ok is
// false when the two values are not orderable (different types, or an
// unsupported type such as geo point/entity/array). Integers and doubles
// compare numerically, matching Datastore semantics.
func valueCompare(a, b datastorestore.Value) (int, bool) {
	an, aNum := numericValue(a)
	bn, bNum := numericValue(b)
	if aNum || bNum {
		if !aNum || !bNum {
			return 0, false
		}
		switch {
		case an < bn:
			return -1, true
		case an > bn:
			return 1, true
		default:
			return 0, true
		}
	}
	switch {
	case a.StringValue != nil || b.StringValue != nil:
		if a.StringValue == nil || b.StringValue == nil {
			return 0, false
		}
		return strings.Compare(*a.StringValue, *b.StringValue), true
	case a.BooleanValue != nil || b.BooleanValue != nil:
		if a.BooleanValue == nil || b.BooleanValue == nil {
			return 0, false
		}
		switch {
		case *a.BooleanValue == *b.BooleanValue:
			return 0, true
		case !*a.BooleanValue && *b.BooleanValue:
			return -1, true
		default:
			return 1, true
		}
	case a.TimestampValue != nil || b.TimestampValue != nil:
		if a.TimestampValue == nil || b.TimestampValue == nil {
			return 0, false
		}
		at, errA := time.Parse(time.RFC3339Nano, *a.TimestampValue)
		bt, errB := time.Parse(time.RFC3339Nano, *b.TimestampValue)
		if errA != nil || errB != nil {
			return 0, false
		}
		switch {
		case at.Before(bt):
			return -1, true
		case at.After(bt):
			return 1, true
		default:
			return 0, true
		}
	case a.KeyValue != nil || b.KeyValue != nil:
		if a.KeyValue == nil || b.KeyValue == nil {
			return 0, false
		}
		return strings.Compare(*a.KeyValue, *b.KeyValue), true
	case a.BlobValue != nil || b.BlobValue != nil:
		if a.BlobValue == nil || b.BlobValue == nil {
			return 0, false
		}
		return bytes.Compare(a.BlobValue, b.BlobValue), true
	}
	return 0, false
}

func valueEqual(a, b datastorestore.Value) bool {
	an, aNum := numericValue(a)
	bn, bNum := numericValue(b)
	if aNum || bNum {
		return aNum && bNum && an == bn
	}
	switch {
	case a.NullValue != nil || b.NullValue != nil:
		return a.NullValue != nil && b.NullValue != nil
	case a.BooleanValue != nil || b.BooleanValue != nil:
		return a.BooleanValue != nil && b.BooleanValue != nil && *a.BooleanValue == *b.BooleanValue
	case a.StringValue != nil || b.StringValue != nil:
		return a.StringValue != nil && b.StringValue != nil && *a.StringValue == *b.StringValue
	case a.TimestampValue != nil || b.TimestampValue != nil:
		return a.TimestampValue != nil && b.TimestampValue != nil && *a.TimestampValue == *b.TimestampValue
	case a.KeyValue != nil || b.KeyValue != nil:
		return a.KeyValue != nil && b.KeyValue != nil && *a.KeyValue == *b.KeyValue
	case a.BlobValue != nil || b.BlobValue != nil:
		return a.BlobValue != nil && b.BlobValue != nil && bytes.Equal(a.BlobValue, b.BlobValue)
	case a.GeoPointValue != nil || b.GeoPointValue != nil:
		return a.GeoPointValue != nil && b.GeoPointValue != nil && *a.GeoPointValue == *b.GeoPointValue
	case a.EntityValue != nil || b.EntityValue != nil:
		if a.EntityValue == nil || b.EntityValue == nil {
			return false
		}
		return entityEqual(*a.EntityValue, *b.EntityValue)
	case a.ArrayValue != nil || b.ArrayValue != nil:
		if a.ArrayValue == nil || b.ArrayValue == nil {
			return false
		}
		if len(a.ArrayValue.Values) != len(b.ArrayValue.Values) {
			return false
		}
		for i := range a.ArrayValue.Values {
			if !valueEqual(a.ArrayValue.Values[i], b.ArrayValue.Values[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func entityEqual(a, b datastorestore.Entity) bool {
	if a.Key != b.Key {
		return false
	}
	if len(a.Properties) != len(b.Properties) {
		return false
	}
	for k, av := range a.Properties {
		bv, ok := b.Properties[k]
		if !ok || !valueEqual(av, bv) {
			return false
		}
	}
	return true
}
