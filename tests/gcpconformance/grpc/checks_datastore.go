package grpcconformance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"cloud.google.com/go/datastore"
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// datastoreChecks covers the Cloud Datastore v1 surface
// (google.datastore.v1.Datastore) via the official cloud.google.com/go/datastore
// client, falling back to the generated datastorepb stub for RPCs the
// high-level client does not expose (ReserveIds) and for shapes it abstracts
// away (a Lookup miss, which the client collapses into ErrNoSuchEntity).
//
// Checks run in registry order and share one entity: Commit leaves it in
// place for Lookup, RunQuery, and RunAggregationQuery. Names are run-unique via
// cfg.ResourceName, so a long-lived emulator never sees cross-run collisions.
func datastoreChecks() []Check {
	return []Check{
		{Service: "datastore", RPC: "Commit (Put)", Method: "Commit", KeyField: "mutation_results[].key", Run: checkDatastoreCommit},
		{Service: "datastore", RPC: "Lookup (Get)", Method: "Lookup", KeyField: "found[].properties.description", Run: checkDatastoreLookup},
		{Service: "datastore", RPC: "Lookup (missing)", Method: "Lookup", KeyField: "missing[].entity.key", Run: checkDatastoreLookupMissing},
		{Service: "datastore", RPC: "RunQuery (filter)", Method: "RunQuery", KeyField: "batch.entity_results[]", Run: checkDatastoreRunQuery},
		{Service: "datastore", RPC: "RunQuery (limit+offset)", Method: "RunQuery", KeyField: "batch.entity_results[] bounded", Run: checkDatastoreRunQueryWindow},
		{Service: "datastore", RPC: "RunAggregationQuery (count)", Method: "RunAggregationQuery", KeyField: "batch.aggregation_results[].count", Run: checkDatastoreRunAggregationQuery},
		{Service: "datastore", RPC: "RunQuery (GQL)", Method: "RunQuery", KeyField: "batch.entity_results[] via GQL", Run: checkDatastoreRunQueryGQL},
		{Service: "datastore", RPC: "RunAggregationQuery (GQL)", Method: "RunAggregationQuery", KeyField: "GQL AGGREGATE COUNT(*)", Run: checkDatastoreRunAggregationQueryGQL},
		{Service: "datastore", RPC: "Commit (Delete)", Method: "Commit", KeyField: "entity absent after delete", Run: checkDatastoreDelete},
		{Service: "datastore", RPC: "BeginTransaction + Commit", Method: "BeginTransaction", KeyField: "txn commit persists entity", Run: checkDatastoreTxnCommit},
		{Service: "datastore", RPC: "Rollback", Method: "Rollback", KeyField: "entity absent after rollback", Run: checkDatastoreRollback},
		{Service: "datastore", RPC: "AllocateIds", Method: "AllocateIds", KeyField: "keys[].id", Run: checkDatastoreAllocateIDs},
		{Service: "datastore", RPC: "ReserveIds", Method: "ReserveIds", KeyField: "keys[].id > reserved", Run: checkDatastoreReserveIDs},
		{Service: "datastore", RPC: "Commit/Lookup (ancestor key)", Method: "Lookup", KeyField: "ancestor-scoped entity round-trip", Run: checkDatastoreAncestor},
		{Service: "datastore", RPC: "RunQuery (namespace)", Method: "RunQuery", KeyField: "namespaced entity isolation", Run: checkDatastoreNamespace},
	}
}

const datastoreKind = "GcpcDsTask"

// dsTask is the entity shape written and read by the checks. Field names map to
// Datastore property names verbatim.
type dsTask struct {
	Description string
	Priority    int64
}

// datastoreValue is the run-unique Description value shared by the entity
// created in Commit and looked up/filtered by the probes that follow it.
func datastoreValue(cfg Config) string { return cfg.ResourceName("gcpc-ds-value") }

// newDatastoreClient points the official high-level client at the emulator via
// its first-class DATASTORE_EMULATOR_HOST hook, which installs the insecure
// transport and emulator credentials for us (mirrors the Firestore probe).
func newDatastoreClient(ctx context.Context, cfg Config) (*datastore.Client, error) {
	if err := os.Setenv("DATASTORE_EMULATOR_HOST", cfg.GRPCAddr()); err != nil {
		return nil, fmt.Errorf("set DATASTORE_EMULATOR_HOST: %w", err)
	}
	return datastore.NewClient(ctx, cfg.Project)
}

// newDatastoreStub dials the emulator with insecure credentials and returns the
// generated datastorepb client for RPCs (or response shapes) the high-level
// client does not expose. The returned conn must be closed by the caller.
func newDatastoreStub(cfg Config) (datastorepb.DatastoreClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(cfg.GRPCAddr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", cfg.GRPCAddr(), err)
	}
	return datastorepb.NewDatastoreClient(conn), conn, nil
}

// datastoreNameKeyProto builds a complete, single-element name key for the raw
// Lookup probe.
func datastoreNameKeyProto(project, kind, name string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: project},
		Path: []*datastorepb.Key_PathElement{{
			Kind:   kind,
			IdType: &datastorepb.Key_PathElement_Name{Name: name},
		}},
	}
}

// Check 1: Commit via the high-level Put (an upsert), leaving the entity in
// place for the Lookup/RunQuery/RunAggregationQuery probes.
func checkDatastoreCommit(ctx context.Context, cfg Config) error {
	client, err := newDatastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	key := datastore.NameKey(datastoreKind, cfg.ResourceName("gcpc-ds-put"), nil)
	got, err := client.Put(ctx, key, &dsTask{Description: datastoreValue(cfg), Priority: 7})
	if err != nil {
		return fmt.Errorf("Put: %w", err)
	}
	if got.Name != key.Name {
		return fmt.Errorf("Put returned key name %q, want %q", got.Name, key.Name)
	}
	return nil
}

// Check 2: Lookup via the high-level Get, asserting the stored property values
// round-trip (both a string and an integer).
func checkDatastoreLookup(ctx context.Context, cfg Config) error {
	client, err := newDatastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	key := datastore.NameKey(datastoreKind, cfg.ResourceName("gcpc-ds-put"), nil)
	var got dsTask
	if err := client.Get(ctx, key, &got); err != nil {
		return fmt.Errorf("Get: %w", err)
	}
	if got.Description != datastoreValue(cfg) {
		return fmt.Errorf("Get description = %q, want %q", got.Description, datastoreValue(cfg))
	}
	if got.Priority != 7 {
		return fmt.Errorf("Get priority = %d, want 7", got.Priority)
	}
	return nil
}

// Check 3: a miss must come back in the proto's Found/Missing split, not as a
// gRPC error. The high-level client collapses this into ErrNoSuchEntity, so
// assert the raw LookupResponse shape through the generated stub instead.
func checkDatastoreLookupMissing(ctx context.Context, cfg Config) error {
	stub, conn, err := newDatastoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	name := cfg.ResourceName("gcpc-ds-absent")
	resp, err := stub.Lookup(ctx, &datastorepb.LookupRequest{
		ProjectId: cfg.Project,
		Keys:      []*datastorepb.Key{datastoreNameKeyProto(cfg.Project, datastoreKind, name)},
	})
	if err != nil {
		return fmt.Errorf("Lookup: %w", err)
	}
	if n := len(resp.GetFound()); n != 0 {
		return fmt.Errorf("Lookup found %d entity(ies), want 0", n)
	}
	if n := len(resp.GetMissing()); n != 1 {
		return fmt.Errorf("Lookup missing count = %d, want 1", n)
	}
	missing := resp.GetMissing()[0].GetEntity().GetKey()
	if missing == nil || len(missing.GetPath()) != 1 {
		return fmt.Errorf("Lookup missing result key = %v, want one path element", missing)
	}
	if got := missing.GetPath()[0].GetName(); got != name {
		return fmt.Errorf("Lookup missing result key name = %q, want %q", got, name)
	}
	return nil
}

// Check 4: a property filter must return the entity inserted by Commit and only
// it (the Description value is run-unique).
func checkDatastoreRunQuery(ctx context.Context, cfg Config) error {
	client, err := newDatastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	q := datastore.NewQuery(datastoreKind).FilterField("Description", "=", datastoreValue(cfg))
	var got []dsTask
	if _, err := client.GetAll(ctx, q, &got); err != nil {
		return fmt.Errorf("GetAll: %w", err)
	}
	if len(got) != 1 {
		return fmt.Errorf("RunQuery returned %d entities, want 1", len(got))
	}
	if got[0].Priority != 7 {
		return fmt.Errorf("RunQuery priority = %d, want 7", got[0].Priority)
	}
	return nil
}

// Check 5: Limit and Offset must bound the result set. The official client
// treats a server that returns more than the requested limit as an internal
// error, so this locks in that RunQuery honors the window (and reports the
// skipped count) rather than returning every match.
func checkDatastoreRunQueryWindow(ctx context.Context, cfg Config) error {
	client, err := newDatastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	group := cfg.ResourceName("gcpc-ds-window")
	var put []*datastore.Key
	for i := 0; i < 3; i++ {
		key := datastore.NameKey(datastoreKind, fmt.Sprintf("%s-%d", group, i), nil)
		if _, err := client.Put(ctx, key, &dsTask{Description: group, Priority: int64(i)}); err != nil {
			return fmt.Errorf("Put %d: %w", i, err)
		}
		put = append(put, key)
	}

	q := datastore.NewQuery(datastoreKind).FilterField("Description", "=", group).Offset(1).Limit(1)
	var got []dsTask
	keys, err := client.GetAll(ctx, q, &got)
	if err != nil {
		return fmt.Errorf("GetAll Offset(1).Limit(1): %w", err)
	}
	if len(got) != 1 {
		return fmt.Errorf("RunQuery Offset(1).Limit(1) returned %d entities, want 1", len(got))
	}
	if keys[0].Name != fmt.Sprintf("%s-1", group) {
		return fmt.Errorf("RunQuery Offset(1) returned %q, want %q", keys[0].Name, fmt.Sprintf("%s-1", group))
	}
	if err := client.DeleteMulti(ctx, put); err != nil {
		return fmt.Errorf("cleanup DeleteMulti: %w", err)
	}
	return nil
}

// Check 6: a COUNT aggregation over the same run-unique filter must report 1.
func checkDatastoreRunAggregationQuery(ctx context.Context, cfg Config) error {
	client, err := newDatastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	q := datastore.NewQuery(datastoreKind).FilterField("Description", "=", datastoreValue(cfg))
	res, err := client.RunAggregationQuery(ctx, q.NewAggregationQuery().WithCount("total"))
	if err != nil {
		return fmt.Errorf("RunAggregationQuery: %w", err)
	}
	raw, ok := res["total"]
	if !ok {
		return fmt.Errorf("aggregation result missing alias %q: %v", "total", res)
	}
	val, ok := raw.(*datastorepb.Value)
	if !ok {
		return fmt.Errorf("aggregation result %q has type %T, want *datastorepb.Value", "total", raw)
	}
	if got := val.GetIntegerValue(); got != 1 {
		return fmt.Errorf("count = %d, want 1", got)
	}
	return nil
}

// Check 6b: a GQL SELECT with a named binding must return the same entity the
// structured RunQuery checks returned. Exercised through the generated stub
// because the high-level client does not expose GQL queries.
func checkDatastoreRunQueryGQL(ctx context.Context, cfg Config) error {
	stub, conn, err := newDatastoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := stub.RunQuery(ctx, &datastorepb.RunQueryRequest{
		ProjectId: cfg.Project,
		QueryType: &datastorepb.RunQueryRequest_GqlQuery{GqlQuery: &datastorepb.GqlQuery{
			QueryString: "SELECT * FROM " + datastoreKind + " WHERE Description = @d",
			NamedBindings: map[string]*datastorepb.GqlQueryParameter{
				"d": gqlStringBinding(datastoreValue(cfg)),
			},
		}},
	})
	if err != nil {
		return fmt.Errorf("GQL RunQuery: %w", err)
	}
	if n := len(resp.GetBatch().GetEntityResults()); n != 1 {
		return fmt.Errorf("GQL RunQuery returned %d entities, want 1", n)
	}
	return nil
}

// Check 6c: a GQL aggregation query must report the same count as the
// structured aggregation check.
func checkDatastoreRunAggregationQueryGQL(ctx context.Context, cfg Config) error {
	stub, conn, err := newDatastoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := stub.RunAggregationQuery(ctx, &datastorepb.RunAggregationQueryRequest{
		ProjectId: cfg.Project,
		QueryType: &datastorepb.RunAggregationQueryRequest_GqlQuery{GqlQuery: &datastorepb.GqlQuery{
			QueryString: "AGGREGATE COUNT(*) OVER (SELECT * FROM " + datastoreKind + " WHERE Description = @d)",
			NamedBindings: map[string]*datastorepb.GqlQueryParameter{
				"d": gqlStringBinding(datastoreValue(cfg)),
			},
		}},
	})
	if err != nil {
		return fmt.Errorf("GQL RunAggregationQuery: %w", err)
	}
	results := resp.GetBatch().GetAggregationResults()
	if len(results) != 1 {
		return fmt.Errorf("GQL RunAggregationQuery returned %d results, want 1", len(results))
	}
	if got := results[0].GetAggregateProperties()["property_1"].GetIntegerValue(); got != 1 {
		return fmt.Errorf("GQL COUNT(*) = %d, want 1", got)
	}
	return nil
}

// gqlStringBinding builds a named GQL string-value binding.
func gqlStringBinding(s string) *datastorepb.GqlQueryParameter {
	return &datastorepb.GqlQueryParameter{
		ParameterType: &datastorepb.GqlQueryParameter_Value{Value: &datastorepb.Value{
			ValueType: &datastorepb.Value_StringValue{StringValue: s},
		}},
	}
}

// Check 7: Commit via the high-level Delete must remove the entity inserted by
// the first probe; a follow-up Get must report it missing.
func checkDatastoreDelete(ctx context.Context, cfg Config) error {
	client, err := newDatastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	key := datastore.NameKey(datastoreKind, cfg.ResourceName("gcpc-ds-put"), nil)
	if err := client.Delete(ctx, key); err != nil {
		return fmt.Errorf("Delete: %w", err)
	}
	var got dsTask
	err = client.Get(ctx, key, &got)
	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		return fmt.Errorf("Get after Delete = %v, want ErrNoSuchEntity", err)
	}
	return nil
}

// Check 8: BeginTransaction + a transactional Commit must persist the write.
func checkDatastoreTxnCommit(ctx context.Context, cfg Config) error {
	client, err := newDatastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	key := datastore.NameKey(datastoreKind, cfg.ResourceName("gcpc-ds-txn"), nil)
	tx, err := client.NewTransaction(ctx)
	if err != nil {
		return fmt.Errorf("NewTransaction: %w", err)
	}
	if _, err := tx.Put(key, &dsTask{Description: datastoreValue(cfg) + "-txn", Priority: 9}); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("txn Put: %w", err)
	}
	if _, err := tx.Commit(); err != nil {
		return fmt.Errorf("txn Commit: %w", err)
	}

	var got dsTask
	if err := client.Get(ctx, key, &got); err != nil {
		return fmt.Errorf("Get after commit: %w", err)
	}
	if got.Priority != 9 {
		return fmt.Errorf("committed priority = %d, want 9", got.Priority)
	}
	if err := client.Delete(ctx, key); err != nil {
		return fmt.Errorf("cleanup Delete: %w", err)
	}
	return nil
}

// Check 9: Rollback must discard the pending transactional write.
func checkDatastoreRollback(ctx context.Context, cfg Config) error {
	client, err := newDatastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	key := datastore.NameKey(datastoreKind, cfg.ResourceName("gcpc-ds-rollback"), nil)
	tx, err := client.NewTransaction(ctx)
	if err != nil {
		return fmt.Errorf("NewTransaction: %w", err)
	}
	if _, err := tx.Put(key, &dsTask{Description: datastoreValue(cfg) + "-rollback", Priority: 3}); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("txn Put: %w", err)
	}
	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("Rollback: %w", err)
	}

	var got dsTask
	err = client.Get(ctx, key, &got)
	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		return fmt.Errorf("Get after Rollback = %v, want ErrNoSuchEntity", err)
	}
	return nil
}

// Check 10: AllocateIds must complete an incomplete key with a positive ID.
func checkDatastoreAllocateIDs(ctx context.Context, cfg Config) error {
	client, err := newDatastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	keys, err := client.AllocateIDs(ctx, []*datastore.Key{datastore.IncompleteKey(datastoreKind, nil)})
	if err != nil {
		return fmt.Errorf("AllocateIDs: %w", err)
	}
	if len(keys) != 1 {
		return fmt.Errorf("AllocateIDs returned %d keys, want 1", len(keys))
	}
	if keys[0].ID <= 0 {
		return fmt.Errorf("AllocateIDs returned ID %d, want > 0", keys[0].ID)
	}
	return nil
}

// Check 11: ReserveIds must advance the allocator past the reserved numeric ID,
// so a later AllocateIds never reissues it. Exercised through the generated
// stub because the high-level client has no ReserveIds method.
func checkDatastoreReserveIDs(ctx context.Context, cfg Config) error {
	stub, conn, err := newDatastoreStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	reserved := datastoreReservedID(cfg)
	reservedKey := &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: cfg.Project},
		Path: []*datastorepb.Key_PathElement{{
			Kind:   datastoreKind,
			IdType: &datastorepb.Key_PathElement_Id{Id: reserved},
		}},
	}
	if _, err := stub.ReserveIds(ctx, &datastorepb.ReserveIdsRequest{
		ProjectId: cfg.Project,
		Keys:      []*datastorepb.Key{reservedKey},
	}); err != nil {
		return fmt.Errorf("ReserveIds: %w", err)
	}

	resp, err := stub.AllocateIds(ctx, &datastorepb.AllocateIdsRequest{
		ProjectId: cfg.Project,
		Keys: []*datastorepb.Key{{
			PartitionId: &datastorepb.PartitionId{ProjectId: cfg.Project},
			Path:        []*datastorepb.Key_PathElement{{Kind: datastoreKind}},
		}},
	})
	if err != nil {
		return fmt.Errorf("AllocateIds after ReserveIds: %w", err)
	}
	if len(resp.GetKeys()) != 1 {
		return fmt.Errorf("AllocateIds returned %d keys, want 1", len(resp.GetKeys()))
	}
	got := resp.GetKeys()[0].GetPath()[0].GetId()
	if got <= reserved {
		return fmt.Errorf("AllocateIds returned %d after reserving %d, want > %d", got, reserved, reserved)
	}
	return nil
}

// Check 12: an entity stored under an ancestor key must round-trip, and an
// ancestor query must return exactly it.
func checkDatastoreAncestor(ctx context.Context, cfg Config) error {
	client, err := newDatastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	parent := datastore.NameKey(datastoreKind+"List", cfg.ResourceName("gcpc-ds-parent"), nil)
	child := datastore.NameKey(datastoreKind, cfg.ResourceName("gcpc-ds-child"), parent)
	if _, err := client.Put(ctx, child, &dsTask{Description: cfg.ResourceName("gcpc-ds-ancestor"), Priority: 4}); err != nil {
		return fmt.Errorf("Put child: %w", err)
	}
	defer client.Delete(ctx, child)

	var got dsTask
	if err := client.Get(ctx, child, &got); err != nil {
		return fmt.Errorf("Get child: %w", err)
	}
	if got.Priority != 4 {
		return fmt.Errorf("child priority = %d, want 4", got.Priority)
	}

	var results []dsTask
	if _, err := client.GetAll(ctx, datastore.NewQuery(datastoreKind).Ancestor(parent), &results); err != nil {
		return fmt.Errorf("ancestor query: %w", err)
	}
	if len(results) != 1 {
		return fmt.Errorf("ancestor query returned %d entities, want 1", len(results))
	}
	return nil
}

// Check 13: a namespaced entity must be isolated from the default namespace.
func checkDatastoreNamespace(ctx context.Context, cfg Config) error {
	client, err := newDatastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	ns := cfg.ResourceName("gcpc-ds-ns")
	name := cfg.ResourceName("gcpc-ds-ns-entity")
	key := datastore.NameKey(datastoreKind, name, nil)
	key.Namespace = ns
	if _, err := client.Put(ctx, key, &dsTask{Description: ns, Priority: 5}); err != nil {
		return fmt.Errorf("Put namespaced: %w", err)
	}
	defer client.Delete(ctx, key)

	// The same kind+name in the default namespace is absent.
	def := datastore.NameKey(datastoreKind, name, nil)
	var got dsTask
	if err := client.Get(ctx, def, &got); !errors.Is(err, datastore.ErrNoSuchEntity) {
		return fmt.Errorf("default-namespace Get = %v, want ErrNoSuchEntity", err)
	}

	var results []dsTask
	if _, err := client.GetAll(ctx, datastore.NewQuery(datastoreKind).Namespace(ns), &results); err != nil {
		return fmt.Errorf("namespace query: %w", err)
	}
	if len(results) != 1 {
		return fmt.Errorf("namespace query returned %d entities, want 1", len(results))
	}
	return nil
}

// datastoreReservedID derives a run-unique, far-from-normal numeric ID from the
// run suffix so repeated runs against a long-lived emulator cannot collide.
func datastoreReservedID(cfg Config) int64 {
	n, err := strconv.ParseInt(cfg.Suffix, 16, 64)
	if err != nil {
		n = 0
	}
	return 1<<40 + (n % (1 << 20))
}
