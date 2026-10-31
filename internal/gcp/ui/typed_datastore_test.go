package ui

import (
	"context"
	"testing"

	core "jaiscloud/internal/gcp/service/datastore"
	dsstore "jaiscloud/internal/gcp/store/datastore"
	restdatastore "jaiscloud/internal/gcp/transport/rest/datastore"
	"jaiscloud/internal/provider"
)

func newDatastore() (*core.Service, map[string]provider.HandlerFunc) {
	svc := core.NewService(dsstore.NewMemoryStore(), "proj")
	return svc, restdatastore.NewProvider(svc, "proj").Routes()
}

func TestDatastoreConsolePassthroughMatchesDirect(t *testing.T) {
	svc, builtins := newDatastore()
	var seen []seenCall
	wrap := map[string]func(provider.HandlerFunc) provider.HandlerFunc{}
	for k := range builtins {
		wrap[k] = passthrough(&seen, k)
	}
	p := routeDatastore(typedFixture(builtins, wrap), svc)
	ctx := context.Background()

	name := "hello"
	key := core.Key{Kind: "Task", Name: "t1", HasName: true}
	ent := dsstore.Entity{Kind: "Task", Properties: map[string]dsstore.Value{"title": {StringValue: &name}}}
	resp, err := p.Commit(ctx, "proj", &core.CommitRequest{Mutations: []core.Mutation{{Op: core.MutationUpsert, Key: key, Entity: ent}}})
	if err != nil || len(resp.Results) != 1 || resp.Results[0].Version == 0 {
		t.Fatalf("Commit = %+v, %v", resp, err)
	}

	want, err := svc.Lookup(ctx, "proj", []core.Key{key}, nil)
	if err != nil || len(want.Found) != 1 {
		t.Fatalf("direct Lookup = %+v, %v", want, err)
	}
	got, err := p.Lookup(ctx, "proj", []core.Key{key}, nil)
	if err != nil || len(got.Found) != 1 || got.Found[0].Version != want.Found[0].Version ||
		got.Found[0].Entity.Properties["title"].StringValue == nil || *got.Found[0].Entity.Properties["title"].StringValue != "hello" {
		t.Fatalf("lookup through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, want, err)
	}
	missing, err := p.Lookup(ctx, "proj", []core.Key{{Kind: "Task", Name: "nope", HasName: true}}, nil)
	if err != nil || len(missing.Missing) != 1 || missing.Missing[0].Name != "nope" {
		t.Fatalf("a missing key must come back missing: %+v, %v", missing, err)
	}

	q := &core.Query{Kind: "Task", Filter: &core.Filter{Property: &core.PropertyFilter{Property: "title", Op: core.PropertyEqual, Value: dsstore.Value{StringValue: &name}}}}
	res, err := p.RunQuery(ctx, "proj", q, nil)
	if err != nil || len(res.Entities) != 1 || res.Entities[0].Entity.Key == "" {
		t.Fatalf("RunQuery = %+v, %v", res, err)
	}
	gql, err := p.RunQueryGQL(ctx, "proj", core.GQLQuery{QueryString: "SELECT * FROM Task", AllowLiterals: true}, nil, "", "")
	if err != nil || len(gql.Entities) != 1 {
		t.Fatalf("RunQueryGQL = %+v, %v", gql, err)
	}
	if len(seen) != 5 {
		t.Fatalf("the plugin saw %d calls (%v), want 5", len(seen), seen)
	}
}

func TestDatastoreConsoleShowsWhatThePluginServes(t *testing.T) {
	svc, builtins := newDatastore()
	p := routeDatastore(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Datastore.RunQuery": replaceWith(map[string]any{"batch": map[string]any{
			"entityResultType": "FULL", "moreResults": "NO_MORE_RESULTS",
			"entityResults": []any{map[string]any{"version": "7", "entity": map[string]any{
				"key":        map[string]any{"path": []any{map[string]any{"kind": "Task", "name": "from-plugin"}}},
				"properties": map[string]any{"title": map[string]any{"stringValue": "served by a plugin"}}}}}}}),
	}), svc)
	res, err := p.RunQuery(context.Background(), "proj", &core.Query{Kind: "Task"}, nil)
	if err != nil || len(res.Entities) != 1 {
		t.Fatalf("RunQuery = %+v, %v", res, err)
	}
	e := res.Entities[0]
	if e.Version != 7 || e.Entity.Properties["title"].StringValue == nil || *e.Entity.Properties["title"].StringValue != "served by a plugin" {
		t.Fatalf("the console must show the plugin's entity, got %+v", e)
	}
}
