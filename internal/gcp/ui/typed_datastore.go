package ui

import (
	"context"
	"encoding/base64"

	core "jaiscloud/internal/gcp/service/datastore"
	restdatastore "jaiscloud/internal/gcp/transport/rest/datastore"
	datastoreui "jaiscloud/internal/gcp/ui/datastore"
)

// typedDatastore serves the Datastore console through the plugin chain when a plugin
// covers the key (see typed.go). Queries, lookups and commits are encoded as the
// REST bodies a client would send, and the answers decoded back.
type typedDatastore struct {
	datastoreui.ProviderInterface
	c typedCaller
}

func routeDatastore(c typedCaller, p datastoreui.ProviderInterface) datastoreui.ProviderInterface {
	return typedDatastore{ProviderInterface: p, c: c}
}

func partition(body map[string]any, project, namespace, database string) {
	pid := map[string]any{"projectId": project}
	if namespace != "" {
		pid["namespaceId"] = namespace
	}
	if database != "" {
		pid["databaseId"] = database
	}
	body["partitionId"] = pid
}

func readOptions(body map[string]any, txn []byte) {
	if len(txn) > 0 {
		body["readOptions"] = map[string]any{"transaction": base64.StdEncoding.EncodeToString(txn)}
	}
}

func (p typedDatastore) RunQuery(ctx context.Context, project string, q *core.Query, txn []byte) (*core.QueryResult, error) {
	if !p.c.chain.Covers("Datastore.RunQuery") {
		return p.ProviderInterface.RunQuery(ctx, project, q, txn)
	}
	body := map[string]any{"query": restdatastore.QueryToWire(q, project)}
	if q != nil {
		partition(body, project, q.Namespace, q.Database)
	}
	readOptions(body, txn)
	data, covered, err := p.c.do(ctx, "Datastore.RunQuery", project, "", map[string]any{"project": project, "body": body}, nil)
	if !covered {
		return p.ProviderInterface.RunQuery(ctx, project, q, txn)
	}
	if err != nil {
		return nil, err
	}
	return restdatastore.QueryResultFromWire(data)
}

func (p typedDatastore) RunQueryGQL(ctx context.Context, project string, q core.GQLQuery, txn []byte, namespace, database string) (*core.QueryResult, error) {
	if !p.c.chain.Covers("Datastore.RunQuery") {
		return p.ProviderInterface.RunQueryGQL(ctx, project, q, txn, namespace, database)
	}
	body := map[string]any{"gqlQuery": restdatastore.GQLQueryToWire(q, project)}
	partition(body, project, namespace, database)
	readOptions(body, txn)
	data, covered, err := p.c.do(ctx, "Datastore.RunQuery", project, "", map[string]any{"project": project, "body": body}, nil)
	if !covered {
		return p.ProviderInterface.RunQueryGQL(ctx, project, q, txn, namespace, database)
	}
	if err != nil {
		return nil, err
	}
	return restdatastore.QueryResultFromWire(data)
}

func (p typedDatastore) Lookup(ctx context.Context, project string, keys []core.Key, txn []byte) (*core.LookupResult, error) {
	if !p.c.chain.Covers("Datastore.Lookup") {
		return p.ProviderInterface.Lookup(ctx, project, keys, txn)
	}
	wire := make([]any, 0, len(keys))
	for _, k := range keys {
		wire = append(wire, restdatastore.KeyToWire(k, project))
	}
	body := map[string]any{"keys": wire}
	readOptions(body, txn)
	data, covered, err := p.c.do(ctx, "Datastore.Lookup", project, "", map[string]any{"project": project, "body": body}, nil)
	if !covered {
		return p.ProviderInterface.Lookup(ctx, project, keys, txn)
	}
	if err != nil {
		return nil, err
	}
	return restdatastore.LookupResultFromWire(data)
}

func (p typedDatastore) Commit(ctx context.Context, project string, req *core.CommitRequest) (*core.CommitResponse, error) {
	if !p.c.chain.Covers("Datastore.Commit") || req == nil {
		return p.ProviderInterface.Commit(ctx, project, req)
	}
	muts := make([]any, 0, len(req.Mutations))
	for _, m := range req.Mutations {
		muts = append(muts, restdatastore.MutationToWire(m, project))
	}
	body := map[string]any{"mutations": muts}
	if mode := restdatastore.CommitModeToWire(req.Mode); mode != "" {
		body["mode"] = mode
	}
	if len(req.Transaction) > 0 {
		body["transaction"] = base64.StdEncoding.EncodeToString(req.Transaction)
	}
	data, covered, err := p.c.do(ctx, "Datastore.Commit", project, "", map[string]any{"project": project, "body": body}, nil)
	if !covered {
		return p.ProviderInterface.Commit(ctx, project, req)
	}
	if err != nil {
		return nil, err
	}
	return restdatastore.CommitResponseFromWire(data)
}
