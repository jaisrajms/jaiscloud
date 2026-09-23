package datastore

import (
	"context"
	"encoding/base64"
	"strconv"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	core "jaiscloud/internal/gcp/service/datastore"
)

// Provider handles the Datastore REST data plane. It is a thin adapter: every
// handler decodes the NormalizedRequest body into the core's typed API, calls
// the shared core Service, and encodes the result as JSON. No business logic
// lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Datastore REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "Datastore.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Datastore.Lookup":              p.Lookup,
		"Datastore.RunQuery":            p.RunQuery,
		"Datastore.RunAggregationQuery": p.RunAggregationQuery,
		"Datastore.BeginTransaction":    p.BeginTransaction,
		"Datastore.Commit":              p.Commit,
		"Datastore.Rollback":            p.Rollback,
		"Datastore.AllocateIds":         p.AllocateIds,
		"Datastore.ReserveIds":          p.ReserveIds,
	}
}

// project resolves the owning project: the path project, else the request's
// account (project) scope, else the configured default.
func (p *Provider) project(nr *model.NormalizedRequest) string {
	if s := strParam(nr, "project"); s != "" {
		return s
	}
	if nr.AccountID != "" {
		return nr.AccountID
	}
	return p.defaultProj
}

// ─── handlers ─────────────────────────────────────────────────────────────────

func (p *Provider) Lookup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	project := p.project(nr)

	txn, err := transactionFromReadOptions(body["readOptions"])
	if err != nil {
		return nil, err
	}
	keys, err := keysFromWire(body["keys"])
	if err != nil {
		return nil, err
	}
	resp, err := p.core.Lookup(ctx, project, keys, txn)
	if err != nil {
		return nil, err
	}

	out := map[string]any{}
	if len(resp.Found) > 0 {
		found := make([]any, 0, len(resp.Found))
		for _, r := range resp.Found {
			found = append(found, map[string]any{
				"entity":  entityToWire(r.Entity, project),
				"version": strconv.FormatInt(r.Version, 10),
			})
		}
		out["found"] = found
	}
	if len(resp.Missing) > 0 {
		missing := make([]any, 0, len(resp.Missing))
		for _, k := range resp.Missing {
			missing = append(missing, map[string]any{
				"entity":  map[string]any{"key": keyToWire(k, project)},
				"version": "1",
			})
		}
		out["missing"] = missing
	}
	return provider.OK(out), nil
}

func (p *Provider) RunQuery(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	project := p.project(nr)

	txn, err := transactionFromReadOptions(body["readOptions"])
	if err != nil {
		return nil, err
	}
	if _, ok := body["gqlQuery"]; ok {
		return nil, invalidArgument("GQL queries are not supported")
	}
	qv, ok := body["query"]
	if !ok {
		return nil, invalidArgument("run query request has no query")
	}
	q, err := queryFromWire(qv)
	if err != nil {
		return nil, err
	}
	resp, err := p.core.RunQuery(ctx, project, q, txn)
	if err != nil {
		return nil, err
	}

	batch := map[string]any{
		"entityResultType": "FULL",
		"moreResults":      "NO_MORE_RESULTS",
	}
	if resp.Skipped > 0 {
		batch["skippedResults"] = resp.Skipped
	}
	if resp.MoreResults == core.MoreResultsAfterLimit {
		batch["moreResults"] = "MORE_RESULTS_AFTER_LIMIT"
	}
	if len(resp.Entities) > 0 {
		ers := make([]any, 0, len(resp.Entities))
		for _, r := range resp.Entities {
			ers = append(ers, map[string]any{
				"entity":  entityToWire(r.Entity, project),
				"version": strconv.FormatInt(r.Version, 10),
			})
		}
		batch["entityResults"] = ers
	}
	return provider.OK(map[string]any{"batch": batch}), nil
}

func (p *Provider) RunAggregationQuery(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	project := p.project(nr)

	if db := strFrom(body["databaseId"]); db != "" {
		return nil, invalidArgument("database-scoped requests are not supported")
	}
	txn, err := transactionFromReadOptions(body["readOptions"])
	if err != nil {
		return nil, err
	}
	if _, ok := body["gqlQuery"]; ok {
		return nil, invalidArgument("GQL aggregation queries are not supported")
	}
	aqv, ok := body["aggregationQuery"]
	if !ok {
		return nil, invalidArgument("run aggregation query request has no query")
	}
	aq, err := aggregationQueryFromWire(aqv)
	if err != nil {
		return nil, err
	}
	resp, err := p.core.RunAggregationQuery(ctx, project, aq, txn)
	if err != nil {
		return nil, err
	}

	props := make(map[string]any, len(resp.Aggregates))
	for alias, v := range resp.Aggregates {
		props[alias] = valueToWire(v, project)
	}
	batch := map[string]any{
		"aggregationResults": []any{map[string]any{"aggregateProperties": props}},
		"moreResults":        "NO_MORE_RESULTS",
		"readTime":           resp.ReadTime.UTC().Format(time.RFC3339Nano),
	}
	return provider.OK(map[string]any{"batch": batch}), nil
}

func (p *Provider) BeginTransaction(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	txn, err := p.core.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"transaction": base64.StdEncoding.EncodeToString(txn)}), nil
}

func (p *Provider) Commit(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	project := p.project(nr)

	txn, err := bytesFromWire(body["transaction"])
	if err != nil {
		return nil, err
	}
	mode := core.CommitModeUnspecified
	switch strFrom(body["mode"]) {
	case "TRANSACTIONAL":
		mode = core.CommitModeTransactional
	case "NON_TRANSACTIONAL":
		mode = core.CommitModeNonTransactional
	}
	mutations, err := mutationsFromWire(body["mutations"])
	if err != nil {
		return nil, err
	}
	resp, err := p.core.Commit(ctx, project, &core.CommitRequest{Mode: mode, Transaction: txn, Mutations: mutations})
	if err != nil {
		return nil, err
	}

	results := make([]any, 0, len(resp.Results))
	for _, r := range resp.Results {
		mr := map[string]any{"version": strconv.FormatInt(r.Version, 10)}
		if r.Key != nil {
			mr["key"] = keyToWire(*r.Key, project)
		}
		if r.ConflictDetected {
			mr["conflictDetected"] = true
		}
		results = append(results, mr)
	}
	out := map[string]any{"mutationResults": results}
	if !resp.CommitTime.IsZero() {
		out["commitTime"] = resp.CommitTime.UTC().Format(time.RFC3339Nano)
	}
	return provider.OK(out), nil
}

func (p *Provider) Rollback(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	// Idempotent for an unknown/expired transaction, matching real Datastore.
	txn, err := bytesFromWire(bodyOf(nr)["transaction"])
	if err != nil {
		return nil, err
	}
	p.core.Rollback(ctx, txn)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) AllocateIds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	project := p.project(nr)

	keys, err := keysFromWire(body["keys"])
	if err != nil {
		return nil, err
	}
	out, err := p.core.AllocateIDs(ctx, project, keys)
	if err != nil {
		return nil, err
	}
	keysWire := make([]any, 0, len(out))
	for _, k := range out {
		keysWire = append(keysWire, keyToWire(k, project))
	}
	return provider.OK(map[string]any{"keys": keysWire}), nil
}

func (p *Provider) ReserveIds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	project := p.project(nr)

	if db := strFrom(body["databaseId"]); db != "" {
		return nil, invalidArgument("database-scoped requests are not supported")
	}
	keys, err := keysFromWire(body["keys"])
	if err != nil {
		return nil, err
	}
	if err := p.core.ReserveIDs(ctx, project, keys); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}
