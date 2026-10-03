package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	core "jaiscloud/internal/gcp/service/datastore"
	dsstore "jaiscloud/internal/gcp/store/datastore"
	"jaiscloud/internal/model"
)

// call decodes a synthetic REST request through the codec and dispatches it to
// the provider, mirroring the gateway's request flow.
func call(t *testing.T, c *Codec, p *Provider, project, verb string, body map[string]any) (*model.ProviderResponse, error) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, "/v1/projects/"+project+":"+verb, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	nr, err := c.Decode(req, raw)
	if err != nil {
		return nil, err
	}
	action, ok := verbActions[verb]
	if !ok {
		t.Fatalf("unknown verb %q", verb)
	}
	return p.Routes()["Datastore."+action](context.Background(), nr)
}

func nameKey(kind, name string) map[string]any {
	return map[string]any{
		"partitionId": map[string]any{"projectId": "test"},
		"path":        []any{map[string]any{"kind": kind, "name": name}},
	}
}

func incompleteKind(kind string) map[string]any {
	return map[string]any{
		"partitionId": map[string]any{"projectId": "test"},
		"path":        []any{map[string]any{"kind": kind}},
	}
}

func newTestProvider(t *testing.T) (*Codec, *Provider) {
	t.Helper()
	c := NewCodec()
	p := NewProvider(core.NewService(dsstore.NewMemoryStore(), "test"), "test")
	return c, p
}

func upsert(t *testing.T, c *Codec, p *Provider, kind, name string, n int) {
	t.Helper()
	body := map[string]any{
		"mode": "NON_TRANSACTIONAL",
		"mutations": []any{
			map[string]any{"upsert": map[string]any{
				"key":        nameKey(kind, name),
				"properties": map[string]any{"n": map[string]any{"integerValue": strconv.Itoa(n)}},
			}},
		},
	}
	resp, err := call(t, c, p, "test", "commit", body)
	if err != nil {
		t.Fatalf("commit upsert: %v", err)
	}
	results, _ := resp.Data["mutationResults"].([]any)
	if len(results) != 1 {
		t.Fatalf("mutationResults = %v", resp.Data["mutationResults"])
	}
	if got := results[0].(map[string]any)["version"]; got != "1" {
		t.Fatalf("version = %v, want \"1\"", got)
	}
}

func TestRESTCommitLookupRunQueryRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)
	upsert(t, c, p, "Task", "a", 1)
	upsert(t, c, p, "Task", "b", 2)

	// Lookup by name key.
	lookup, err := call(t, c, p, "test", "lookup", map[string]any{"keys": []any{nameKey("Task", "a")}})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	found, _ := lookup.Data["found"].([]any)
	if len(found) != 1 {
		t.Fatalf("found = %v", lookup.Data["found"])
	}
	ent := found[0].(map[string]any)["entity"].(map[string]any)
	props := ent["properties"].(map[string]any)
	if got := props["n"].(map[string]any)["integerValue"]; got != "1" {
		t.Fatalf("n = %v, want \"1\"", got)
	}

	// RunQuery with an equality filter.
	q := map[string]any{
		"query": map[string]any{
			"kind": []any{map[string]any{"name": "Task"}},
			"filter": map[string]any{
				"propertyFilter": map[string]any{
					"property": map[string]any{"name": "n"},
					"op":       "EQUAL",
					"value":    map[string]any{"integerValue": "2"},
				},
			},
		},
	}
	rq, err := call(t, c, p, "test", "runQuery", q)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	batch := rq.Data["batch"].(map[string]any)
	ers, _ := batch["entityResults"].([]any)
	if len(ers) != 1 {
		t.Fatalf("query results = %v", batch["entityResults"])
	}
	if got := batch["moreResults"]; got != "NO_MORE_RESULTS" {
		t.Fatalf("moreResults = %v", got)
	}
}

func TestRESTRunAggregationQuery(t *testing.T) {
	c, p := newTestProvider(t)
	upsert(t, c, p, "Task", "a", 1)
	upsert(t, c, p, "Task", "b", 2)

	body := map[string]any{
		"aggregationQuery": map[string]any{
			"nestedQuery": map[string]any{"kind": []any{map[string]any{"name": "Task"}}},
			"aggregations": []any{
				map[string]any{"alias": "total", "count": map[string]any{}},
			},
		},
	}
	resp, err := call(t, c, p, "test", "runAggregationQuery", body)
	if err != nil {
		t.Fatalf("runAggregationQuery: %v", err)
	}
	batch := resp.Data["batch"].(map[string]any)
	results, _ := batch["aggregationResults"].([]any)
	if len(results) != 1 {
		t.Fatalf("aggregationResults = %v", batch["aggregationResults"])
	}
	props := results[0].(map[string]any)["aggregateProperties"].(map[string]any)
	if got := props["total"].(map[string]any)["integerValue"]; got != "2" {
		t.Fatalf("count = %v, want \"2\"", got)
	}
}

func TestRESTRunQueryGQL(t *testing.T) {
	c, p := newTestProvider(t)
	upsert(t, c, p, "Task", "a", 1)
	upsert(t, c, p, "Task", "b", 2)

	// Literal filter.
	resp, err := call(t, c, p, "test", "runQuery", map[string]any{
		"gqlQuery": map[string]any{"queryString": "SELECT * FROM Task WHERE n = 2", "allowLiterals": true},
	})
	if err != nil {
		t.Fatalf("GQL runQuery: %v", err)
	}
	batch := resp.Data["batch"].(map[string]any)
	if ers, _ := batch["entityResults"].([]any); len(ers) != 1 {
		t.Fatalf("GQL literal results = %v", batch["entityResults"])
	}

	// Named binding (literals disallowed).
	resp, err = call(t, c, p, "test", "runQuery", map[string]any{
		"gqlQuery": map[string]any{
			"queryString":   "SELECT * FROM Task WHERE n = @n",
			"allowLiterals": false,
			"namedBindings": map[string]any{
				"n": map[string]any{"value": map[string]any{"integerValue": "1"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("GQL named binding: %v", err)
	}
	batch = resp.Data["batch"].(map[string]any)
	ers, _ := batch["entityResults"].([]any)
	if len(ers) != 1 {
		t.Fatalf("GQL binding results = %v", batch["entityResults"])
	}
	if got := ers[0].(map[string]any)["entity"].(map[string]any)["key"].(map[string]any)["path"].([]any)[0].(map[string]any)["name"]; got != "a" {
		t.Fatalf("GQL binding matched key %v, want a", got)
	}

	// A literal with allow_literals=false is an InvalidArgument.
	if _, err := call(t, c, p, "test", "runQuery", map[string]any{
		"gqlQuery": map[string]any{"queryString": "SELECT * FROM Task WHERE n = 2"},
	}); err == nil {
		t.Fatal("GQL literal with allow_literals=false should error")
	}
}

func TestRESTRunAggregationQueryGQL(t *testing.T) {
	c, p := newTestProvider(t)
	upsert(t, c, p, "Task", "a", 1)
	upsert(t, c, p, "Task", "b", 2)

	resp, err := call(t, c, p, "test", "runAggregationQuery", map[string]any{
		"gqlQuery": map[string]any{"queryString": "AGGREGATE COUNT(*) AS total OVER (SELECT * FROM Task)"},
	})
	if err != nil {
		t.Fatalf("GQL runAggregationQuery: %v", err)
	}
	batch := resp.Data["batch"].(map[string]any)
	results, _ := batch["aggregationResults"].([]any)
	if len(results) != 1 {
		t.Fatalf("GQL aggregationResults = %v", batch["aggregationResults"])
	}
	props := results[0].(map[string]any)["aggregateProperties"].(map[string]any)
	if got := props["total"].(map[string]any)["integerValue"]; got != "2" {
		t.Fatalf("GQL count = %v, want \"2\"", got)
	}
}

func TestRESTAllocateAndReserveIds(t *testing.T) {
	c, p := newTestProvider(t)

	alloc, err := call(t, c, p, "test", "allocateIds", map[string]any{"keys": []any{incompleteKind("Task")}})
	if err != nil {
		t.Fatalf("allocateIds: %v", err)
	}
	keys := alloc.Data["keys"].([]any)
	el := keys[0].(map[string]any)["path"].([]any)[0].(map[string]any)
	idStr, _ := el["id"].(string)
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		t.Fatalf("allocated id = %q", idStr)
	}

	// Reserve a higher ID, then allocate again: the new ID must be greater.
	reserved := id + 100
	if _, err := call(t, c, p, "test", "reserveIds", map[string]any{
		"keys": []any{map[string]any{
			"partitionId": map[string]any{"projectId": "test"},
			"path":        []any{map[string]any{"kind": "Task", "id": strconv.FormatInt(reserved, 10)}},
		}},
	}); err != nil {
		t.Fatalf("reserveIds: %v", err)
	}
	alloc2, err := call(t, c, p, "test", "allocateIds", map[string]any{"keys": []any{incompleteKind("Task")}})
	if err != nil {
		t.Fatalf("allocateIds 2: %v", err)
	}
	el2 := alloc2.Data["keys"].([]any)[0].(map[string]any)["path"].([]any)[0].(map[string]any)
	id2, _ := strconv.ParseInt(el2["id"].(string), 10, 64)
	if id2 <= reserved {
		t.Fatalf("allocated id %d not past reserved id %d", id2, reserved)
	}
}

func TestRESTTransactionCommitAndRollback(t *testing.T) {
	c, p := newTestProvider(t)

	begin, err := call(t, c, p, "test", "beginTransaction", map[string]any{})
	if err != nil {
		t.Fatalf("beginTransaction: %v", err)
	}
	txn, _ := begin.Data["transaction"].(string)
	if txn == "" {
		t.Fatal("empty transaction handle")
	}

	// A rollback is idempotent and leaves nothing behind.
	if _, err := call(t, c, p, "test", "rollback", map[string]any{"transaction": txn}); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	begin2, err := call(t, c, p, "test", "beginTransaction", map[string]any{})
	if err != nil {
		t.Fatalf("beginTransaction 2: %v", err)
	}
	txn2 := begin2.Data["transaction"].(string)
	commit := map[string]any{
		"mode":        "TRANSACTIONAL",
		"transaction": txn2,
		"mutations": []any{map[string]any{"insert": map[string]any{
			"key":        nameKey("Task", "txn"),
			"properties": map[string]any{"n": map[string]any{"integerValue": "7"}},
		}}},
	}
	resp, err := call(t, c, p, "test", "commit", commit)
	if err != nil {
		t.Fatalf("transactional commit: %v", err)
	}
	if _, ok := resp.Data["commitTime"]; !ok {
		t.Fatalf("transactional commit missing commitTime: %v", resp.Data)
	}

	lookup, err := call(t, c, p, "test", "lookup", map[string]any{"keys": []any{nameKey("Task", "txn")}})
	if err != nil {
		t.Fatalf("lookup after commit: %v", err)
	}
	if found, _ := lookup.Data["found"].([]any); len(found) != 1 {
		t.Fatalf("entity not committed: %v", lookup.Data)
	}
}

// TestRESTUpdateMissingReturnsEnvelope locks the core/adapter error contract:
// an Update for a missing entity surfaces as FAILED_PRECONDITION with the
// canonical HTTP 400 (never 404, which would contradict the status name).
func TestRESTUpdateMissingReturnsEnvelope(t *testing.T) {
	c, p := newTestProvider(t)
	_, err := call(t, c, p, "test", "commit", map[string]any{
		"mode": "NON_TRANSACTIONAL",
		"mutations": []any{map[string]any{"update": map[string]any{
			"key":        nameKey("Task", "ghost"),
			"properties": map[string]any{"n": map[string]any{"integerValue": "1"}},
		}}},
	})
	perr, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("err = %v (%T), want *model.ProviderError", err, err)
	}
	if perr.HTTPStatus != 400 || perr.Code != "FailedPrecondition" {
		t.Fatalf("err = %+v, want 400/FailedPrecondition", perr)
	}
}

// TestRESTAllocateIdsRejectsEmptyKeyPath asserts the empty-key-path validation
// still reaches the REST surface as InvalidArgument.
func TestRESTAllocateIdsRejectsEmptyKeyPath(t *testing.T) {
	c, p := newTestProvider(t)
	_, err := call(t, c, p, "test", "allocateIds", map[string]any{"keys": []any{map[string]any{}}})
	if err == nil {
		t.Fatal("empty key path should be rejected")
	}
}

// TestRESTAncestorNamespaceDatabaseRoundTrip drives Commit/Lookup/RunQuery with
// an ancestor path plus namespace and request-level database, asserting the
// full partition+path round-trips and scopes results.
func TestRESTAncestorNamespaceDatabaseRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)

	childKey := func(ns string) map[string]any {
		pid := map[string]any{"projectId": "test"}
		if ns != "" {
			pid["namespaceId"] = ns
		}
		// databaseId is deliberately omitted: the request-level field fills it.
		return map[string]any{
			"partitionId": pid,
			"path": []any{
				map[string]any{"kind": "Parent", "id": "1"},
				map[string]any{"kind": "Child", "name": "c"},
			},
		}
	}

	if _, err := call(t, c, p, "test", "commit", map[string]any{
		"databaseId": "db1",
		"mode":       "NON_TRANSACTIONAL",
		"mutations": []any{map[string]any{"upsert": map[string]any{
			"key":        childKey("ns1"),
			"properties": map[string]any{"n": map[string]any{"integerValue": "1"}},
		}}},
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	lookup, err := call(t, c, p, "test", "lookup", map[string]any{
		"databaseId": "db1",
		"keys":       []any{childKey("ns1")},
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	found, _ := lookup.Data["found"].([]any)
	if len(found) != 1 {
		t.Fatalf("found = %v, want 1", lookup.Data["found"])
	}
	gotKey := found[0].(map[string]any)["entity"].(map[string]any)["key"].(map[string]any)
	pid, _ := gotKey["partitionId"].(map[string]any)
	if pid["namespaceId"] != "ns1" || pid["databaseId"] != "db1" {
		t.Fatalf("round-tripped partition = %+v", pid)
	}
	if path, _ := gotKey["path"].([]any); len(path) != 2 {
		t.Fatalf("round-tripped path = %+v", gotKey["path"])
	}

	// A different namespace is a different entity.
	miss, err := call(t, c, p, "test", "lookup", map[string]any{
		"databaseId": "db1",
		"keys":       []any{childKey("ns2")},
	})
	if err != nil {
		t.Fatalf("lookup ns2: %v", err)
	}
	if f, _ := miss.Data["found"].([]any); len(f) != 0 {
		t.Fatalf("ns2 found = %v, want none", miss.Data["found"])
	}

	// An ancestor query scoped to ns1/db1 returns the child.
	rq, err := call(t, c, p, "test", "runQuery", map[string]any{
		"databaseId":  "db1",
		"partitionId": map[string]any{"namespaceId": "ns1"},
		"query": map[string]any{
			"kind": []any{map[string]any{"name": "Child"}},
			"filter": map[string]any{"propertyFilter": map[string]any{
				"property": map[string]any{"name": "__key__"},
				"op":       "HAS_ANCESTOR",
				"value": map[string]any{"keyValue": map[string]any{
					"partitionId": map[string]any{"projectId": "test"},
					"path":        []any{map[string]any{"kind": "Parent", "id": "1"}},
				}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	batch, _ := rq.Data["batch"].(map[string]any)
	if ers, _ := batch["entityResults"].([]any); len(ers) != 1 {
		t.Fatalf("ancestor query results = %v, want 1", batch["entityResults"])
	}
}

// TestRESTOccUpdateTimeToken locks the REST encoding of the update_time
// optimistic-concurrency token: it is emitted on commit and lookup, is strictly
// monotonic under a frozen clock, and a stale precondition is reported as
// conflictDetected while still echoing the current entity's updateTime.
func TestRESTOccUpdateTimeToken(t *testing.T) {
	c, p := newTestProvider(t)
	frozen := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: frozen})
	defer clock.SetGlobalClock(clock.RealClock{})

	parse := func(s string) time.Time {
		t.Helper()
		tt, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatalf("parse updateTime %q: %v", s, err)
		}
		return tt
	}

	insert, err := call(t, c, p, "test", "commit", map[string]any{
		"mode": "NON_TRANSACTIONAL",
		"mutations": []any{map[string]any{"insert": map[string]any{
			"key":        nameKey("Task", "occ"),
			"properties": map[string]any{"n": map[string]any{"integerValue": "1"}},
		}}},
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	token, _ := insert.Data["mutationResults"].([]any)[0].(map[string]any)["updateTime"].(string)
	if token == "" {
		t.Fatal("commit mutationResults[].updateTime must be set")
	}

	lookup, err := call(t, c, p, "test", "lookup", map[string]any{"keys": []any{nameKey("Task", "occ")}})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got := lookup.Data["found"].([]any)[0].(map[string]any)["updateTime"]; got != token {
		t.Fatalf("lookup updateTime = %v, want %v (the commit token)", got, token)
	}

	updateBody := func(n int, ut string) map[string]any {
		return map[string]any{
			"mode": "NON_TRANSACTIONAL",
			"mutations": []any{map[string]any{
				"update": map[string]any{
					"key":        nameKey("Task", "occ"),
					"properties": map[string]any{"n": map[string]any{"integerValue": strconv.Itoa(n)}},
				},
				"updateTime": ut,
			}},
		}
	}

	applied, err := call(t, c, p, "test", "commit", updateBody(2, token))
	if err != nil {
		t.Fatalf("update with token: %v", err)
	}
	res0 := applied.Data["mutationResults"].([]any)[0].(map[string]any)
	if cd, _ := res0["conflictDetected"].(bool); cd {
		t.Fatal("a matching updateTime precondition must not conflict")
	}
	advanced, _ := res0["updateTime"].(string)
	if advanced == "" || !parse(advanced).After(parse(token)) {
		t.Fatalf("applied updateTime = %q, want strictly after %q", advanced, token)
	}

	stale, err := call(t, c, p, "test", "commit", updateBody(3, token))
	if err != nil {
		t.Fatalf("stale update: %v", err)
	}
	res1 := stale.Data["mutationResults"].([]any)[0].(map[string]any)
	if cd, _ := res1["conflictDetected"].(bool); !cd {
		t.Fatal("a stale updateTime precondition must set conflictDetected")
	}
	if got, _ := res1["updateTime"].(string); got != advanced {
		t.Fatalf("conflict updateTime = %q, want %q (the current entity's)", got, advanced)
	}

	after, err := call(t, c, p, "test", "lookup", map[string]any{"keys": []any{nameKey("Task", "occ")}})
	if err != nil {
		t.Fatalf("lookup after conflict: %v", err)
	}
	props := after.Data["found"].([]any)[0].(map[string]any)["entity"].(map[string]any)["properties"].(map[string]any)
	if got := props["n"].(map[string]any)["integerValue"]; got != "2" {
		t.Fatalf("n = %v after the rejected stale write, want \"2\"", got)
	}
}

func TestCodecRejectsNonPostAndUnknownVerb(t *testing.T) {
	c := NewCodec()
	raw := []byte(`{}`)

	get, _ := http.NewRequest(http.MethodGet, "/v1/projects/test:commit", bytes.NewReader(raw))
	if _, err := c.Decode(get, raw); err == nil {
		t.Fatal("GET decode should fail")
	}

	post, _ := http.NewRequest(http.MethodPost, "/v1/projects/test:bogus", bytes.NewReader(raw))
	if _, err := c.Decode(post, raw); err == nil {
		t.Fatal("unknown verb decode should fail")
	}
}
