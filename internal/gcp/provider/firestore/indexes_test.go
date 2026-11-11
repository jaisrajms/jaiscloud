package firestore

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func TestIndexPath(t *testing.T) {
	cases := []struct {
		name string
		want struct {
			db, cg, id string
			ok         bool
		}
	}{
		{
			name: "databases/(default)/collectionGroups/cities/indexes",
			want: struct {
				db, cg, id string
				ok         bool
			}{"(default)", "cities", "", true},
		},
		{
			name: "databases/(default)/collectionGroups/cities/indexes/abc123",
			want: struct {
				db, cg, id string
				ok         bool
			}{"(default)", "cities", "abc123", true},
		},
		{name: "databases/(default)/collectionGroups/cities", want: struct {
			db, cg, id string
			ok         bool
		}{"", "", "", false}},
	}
	for _, tc := range cases {
		db, cg, id, ok := indexPath(tc.name)
		if ok != tc.want.ok || db != tc.want.db || cg != tc.want.cg || id != tc.want.id {
			t.Errorf("indexPath(%q) = (%q,%q,%q,%v), want (%q,%q,%q,%v)",
				tc.name, db, cg, id, ok, tc.want.db, tc.want.cg, tc.want.id, tc.want.ok)
		}
	}
}

func TestIndexMapState(t *testing.T) {
	m := indexMap(indexDef{Name: "n", QueryScope: "COLLECTION", Fields: []indexField{{FieldPath: "a", Order: "ASCENDING"}}})
	if m["state"] != "READY" {
		t.Errorf("indexMap state = %v, want READY", m["state"])
	}
}

func TestCreateIndexReturnsOperation(t *testing.T) {
	ctx := context.Background()
	p := New(nil, store.NewMemoryResourceStore())

	nr := &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     map[string]any{},
		ResourceID: func(rt, n string) string { return "projects/proj/" + n },
	}
	nr.Params["name"] = "databases/(default)/collectionGroups/cities/indexes"
	nr.Params["body"] = map[string]any{
		"queryScope": "COLLECTION",
		"fields": []any{
			map[string]any{"fieldPath": "a", "order": "ASCENDING"},
			map[string]any{"fieldPath": "b", "order": "ASCENDING"},
		},
	}

	resp, err := p.CreateIndex(ctx, nr)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if resp.Data["done"] != true {
		t.Errorf("CreateIndex should return done=true, got %v", resp.Data["done"])
	}
	name, _ := resp.Data["name"].(string)
	if want := "projects/proj/databases/(default)/operations/"; len(name) < len(want) || name[:len(want)] != want {
		t.Errorf("CreateIndex operation name = %q, want prefix %q", name, want)
	}
	respIdx, ok := resp.Data["response"].(map[string]any)
	if !ok {
		t.Fatalf("CreateIndex response should contain the created index, got %v", resp.Data["response"])
	}
	if respIdx["state"] != "READY" {
		t.Errorf("created index state = %v, want READY", respIdx["state"])
	}
}

func TestListIndexesPaginationAndFilter(t *testing.T) {
	ctx := context.Background()
	p := New(nil, store.NewMemoryResourceStore())

	// Seed three indexes.
	for i, fp := range []string{"a", "b", "c"} {
		nr := &model.NormalizedRequest{
			AccountID:  "proj",
			Params:     map[string]any{},
			ResourceID: func(rt, n string) string { return "projects/proj/" + n },
		}
		nr.Params["name"] = "databases/(default)/collectionGroups/cities/indexes"
		nr.Params["body"] = map[string]any{
			"queryScope": "COLLECTION",
			"fields": []any{
				map[string]any{"fieldPath": fp, "order": "ASCENDING"},
				map[string]any{"fieldPath": "x", "order": "ASCENDING"},
			},
		}
		if _, err := p.CreateIndex(ctx, nr); err != nil {
			t.Fatalf("seed index %d: %v", i, err)
		}
	}

	nr := &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     map[string]any{"pageSize": "2"},
		ResourceID: func(rt, n string) string { return "projects/proj/" + n },
	}
	nr.Params["name"] = "databases/(default)/collectionGroups/cities/indexes"

	resp, err := p.ListIndexes(ctx, nr)
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	indexes, _ := resp.Data["indexes"].([]any)
	if len(indexes) != 2 {
		t.Errorf("expected 2 indexes (pageSize=2), got %d", len(indexes))
	}
	if resp.Data["nextPageToken"] == "" {
		t.Errorf("expected nextPageToken with 3 indexes and pageSize=2")
	}

	// Filter: substring match on fieldPath.
	nr.Params["filter"] = "c"
	resp, err = p.ListIndexes(ctx, nr)
	if err != nil {
		t.Fatalf("ListIndexes with filter: %v", err)
	}
	indexes, _ = resp.Data["indexes"].([]any)
	if len(indexes) != 1 {
		t.Errorf("expected 1 index matching filter 'c', got %d", len(indexes))
	}
}

// TestListIndexesWildcardCollectionGroup verifies the AIP-123 "-" wildcard lists
// indexes from every collection group (the real API behavior gcloud and the
// console rely on), not just one.
func TestListIndexesWildcardCollectionGroup(t *testing.T) {
	ctx := context.Background()
	p := New(nil, store.NewMemoryResourceStore())

	for _, cg := range []string{"cities", "states"} {
		nr := &model.NormalizedRequest{
			AccountID:  "proj",
			Params:     map[string]any{},
			ResourceID: func(rt, n string) string { return "projects/proj/" + n },
		}
		nr.Params["name"] = "databases/(default)/collectionGroups/" + cg + "/indexes"
		nr.Params["body"] = map[string]any{
			"queryScope": "COLLECTION",
			"fields": []any{
				map[string]any{"fieldPath": "a", "order": "ASCENDING"},
				map[string]any{"fieldPath": "b", "order": "ASCENDING"},
			},
		}
		if _, err := p.CreateIndex(ctx, nr); err != nil {
			t.Fatalf("seed index in %s: %v", cg, err)
		}
	}

	nr := &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     map[string]any{},
		ResourceID: func(rt, n string) string { return "projects/proj/" + n },
	}
	nr.Params["name"] = "databases/(default)/collectionGroups/-/indexes"

	resp, err := p.ListIndexes(ctx, nr)
	if err != nil {
		t.Fatalf("ListIndexes wildcard: %v", err)
	}
	indexes, _ := resp.Data["indexes"].([]any)
	if len(indexes) != 2 {
		t.Fatalf("wildcard list returned %d indexes, want 2 (all collection groups)", len(indexes))
	}

	// A concrete group must still be scoped to itself.
	nr.Params["name"] = "databases/(default)/collectionGroups/cities/indexes"
	resp, err = p.ListIndexes(ctx, nr)
	if err != nil {
		t.Fatalf("ListIndexes cities: %v", err)
	}
	indexes, _ = resp.Data["indexes"].([]any)
	if len(indexes) != 1 {
		t.Fatalf("cities list returned %d indexes, want 1", len(indexes))
	}
}

// ─── FE1: the Enterprise edition index surface ───────────────────────────────

// createTestDatabase creates a database with the given edition through the REST
// adapter and returns the provider.
func createTestDatabase(t *testing.T, edition string) *Provider {
	t.Helper()
	p := newAdminProvider()
	ctx := context.Background()
	body := map[string]any{"locationId": "nam5"}
	if edition != "" {
		body["databaseEdition"] = edition
	}
	dbID := "ent-db"
	if edition != databaseEditionEnterprise {
		dbID = "std-db"
	}
	if _, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": dbID,
		"body":       body,
	})); err != nil {
		t.Fatalf("CreateDatabase(%s): %v", dbID, err)
	}
	return p
}

// createTestIndex POSTs one index through the REST adapter and returns the
// created index's response object plus its full resource name.
func createTestIndex(t *testing.T, p *Provider, db, cg string, body map[string]any) (map[string]any, string) {
	t.Helper()
	resp, err := p.CreateIndex(context.Background(), adminNR(
		"databases/"+db+"/collectionGroups/"+cg+"/indexes", map[string]any{"body": body}))
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if resp.Data["done"] != true {
		t.Fatalf("CreateIndex done = %v, want true", resp.Data["done"])
	}
	idx, ok := resp.Data["response"].(map[string]any)
	if !ok {
		t.Fatalf("CreateIndex response = %v, want the created index", resp.Data["response"])
	}
	name, _ := idx["name"].(string)
	return idx, name
}

// indexData unwraps a REST adapter response body.
func indexData(t *testing.T, resp *model.ProviderResponse, err error) map[string]any {
	t.Helper()
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return resp.Data
}

// indexID returns the trailing id of a full index resource name.
func indexID(t *testing.T, name string) string {
	t.Helper()
	parts := strings.Split(name, "/")
	if len(parts) == 0 {
		t.Fatalf("empty index name")
	}
	return parts[len(parts)-1]
}

// TestEnterpriseSingleFieldIndexRoundTrip is the headline FE1 behaviour: in an
// Enterprise edition database a single-field index is a first-class Index
// resource (Discovery: "In Enterprise edition databases, both single-field and
// composite indexes are managed using the google.firestore.admin.v1.Index
// resource"), and it carries the Enterprise index options.
func TestEnterpriseSingleFieldIndexRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := createTestDatabase(t, databaseEditionEnterprise)

	created, name := createTestIndex(t, p, "ent-db", "orders", map[string]any{
		"density":    "SPARSE_ANY",
		"apiScope":   "MONGODB_COMPATIBLE_API",
		"multikey":   true,
		"unique":     true,
		"shardCount": int32(3),
		"fields":     []any{map[string]any{"fieldPath": "status", "order": "ASCENDING"}},
	})
	want := map[string]any{
		"queryScope": "COLLECTION",
		"density":    "SPARSE_ANY",
		"apiScope":   "MONGODB_COMPATIBLE_API",
		"multikey":   true,
		"unique":     true,
		"shardCount": int32(3),
		"state":      "READY",
	}
	for k, v := range want {
		if created[k] != v {
			t.Errorf("created[%q] = %v, want %v", k, created[k], v)
		}
	}
	fields, _ := created["fields"].([]any)
	if len(fields) != 1 {
		t.Fatalf("created fields = %v, want exactly the single indexed field", created["fields"])
	}

	getResp, getErr := p.GetIndex(ctx, adminNR("databases/ent-db/collectionGroups/orders/indexes/"+indexID(t, name), nil))
	got := indexData(t, getResp, getErr)
	if got["density"] != "SPARSE_ANY" || got["apiScope"] != "MONGODB_COMPATIBLE_API" ||
		got["multikey"] != true || got["unique"] != true || got["shardCount"] != int32(3) {
		t.Errorf("GetIndex = %v, want the Enterprise options echoed", got)
	}

	listResp, listErr := p.ListIndexes(ctx, adminNR("databases/ent-db/collectionGroups/orders/indexes", nil))
	list := indexData(t, listResp, listErr)
	rows, _ := list["indexes"].([]any)
	if len(rows) != 1 {
		t.Fatalf("ListIndexes len = %d, want 1", len(rows))
	}
	if rows[0].(map[string]any)["density"] != "SPARSE_ANY" {
		t.Errorf("ListIndexes row = %v, want the stored density", rows[0])
	}

	if _, err := p.DeleteIndex(ctx, adminNR("databases/ent-db/collectionGroups/orders/indexes/"+indexID(t, name), nil)); err != nil {
		t.Fatalf("DeleteIndex: %v", err)
	}
	if _, err := p.GetIndex(ctx, adminNR("databases/ent-db/collectionGroups/orders/indexes/"+indexID(t, name), nil)); !isNotFound(err) {
		t.Errorf("GetIndex after delete = %v, want NotFound", err)
	}
}

// TestIndexDensityEditionDefaults covers Index.Density's documented defaults:
// DENSE for Enterprise, SPARSE_ALL for Standard, and the INPUT_ONLY
// DENSITY_UNSPECIFIED resolving to the edition default rather than being served.
func TestIndexDensityEditionDefaults(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		edition string
		db      string
		want    string
	}{
		{"enterprise", databaseEditionEnterprise, "ent-db", "DENSE"},
		{"standard", databaseEditionStandard, "std-db", "SPARSE_ALL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := createTestDatabase(t, tc.edition)
			_, name := createTestIndex(t, p, tc.db, "orders", map[string]any{
				"fields": []any{
					map[string]any{"fieldPath": "a", "order": "ASCENDING"},
					map[string]any{"fieldPath": "b", "order": "ASCENDING"},
				},
			})
			dbResp, dbErr := p.GetIndex(ctx, adminNR("databases/"+tc.db+"/collectionGroups/orders/indexes/"+indexID(t, name), nil))
			got := indexData(t, dbResp, dbErr)
			if got["density"] != tc.want {
				t.Errorf("density = %v, want %s", got["density"], tc.want)
			}
			// The INPUT_ONLY sentinel must never be stored or served.
			if _, err := p.CreateIndex(ctx, adminNR("databases/"+tc.db+"/collectionGroups/orders/indexes", map[string]any{
				"body": map[string]any{
					"density": "DENSITY_UNSPECIFIED",
					"fields": []any{
						map[string]any{"fieldPath": "c", "order": "ASCENDING"},
						map[string]any{"fieldPath": "d", "order": "ASCENDING"},
					},
				},
			})); err != nil {
				t.Fatalf("CreateIndex(DENSITY_UNSPECIFIED): %v", err)
			}
			lr, le := p.ListIndexes(ctx, adminNR("databases/"+tc.db+"/collectionGroups/orders/indexes", nil))
			list := indexData(t, lr, le)
			for _, raw := range list["indexes"].([]any) {
				if d := raw.(map[string]any)["density"]; d != tc.want {
					t.Errorf("listed density = %v, want %s", d, tc.want)
				}
			}
		})
	}
}

// TestIndexEditionRules covers the edition-conditional create rules the
// Discovery document states: a Standard edition index needs at least 2 fields
// (single-field indexes live on the Field resource) and accepts only SPARSE_ALL,
// while an Enterprise index may be single-field and carries any density.
func TestIndexEditionRules(t *testing.T) {
	ctx := context.Background()
	oneField := []any{map[string]any{"fieldPath": "status", "order": "ASCENDING"}}

	std := createTestDatabase(t, databaseEditionStandard)
	_, err := std.CreateIndex(ctx, adminNR("databases/std-db/collectionGroups/orders/indexes",
		map[string]any{"body": map[string]any{"fields": oneField}}))
	if err == nil || !strings.Contains(err.Error(), "Field resource") {
		t.Errorf("Standard single-field index error = %v, want the Field-resource InvalidArgument", err)
	}
	_, err = std.CreateIndex(ctx, adminNR("databases/std-db/collectionGroups/orders/indexes", map[string]any{
		"body": map[string]any{
			"density": "DENSE",
			"fields": []any{
				map[string]any{"fieldPath": "a", "order": "ASCENDING"},
				map[string]any{"fieldPath": "b", "order": "ASCENDING"},
			},
		},
	}))
	if err == nil || !strings.Contains(err.Error(), "SPARSE_ALL") {
		t.Errorf("Standard DENSE index error = %v, want the SPARSE_ALL InvalidArgument", err)
	}

	ent := createTestDatabase(t, databaseEditionEnterprise)
	entOneField, _ := createTestIndex(t, ent, "ent-db", "orders", map[string]any{"fields": oneField})
	if entOneField["density"] != "DENSE" || entOneField["queryScope"] != "COLLECTION" {
		t.Errorf("Enterprise single-field index = %v, want COLLECTION/DENSE", entOneField)
	}
	for _, density := range []string{"SPARSE_ALL", "SPARSE_ANY", "DENSE"} {
		idx, _ := createTestIndex(t, ent, "ent-db", "orders", map[string]any{
			"density": density,
			"fields": []any{
				map[string]any{"fieldPath": "a", "order": "ASCENDING"},
				map[string]any{"fieldPath": "b", "order": "ASCENDING"},
			},
		})
		if idx["density"] != density {
			t.Errorf("Enterprise density %s echoed as %v", density, idx["density"])
		}
	}
}

// TestIndexFieldCountAndEnumValidation covers the field-count bounds and the
// proto3-JSON enum rule, so an unknown enum name is rejected instead of stored
// and echoed verbatim.
func TestIndexFieldCountAndEnumValidation(t *testing.T) {
	ctx := context.Background()
	p := createTestDatabase(t, databaseEditionEnterprise)
	base := "databases/ent-db/collectionGroups/orders/indexes"

	if _, err := p.CreateIndex(ctx, adminNR(base, map[string]any{"body": map[string]any{}})); err == nil {
		t.Errorf("empty-field index error = nil, want InvalidArgument")
	}

	fields := make([]any, 0, 101)
	for i := 0; i < 101; i++ {
		fields = append(fields, map[string]any{"fieldPath": "f" + strconv.Itoa(i), "order": "ASCENDING"})
	}
	if _, err := p.CreateIndex(ctx, adminNR(base, map[string]any{"body": map[string]any{"fields": fields}})); err == nil ||
		!strings.Contains(err.Error(), "at most 100") {
		t.Errorf("101-field index error = %v, want the 100-field InvalidArgument", err)
	}

	twoFields := []any{
		map[string]any{"fieldPath": "a", "order": "ASCENDING"},
		map[string]any{"fieldPath": "b", "order": "DESCENDING"},
	}
	for name, body := range map[string]map[string]any{
		"queryScope": {"queryScope": "COLLECTION_EVERYTHING", "fields": twoFields},
		"apiScope":   {"apiScope": "SOME_API", "fields": twoFields},
		"density":    {"density": "SPARSE_SOME", "fields": twoFields},
		"order":      {"fields": []any{map[string]any{"fieldPath": "a", "order": "SIDEWAYS"}}},
	} {
		if _, err := p.CreateIndex(ctx, adminNR(base, map[string]any{"body": body})); err == nil {
			t.Errorf("invalid %s accepted, want InvalidArgument", name)
		}
	}
}

// TestIndexParentedByDatabase covers the child-resource rule on the index read
// and delete paths: an index is parented by its database, so both answer
// NOT_FOUND once the parent is gone (the delete cascade removes the records,
// and a database that never existed has none either).
func TestIndexParentedByDatabase(t *testing.T) {
	ctx := context.Background()
	p := createTestDatabase(t, databaseEditionEnterprise)
	_, name := createTestIndex(t, p, "ent-db", "orders", map[string]any{
		"fields": []any{
			map[string]any{"fieldPath": "a", "order": "ASCENDING"},
			map[string]any{"fieldPath": "b", "order": "ASCENDING"},
		},
	})
	rel := "databases/ent-db/collectionGroups/orders/indexes/" + indexID(t, name)
	if _, err := p.GetIndex(ctx, adminNR(rel, nil)); err != nil {
		t.Fatalf("GetIndex before delete: %v", err)
	}
	dbResp, dbErr := p.GetDatabase(ctx, adminNR("databases/ent-db", nil))
	db := indexData(t, dbResp, dbErr)
	etag, _ := db["etag"].(string)
	if _, err := p.DeleteDatabase(ctx, adminNR("databases/ent-db", map[string]any{"etag": etag})); err != nil {
		t.Fatalf("DeleteDatabase: %v", err)
	}
	if _, err := p.GetIndex(ctx, adminNR(rel, nil)); !isNotFound(err) {
		t.Errorf("GetIndex after database delete = %v, want NotFound", err)
	}
	if _, err := p.DeleteIndex(ctx, adminNR(rel, nil)); !isNotFound(err) {
		t.Errorf("DeleteIndex after database delete = %v, want NotFound", err)
	}
	if _, err := p.ListIndexes(ctx, adminNR("databases/ent-db/collectionGroups/orders/indexes", nil)); !isNotFound(err) {
		t.Errorf("ListIndexes after database delete = %v, want NotFound", err)
	}
}

// TestNormalizeIndexLegacyRecord covers the read-time repair: a record written
// before the index options were stored carries no density, and a read must
// serve the edition default rather than an empty value.
func TestNormalizeIndexLegacyRecord(t *testing.T) {
	legacy := indexDef{Name: "n", QueryScope: "COLLECTION", Fields: []indexField{{FieldPath: "a", Order: "ASCENDING"}}}
	if got := normalizeIndex(databaseEditionEnterprise, legacy).Density; got != indexDensityDense {
		t.Errorf("legacy Enterprise density = %q, want DENSE", got)
	}
	if got := normalizeIndex(databaseEditionStandard, legacy).Density; got != indexDensitySparseAll {
		t.Errorf("legacy Standard density = %q, want SPARSE_ALL", got)
	}
	if got := normalizeIndex(databaseEditionEnterprise, indexDef{APIScope: indexAPIScopeAny}).APIScope; got != "" {
		t.Errorf("ANY_API must be normalized to unset, got %q", got)
	}
}
