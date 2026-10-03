// Package iceberg implements the BigLake Metastore Iceberg REST Catalog,
// mounted at /iceberg/v1/... (Spark configures uri=http://host:port/iceberg/,
// so real requests arrive as /iceberg/v1/...). BigLake Metastore is GCP's
// managed-Iceberg product, and its catalog is the standard Apache Iceberg REST
// catalog surface — Spark, Trino, and Flink attach over the standard spec. The
// catalog is addressed by warehouse prefix + namespace (multi-level, levels
// joined by "/"), and it speaks Iceberg's own REST JSON (kebab-case
// TableMetadata, camelCase request envelopes) with Iceberg's ErrorResponse
// error shape — not the GCP error envelope.
//
// The catalog never writes metadata.json to object storage (the client's
// FileIO does); it persists the TableMetadata JSON plus a metadata-location
// pointer in its own store, and the commit path applies requirements + updates
// under an atomic read-modify-write so concurrent commits cannot lose updates.
package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	icebergstore "jaiscloud/internal/gcp/store/iceberg"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	"github.com/google/uuid"
)

// Provider handles the Iceberg REST catalog surface.
type Provider struct {
	store icebergstore.Store
}

// New returns a Provider backed by the given store.
func New(s icebergstore.Store) *Provider {
	return &Provider{store: s}
}

// Reset wipes the store.
func (p *Provider) Reset(ctx context.Context) { p.store.Reset(ctx) }

// Routes returns the "Iceberg.*" handler map. The action names mirror the
// codec's method/path mapping in internal/gcp/adapter/iceberg.go.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Iceberg.GetConfig":                 p.GetConfig,
		"Iceberg.ListNamespaces":            p.ListNamespaces,
		"Iceberg.CreateNamespace":           p.CreateNamespace,
		"Iceberg.GetNamespace":              p.GetNamespace,
		"Iceberg.NamespaceExists":           p.NamespaceExists,
		"Iceberg.DropNamespace":             p.DropNamespace,
		"Iceberg.UpdateNamespaceProperties": p.UpdateNamespaceProperties,
		"Iceberg.ListTables":                p.ListTables,
		"Iceberg.CreateTable":               p.CreateTable,
		"Iceberg.LoadTable":                 p.LoadTable,
		"Iceberg.CommitTable":               p.CommitTable,
		"Iceberg.DropTable":                 p.DropTable,
		"Iceberg.RenameTable":               p.RenameTable,
		"Iceberg.TableMetrics":              p.TableMetrics,
	}
}

// --- helpers ---

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

// bodyObj returns nr.Params["body"] as a map.
func bodyObj(nr *model.NormalizedRequest) map[string]any {
	m, _ := nr.Params["body"].(map[string]any)
	return m
}

// mapErr maps store sentinel errors to Iceberg ErrorResponse exceptions.
func mapErr(err error) error {
	switch {
	case errors.Is(err, icebergstore.ErrNamespaceExists):
		return model.NewProviderError("AlreadyExistsException", "namespace already exists", 409)
	case errors.Is(err, icebergstore.ErrNamespaceNotFound):
		return model.NewProviderError("NoSuchNamespaceException", "namespace not found", 404)
	case errors.Is(err, icebergstore.ErrNamespaceNotEmpty):
		return model.NewProviderError("NamespaceNotEmptyException", "namespace is not empty", 409)
	case errors.Is(err, icebergstore.ErrTableExists):
		return model.NewProviderError("AlreadyExistsException", "table already exists", 409)
	case errors.Is(err, icebergstore.ErrTableNotFound):
		return model.NewProviderError("NoSuchTableException", "table not found", 404)
	}
	return err
}

func badRequest(msg string) error {
	return model.NewProviderError("BadRequestException", msg, 400)
}

func commitFailed(msg string) error {
	return model.NewProviderError("CommitFailedException", msg, 409)
}

func internalError(msg string) error {
	return model.NewProviderError("ServiceUnavailableException", msg, 500)
}

// --- JSON value helpers (numbers decode as float64) ---

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func asInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

func asInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, _ := n.Int64()
		return i
	}
	return 0
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func asList(v any) []any {
	if v == nil {
		return nil
	}
	switch l := v.(type) {
	case []any:
		return l
	case []map[string]any:
		out := make([]any, len(l))
		for i, m := range l {
			out[i] = m
		}
		return out
	}
	return nil
}

// listOf returns meta[key] as []any (nil if absent or not a list).
func listOf(meta map[string]any, key string) []any {
	return asList(meta[key])
}

// stringMap coerces a map[string]any to map[string]string.
func stringMap(m map[string]any) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// splitNamespace splits a "/"-joined namespace back into levels.
func splitNamespace(ns string) []string {
	if ns == "" {
		return []string{}
	}
	return strings.Split(ns, "/")
}

// --- Config ---

func (p *Provider) GetConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"defaults":  map[string]any{},
		"overrides": map[string]any{},
		"endpoints": []any{},
	}), nil
}

// --- Namespaces ---

func (p *Provider) CreateNamespace(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyObj(nr)
	nsLevels, _ := body["namespace"].([]any)
	ns := strings.Join(anyStrings(nsLevels), "/")
	props := stringMap(asMap(body["properties"]))
	if err := p.store.CreateNamespace(ctx, ns, props); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(map[string]any{
		"namespace":  nsLevels,
		"properties": props,
	}), nil
}

func (p *Provider) ListNamespaces(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	namespaces, err := p.store.ListNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(namespaces, func(n icebergstore.Namespace) string { return n.Namespace }, nr.Params)
	items := make([]any, 0, len(page))
	for _, n := range page {
		items = append(items, splitNamespace(n.Namespace))
	}
	resp := map[string]any{"namespaces": items}
	if next != "" {
		resp["next-page-token"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) GetNamespace(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns := strParam(nr, "namespace")
	props, err := p.store.GetNamespace(ctx, ns)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(map[string]any{
		"namespace":  splitNamespace(ns),
		"properties": props,
	}), nil
}

func (p *Provider) NamespaceExists(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns := strParam(nr, "namespace")
	ok, err := p.store.NamespaceExists(ctx, ns)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, mapErr(icebergstore.ErrNamespaceNotFound)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *Provider) DropNamespace(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns := strParam(nr, "namespace")
	if err := p.store.DropNamespace(ctx, ns); err != nil {
		return nil, mapErr(err)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *Provider) UpdateNamespaceProperties(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns := strParam(nr, "namespace")
	body := bodyObj(nr)
	removals := anyStrings(asList(body["removals"]))
	updates := stringMap(asMap(body["updates"]))

	// The store returns the pre-update map from the same locked read that
	// produced the write, so the removed/missing split below reflects the
	// properties as they actually were — not a separate GetNamespace read that
	// a concurrent writer could have invalidated.
	result, err := p.store.UpdateNamespaceProperties(ctx, ns, removals, updates)
	if err != nil {
		return nil, mapErr(err)
	}

	// Iceberg's UpdateNamespacePropertiesResponse reports which keys were
	// updated, which were removed, and which removals referred to missing keys.
	var updated, removed, missing []string
	for k := range updates {
		updated = append(updated, k)
	}
	for _, k := range removals {
		if _, ok := result.Before[k]; ok {
			removed = append(removed, k)
		} else {
			missing = append(missing, k)
		}
	}
	return provider.OK(map[string]any{
		"updated": sortedStrings(updated),
		"removed": sortedStrings(removed),
		"missing": sortedStrings(missing),
	}), nil
}

// --- Tables ---

func (p *Provider) ListTables(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns := strParam(nr, "namespace")
	if _, err := p.store.GetNamespace(ctx, ns); err != nil {
		return nil, mapErr(err)
	}
	tables, err := p.store.ListTables(ctx, ns)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(tables, func(t icebergstore.Table) string { return t.Name }, nr.Params)
	identifiers := make([]any, 0, len(page))
	for _, t := range page {
		identifiers = append(identifiers, map[string]any{
			"namespace": splitNamespace(ns),
			"name":      t.Name,
		})
	}
	resp := map[string]any{"identifiers": identifiers}
	if next != "" {
		resp["next-page-token"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) CreateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns := strParam(nr, "namespace")
	body := bodyObj(nr)
	name := asString(body["name"])
	if name == "" {
		return nil, badRequest("missing name")
	}
	location := asString(body["location"])
	if location == "" {
		return nil, badRequest("missing location")
	}
	schema := asMap(body["schema"])
	if schema == nil {
		return nil, badRequest("missing schema")
	}
	if _, err := p.store.GetNamespace(ctx, ns); err != nil {
		return nil, mapErr(err)
	}

	tableUUID := uuid.NewString()
	now := clock.Now().UnixMilli()

	normalizedSchema := normalizeSchema(schema, 0)
	lastColumnID := maxSchemaColumnID(normalizedSchema)
	normalizedSpec := normalizePartitionSpec(asMap(body["partition-spec"]), 0)
	lastPartitionID := maxPartitionFieldID(normalizedSpec)
	normalizedOrder := normalizeSortOrder(asMap(body["write-order"]), 0)
	properties := stringMap(asMap(body["properties"]))
	if properties == nil {
		properties = map[string]string{}
	}

	meta := map[string]any{
		"format-version":        2,
		"table-uuid":            tableUUID,
		"location":              location,
		"last-sequence-number":  0,
		"last-updated-ms":       now,
		"last-column-id":        lastColumnID,
		"schemas":               []any{normalizedSchema},
		"current-schema-id":     0,
		"partition-specs":       []any{normalizedSpec},
		"default-spec-id":       0,
		"last-partition-id":     lastPartitionID,
		"properties":            properties,
		"current-snapshot-id":   -1,
		"snapshots":             []any{},
		"snapshot-log":          []any{},
		"metadata-log":          []any{},
		"sort-orders":           []any{normalizedOrder},
		"default-sort-order-id": 0,
		"statistics":            []any{},
		"partition-statistics":  []any{},
		"refs":                  map[string]any{},
	}

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	t := icebergstore.Table{
		Namespace:        ns,
		Name:             name,
		Metadata:         metaJSON,
		MetadataLocation: metadataLocation(location, 0, tableUUID),
		UUID:             tableUUID,
		Version:          0,
	}
	if err := p.store.CreateTable(ctx, ns, name, t); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(loadTableResult(t)), nil
}

func (p *Provider) LoadTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns := strParam(nr, "namespace")
	name := strParam(nr, "table")
	t, err := p.store.GetTable(ctx, ns, name)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(loadTableResult(t)), nil
}

func (p *Provider) DropTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns := strParam(nr, "namespace")
	name := strParam(nr, "table")
	if err := p.store.DropTable(ctx, ns, name); err != nil {
		return nil, mapErr(err)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *Provider) RenameTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyObj(nr)
	src := identifierOf(asMap(body["source"]))
	dst := identifierOf(asMap(body["destination"]))
	if src.namespace == "" || src.name == "" || dst.namespace == "" || dst.name == "" {
		return nil, badRequest("missing source or destination identifier")
	}
	if _, err := p.store.RenameTable(ctx, src.namespace, src.name, dst.namespace, dst.name); err != nil {
		return nil, mapErr(err)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// TableMetrics implements the Iceberg REST reportMetrics endpoint:
// POST .../namespaces/{namespace}/tables/{table}/metrics with a
// ReportMetricsRequest body. The catalog validates the target table and the
// report type and acknowledges the report with 204. The spec exposes no
// read-back for reports, so they are not persisted — aggregating them is a
// backend concern, not catalog state.
func (p *Provider) TableMetrics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns := strParam(nr, "namespace")
	name := strParam(nr, "table")
	if _, err := p.store.GetTable(ctx, ns, name); err != nil {
		return nil, mapErr(err)
	}
	if asString(bodyObj(nr)["report-type"]) == "" {
		return nil, badRequest("missing report-type")
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// --- commit ---

// CommitTable evaluates requirements against the current table state, applies
// the updates in order, and atomically persists the new metadata.
func (p *Provider) CommitTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns := strParam(nr, "namespace")
	name := strParam(nr, "table")
	body := bodyObj(nr)
	requirements := body["requirements"]
	updates := body["updates"]

	committed, err := p.store.CommitTable(ctx, ns, name, func(cur icebergstore.Table) (icebergstore.Table, error) {
		var meta map[string]any
		if len(cur.Metadata) > 0 {
			if err := json.Unmarshal(cur.Metadata, &meta); err != nil {
				return icebergstore.Table{}, internalError("corrupt table metadata")
			}
		}
		if meta == nil {
			meta = map[string]any{}
		}

		if err := checkRequirements(meta, requirements); err != nil {
			return icebergstore.Table{}, err
		}
		if err := applyUpdates(meta, updates); err != nil {
			return icebergstore.Table{}, err
		}

		next := meta
		now := clock.Now().UnixMilli()
		next["last-updated-ms"] = now
		tableUUID := asString(next["table-uuid"])
		if tableUUID == "" {
			tableUUID = cur.UUID
			next["table-uuid"] = tableUUID
		}
		location := asString(next["location"])
		newVersion := cur.Version + 1

		// metadata-log is append-only and is written once per successful commit
		// (not just on add-snapshot): every commit in real Iceberg supersedes the
		// previous metadata file, so record the file this commit replaces.
		next["metadata-log"] = append(listOf(next, "metadata-log"), map[string]any{
			"timestamp-ms":  now,
			"metadata-file": cur.MetadataLocation,
		})

		nextJSON, err := json.Marshal(next)
		if err != nil {
			return icebergstore.Table{}, err
		}
		return icebergstore.Table{
			Namespace:        ns,
			Name:             name,
			Metadata:         nextJSON,
			MetadataLocation: metadataLocation(location, newVersion, tableUUID),
			UUID:             tableUUID,
			Version:          newVersion,
		}, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(map[string]any{
		"metadata-location": committed.MetadataLocation,
		"metadata":          jsonAny(committed.Metadata),
	}), nil
}

// checkRequirements evaluates each requirement against the current metadata. A
// failing requirement aborts the whole commit with CommitFailedException.
func checkRequirements(meta map[string]any, requirements any) error {
	for _, r := range asList(requirements) {
		m := asMap(r)
		switch asString(m["type"]) {
		case "assert-create":
			// CommitTable operates on an existing table, so assert-create can
			// never be satisfied here (it is used by create-in-commit/rename,
			// which are separate operations).
			return commitFailed("table already exists")
		case "assert-table-uuid":
			if got := asString(meta["table-uuid"]); got != asString(m["uuid"]) {
				return commitFailed(fmt.Sprintf("requirement failed: table uuid %s != %s", got, asString(m["uuid"])))
			}
		case "assert-ref-snapshot-id":
			ref := asString(m["ref"])
			want := asInt64(m["snapshot-id"])
			if got := refSnapshotID(meta, ref); got != want {
				return commitFailed(fmt.Sprintf("requirement failed: ref %s snapshot id %d != %d", ref, got, want))
			}
		case "assert-last-assigned-field-id":
			if got := asInt(meta["last-column-id"]); got != asInt(m["last-assigned-field-id"]) {
				return commitFailed(fmt.Sprintf("requirement failed: last assigned field id %d != %d", got, asInt(m["last-assigned-field-id"])))
			}
		case "assert-current-schema-id":
			if got := asInt(meta["current-schema-id"]); got != asInt(m["current-schema-id"]) {
				return commitFailed(fmt.Sprintf("requirement failed: current schema id %d != %d", got, asInt(m["current-schema-id"])))
			}
		case "assert-last-assigned-partition-id":
			if got := asInt(meta["last-partition-id"]); got != asInt(m["last-assigned-partition-id"]) {
				return commitFailed(fmt.Sprintf("requirement failed: last assigned partition id %d != %d", got, asInt(m["last-assigned-partition-id"])))
			}
		case "assert-default-spec-id":
			if got := asInt(meta["default-spec-id"]); got != asInt(m["default-spec-id"]) {
				return commitFailed(fmt.Sprintf("requirement failed: default spec id %d != %d", got, asInt(m["default-spec-id"])))
			}
		case "assert-default-sort-order-id":
			if got := asInt(meta["default-sort-order-id"]); got != asInt(m["default-sort-order-id"]) {
				return commitFailed(fmt.Sprintf("requirement failed: default sort order id %d != %d", got, asInt(m["default-sort-order-id"])))
			}
		default:
			return commitFailed("unsupported requirement: " + asString(m["type"]))
		}
	}
	return nil
}

// applyUpdates applies each update to the metadata map in order.
func applyUpdates(meta map[string]any, updates any) error {
	for _, u := range asList(updates) {
		m := asMap(u)
		switch asString(m["action"]) {
		case "assign-uuid":
			newUUID := asString(m["uuid"])
			if existing := asString(meta["table-uuid"]); existing != "" && existing != newUUID {
				return commitFailed(fmt.Sprintf("table-uuid already set to %s, cannot assign %s", existing, newUUID))
			}
			meta["table-uuid"] = newUUID
		case "upgrade-format-version":
			v := asInt(m["format-version"])
			if current := asInt(meta["format-version"]); v < current {
				return commitFailed(fmt.Sprintf("format-version %d < current %d", v, current))
			}
			if v != 1 && v != 2 {
				return commitFailed(fmt.Sprintf("unsupported format-version %d", v))
			}
			meta["format-version"] = v
		case "add-schema":
			schema := asMap(m["schema"])
			if schema == nil {
				return commitFailed("add-schema missing schema")
			}
			if _, ok := schema["schema-id"]; !ok {
				schema["schema-id"] = asInt(meta["current-schema-id"]) + 1
			}
			schema = normalizeSchema(schema, asInt(schema["schema-id"]))
			meta["schemas"] = append(listOf(meta, "schemas"), schema)
			if v, ok := m["last-column-id"]; ok {
				meta["last-column-id"] = asInt(v)
			} else {
				meta["last-column-id"] = maxSchemaColumnID(schema)
			}
		case "set-current-schema":
			id := asInt(m["schema-id"])
			if !schemaIDExists(meta, id) {
				return commitFailed(fmt.Sprintf("schema-id %d does not exist", id))
			}
			meta["current-schema-id"] = id
		case "add-spec":
			spec := asMap(m["spec"])
			if spec == nil {
				return commitFailed("add-spec missing spec")
			}
			if _, ok := spec["spec-id"]; !ok {
				spec["spec-id"] = asInt(meta["default-spec-id"]) + 1
			}
			spec = normalizePartitionSpec(spec, asInt(spec["spec-id"]))
			meta["partition-specs"] = append(listOf(meta, "partition-specs"), spec)
		case "set-default-spec":
			id := asInt(m["spec-id"])
			if !specIDExists(meta, id) {
				return commitFailed(fmt.Sprintf("spec-id %d does not exist", id))
			}
			meta["default-spec-id"] = id
		case "add-sort-order":
			order := asMap(m["sort-order"])
			if order == nil {
				return commitFailed("add-sort-order missing sort-order")
			}
			if _, ok := order["order-id"]; !ok {
				order["order-id"] = asInt(meta["default-sort-order-id"]) + 1
			}
			order = normalizeSortOrder(order, asInt(order["order-id"]))
			meta["sort-orders"] = append(listOf(meta, "sort-orders"), order)
		case "set-default-sort-order":
			id := asInt(m["sort-order-id"])
			if !sortOrderIDExists(meta, id) {
				return commitFailed(fmt.Sprintf("sort-order-id %d does not exist", id))
			}
			meta["default-sort-order-id"] = id
		case "add-snapshot":
			snap := asMap(m["snapshot"])
			if snap == nil {
				return commitFailed("add-snapshot missing snapshot")
			}
			meta["snapshots"] = append(listOf(meta, "snapshots"), snap)
			meta["current-snapshot-id"] = asInt64(snap["snapshot-id"])
			ts := clock.Now().UnixMilli()
			meta["snapshot-log"] = append(listOf(meta, "snapshot-log"), map[string]any{
				"snapshot-id":  asInt64(snap["snapshot-id"]),
				"timestamp-ms": ts,
			})
		case "remove-snapshots":
			removeSnapshots(meta, asList(m["snapshot-ids"]))
		case "remove-snapshot-ref":
			deleteRef(meta, asString(m["ref-name"]))
		case "set-snapshot-ref":
			setRef(meta, m)
		case "set-properties":
			setProperties(meta, asMap(m["updates"]))
		case "remove-properties":
			removeProperties(meta, asList(m["removals"]))
		case "set-location":
			meta["location"] = asString(m["location"])
		case "set-statistics":
			stats := asMap(m["statistics"])
			if stats == nil {
				return commitFailed("set-statistics missing statistics")
			}
			snapID := asInt64(stats["snapshot-id"])
			if _, ok := stats["snapshot-id"]; !ok {
				// snapshot-id is DEPRECATED for removal in the update wrapper;
				// the authoritative value is statistics.snapshot-id. Fall back to
				// the wrapper field for older clients that only send it there.
				snapID = asInt64(m["snapshot-id"])
				stats["snapshot-id"] = snapID
			}
			meta["statistics"] = upsertStatisticsFile(listOf(meta, "statistics"), stats, snapID)
		case "remove-statistics":
			meta["statistics"] = removeStatisticsFile(listOf(meta, "statistics"), asInt64(m["snapshot-id"]))
		case "set-partition-statistics":
			stats := asMap(m["partition-statistics"])
			if stats == nil {
				return commitFailed("set-partition-statistics missing partition-statistics")
			}
			snapID := asInt64(stats["snapshot-id"])
			meta["partition-statistics"] = upsertStatisticsFile(listOf(meta, "partition-statistics"), stats, snapID)
		case "remove-partition-statistics":
			meta["partition-statistics"] = removeStatisticsFile(listOf(meta, "partition-statistics"), asInt64(m["snapshot-id"]))
		default:
			return commitFailed("unsupported update action: " + asString(m["action"]))
		}
	}
	return nil
}

// upsertStatisticsFile replaces any existing entry with the same snapshot id and
// appends the new statistics/partition-statistics file — matching Iceberg's
// TableMetadata.setStatistics / setPartitionStatistics upsert semantics.
func upsertStatisticsFile(list []any, stats map[string]any, snapshotID int64) []any {
	out := removeStatisticsFile(list, snapshotID)
	return append(out, stats)
}

// removeStatisticsFile drops the entry whose snapshot-id matches, preserving the
// order of the remaining entries.
func removeStatisticsFile(list []any, snapshotID int64) []any {
	out := make([]any, 0, len(list))
	for _, e := range list {
		if asInt64(asMap(e)["snapshot-id"]) != snapshotID {
			out = append(out, e)
		}
	}
	return out
}

func removeSnapshots(meta map[string]any, ids []any) {
	removed := map[int64]bool{}
	for _, id := range ids {
		removed[asInt64(id)] = true
	}
	snapshots := listOf(meta, "snapshots")
	kept := make([]any, 0, len(snapshots))
	for _, s := range snapshots {
		if !removed[asInt64(asMap(s)["snapshot-id"])] {
			kept = append(kept, s)
		}
	}
	meta["snapshots"] = kept
	// Removing the current snapshot clears current-snapshot-id to -1; it is NOT
	// auto-promoted to the last remaining snapshot, and snapshot-log (append-only
	// in Iceberg) is left untouched.
	if removed[asInt64(meta["current-snapshot-id"])] {
		meta["current-snapshot-id"] = -1
	}
}

func deleteRef(meta map[string]any, refName string) {
	refs := asMap(meta["refs"])
	if refs == nil {
		refs = map[string]any{}
	}
	delete(refs, refName)
	meta["refs"] = refs
	// The "main" ref and current-snapshot-id move in lockstep: deleting main
	// clears the current snapshot.
	if refName == "main" {
		meta["current-snapshot-id"] = -1
	}
}

func setRef(meta map[string]any, m map[string]any) {
	refName := asString(m["ref-name"])
	refs := asMap(meta["refs"])
	if refs == nil {
		refs = map[string]any{}
	}
	entry := map[string]any{
		"snapshot-id": asInt64(m["snapshot-id"]),
		"type":        asString(m["type"]),
	}
	if v, ok := m["max-ref-age-ms"]; ok {
		entry["max-ref-age-ms"] = asInt64(v)
	}
	if v, ok := m["max-snapshot-age-ms"]; ok {
		entry["max-snapshot-age-ms"] = asInt64(v)
	}
	if v, ok := m["min-snapshots-to-keep"]; ok {
		entry["min-snapshots-to-keep"] = asInt(v)
	}
	refs[refName] = entry
	meta["refs"] = refs
	// The "main" ref and current-snapshot-id move in lockstep: setting main
	// also updates the current snapshot.
	if refName == "main" {
		meta["current-snapshot-id"] = asInt64(m["snapshot-id"])
	}
}

func setProperties(meta map[string]any, updates map[string]any) {
	props := asMap(meta["properties"])
	if props == nil {
		props = map[string]any{}
	}
	for k, v := range updates {
		props[k] = v
	}
	meta["properties"] = props
}

func removeProperties(meta map[string]any, removals []any) {
	props := asMap(meta["properties"])
	if props == nil {
		return
	}
	for _, r := range removals {
		delete(props, asString(r))
	}
	meta["properties"] = props
}

// refSnapshotID returns the snapshot id for a named ref, honoring the implicit
// "main" ref (the current snapshot) when no explicit ref entry exists.
func refSnapshotID(meta map[string]any, ref string) int64 {
	refs := asMap(meta["refs"])
	if r := asMap(refs[ref]); r != nil {
		return asInt64(r["snapshot-id"])
	}
	if ref == "main" {
		return asInt64(meta["current-snapshot-id"])
	}
	return -1
}

// idInList reports whether any entry in the meta[key] list carries an id field
// (keyed by idField) equal to id.
func idInList(meta map[string]any, key, idField string, id int) bool {
	for _, e := range listOf(meta, key) {
		if asInt(asMap(e)[idField]) == id {
			return true
		}
	}
	return false
}

func schemaIDExists(meta map[string]any, id int) bool {
	return idInList(meta, "schemas", "schema-id", id)
}

func specIDExists(meta map[string]any, id int) bool {
	return idInList(meta, "partition-specs", "spec-id", id)
}

func sortOrderIDExists(meta map[string]any, id int) bool {
	return idInList(meta, "sort-orders", "order-id", id)
}

// --- initial metadata normalization ---

func normalizeSchema(schema map[string]any, id int) map[string]any {
	if schema == nil {
		schema = map[string]any{}
	}
	if _, ok := schema["type"]; !ok {
		schema["type"] = "struct"
	}
	if _, ok := schema["schema-id"]; !ok {
		schema["schema-id"] = id
	}
	return schema
}

// maxSchemaColumnID returns the highest field id in a schema, assigning
// sequential ids to fields that lack one (so last-column-id is well defined).
// It recurses through nested struct/list/map types so ids are counted across
// the whole schema tree, not just the top-level fields.
func maxSchemaColumnID(schema map[string]any) int {
	next := 1
	return assignStructFieldIDs(schema, &next)
}

// assignStructFieldIDs assigns sequential ids to any field in a struct's
// fields array that lacks one, then recurses into each field's nested type.
// It returns the highest id seen (or assigned) in the subtree, updating next
// past every id it consumes.
func assignStructFieldIDs(schema map[string]any, next *int) int {
	fields := asList(schema["fields"])
	max := 0
	for i, f := range fields {
		fm, ok := f.(map[string]any)
		if !ok {
			continue
		}
		if id := asInt(fm["id"]); id > 0 {
			if id > max {
				max = id
			}
			if id >= *next {
				*next = id + 1
			}
		} else {
			fm["id"] = *next
			if *next > max {
				max = *next
			}
			*next++
		}
		fields[i] = fm
		if n := assignNestedTypeIDs(fm["type"], next); n > max {
			max = n
		}
	}
	schema["fields"] = fields
	return max
}

// assignNestedTypeIDs recurses into a field's type value. A struct nests more
// fields; a list/map nests an element/key/value id plus a further nested type.
func assignNestedTypeIDs(typ any, next *int) int {
	tm, ok := typ.(map[string]any)
	if !ok {
		return 0
	}
	switch asString(tm["type"]) {
	case "struct":
		return assignStructFieldIDs(tm, next)
	case "list":
		return assignElemID(tm, "element-id", "element", next)
	case "map":
		m1 := assignElemID(tm, "key-id", "key", next)
		m2 := assignElemID(tm, "value-id", "value", next)
		if m2 > m1 {
			return m2
		}
		return m1
	}
	return 0
}

// assignElemID assigns an id to a list element / map key / map value (the id
// field keyed by idKey) and recurses into its nested type.
func assignElemID(tm map[string]any, idKey, typeKey string, next *int) int {
	max := 0
	if id := asInt(tm[idKey]); id > 0 {
		max = id
		if id >= *next {
			*next = id + 1
		}
	} else {
		tm[idKey] = *next
		max = *next
		*next++
	}
	if n := assignNestedTypeIDs(tm[typeKey], next); n > max {
		max = n
	}
	return max
}

func normalizePartitionSpec(spec map[string]any, id int) map[string]any {
	if spec == nil {
		spec = map[string]any{}
	}
	if _, ok := spec["spec-id"]; !ok {
		spec["spec-id"] = id
	}
	if _, ok := spec["fields"]; !ok {
		spec["fields"] = []any{}
	}
	return spec
}

func maxPartitionFieldID(spec map[string]any) int {
	max := 999
	for _, f := range asList(spec["fields"]) {
		if id := asInt(asMap(f)["field-id"]); id > max {
			max = id
		}
	}
	return max
}

func normalizeSortOrder(order map[string]any, id int) map[string]any {
	if order == nil {
		order = map[string]any{}
	}
	if _, ok := order["order-id"]; !ok {
		order["order-id"] = id
	}
	if _, ok := order["fields"]; !ok {
		order["fields"] = []any{}
	}
	return order
}

// --- response rendering ---

// metadataLocation is the derived metadata.json pointer for a table at a given
// version: {location}/metadata/{version:05d}-{uuid}.metadata.json.
func metadataLocation(location string, version int, tableUUID string) string {
	return fmt.Sprintf("%s/metadata/%05d-%s.metadata.json", location, version, tableUUID)
}

// jsonAny decodes a raw JSON blob into an arbitrary value for wire rendering.
func jsonAny(raw []byte) any {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return map[string]any{}
	}
	return v
}

// loadTableResult renders a stored Table as a LoadTableResult.
func loadTableResult(t icebergstore.Table) map[string]any {
	return map[string]any{
		"metadata-location": t.MetadataLocation,
		"metadata":          jsonAny(t.Metadata),
	}
}

// --- misc ---

type identifier struct {
	namespace string
	name      string
}

// identifierOf extracts a {namespace, name} identifier from a JSON object.
func identifierOf(m map[string]any) identifier {
	nsLevels := asList(m["namespace"])
	return identifier{
		namespace: strings.Join(anyStrings(nsLevels), "/"),
		name:      asString(m["name"]),
	}
}

func anyStrings(v []any) []string {
	out := make([]string, 0, len(v))
	for _, s := range v {
		if str, ok := s.(string); ok {
			out = append(out, str)
		}
	}
	return out
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
