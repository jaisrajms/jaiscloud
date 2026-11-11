package firestore

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

const rtIndex = "firestore_index"

// Index.density values (google.firestore.admin.v1.Index.Density). DENSITY_
// UNSPECIFIED is INPUT_ONLY ("Unspecified. It will use database default
// setting"), so it is never stored: the create path resolves it to the edition
// default (indexDensityDefault) and every stored index carries the effective
// value real GCP serves on read.
//
// The Discovery document pins the per-edition rules these constants implement:
// SPARSE_ALL "is both the default and only allowed value for Standard edition
// databases", and DENSE "is the default density for an Enterprise edition
// database". Enterprise edition creates no indexes by default, so its index
// surface is entirely client-defined (enterprise-index-overview).
const (
	indexDensityUnspecified = "DENSITY_UNSPECIFIED"
	indexDensitySparseAll   = "SPARSE_ALL"
	indexDensitySparseAny   = "SPARSE_ANY"
	indexDensityDense       = "DENSE"
)

// Index.state values (google.firestore.admin.v1.Index.State). The emulator
// completes every build synchronously, so a stored index is READY — except a
// unique index whose data already held a duplicate entry: real GCP stores it in
// NEEDS_REPAIR ("The index was being created, but something went wrong ... The
// index is not updated when writing a document. Some index data may exist. Use
// the google.longrunning.Operations API to determine why the operation that
// last attempted to create this index failed, then re-create the index."), and
// the create's finished operation carries the failure. An index in that state
// is neither served by the planner nor enforced on writes (FE6).
const (
	indexStateReady       = "READY"
	indexStateNeedsRepair = "NEEDS_REPAIR"
)

// Index.apiScope values (google.firestore.admin.v1.Index.ApiScope). ANY_API is
// the proto3 zero value and "the default", so it is the unset spelling: it is
// never stored and never rendered (protojson omits a zero enum, and the
// Terraform provider reads a missing member back as its own unset value).
const (
	indexAPIScopeAny       = "ANY_API"
	indexAPIScopeDatastore = "DATASTORE_MODE_API"
	indexAPIScopeMongo     = "MONGODB_COMPATIBLE_API"
)

// indexEnumFields maps the Index members whose proto type is an enum to the
// enum names the Discovery document declares. proto3 JSON accepts only a
// declared name, so any other string is rejected rather than stored and echoed
// verbatim (the same rule validateDatabaseEnumFields applies to Database).
var indexEnumFields = map[string]map[string]bool{
	"queryScope": {
		"QUERY_SCOPE_UNSPECIFIED": true,
		"COLLECTION":              true,
		"COLLECTION_GROUP":        true,
		"COLLECTION_RECURSIVE":    true,
	},
	"apiScope": {
		indexAPIScopeAny:       true,
		indexAPIScopeDatastore: true,
		indexAPIScopeMongo:     true,
	},
	"density": {
		indexDensityUnspecified: true,
		indexDensitySparseAll:   true,
		indexDensitySparseAny:   true,
		indexDensityDense:       true,
	},
}

// indexFieldEnumFields is the same rule for the per-field enums.
var indexFieldEnumFields = map[string]map[string]bool{
	"order": {
		"ORDER_UNSPECIFIED": true,
		"ASCENDING":         true,
		"DESCENDING":        true,
	},
	"arrayConfig": {
		"ARRAY_CONFIG_UNSPECIFIED": true,
		"CONTAINS":                 true,
	},
}

// indexDensityDefault is the density a database serves when a create leaves it
// unset: DENSE for Enterprise edition, SPARSE_ALL otherwise (Discovery:
// DENSITY_UNSPECIFIED "will use database default setting"; DENSE "is the
// default density for an Enterprise edition database"; SPARSE_ALL is "the
// default and only allowed value for Standard edition databases").
// indexIsReady reports whether a stored index is served and maintained. An index
// record written before the state was modelled has none and is READY.
func indexIsReady(idx indexDef) bool {
	return idx.State == "" || idx.State == indexStateReady
}

// IndexBuildError reports a unique index build that failed on the database's
// existing data. The index is stored in NEEDS_REPAIR; the create returns a
// finished google.longrunning.Operation carrying this failure as its error
// (REST `error`, gRPC Operation.error), exactly as the v1beta1 create method
// describes: "During creation, the process could result in an error, in which
// case the index will move to the ERROR state. The process can be recovered by
// fixing the data that caused the error, removing the index ... then re-creating
// the index." The exact status code/message is not published, so the emulator
// serves FAILED_PRECONDITION with the message below; the ledger records that as
// an unverified wire detail (FE6).
type IndexBuildError struct {
	// Code is the google.rpc status code the finished operation carries.
	Code int32
	// Message is the operation error message.
	Message string
}

func (e *IndexBuildError) Error() string { return e.Message }

// indexBuildFailureMessage is the message a failed unique index build carries.
const indexBuildFailureMessage = "The index build failed: " + uniqueViolationMessage

func indexDensityDefault(edition string) string {
	if edition == databaseEditionEnterprise {
		return indexDensityDense
	}
	return indexDensitySparseAll
}

// resolveIndexDensity maps the INPUT_ONLY DENSITY_UNSPECIFIED (and an absent
// member) onto the edition default, and leaves every other value alone.
func resolveIndexDensity(requested, edition string) string {
	if requested == "" || requested == indexDensityUnspecified {
		return indexDensityDefault(edition)
	}
	return requested
}

// normalizeIndex fills the INPUT_ONLY members of a stored index record so a
// read serves the effective values real GCP reports — including for a record
// written by a build that did not store the index options (FE1) or the Standard
// edition's server-appended __name__ field (FE3).
func normalizeIndex(edition string, idx indexDef) indexDef {
	idx.Density = resolveIndexDensity(idx.Density, edition)
	if idx.APIScope == indexAPIScopeAny {
		idx.APIScope = ""
	}
	if edition != databaseEditionEnterprise {
		idx = appendStandardNameField(idx)
	}
	return idx
}

// appendStandardNameField makes a Standard edition composite index definition
// match the server shape the Discovery document documents on Index.fields:
//
//	"In Standard edition databases only: ... The last field entry is always for
//	the field path `__name__`. If, on creation, `__name__` was not specified as
//	the last field, it will be added automatically with the same direction as
//	that of the last field defined. If the final field in the index is not
//	directional, the `__name__` will be ordered ASCENDING (unless explicitly
//	specified)."
//
// Enterprise edition appends nothing (its index is exactly what the client
// declared), and single-field definitions are left alone: Standard edition keeps
// single-field indexes on the Field resource (Field.index_config), so a
// one-field Index record is not a composite index this rule applies to.
//
// An explicitly supplied __name__ that is not last is moved to the end instead
// of being duplicated, keeping its explicit direction when it has one.
func appendStandardNameField(idx indexDef) indexDef {
	if len(idx.Fields) < 2 {
		return idx
	}
	if idx.Fields[len(idx.Fields)-1].FieldPath == "__name__" {
		return idx
	}
	fields := make([]indexField, 0, len(idx.Fields)+1)
	var explicit *indexField
	for _, f := range idx.Fields {
		if f.FieldPath == "__name__" && explicit == nil {
			moved := f
			explicit = &moved
			continue
		}
		fields = append(fields, f)
	}
	order := "ASCENDING"
	if len(fields) > 0 {
		// "the same direction as that of the last field defined"; a field with
		// no direction (array-contains) leaves the default ASCENDING.
		if o := fields[len(fields)-1].Order; o != "" {
			order = o
		}
	}
	if explicit != nil && explicit.Order != "" {
		order = explicit.Order
	}
	idx.Fields = append(fields, indexField{FieldPath: "__name__", Order: order})
	return idx
}

// validateIndexEnums applies proto3 JSON's enum-name rule to an Index
// definition. The gRPC transport decodes into enum-typed protobuf fields (so it
// cannot carry an unknown name); this is the REST adapter's equivalent, kept in
// the transport-neutral core because the stored value is the enum name.
func validateIndexEnums(idx indexDef) error {
	check := func(field, value string, allowed map[string]bool) error {
		if value == "" || allowed[value] {
			return nil
		}
		return gcpInvalidArgument(fmt.Sprintf("invalid Index.%s value %q", field, value))
	}
	if err := check("queryScope", idx.QueryScope, indexEnumFields["queryScope"]); err != nil {
		return err
	}
	if err := check("apiScope", idx.APIScope, indexEnumFields["apiScope"]); err != nil {
		return err
	}
	if err := check("density", idx.Density, indexEnumFields["density"]); err != nil {
		return err
	}
	for _, f := range idx.Fields {
		if err := check("fields[].order", f.Order, indexFieldEnumFields["order"]); err != nil {
			return err
		}
		if err := check("fields[].arrayConfig", f.ArrayConfig, indexFieldEnumFields["arrayConfig"]); err != nil {
			return err
		}
	}
	return nil
}

// indexDef is the persisted GoogleFirestoreAdminV1Index shape.
//
// The members follow the Discovery document's Index object: name, queryScope,
// fields and state are the original composite-index surface; apiScope, density,
// unique and multikey (plus shard_count) are the Enterprise edition index
// options (FE1). A single-field index is stored here too when its database is
// Enterprise edition — the Discovery document says that in Enterprise edition
// "both single-field and composite indexes are managed using the
// google.firestore.admin.v1.Index resource", whereas in Standard edition
// single-field indexes live on the Field resource (Field.index_config).
//
// The Enterprise-only vector and search index types (Index.fields[].vector_
// config / search_config and Index.search_index_options) are NOT modelled; the
// record type is deliberately scalar so an index definition can never claim a
// query capability the emulator does not serve (see the FE1 ledger row).
type indexDef struct {
	Name       string `json:"name,omitempty"`
	QueryScope string `json:"queryScope,omitempty"`
	APIScope   string `json:"apiScope,omitempty"`
	Density    string `json:"density,omitempty"`
	Multikey   bool   `json:"multikey,omitempty"`
	Unique     bool   `json:"unique,omitempty"`
	ShardCount int32  `json:"shardCount,omitempty"`
	// State is OUTPUT_ONLY in the Discovery document. A record written before
	// the state was modelled has none and reads back as READY.
	State  string       `json:"state,omitempty"`
	Fields []indexField `json:"fields,omitempty"`
}

type indexField struct {
	FieldPath   string `json:"fieldPath,omitempty"`
	Order       string `json:"order,omitempty"`
	ArrayConfig string `json:"arrayConfig,omitempty"`
}

// indexPath parses an index resource name (relative, after the project):
// "databases/{db}/collectionGroups/{cg}/indexes" (5 segments) or
// "databases/{db}/collectionGroups/{cg}/indexes/{id}" (6 segments).
func indexPath(name string) (database, cg, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) < 5 || parts[0] != "databases" || parts[2] != "collectionGroups" || parts[4] != "indexes" {
		return "", "", "", false
	}
	database, cg = parts[1], parts[3]
	if len(parts) >= 6 {
		id = parts[5]
	}
	return database, cg, id, true
}

// fullIndexName builds the full index resource name.
func fullIndexName(project, database, cg, id string) string {
	return "projects/" + project + "/databases/" + database + "/collectionGroups/" + cg + "/indexes/" + id
}

// indexRelName builds the resource-store ID (relative index name).
func indexRelName(database, cg, id string) string {
	return "databases/" + database + "/collectionGroups/" + cg + "/indexes/" + id
}

// indexMap renders an indexDef to its REST wire form.
//
// apiScope, density, multikey, unique and shardCount follow protojson: a zero
// value is omitted (ANY_API, false, 0), while density is always present because
// every stored index carries its effective value. The same renderer is used for
// the indexes embedded in Field.index_config, where those members are only
// present if a client wrote them — the Standard edition field path is unchanged
// because the emulator never synthesizes the Enterprise defaults there (FE1).
func indexMap(idx indexDef) map[string]any {
	fields := make([]any, 0, len(idx.Fields))
	for _, f := range idx.Fields {
		m := map[string]any{"fieldPath": f.FieldPath}
		if f.Order != "" {
			m["order"] = f.Order
		}
		if f.ArrayConfig != "" {
			m["arrayConfig"] = f.ArrayConfig
		}
		fields = append(fields, m)
	}
	state := idx.State
	if state == "" {
		state = indexStateReady
	}
	m := map[string]any{
		"name":       idx.Name,
		"queryScope": idx.QueryScope,
		"state":      state,
		"fields":     fields,
	}
	if idx.APIScope != "" && idx.APIScope != indexAPIScopeAny {
		m["apiScope"] = idx.APIScope
	}
	if idx.Density != "" {
		m["density"] = idx.Density
	}
	if idx.Multikey {
		m["multikey"] = true
	}
	if idx.Unique {
		m["unique"] = true
	}
	if idx.ShardCount != 0 {
		m["shardCount"] = idx.ShardCount
	}
	return m
}

// indexMatchesFilter reports whether any index field path contains the filter
// substring (the ListIndexes `filter` parameter).
func indexMatchesFilter(idx indexDef, filter string) bool {
	for _, f := range idx.Fields {
		if strings.Contains(f.FieldPath, filter) {
			return true
		}
	}
	return false
}

// randomHex returns a random hex string of the given length.
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}
