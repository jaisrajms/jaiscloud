package firestore

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

const rtIndex = "firestore_index"

// indexDef is the persisted GoogleFirestoreAdminV1Index shape (minimal: name,
// queryScope, fields). Composite indexes only; single-field indexes are
// implicit and never registered.
type indexDef struct {
	Name       string       `json:"name,omitempty"`
	QueryScope string       `json:"queryScope,omitempty"`
	Fields     []indexField `json:"fields,omitempty"`
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
	return map[string]any{
		"name":       idx.Name,
		"queryScope": idx.QueryScope,
		"state":      "READY",
		"fields":     fields,
	}
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
