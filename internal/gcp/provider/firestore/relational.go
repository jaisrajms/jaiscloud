package firestore

import (
	"encoding/json"
	"sort"
	"strings"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// This file exposes the structured-query engine's evaluation primitives to the
// ExecutePipeline gRPC transport as transport-neutral functions, so the pipeline
// stages run over the same filter/order/projection semantics as runQuery
// instead of a second, forked engine. The gRPC layer only transcodes proto
// stage arguments into these core types and applies the functions in order.

// FilterDocuments returns the subset of docs satisfying the Filter tree,
// reusing the structured-query filter evaluator (eval). The same operator
// limits runQuery enforces (non-empty array for IN/NOT_IN/ARRAY_CONTAINS_ANY,
// value and disjunction caps) are applied first so an invalid pipeline filter
// fails with InvalidArgument rather than evaluating leniently.
func FilterDocuments(docs []firestorestore.Document, where *Filter) ([]firestorestore.Document, error) {
	if where == nil {
		return docs, nil
	}
	if err := validateFilter(where); err != nil {
		return nil, err
	}
	out := make([]firestorestore.Document, 0, len(docs))
	for i := range docs {
		ok, err := eval(&docs[i], where)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, docs[i])
		}
	}
	return out, nil
}

// SortDocuments returns docs ordered by the orderBy clauses, applying the same
// implicit __name__ tie-breaker as the structured-query engine. An empty orderBy
// leaves the input order unchanged (a pipeline sort stage always carries at
// least one ordering).
func SortDocuments(docs []firestorestore.Document, orderBy []Order) []firestorestore.Document {
	if len(orderBy) == 0 {
		return docs
	}
	desc := sortDesc(orderBy)
	keyed := make([]sortEntry, 0, len(docs))
	for i := range docs {
		keyed = append(keyed, sortEntry{doc: &docs[i], key: docSortKey(&docs[i], orderBy)})
	}
	sort.SliceStable(keyed, func(i, j int) bool {
		return lessSortKeys(keyed[i].key, keyed[j].key, desc)
	})
	out := make([]firestorestore.Document, 0, len(keyed))
	for _, e := range keyed {
		out = append(out, *e.doc)
	}
	return out
}

// ProjectDocuments applies a field projection to a copy of each document. A nil
// projection returns the documents unchanged; a non-nil projection replaces the
// field map. Projected values are deep-copied so the result never shares (and
// can never mutate) the store's nested map/array values.
func ProjectDocuments(docs []firestorestore.Document, selectFields *Projection) []firestorestore.Document {
	if selectFields == nil {
		return docs
	}
	out := make([]firestorestore.Document, 0, len(docs))
	for i := range docs {
		d := docs[i]
		d.Fields = projectFields(&docs[i], selectFields)
		out = append(out, d)
	}
	return out
}

// DistinctDocuments removes duplicate documents, preserving first-occurrence
// order. With a non-empty field projection, two documents are duplicates when
// their projected values are equal; with no fields, the whole document — its
// resource name plus its fields — determines distinctness (so a document
// repeated by a union/documents source collapses, while two distinct documents
// that happen to share a field map do not).
func DistinctDocuments(docs []firestorestore.Document, fields *Projection) []firestorestore.Document {
	seen := make(map[string]struct{}, len(docs))
	out := make([]firestorestore.Document, 0, len(docs))
	for i := range docs {
		key := distinctKey(&docs[i], fields)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, docs[i])
	}
	return out
}

// distinctKey renders a document's distinctness key. Field-based distinctness
// uses the ordered tuple of projected values (sorted by field path so map
// iteration order cannot affect it). Whole-document distinctness combines the
// document's resource name with its field map, so identity is part of the
// document. Values are rendered with their wire encoding, which distinguishes
// an absent field (null) from an explicit null value and preserves the
// Firestore type of each value.
func distinctKey(doc *firestorestore.Document, fields *Projection) string {
	if fields == nil || len(fields.Fields) == 0 {
		return doc.Name + "\x00" + valueKey(firestorestore.MapVal(doc.Fields))
	}
	parts := make([]string, 0, len(fields.Fields))
	for _, fr := range fields.Fields {
		parts = append(parts, fr.FieldPath+"\x01"+valueKey(fieldValue(doc, fr.FieldPath)))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x02")
}

// valueKey renders a value as a stable, type-preserving key. A nil value (an
// absent field) is distinct from an explicit nullValue.
func valueKey(v *firestorestore.Value) string {
	if v == nil {
		return "\x00absent"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "\x00error:" + err.Error()
	}
	return string(b)
}
