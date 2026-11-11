package firestore

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// uniqueViolationMessage is the message real Firestore returns with
// ALREADY_EXISTS when a write would create a duplicate entry in a unique index.
//
// Google's Enterprise edition index overview documents the rejection ("The
// database rejects any update and insert operations that attempt to create index
// entries with duplicate values") but not its envelope; the create/update/commit
// ALREADY_EXISTS answer together with this message is what official clients
// surface for real GCP (recorded by the firestore-rust Enterprise index
// documentation), so that is the contract implemented here.
const uniqueViolationMessage = "Duplicate values are found for unique index."

// newUniqueViolationErr returns the ALREADY_EXISTS error a duplicate unique
// index entry produces. Both transports render it from the ProviderError: REST
// as HTTP 409 + google.rpc status ALREADY_EXISTS, gRPC as codes.AlreadyExists,
// and BatchWrite reports it as that write's google.rpc code 6.
func newUniqueViolationErr() error {
	return model.NewProviderError("AlreadyExists", uniqueViolationMessage, 409).WithStatus("ALREADY_EXISTS")
}

// docScope is one project+database, the scope of a document write and of the
// indexes that cover it.
type docScope struct {
	project  string
	database string
}

// docCollectionGroup parses a document name into its scope and collection group
// (the collection ID of the document's parent collection). Firestore's
// collection-group indexes are keyed by that ID at any nesting depth, so
// "users/alice" and "teams/t1/members/alice" belong to the groups "users" and
// "members".
func docCollectionGroup(name string) (project, database, cg string, ok bool) {
	project, database, path, ok := firestorestore.ParseDocumentName(name)
	if !ok {
		return "", "", "", false
	}
	segs := strings.Split(path, "/")
	if len(segs) < 2 {
		return "", "", "", false
	}
	return project, database, segs[len(segs)-2], true
}

// writeNames returns the document names a write set targets (deletes included;
// a write with an unparseable target contributes nothing and its own validation
// reports it).
func writeNames(writes []*writeWire) []string {
	names := make([]string, 0, len(writes))
	for _, w := range writes {
		switch {
		case w == nil:
		case w.Delete != "":
			names = append(names, w.Delete)
		case w.Transform != nil:
			names = append(names, w.Transform.Document)
		case w.Update != nil:
			names = append(names, w.Update.Name)
		}
	}
	return names
}

// uniqueIndexesForGroups returns the stored indexes marked unique whose
// collection group is one of cgs, for one database. It is the read side of
// enforcement: the emulator keeps no index entries, so "which indexes cover
// this write" is answered from the control-plane index records.
func (s *Service) uniqueIndexesForGroups(ctx context.Context, project, database string, cgs map[string]bool) ([]indexDef, error) {
	if s.resources == nil || len(cgs) == 0 {
		return nil, nil
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtIndex, "databases/"+database+"/")
	if err != nil {
		return nil, err
	}
	var out []indexDef
	for _, e := range entries {
		cg, ok := indexRecordCollectionGroup(e.ID)
		if !ok || !cgs[cg] {
			continue
		}
		var idx indexDef
		if json.Unmarshal(e.Data, &idx) != nil {
			continue
		}
		if !idx.Unique || !indexIsReady(idx) {
			// A NEEDS_REPAIR index is not updated on writes ("The index is not
			// updated when writing a document"), so it cannot enforce uniqueness.
			continue
		}
		out = append(out, idx)
	}
	return out, nil
}

// indexRecordCollectionGroup extracts the collection group from a stored index
// record id ("databases/{db}/collectionGroups/{cg}/indexes/{id}").
func indexRecordCollectionGroup(relID string) (string, bool) {
	parts := strings.Split(relID, "/")
	if len(parts) < 5 || parts[0] != "databases" || parts[2] != "collectionGroups" || parts[4] != "indexes" {
		return "", false
	}
	return parts[3], true
}

// indexDefCollectionGroup extracts the collection group from a stored index's
// full resource name
// ("projects/{p}/databases/{db}/collectionGroups/{cg}/indexes/{id}").
func indexDefCollectionGroup(idx indexDef) (string, bool) {
	parts := strings.Split(idx.Name, "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "collectionGroups" {
			return parts[i+1], true
		}
	}
	return "", false
}

// uniqueIndexesForScopes resolves the unique indexes covering every collection
// group the writes touch, per database scope. Scopes with no unique index are
// omitted, so an ordinary write does no extra work.
func (s *Service) uniqueIndexesForScopes(ctx context.Context, scopes map[docScope]map[string]bool) (map[docScope][]indexDef, error) {
	out := make(map[docScope][]indexDef)
	for sc, cgs := range scopes {
		idxs, err := s.uniqueIndexesForGroups(ctx, sc.project, sc.database, cgs)
		if err != nil {
			return nil, err
		}
		if len(idxs) > 0 {
			out[sc] = idxs
		}
	}
	return out, nil
}

// uniqueGuardForWrites resolves the unique indexes covering the writes and, when
// at least one applies, takes the unique-write lock for the caller to hold until
// the store write has been applied. The returned release is never nil and is a
// no-op when no unique index applies, so ordinary writes keep their concurrency.
//
// The lock is what makes the scan-based check atomic: the emulator keeps no
// index entries, so uniqueness is checked by scanning the collection group's
// documents, and two same-value writers could both pass that scan before either
// store write lands. Serializing the writes that touch a unique-indexed
// collection group closes that window in-process (the emulator is a single
// process; a shared Postgres DSN is for durability, not for multi-writer
// scaling). The indexes are re-read under the lock so an index created between
// the unlocked probe and the lock is never missed.
func (s *Service) uniqueGuardForWrites(ctx context.Context, writes []*writeWire) (map[docScope][]indexDef, func(), error) {
	return s.uniqueGuardForNames(ctx, writeNames(writes)...)
}

// uniqueGuardForNames is uniqueGuardForWrites for callers that already know the
// document names a write targets (the single-document create/patch paths).
func (s *Service) uniqueGuardForNames(ctx context.Context, names ...string) (map[docScope][]indexDef, func(), error) {
	noop := func() {}
	scopes := make(map[docScope]map[string]bool)
	for _, name := range names {
		project, database, cg, ok := docCollectionGroup(name)
		if !ok {
			continue
		}
		sc := docScope{project: project, database: database}
		if scopes[sc] == nil {
			scopes[sc] = make(map[string]bool)
		}
		scopes[sc][cg] = true
	}
	if len(scopes) == 0 {
		return nil, noop, nil
	}
	idxs, err := s.uniqueIndexesForScopes(ctx, scopes)
	if err != nil {
		return nil, noop, err
	}
	if len(idxs) == 0 {
		return nil, noop, nil
	}
	s.uniqueMu.Lock()
	idxs, err = s.uniqueIndexesForScopes(ctx, scopes)
	if err != nil {
		s.uniqueMu.Unlock()
		return nil, noop, err
	}
	return idxs, s.uniqueMu.Unlock, nil
}

// checkUniqueWrites validates the post-write state of a store write set against
// the unique indexes resolved by uniqueGuardForWrites. Every document the set
// writes or deletes is excluded from the stored-document scan, so a batch that
// deletes the current holder and creates a new one with the same value is legal,
// while two writes in the same batch that collide are rejected before anything
// is applied (the caller runs this before store.Commit).
func (s *Service) checkUniqueWrites(ctx context.Context, idxs map[docScope][]indexDef, ws []firestorestore.Write) error {
	if len(idxs) == 0 {
		return nil
	}
	touched := make(map[string]bool, len(ws))
	pending := make([]firestorestore.Document, 0, len(ws))
	for _, w := range ws {
		touched[w.Name] = true
		if w.Document != nil {
			pending = append(pending, *w.Document)
		}
	}
	return s.checkUniqueState(ctx, idxs, touched, pending)
}

// checkUniqueState is the enforcement primitive: it compares the request's
// post-state documents (pending) with the stored documents of each unique
// index's collection group, ignoring every name in touched.
func (s *Service) checkUniqueState(ctx context.Context, idxs map[docScope][]indexDef, touched map[string]bool, pending []firestorestore.Document) error {
	if len(idxs) == 0 {
		return nil
	}
	// A later write to the same document wins, so only the final state counts.
	post := make(map[string]firestorestore.Document, len(pending))
	for _, d := range pending {
		post[d.Name] = d
	}
	postNames := make([]string, 0, len(post))
	for name := range post {
		postNames = append(postNames, name)
	}
	sort.Strings(postNames)

	for sc, list := range idxs {
		stored, err := s.store.ListDocuments(ctx, sc.project, sc.database)
		if err != nil {
			return err
		}
		for _, idx := range list {
			cg, ok := indexDefCollectionGroup(idx)
			if !ok {
				continue
			}
			// entry key → the document that owns it. Pending documents are
			// inserted first: they are the request's result and win over the
			// stored state they replace.
			seen := make(map[string]string)
			for _, name := range postNames {
				d := post[name]
				if project, database, docCG, ok := docCollectionGroup(d.Name); !ok ||
					project != sc.project || database != sc.database || docCG != cg {
					continue
				}
				for _, key := range uniqueIndexEntries(idx, d) {
					if _, dup := seen[key]; dup {
						return newUniqueViolationErr()
					}
					seen[key] = d.Name
				}
			}
			for _, d := range stored {
				if touched[d.Name] {
					continue
				}
				if project, database, docCG, ok := docCollectionGroup(d.Name); !ok ||
					project != sc.project || database != sc.database || docCG != cg {
					continue
				}
				for _, key := range uniqueIndexEntries(idx, d) {
					if _, dup := seen[key]; dup {
						return newUniqueViolationErr()
					}
					seen[key] = d.Name
				}
			}
		}
	}
	return nil
}

// uniqueIndexDataHasDuplicates reports whether the collection group already
// holds two documents whose entries for idx are equal. It is the index-build
// check (FE6): real GCP fails a unique index build on such data, stores the
// index in NEEDS_REPAIR and finishes the operation with an error. The caller
// holds the unique write lock, so the scan and the index record's store write
// are atomic with respect to duplicate-producing writes.
func (s *Service) uniqueIndexDataHasDuplicates(ctx context.Context, project, database, cg string, idx indexDef) (bool, error) {
	if s.store == nil {
		return false, nil
	}
	docs, err := s.store.ListDocuments(ctx, project, database)
	if err != nil {
		return false, err
	}
	seen := make(map[string]string)
	for _, d := range docs {
		p, db, docCG, ok := docCollectionGroup(d.Name)
		if !ok || p != project || db != database || docCG != cg {
			continue
		}
		for _, key := range uniqueIndexEntries(idx, d) {
			if owner, dup := seen[key]; dup && owner != d.Name {
				return true, nil
			}
			seen[key] = d.Name
		}
	}
	return false, nil
}

// indexFieldValue returns the value at a dot-delimited field path together with
// whether the field is present. Unlike fieldValue it distinguishes an absent
// field (present=false) from a stored null (present=true, NullValue), which the
// density rules need.
func indexFieldValue(doc *firestorestore.Document, fieldPath string) (*firestorestore.Value, bool) {
	if fieldPath == "__name__" {
		return firestorestore.ReferenceVal(doc.Name), true
	}
	if doc.Fields == nil {
		return nil, false
	}
	parts := strings.Split(fieldPath, ".")
	cur, ok := doc.Fields[parts[0]]
	if !ok {
		return nil, false
	}
	for _, p := range parts[1:] {
		if cur == nil || cur.MapValue == nil {
			return nil, false
		}
		cur, ok = cur.MapValue.Fields[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// uniqueIndexEntries returns the canonical entry keys doc produces for idx.
//
// The rules are the Enterprise edition index overview's: an index has a "collection
// ID" (its collection group) and a density that decides which documents get an
// entry — DENSE (the Enterprise default) for every document with missing fields
// stored as null, SPARSE_ALL only when all indexed fields are present, SPARSE_ANY
// when any is (a stored null counts as present). A field in array-contains mode
// (or, on a multikey index, the single path traversing an array) is indexed by
// element: one entry per distinct element, no entry at all when the value is
// missing / not an array / an empty array, and the remaining ordered fields
// supply the other positions (null when DENSE and absent). That mirrors the
// documented example table, where {tags:["news"]} is not indexed by a sparse
// (tags[*], status) index.
//
// An empty result means the document has no entry in the index, so it cannot be
// part of a duplicate.
func uniqueIndexEntries(idx indexDef, doc firestorestore.Document) []string {
	vals := make([]indexFieldState, len(idx.Fields))
	arrayField := -1
	for i, f := range idx.Fields {
		v, present := indexFieldValue(&doc, f.FieldPath)
		vals[i] = indexFieldState{value: v, present: present}
		if arrayField < 0 && f.ArrayConfig == "CONTAINS" {
			arrayField = i
		}
	}
	// A multikey index may traverse an array without an array-contains mode;
	// Discovery allows at most one such path.
	if arrayField < 0 && idx.Multikey {
		for i := range vals {
			if vals[i].present && vals[i].value != nil && vals[i].value.ArrayValue != nil {
				arrayField = i
				break
			}
		}
	}
	if arrayField >= 0 {
		v := vals[arrayField].value
		if !vals[arrayField].present || v == nil || v.ArrayValue == nil || len(v.ArrayValue.Values) == 0 {
			return nil
		}
	}
	ordered := make([]int, 0, len(vals))
	for i := range vals {
		if i != arrayField {
			ordered = append(ordered, i)
		}
	}
	allPresent, anyPresent := true, false
	for _, i := range ordered {
		if vals[i].present {
			anyPresent = true
		} else {
			allPresent = false
		}
	}
	if len(ordered) == 0 {
		// The array field is the only indexed field, so the density rules have
		// nothing to range over: the non-empty array requirement already
		// decided the document is indexed.
		allPresent, anyPresent = true, true
	}
	var include bool
	switch idx.Density {
	case indexDensitySparseAll:
		include = allPresent
	case indexDensitySparseAny:
		include = anyPresent
	default:
		// DENSE, plus an absent density (Enterprise is the only edition that
		// supports unique, and DENSE is its default).
		include = true
	}
	if !include {
		return nil
	}
	if arrayField < 0 {
		return []string{uniqueEntryKey(idx.Fields, vals, -1, nil)}
	}
	seen := make(map[string]bool)
	keys := make([]string, 0, len(vals[arrayField].value.ArrayValue.Values))
	for _, el := range vals[arrayField].value.ArrayValue.Values {
		key := uniqueEntryKey(idx.Fields, vals, arrayField, el)
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	return keys
}

// indexFieldState is one indexed field's value in a document plus whether the
// field is present at all (an absent field and a stored null are different for
// the density rules, though both contribute a null entry).
type indexFieldState struct {
	value   *firestorestore.Value
	present bool
}

// uniqueEntryKey renders one index entry from the document's field states, with
// element taking the arrayField position when arrayField >= 0 (one entry per
// array element). Field paths are length-prefixed so a value containing the
// separator bytes cannot forge a different tuple.
func uniqueEntryKey(fields []indexField, vals []indexFieldState, arrayField int, element *firestorestore.Value) string {
	var b strings.Builder
	for i, f := range fields {
		part := "null"
		switch {
		case i == arrayField:
			part = uniqueKeyPart(element)
		case vals[i].present:
			part = uniqueKeyPart(vals[i].value)
		}
		b.WriteString(strconv.Itoa(len(f.FieldPath)))
		b.WriteByte(':')
		b.WriteString(f.FieldPath)
		b.WriteByte('=')
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
		b.WriteByte(';')
	}
	return b.String()
}

// uniqueKeyPart canonicalizes one field value for index-entry comparison. Values
// Firestore compares as equal must produce the same string: every null (a stored
// null and an absent field alike), int 1 and double 1.0, and timestamps at
// Firestore's microsecond precision. Containers are encoded recursively with
// sorted map keys, so a map's field order never matters.
func uniqueKeyPart(v *firestorestore.Value) string {
	switch valueKind(v) {
	case 0:
		return "null"
	case 1:
		if b, _ := v.AsBool(); b {
			return "true"
		}
		return "false"
	case 2:
		return uniqueNumberKey(v)
	case 3:
		if t, ok := v.AsTimestamp(); ok {
			return "ts:" + strconv.FormatInt(t.UTC().UnixMicro(), 10)
		}
		return "null"
	case 4:
		if s, ok := v.AsString(); ok {
			return "s:" + s
		}
		return "null"
	case 5:
		if bs, ok := v.AsBytes(); ok {
			return "b:" + hex.EncodeToString(bs)
		}
		return "null"
	case 6:
		if r, ok := v.AsReference(); ok {
			return "r:" + r
		}
		return "null"
	case 7:
		if g, ok := v.AsGeoPoint(); ok {
			return "g:" + canonicalFloat(g.Latitude) + "," + canonicalFloat(g.Longitude)
		}
		return "null"
	case 8:
		arr, _ := v.AsArray()
		var b strings.Builder
		b.WriteString("a[")
		for _, el := range arr {
			part := uniqueKeyPart(el)
			b.WriteString(strconv.Itoa(len(part)))
			b.WriteByte(':')
			b.WriteString(part)
		}
		b.WriteString("]")
		return b.String()
	case 9:
		m, _ := v.AsMap()
		var b strings.Builder
		b.WriteString("m{")
		for _, k := range sortedKeys(m) {
			part := uniqueKeyPart(m[k])
			b.WriteString(strconv.Itoa(len(k)))
			b.WriteByte(':')
			b.WriteString(k)
			b.WriteByte('=')
			b.WriteString(strconv.Itoa(len(part)))
			b.WriteByte(':')
			b.WriteString(part)
		}
		b.WriteString("}")
		return b.String()
	}
	return "null"
}

// uniqueNumberKey canonicalizes a number the way compareNumbers treats it: two
// integers compare exactly, and a mixed int/double pair compares as float64, so
// an integral double in the int64 range takes the integer form (int 1 and double
// 1.0 collide). A double outside that range keeps its shortest representation.
func uniqueNumberKey(v *firestorestore.Value) string {
	isInt, i, f := numberParts(v)
	if isInt {
		return "n:" + strconv.FormatInt(i, 10)
	}
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "n:inf"
	case math.IsInf(f, -1):
		return "n:-inf"
	}
	if f == math.Trunc(f) && f >= -9223372036854775808 && f < 9223372036854775808 && float64(int64(f)) == f {
		return "n:" + strconv.FormatInt(int64(f), 10)
	}
	return "n:" + canonicalFloat(f)
}

// canonicalFloat formats a float for index-entry comparison, folding -0.0 onto
// +0.0 (compareValues treats them equal).
func canonicalFloat(f float64) string {
	if f == 0 {
		f = 0
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
