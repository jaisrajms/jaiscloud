package firestore

import (
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"
)

// ─── StructuredQuery wire types ───────────────────────────────────────────────
//
// These mirror the Firestore REST wire shapes (StructuredQuery, Filter,
// FieldFilter, Order, Cursor, Projection, CollectionSelector). Values are the
// store's Value type, which round-trips the Firestore wire encoding.

type collectionSelector struct {
	CollectionID   string `json:"collectionId,omitempty"`
	AllDescendants bool   `json:"allDescendants,omitempty"`
}

type fieldReference struct {
	FieldPath string `json:"fieldPath,omitempty"`
	// OutputPath is an optional pipeline-projection alias: when set, the field
	// read from FieldPath is written under OutputPath. It is internal to the
	// ExecutePipeline relational subset and never part of the wire
	// StructuredQuery.
	OutputPath string `json:"-"`
}

type fieldFilter struct {
	Field fieldReference        `json:"field,omitempty"`
	Op    string                `json:"op,omitempty"`
	Value *firestorestore.Value `json:"value,omitempty"`
}

type compositeFilter struct {
	Op      string    `json:"op,omitempty"`
	Filters []*filter `json:"filters,omitempty"`
}

type filter struct {
	CompositeFilter *compositeFilter `json:"compositeFilter,omitempty"`
	FieldFilter     *fieldFilter     `json:"fieldFilter,omitempty"`
	UnaryFilter     *unaryFilter     `json:"unaryFilter,omitempty"`
}

type unaryFilter struct {
	Op    string         `json:"op,omitempty"`
	Field fieldReference `json:"field,omitempty"`
}

type order struct {
	Field     fieldReference `json:"field,omitempty"`
	Direction string         `json:"direction,omitempty"`
}

type cursor struct {
	Values []*firestorestore.Value `json:"values,omitempty"`
	Before bool                    `json:"before,omitempty"`
}

type projection struct {
	Fields []fieldReference `json:"fields,omitempty"`
}

type structuredQuery struct {
	From    []collectionSelector `json:"from,omitempty"`
	Where   *filter              `json:"where,omitempty"`
	Select  *projection          `json:"select,omitempty"`
	OrderBy []order              `json:"orderBy,omitempty"`
	StartAt *cursor              `json:"startAt,omitempty"`
	EndAt   *cursor              `json:"endAt,omitempty"`
	Offset  int64                `json:"offset,omitempty"`
	Limit   int64                `json:"limit,omitempty"`
}

// ─── query limits ─────────────────────────────────────────────────────────────

const (
	maxNotInValues    = 10
	maxInValues       = 30 // IN and ARRAY_CONTAINS_ANY per-filter value limit
	maxDisjunctions   = 30
	maxOrderByFields  = 1000
	maxWriteBatchSize = 500
	maxTxnBytes       = 10 << 20 // 10 MiB
)

func isRangeOp(op string) bool {
	switch op {
	case "LESS_THAN", "LESS_THAN_OR_EQUAL", "GREATER_THAN", "GREATER_THAN_OR_EQUAL":
		return true
	}
	return false
}

func isDisjunctiveOp(op string) bool {
	return op == "IN" || op == "NOT_IN" || op == "ARRAY_CONTAINS_ANY"
}

// validateFilter walks the filter tree enforcing Firestore operator limits:
// IN/NOT_IN/ARRAY_CONTAINS_ANY values must be a non-empty array of at most
// maxInValues values, and the combined disjunction count must not exceed
// maxDisjunctions.
func validateFilter(f *filter) error {
	disjunctions := 1
	var walk func(f *filter) error
	walk = func(f *filter) error {
		if f == nil {
			return nil
		}
		if f.CompositeFilter != nil {
			if f.CompositeFilter.Op != "AND" && f.CompositeFilter.Op != "OR" {
				return model.NewProviderError("InvalidArgument", "invalid composite filter operator "+f.CompositeFilter.Op, 400)
			}
			for _, c := range f.CompositeFilter.Filters {
				if err := walk(c); err != nil {
					return err
				}
			}
			return nil
		}
		if f.FieldFilter != nil {
			ff := f.FieldFilter
			if isDisjunctiveOp(ff.Op) {
				arr, ok := ff.Value.AsArray()
				if !ok || len(arr) == 0 {
					return model.NewProviderError("InvalidArgument", ff.Op+" requires a non-empty array value", 400)
				}
				limit := maxNotInValues
				if ff.Op == "IN" || ff.Op == "ARRAY_CONTAINS_ANY" {
					limit = maxInValues
				}
				if len(arr) > limit {
					return model.NewProviderError("InvalidArgument", ff.Op+" supports at most "+strconv.Itoa(limit)+" values", 400)
				}
				disjunctions *= len(arr)
				if disjunctions > maxDisjunctions {
					return model.NewProviderError("InvalidArgument", "query exceeds the maximum of "+strconv.Itoa(maxDisjunctions)+" disjunctions", 400)
				}
			}
			return nil
		}
		if f.UnaryFilter != nil {
			uf := f.UnaryFilter
			switch uf.Op {
			case "IS_NULL", "IS_NOT_NULL", "IS_NAN", "IS_NOT_NAN":
			default:
				return model.NewProviderError("InvalidArgument", "invalid unary filter operator "+uf.Op, 400)
			}
			if uf.Field.FieldPath == "__name__" {
				return model.NewProviderError("InvalidArgument", "unary filter on __name__ is not supported", 400)
			}
			return nil
		}
		return nil
	}
	return walk(f)
}

// ─── index requirement analysis ───────────────────────────────────────────────

// analyzeQuery determines whether a query requires a composite index and, if so,
// the ordered list of field paths the index must cover. It also enforces the
// "inequality field must be the first orderBy field" rule.
func analyzeQuery(q *structuredQuery) (required []string, err error) {
	var inequality []string
	var equality []string
	var orderFields []string

	appendUnique := func(dst []string, v string) []string {
		for _, e := range dst {
			if e == v {
				return dst
			}
		}
		return append(dst, v)
	}

	var walk func(f *filter)
	walk = func(f *filter) {
		if f == nil {
			return
		}
		if f.CompositeFilter != nil {
			for _, c := range f.CompositeFilter.Filters {
				walk(c)
			}
			return
		}
		if f.FieldFilter != nil {
			fp := f.FieldFilter.Field.FieldPath
			if isRangeOp(f.FieldFilter.Op) {
				inequality = appendUnique(inequality, fp)
			} else {
				equality = appendUnique(equality, fp)
			}
		}
	}
	walk(q.Where)

	for _, o := range q.OrderBy {
		if o.Field.FieldPath != "__name__" {
			orderFields = appendUnique(orderFields, o.Field.FieldPath)
		}
	}

	// Rule: an inequality field must be the first orderBy field.
	if len(inequality) > 0 && len(orderFields) > 0 && orderFields[0] != inequality[0] {
		return nil, model.NewProviderError("InvalidArgument",
			"the first order-by must be on the same field as the range filter "+inequality[0], 400)
	}

	// Build the required index as equality-filter leaf fields (in filter order),
	// then inequality fields, then orderBy fields — deduplicated. Equality fields
	// are folded into the head before appending orderBy fields so that
	// `WHERE a==1 ORDER BY b` requires (a,b) and `WHERE a==1 ORDER BY b,c`
	// requires (a,b,c).
	for _, f := range equality {
		required = appendUnique(required, f)
	}
	for _, f := range inequality {
		required = appendUnique(required, f)
	}
	for _, f := range orderFields {
		required = appendUnique(required, f)
	}

	// A single non-__name__ field never requires a composite index.
	if len(required) < 2 {
		required = nil
	}
	return required, nil
}

// ─── value comparison (Firestore type ordering) ───────────────────────────────

// valueKind returns the Firestore type ordering rank. nil is treated as null.
func valueKind(v *firestorestore.Value) int {
	if v == nil {
		return 0 // null
	}
	switch {
	case v.NullValue != nil:
		return 0
	case v.BooleanValue != nil:
		return 1
	case v.IntegerValue != nil || v.DoubleValue != nil:
		return 2
	case v.TimestampValue != nil:
		return 3
	case v.StringValue != nil:
		return 4
	case v.BytesValue != nil:
		return 5
	case v.ReferenceValue != nil:
		return 6
	case v.GeoPointValue != nil:
		return 7
	case v.ArrayValue != nil:
		return 8
	case v.MapValue != nil:
		return 9
	}
	return 0
}

// numberParts returns a numeric value as (isInt, intPart, floatPart).
func numberParts(v *firestorestore.Value) (isInt bool, i int64, f float64) {
	if v == nil {
		return false, 0, 0
	}
	if v.IntegerValue != nil {
		return true, *v.IntegerValue, 0
	}
	if v.DoubleValue != nil {
		return false, 0, *v.DoubleValue
	}
	return false, 0, 0
}

// compareNumbers compares two numeric Values (integer/double, mixed promoted to
// double per IEEE 754).
func compareNumbers(a, b *firestorestore.Value) int {
	ai, aint, afloat := numberParts(a)
	bi, bint, bfloat := numberParts(b)
	if ai && bi {
		switch {
		case aint < bint:
			return -1
		case aint > bint:
			return 1
		}
		return 0
	}
	af := afloat
	if ai {
		af = float64(aint)
	}
	bf := bfloat
	if bi {
		bf = float64(bint)
	}
	switch {
	case af < bf:
		return -1
	case af > bf:
		return 1
	}
	return 0
}

// compareValues returns -1/0/1 per Firestore type ordering. nil is null.
func compareValues(a, b *firestorestore.Value) int {
	ka, kb := valueKind(a), valueKind(b)
	if ka != kb {
		if ka < kb {
			return -1
		}
		return 1
	}
	switch ka {
	case 0: // null
		return 0
	case 1: // bool: false < true
		av, _ := a.AsBool()
		bv, _ := b.AsBool()
		if !av && bv {
			return -1
		}
		if av && !bv {
			return 1
		}
		return 0
	case 2: // number
		return compareNumbers(a, b)
	case 3: // timestamp
		at, _ := a.AsTimestamp()
		bt, _ := b.AsTimestamp()
		return cmpTime(at, bt)
	case 4: // string
		as, _ := a.AsString()
		bs, _ := b.AsString()
		return strings.Compare(as, bs)
	case 5: // bytes
		ab, _ := a.AsBytes()
		bb, _ := b.AsBytes()
		return compareBytes(ab, bb)
	case 6: // reference
		ar, _ := a.AsReference()
		br, _ := b.AsReference()
		return strings.Compare(ar, br)
	case 7: // geo point (lat then lon)
		ag, _ := a.AsGeoPoint()
		bg, _ := b.AsGeoPoint()
		if c := cmpFloat(ag.Latitude, bg.Latitude); c != 0 {
			return c
		}
		return cmpFloat(ag.Longitude, bg.Longitude)
	case 8: // array (lexicographic)
		aa, _ := a.AsArray()
		ba, _ := b.AsArray()
		n := len(aa)
		if len(ba) < n {
			n = len(ba)
		}
		for i := 0; i < n; i++ {
			if c := compareValues(aa[i], ba[i]); c != 0 {
				return c
			}
		}
		return cmpInt(len(aa), len(ba))
	case 9: // map (sorted key lexicographic)
		am, _ := a.AsMap()
		bm, _ := b.AsMap()
		akeys := sortedKeys(am)
		bkeys := sortedKeys(bm)
		n := len(akeys)
		if len(bkeys) < n {
			n = len(bkeys)
		}
		for i := 0; i < n; i++ {
			if c := strings.Compare(akeys[i], bkeys[i]); c != 0 {
				return c
			}
			if c := compareValues(am[akeys[i]], bm[bkeys[i]]); c != 0 {
				return c
			}
		}
		return cmpInt(len(akeys), len(bkeys))
	}
	return 0
}

// valuesEqual reports Firestore equality (numbers compare by value across
// int/double; NaN == NaN; null == null). nil is null.
func valuesEqual(a, b *firestorestore.Value) bool {
	if valueKind(a) != valueKind(b) {
		return false
	}
	switch valueKind(a) {
	case 0:
		return true
	case 2:
		return compareNumbers(a, b) == 0
	case 8:
		aa, _ := a.AsArray()
		ba, _ := b.AsArray()
		if len(aa) != len(ba) {
			return false
		}
		for i := range aa {
			if !valuesEqual(aa[i], ba[i]) {
				return false
			}
		}
		return true
	case 9:
		am, _ := a.AsMap()
		bm, _ := b.AsMap()
		if len(am) != len(bm) {
			return false
		}
		for k, av := range am {
			bv, ok := bm[k]
			if !ok || !valuesEqual(av, bv) {
				return false
			}
		}
		return true
	default:
		return compareValues(a, b) == 0
	}
}

func cmpTime(a, b time.Time) int {
	switch {
	case a.Before(b):
		return -1
	case a.After(b):
		return 1
	}
	return 0
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func compareBytes(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return cmpInt(len(a), len(b))
}

func sortedKeys(m map[string]*firestorestore.Value) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ─── field access ─────────────────────────────────────────────────────────────

// fieldValue returns the value at a dot-delimited field path, or nil when
// absent. "__name__" resolves to the document's full name as a reference.
func fieldValue(doc *firestorestore.Document, fieldPath string) *firestorestore.Value {
	if fieldPath == "__name__" {
		return firestorestore.ReferenceVal(doc.Name)
	}
	if doc.Fields == nil {
		return nil
	}
	parts := strings.Split(fieldPath, ".")
	cur, ok := doc.Fields[parts[0]]
	if !ok {
		return nil
	}
	for _, p := range parts[1:] {
		if cur == nil || cur.MapValue == nil {
			return nil
		}
		cur = cur.MapValue.Fields[p]
	}
	return cur
}

// ─── filter evaluation ────────────────────────────────────────────────────────

// eval returns whether doc satisfies the Filter tree.
func eval(doc *firestorestore.Document, f *filter) (bool, error) {
	if f == nil {
		return true, nil
	}
	if f.CompositeFilter != nil {
		cf := f.CompositeFilter
		switch cf.Op {
		case "AND":
			for _, c := range cf.Filters {
				ok, err := eval(doc, c)
				if err != nil || !ok {
					return ok, err
				}
			}
			return true, nil
		case "OR":
			for _, c := range cf.Filters {
				ok, err := eval(doc, c)
				if err != nil {
					return false, err
				}
				if ok {
					return true, nil
				}
			}
			return false, nil
		default:
			return false, errors.New("unsupported composite operator " + cf.Op)
		}
	}
	if f.FieldFilter != nil {
		return evalFieldFilter(doc, f.FieldFilter)
	}
	if f.UnaryFilter != nil {
		return evalUnaryFilter(doc, f.UnaryFilter), nil
	}
	return true, nil
}

// evalUnaryFilter evaluates a unary filter (IS_NULL/IS_NOT_NULL/IS_NAN/IS_NOT_NAN)
// against a document. A nil value means the field is absent.
func evalUnaryFilter(doc *firestorestore.Document, uf *unaryFilter) bool {
	v := fieldValue(doc, uf.Field.FieldPath)
	switch uf.Op {
	case "IS_NULL":
		return v == nil || v.NullValue != nil
	case "IS_NOT_NULL":
		return v != nil && v.NullValue == nil
	case "IS_NAN":
		return v != nil && v.DoubleValue != nil && math.IsNaN(*v.DoubleValue)
	case "IS_NOT_NAN":
		return !(v != nil && v.DoubleValue != nil && math.IsNaN(*v.DoubleValue))
	}
	return false
}

func evalFieldFilter(doc *firestorestore.Document, ff *fieldFilter) (bool, error) {
	v := fieldValue(doc, ff.Field.FieldPath)
	target := ff.Value
	switch ff.Op {
	case "EQUAL":
		return valueMatchesEqual(v, target), nil
	case "NOT_EQUAL":
		return !valueMatchesEqual(v, target), nil
	case "ARRAY_CONTAINS":
		return arrayContains(v, target), nil
	case "IN":
		tv, _ := target.AsArray()
		return inValues(v, tv), nil
	case "ARRAY_CONTAINS_ANY":
		tv, _ := target.AsArray()
		return arrayContainsAny(v, tv), nil
	case "NOT_IN":
		tv, _ := target.AsArray()
		return !inValues(v, tv), nil
	case "LESS_THAN":
		return v != nil && compareValues(v, target) < 0, nil
	case "LESS_THAN_OR_EQUAL":
		return v != nil && compareValues(v, target) <= 0, nil
	case "GREATER_THAN":
		return v != nil && compareValues(v, target) > 0, nil
	case "GREATER_THAN_OR_EQUAL":
		return v != nil && compareValues(v, target) >= 0, nil
	default:
		return false, errors.New("unsupported field filter operator " + ff.Op)
	}
}

// valueMatchesEqual handles EQUAL semantics: an array document field matches if
// any element equals the target.
func valueMatchesEqual(v, target *firestorestore.Value) bool {
	if v == nil {
		return false
	}
	if v.ArrayValue != nil {
		for _, e := range v.ArrayValue.Values {
			if valuesEqual(e, target) {
				return true
			}
		}
		return false
	}
	return valuesEqual(v, target)
}

// arrayContains reports whether v (an array) contains target.
func arrayContains(v, target *firestorestore.Value) bool {
	if v == nil || v.ArrayValue == nil {
		return false
	}
	for _, e := range v.ArrayValue.Values {
		if valuesEqual(e, target) {
			return true
		}
	}
	return false
}

// inValues reports whether v equals any of targets, or (array field) intersects.
func inValues(v *firestorestore.Value, targets []*firestorestore.Value) bool {
	if v == nil {
		return false
	}
	if v.ArrayValue != nil {
		for _, e := range v.ArrayValue.Values {
			for _, t := range targets {
				if valuesEqual(e, t) {
					return true
				}
			}
		}
		return false
	}
	for _, t := range targets {
		if valuesEqual(v, t) {
			return true
		}
	}
	return false
}

// arrayContainsAny reports whether array field v intersects targets.
func arrayContainsAny(v *firestorestore.Value, targets []*firestorestore.Value) bool {
	if v == nil || v.ArrayValue == nil {
		return false
	}
	for _, e := range v.ArrayValue.Values {
		for _, t := range targets {
			if valuesEqual(e, t) {
				return true
			}
		}
	}
	return false
}

// ─── ordering / cursor / projection ───────────────────────────────────────────

// sortDesc encodes the per-field sort directions including the implicit
// __name__ tie-breaker (same direction as the last order, or ASCENDING).
func sortDesc(orders []order) []bool {
	desc := make([]bool, len(orders)+1)
	for i, o := range orders {
		desc[i] = o.Direction == "DESCENDING"
	}
	desc[len(orders)] = len(orders) > 0 && orders[len(orders)-1].Direction == "DESCENDING"
	return desc
}

// docSortKey builds the full sort key for a document: one value per orderBy
// field (missing → nil/null), then the __name__ reference tie-breaker.
func docSortKey(doc *firestorestore.Document, orders []order) []*firestorestore.Value {
	key := make([]*firestorestore.Value, 0, len(orders)+1)
	for _, o := range orders {
		key = append(key, fieldValue(doc, o.Field.FieldPath))
	}
	key = append(key, firestorestore.ReferenceVal(doc.Name))
	return key
}

// compareCursor compares a document's sort key against a cursor position (a
// prefix of the order-by key), honouring per-field direction.
func compareCursor(docKey []*firestorestore.Value, cursorVals []*firestorestore.Value, desc []bool) int {
	for i := 0; i < len(cursorVals) && i < len(docKey); i++ {
		c := compareValues(docKey[i], cursorVals[i])
		if c != 0 {
			if desc[i] {
				return -c
			}
			return c
		}
	}
	return 0
}

// lessSortKeys compares two full sort keys.
func lessSortKeys(a, b []*firestorestore.Value, desc []bool) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		c := compareValues(a[i], b[i])
		if c != 0 {
			if desc[i] {
				return c > 0
			}
			return c < 0
		}
	}
	return false
}

// matchesCollection reports whether a document is selected by a collection
// selector. parent is the runQuery parent resource prefix
// ("projects/{p}/databases/{db}/documents[/{doc path}]"): collectionId is
// resolved relative to it. When allDescendants is false the selector matches
// only the collection that is an immediate child of parent; when true it
// matches all descendant collections of parent with that ID (collection group).
func matchesCollection(doc *firestorestore.Document, sel collectionSelector, parent string) bool {
	if doc.CollectionID != sel.CollectionID {
		return false
	}
	if sel.AllDescendants {
		return strings.HasPrefix(doc.Name, parent+"/")
	}
	return doc.ParentPath == parent+"/"+sel.CollectionID
}

// executeQuery runs a StructuredQuery over the given documents and returns the
// matching documents in query order, with projection applied.
func executeQuery(docs []*firestorestore.Document, q *structuredQuery, parent string) ([]*firestorestore.Document, error) {
	if err := validateFilter(q.Where); err != nil {
		return nil, err
	}

	// from
	if len(q.From) > 0 {
		filtered := make([]*firestorestore.Document, 0, len(docs))
		for _, d := range docs {
			for _, sel := range q.From {
				if matchesCollection(d, sel, parent) {
					filtered = append(filtered, d)
					break
				}
			}
		}
		docs = filtered
	}

	// where
	if q.Where != nil {
		filtered := make([]*firestorestore.Document, 0, len(docs))
		for _, d := range docs {
			ok, err := eval(d, q.Where)
			if err != nil {
				return nil, err
			}
			if ok {
				filtered = append(filtered, d)
			}
		}
		docs = filtered
	}

	// orderBy (+ implicit __name__ tie-breaker)
	desc := sortDesc(q.OrderBy)
	keyed := make([]sortEntry, 0, len(docs))
	for _, d := range docs {
		keyed = append(keyed, sortEntry{doc: d, key: docSortKey(d, q.OrderBy)})
	}
	sort.SliceStable(keyed, func(i, j int) bool {
		return lessSortKeys(keyed[i].key, keyed[j].key, desc)
	})

	// cursor filtering (startAt / endAt)
	//
	// Cursor.before is a position relative to the sort order, not an
	// inclusivity flag: before=true is the position just *before* the given
	// values, before=false just *after* them. So for start_at, before=true
	// keeps the cursor document itself (inclusive — startAt) while before=false
	// skips it (exclusive — startAfter); for end_at it is the other way around
	// (before=false keeps it — endAt; before=true skips it — endBefore). See
	// google/firestore/v1/query.proto Cursor and the Java SDK's
	// startAt/startAfter/endAt/endBefore mappings.
	startIdx := 0
	endIdx := len(keyed)
	if q.StartAt != nil {
		for _, e := range keyed {
			c := compareCursor(e.key, q.StartAt.Values, desc)
			skip := c < 0
			if !q.StartAt.Before {
				skip = c <= 0
			}
			if skip {
				startIdx++
			} else {
				break
			}
		}
	}
	if q.EndAt != nil {
		for endIdx > startIdx {
			e := keyed[endIdx-1]
			c := compareCursor(e.key, q.EndAt.Values, desc)
			after := c > 0
			if q.EndAt.Before {
				after = c >= 0
			}
			if after {
				endIdx--
			} else {
				break
			}
		}
	}
	keyed = keyed[startIdx:endIdx]

	// offset
	if q.Offset > 0 {
		if int(q.Offset) >= len(keyed) {
			keyed = nil
		} else {
			keyed = keyed[q.Offset:]
		}
	}

	// limit
	if q.Limit > 0 && int(q.Limit) < len(keyed) {
		keyed = keyed[:q.Limit]
	}

	// project (a copy so the original store document is untouched)
	result := make([]*firestorestore.Document, 0, len(keyed))
	for _, e := range keyed {
		d := *e.doc
		if q.Select != nil && len(q.Select.Fields) > 0 {
			d.Fields = projectFields(e.doc, q.Select)
		}
		result = append(result, &d)
	}
	return result, nil
}

type sortEntry struct {
	doc *firestorestore.Document
	key []*firestorestore.Value
}

// projectFields returns only the projected field paths (a "__name__"-only
// projection yields an empty fields map; the name is always on the wire). A
// field reference's OutputPath, when set, renames the projected field. Values
// are deep-copied so the projected document never shares (and can never mutate)
// the store's nested map/array values.
func projectFields(doc *firestorestore.Document, proj *projection) map[string]*firestorestore.Value {
	out := map[string]*firestorestore.Value{}
	for _, fr := range proj.Fields {
		if fr.FieldPath == "__name__" {
			continue
		}
		v := fieldValue(doc, fr.FieldPath)
		if v == nil {
			continue
		}
		dst := fr.OutputPath
		if dst == "" {
			dst = fr.FieldPath
		}
		setFieldPath(out, strings.Split(dst, "."), cloneValue(v))
	}
	return out
}

// cloneValue returns a deep copy of a Value, so a projected (or otherwise
// derived) document owns its nested maps and arrays instead of aliasing the
// store's. Scalars share their underlying pointer fields, which are never
// mutated in place.
func cloneValue(v *firestorestore.Value) *firestorestore.Value {
	if v == nil {
		return nil
	}
	out := *v
	if v.ArrayValue != nil {
		vals := make([]*firestorestore.Value, len(v.ArrayValue.Values))
		for i, e := range v.ArrayValue.Values {
			vals[i] = cloneValue(e)
		}
		out.ArrayValue = &firestorestore.ArrayValue{Values: vals}
	}
	if v.MapValue != nil {
		fields := make(map[string]*firestorestore.Value, len(v.MapValue.Fields))
		for k, e := range v.MapValue.Fields {
			fields[k] = cloneValue(e)
		}
		out.MapValue = &firestorestore.MapValue{Fields: fields}
	}
	return &out
}

// numericValuesEqual reports whether two Values are numerically equal, used by
// transforms. Kept for symmetry with Firestore's "equivalent numbers".
func numericValuesEqual(a, b *firestorestore.Value) bool {
	return valueKind(a) == valueKind(b) && valueKind(a) == 2 && compareNumbers(a, b) == 0
}
