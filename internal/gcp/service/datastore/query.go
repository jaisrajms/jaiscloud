package datastore

import (
	"bytes"
	"cmp"
	"fmt"
	"math"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// matchesFilter evaluates a query filter against an entity. It supports
// PropertyFilter EQUAL, NOT_EQUAL, IN, the four comparison operators
// (LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL) — on an
// ordinary property, on a key-valued property, or on the reserved "__key__"
// property (documented "Key filters") — plus __key__ HAS ANCESTOR, plus
// CompositeFilter AND over those property filters. Its static shape is enforced
// by validateFilter — the same authority RunQuery and RunAggregationQuery call
// once up front — so an unsupported operator (NOT_IN, OR, unspecified/unknown),
// IN without a non-empty array, or an empty AND fails closed with
// InvalidArgument rather than matching everything, whether or not the scan
// reaches a candidate.
func matchesFilter(e dsstore.Entity, f *Filter) (bool, error) {
	if err := validateFilter(f); err != nil {
		return false, err
	}
	return matchValidFilter(e, f), nil
}

// matchValidFilter evaluates a filter validateFilter has already accepted.
func matchValidFilter(e dsstore.Entity, f *Filter) bool {
	if f == nil {
		return true
	}
	if f.Property != nil {
		return matchPropertyFilter(e, f.Property)
	}
	// validateFilter admits only an AND composite here, with at least one
	// sub-filter.
	for _, sub := range f.Composite.Filters {
		if !matchValidFilter(e, sub) {
			return false
		}
	}
	return true
}

// unsupportedQueryOperator returns an InvalidArgument error for a filter
// operator the emulator does not implement (fail closed, never match-all).
func unsupportedQueryOperator(op string) error {
	return invalidArgument("unsupported query operator: " + op)
}

// validateFilter checks a filter's static shape before any entity is scanned.
// It is the single authority for the contract matchesFilter relies on: without
// the up-front pass a request real Datastore rejects (an unsupported operator,
// IN with a non-array or empty array value, an empty AND, or a query whose
// disjunctive normal form exceeds the per-query disjunction budget) would be
// reached only when the scan visits a candidate, so it would silently return an
// empty result set on an empty or fully-filtered collection instead of failing
// loud. RunQuery and RunAggregationQuery call it once up front; matchesFilter
// calls it too, so no caller can reach a fail-open path.
func validateFilter(f *Filter) error {
	disjunctions := 1
	return validateFilterDisjunctions(f, &disjunctions)
}

// validateFilterDisjunctions walks the filter tree enforcing each property
// filter's static shape and the per-query disjunction budget (TD43). The
// emulator is deliberately AND-only (OR fails closed in the composite arm
// below), so the only way a query multiplies its disjunctions is several
// disjunctive IN filters in one AND: each IN contributes its operand count and
// the running product is the DNF size real GCP evaluates.
func validateFilterDisjunctions(f *Filter, disjunctions *int) error {
	if f == nil {
		return nil
	}
	if f.Property != nil {
		return validatePropertyFilter(f.Property, disjunctions)
	}
	if f.Composite != nil {
		if f.Composite.Op != CompositeAnd {
			return unsupportedQueryOperator(compositeOpName(f.Composite.Op))
		}
		// The Discovery requires "at least one filter is present"; an empty AND
		// would otherwise match every entity (fail open).
		if len(f.Composite.Filters) == 0 {
			return invalidArgument("AND requires at least one filter")
		}
		for _, sub := range f.Composite.Filters {
			if err := validateFilterDisjunctions(sub, disjunctions); err != nil {
				return err
			}
		}
		return nil
	}
	return unsupportedQueryOperator("unknown filter")
}

// maxInValues is the documented cap on the number of values an IN filter's
// operand array may carry. The Discovery states only "subject to disjunction
// limits"; the query docs pin the number:
// "Use the IN operator to combine up to 30 equality (==) clauses on the same
// property with a logical OR"
// (https://cloud.google.com/datastore/docs/concepts/queries#in).
const maxInValues = 30

// maxDisjunctions is the documented cap on the *total* number of disjunctions a
// query's disjunctive normal form may contain, across every filter — not just
// within one IN operand (maxInValues). Datastore mode runs on the shared
// Firestore backend, whose limits section pins the aggregate: "Cloud Firestore
// limits a query to a maximum of 30 disjunctions based on the query's
// disjunctive normal form. This limit is fixed and cannot be adjusted."
// (https://firebase.google.com/docs/firestore/query-data/queries#in_not-in_and_array-contains-any)
// The Datastore-mode Discovery itself names no number — its IN operator is only
// "subject to disjunction limits" — but the shared backend enforces the same
// DNF cap, and the repo's Firestore provider already models it
// (internal/gcp/provider/firestore/query.go maxDisjunctions). Because the
// emulator is AND-only, each IN filter multiplies the running count, so
// AND(IN[20], IN[20]) — 400 disjunctions — is rejected here as real GCP rejects
// it, while a single IN at maxInValues (30) is exactly at the limit.
const maxDisjunctions = 30

// validatePropertyFilter validates one property filter's static shape.
func validatePropertyFilter(pf *PropertyFilter, disjunctions *int) error {
	switch pf.Op {
	case PropertyEqual, PropertyNotEqual,
		PropertyLessThan, PropertyLessThanOrEqual,
		PropertyGreaterThan, PropertyGreaterThanOrEqual:
		return nil
	case PropertyIn:
		// Real Datastore requires the IN operand to be a non-empty ArrayValue
		// (Discovery: "That `value` is a non-empty `ArrayValue`, subject to
		// disjunction limits"). The Discovery names no number; the query docs
		// pin the per-IN disjunction cap at 30 values ("Use the IN operator to
		// combine up to 30 equality (==) clauses on the same property with a
		// logical OR"), so an over-cap array fails closed here rather than being
		// evaluated. NOT_IN — whose operand is capped at 10 — is unsupported
		// here and never reaches this arm (the default arm fails it closed; the
		// TD16 precedent).
		if pf.Value.ArrayValue == nil || len(pf.Value.ArrayValue.Values) == 0 {
			return invalidArgument("IN requires a non-empty array value")
		}
		n := len(pf.Value.ArrayValue.Values)
		if n > maxInValues {
			return invalidArgument(fmt.Sprintf("IN supports at most %d values, got %d", maxInValues, n))
		}
		// Each IN contributes its operand count to the query's DNF; several in
		// one AND multiply. Enforce the per-query aggregate budget (TD43).
		*disjunctions *= n
		if *disjunctions > maxDisjunctions {
			return invalidArgument(fmt.Sprintf("query exceeds the maximum of %d disjunctions", maxDisjunctions))
		}
		return nil
	case PropertyHasAncestor:
		if pf.Property != keyPropertyName {
			return invalidArgument("HAS_ANCESTOR requires the " + keyPropertyName + " property")
		}
		if pf.Value.KeyValue == nil {
			return invalidArgument("HAS_ANCESTOR requires a key value")
		}
		return nil
	default:
		return unsupportedQueryOperator(propertyOpName(pf.Op))
	}
}

// matchPropertyFilter applies an already-validated property filter to an entity
// (validatePropertyFilter has guaranteed a non-empty array for IN and a key
// value on the reserved __key__ property for HAS_ANCESTOR).
//
// An array-valued property is compared elementwise, as real Datastore's
// exploded index does: "If the property of the given name is array-valued, the
// entity satisfies the filter if any of the values compares to the value
// specified in the filter in the manner described by the comparison operator"
// (https://cloud.google.com/datastore/docs/concepts/queries). Every operator
// here therefore goes through propertyElements/propertyEqual/propertyNotEqual
// rather than valueEqual/valueCompare on the stored value as a whole.
func matchPropertyFilter(e dsstore.Entity, pf *PropertyFilter) bool {
	if pf.Property == keyPropertyName {
		return matchKeyPropertyFilter(e, pf)
	}
	val, ok := e.Properties[pf.Property]
	switch pf.Op {
	case PropertyHasAncestor:
		// validatePropertyFilter restricts HAS_ANCESTOR to the reserved
		// "__key__" property, so this arm is unreachable; kept so an
		// out-of-band PropertyFilter cannot fall through to a comparison.
		return false
	case PropertyEqual:
		return ok && propertyEqual(val, pf.Value)
	case PropertyNotEqual:
		// Datastore's multi-valued behavior makes != not the negation of EQUAL
		// at the property level: "Not Equal requires only one element to match
		// the given predicate" (Google Cloud blog, "IN, NOT_IN and NOT EQUAL
		// query operators", 2022), so an array that merely contains the operand
		// still matches when it also has another element, and an empty array
		// (no element to compare, excluded from indexing) matches nothing.
		return ok && propertyNotEqual(val, pf.Value)
	case PropertyIn:
		if !ok {
			return false
		}
		for _, el := range pf.Value.ArrayValue.Values {
			if propertyEqual(val, el) {
				return true
			}
		}
		return false
	default:
		// validatePropertyFilter admits only the four ordering comparisons
		// besides the operators above.
		if !ok {
			return false
		}
		// The ordering operators use the same array expansion as EQUAL: an
		// array-valued property satisfies the filter when any element compares
		// in the manner of the operator.
		for _, el := range propertyElements(val) {
			c, comparable := valueCompare(el, pf.Value)
			if !comparable {
				continue
			}
			switch pf.Op {
			case PropertyLessThan:
				if c < 0 {
					return true
				}
			case PropertyLessThanOrEqual:
				if c <= 0 {
					return true
				}
			case PropertyGreaterThan:
				if c > 0 {
					return true
				}
			default:
				if c >= 0 {
					return true
				}
			}
		}
		return false
	}
}

// keyPropertyName is the reserved Datastore property that exposes an entity's
// own key; it is the operand property of every key filter (EQUAL, NOT_EQUAL,
// IN, the four ordering operators) and the only legal operand of HAS_ANCESTOR.
const keyPropertyName = "__key__"

// matchKeyPropertyFilter applies an already-validated filter on the reserved
// "__key__" property. No entity stores such an entry — validateReservedMutations
// rejects a user property matching `__.*__` — so the property value is the
// entity's own key, taken from e.Key.
//
// HAS_ANCESTOR is a path-prefix test. Every other supported operator compares
// the entity's key with the operand key using the documented key ordering
// (keyCompare), which — exactly like keyHasAncestor — compares only the paths:
// a query's partition already scopes the result set, and a KEY(...) literal or
// SDK key carries no partition of its own. Key *values* (a key-typed property or
// an embedded entity's key) keep the partition-sensitive comparison instead
// (fullKeyCompare), because there the partition is part of the value.
// https://cloud.google.com/datastore/docs/concepts/queries#key_filters
func matchKeyPropertyFilter(e dsstore.Entity, pf *PropertyFilter) bool {
	if pf.Op == PropertyHasAncestor {
		// validatePropertyFilter guarantees a key operand here.
		return keyHasAncestor(e.Key, *pf.Value.KeyValue)
	}
	if pf.Op == PropertyIn {
		// validatePropertyFilter guarantees a non-empty array; an element that
		// is not a key simply never matches.
		for _, el := range pf.Value.ArrayValue.Values {
			if el.KeyValue != nil && keyCompare(e.Key, *el.KeyValue) == 0 {
				return true
			}
		}
		return false
	}
	if pf.Value.KeyValue == nil {
		// A non-key operand cannot match a key (fail closed, as any other
		// property-type mismatch does).
		return false
	}
	c := keyCompare(e.Key, *pf.Value.KeyValue)
	switch pf.Op {
	case PropertyEqual:
		return c == 0
	case PropertyNotEqual:
		return c != 0
	case PropertyLessThan:
		return c < 0
	case PropertyLessThanOrEqual:
		return c <= 0
	case PropertyGreaterThan:
		return c > 0
	default:
		// validatePropertyFilter admits only GREATER_THAN_OR_EQUAL besides the
		// operators above.
		return c >= 0
	}
}

// propertyElements returns the values a property filter's comparison operators
// see. An array-valued property is expanded into its elements (real Datastore's
// index explodes each element into its own entry); any other value, including
// the two non-array composite value types, is a single-element set.
func propertyElements(val dsstore.Value) []dsstore.Value {
	if val.ArrayValue != nil {
		return val.ArrayValue.Values
	}
	return []dsstore.Value{val}
}

// propertyEqual reports whether a stored property value matches an equality
// operand. An array-valued property matches when any element equals the operand;
// a scalar matches by ordinary value equality. Note this is the documented
// elementwise comparison, not valueEqual on the array as a whole: filtering
// `tags = "a"` matches a `tags` of ["a","b"], while `tags = ["a","b"]` (an array
// operand, which no element can equal) matches nothing, exactly as the exploded
// index does.
func propertyEqual(val, operand dsstore.Value) bool {
	for _, el := range propertyElements(val) {
		if valueEqual(el, operand) {
			return true
		}
	}
	return false
}

// propertyNotEqual reports whether a stored property value satisfies a
// not-equal filter. Datastore's multi-valued behavior makes != the elementwise
// predicate, not the negation of propertyEqual: "Not Equal requires only one
// element to match the given predicate" (Google Cloud blog, "IN, NOT_IN and NOT
// EQUAL query operators", 2022), so an array that merely contains the operand
// still matches when it also has another element. An empty array has no element
// to compare and so matches nothing (it is excluded from indexing), and a scalar
// property is the ordinary value inequality.
func propertyNotEqual(val, operand dsstore.Value) bool {
	for _, el := range propertyElements(val) {
		if !valueEqual(el, operand) {
			return true
		}
	}
	return false
}

// keyHasAncestor reports whether ancestorKey is a proper path prefix of
// entityKey. Both arguments are canonical store keys, so the comparison is
// structural — a raw string prefix would be wrong (kind "A" is lexically a
// prefix of kind "AB").
//
// Only the paths are compared: the partition is the query's concern (the query
// already restricts results to its namespace/database), and a GQL KEY(...)
// literal — or an SDK ancestor key without an explicit namespace — carries no
// partition of its own. Requiring partition equality would wrongly reject those.
func keyHasAncestor(entityKey, ancestorKey string) bool {
	_, _, epath, eok := dsstore.ParseKey(entityKey)
	_, _, apath, aok := dsstore.ParseKey(ancestorKey)
	if !eok || !aok || len(apath) == 0 || len(apath) >= len(epath) {
		return false
	}
	for i := range apath {
		if apath[i] != epath[i] {
			return false
		}
	}
	return true
}

// keyCompare orders two canonical store keys by the documented key ordering
// over their *paths*, ignoring the partition. Real Datastore: "When comparing
// for inequality, keys are ordered by the following criteria, in order:
// 1. Ancestor path 2. Entity kind 3. Identifier (key name or numeric ID). ...
// Kinds and key names are strings and are ordered by byte value; numeric IDs are
// integers and are ordered numerically. If entities with the same parent and
// kind use a mix of key name strings and numeric IDs, those with numeric IDs
// precede those with key names."
// (https://cloud.google.com/datastore/docs/concepts/queries#key_filters)
//
// Comparing the canonical strings directly would not give that order: the
// encoding length-prefixes every field, so kinds would sort by length before
// byte value ("b" after "aa") and numeric IDs by digit count. This parses both
// keys and compares path element by element instead.
//
// Only the paths are compared: the partition (database, namespace) is not one
// of the documented criteria — a query's partition already scopes the result
// set — and a KEY(...) literal or SDK key carries no partition of its own (the
// same rationale keyHasAncestor documents). This is the comparison the reserved
// "__key__" property uses; key *values* use fullKeyCompare. A value that is not
// a canonical key is reachable only from a hand-built internal Value, never from
// a transport, and falls back to raw byte order; keys are always orderable, so
// unlike valueCompare this has no incomparable case.
func keyCompare(a, b string) int {
	_, _, apath, aok := dsstore.ParseKey(a)
	_, _, bpath, bok := dsstore.ParseKey(b)
	if !aok || !bok {
		return cmp.Compare(a, b)
	}
	return comparePaths(apath, bpath)
}

// fullKeyCompare orders two canonical store keys as key *values* (a key-typed
// property or an embedded entity's key): by partition (database, then
// namespace) and then by the documented key ordering (comparePaths). The
// partition is part of such a value, so unlike the reserved "__key__" property
// (keyCompare) it takes part in the comparison.
func fullKeyCompare(a, b string) int {
	adb, ans, apath, aok := dsstore.ParseKey(a)
	bdb, bns, bpath, bok := dsstore.ParseKey(b)
	if !aok || !bok {
		return cmp.Compare(a, b)
	}
	if c := cmp.Compare(adb, bdb); c != 0 {
		return c
	}
	if c := cmp.Compare(ans, bns); c != 0 {
		return c
	}
	return comparePaths(apath, bpath)
}

// comparePaths compares two parsed key paths by the documented key ordering:
// element by element (kind, then identifier), and a proper-prefix path before a
// longer one.
func comparePaths(apath, bpath []dsstore.PathElement) int {
	for i := 0; i < len(apath) && i < len(bpath); i++ {
		if c := cmp.Compare(apath[i].Kind, bpath[i].Kind); c != 0 {
			return c
		}
		if c := comparePathIdentifier(apath[i], bpath[i]); c != 0 {
			return c
		}
	}
	// One path is a proper prefix of the other: the shorter (ancestor) path
	// sorts first.
	return cmp.Compare(len(apath), len(bpath))
}

// comparePathIdentifier orders two key path elements' identifiers: numeric IDs
// numerically, key names by byte value, and every numeric ID before every key
// name (the documented mixed-id rule). An incomplete element — neither ID nor
// name, legal only on the final element of an insert/upsert key and never on a
// stored key — is treated as an empty name, so it sorts after every numeric ID
// and before every non-empty name.
func comparePathIdentifier(a, b dsstore.PathElement) int {
	switch {
	case a.HasID && b.HasID:
		return cmp.Compare(a.ID, b.ID)
	case a.HasID:
		return -1
	case b.HasID:
		return 1
	default:
		return cmp.Compare(a.Name, b.Name)
	}
}

// Datastore value-type ranks. Firestore in Datastore mode orders a property
// whose values have mixed types by the value's *type* first, then by value
// within the type (the documented "Value type ordering" table,
// https://cloud.google.com/datastore/docs/concepts/entities#value_type_ordering):
//
//	null < integers < dates/times < booleans < byte strings
//	     < unicode strings < floats < NaN < geo points < keys
//
// Integers and floats are separate types, so every integer sorts before every
// float (the documented `7 < 3.2`).
const (
	rankNull = iota + 1
	rankInteger
	rankTimestamp
	rankBoolean
	rankBlob
	rankString
	rankDouble
	rankNaN
	rankGeoPoint
	rankKey
)

// valueRank returns v's type rank in the documented cross-type ordering. ok is
// false for the two value types with no sort order — array values and embedded
// entities — and for an absent (zero) value; such values are never orderable.
func valueRank(v dsstore.Value) (int, bool) {
	switch {
	case v.NullValue != nil:
		return rankNull, true
	case v.IntegerValue != nil:
		return rankInteger, true
	case v.TimestampValue != nil:
		return rankTimestamp, true
	case v.BooleanValue != nil:
		return rankBoolean, true
	case v.BlobValue != nil:
		return rankBlob, true
	case v.StringValue != nil:
		return rankString, true
	case v.DoubleValue != nil:
		if math.IsNaN(*v.DoubleValue) {
			return rankNaN, true
		}
		return rankDouble, true
	case v.GeoPointValue != nil:
		return rankGeoPoint, true
	case v.KeyValue != nil:
		return rankKey, true
	default:
		return 0, false
	}
}

// valueCompare compares two values for ordering, returning -1, 0, or 1. ok is
// false when either value has no sort order (an array, an embedded entity, or an
// absent value); every other cross-type pair is orderable via the documented
// type ordering (valueRank).
func valueCompare(a, b dsstore.Value) (int, bool) {
	ra, aok := valueRank(a)
	rb, bok := valueRank(b)
	if !aok || !bok {
		return 0, false
	}
	if ra != rb {
		return cmp.Compare(ra, rb), true
	}
	switch ra {
	case rankNull, rankNaN:
		// A null is the same value as any other null, and a NaN is the same
		// (NaN) value as any other NaN.
		return 0, true
	case rankInteger:
		return cmp.Compare(*a.IntegerValue, *b.IntegerValue), true
	case rankTimestamp:
		// Compare at Datastore's microsecond resolution, so two timestamps that
		// differ only below the microsecond are the same value (see
		// timestampInstant).
		at, errA := timestampInstant(*a.TimestampValue)
		bt, errB := timestampInstant(*b.TimestampValue)
		if errA != nil || errB != nil {
			return 0, false
		}
		switch {
		case at.Before(bt):
			return -1, true
		case at.After(bt):
			return 1, true
		default:
			return 0, true
		}
	case rankBoolean:
		switch {
		case *a.BooleanValue == *b.BooleanValue:
			return 0, true
		case !*a.BooleanValue:
			return -1, true
		default:
			return 1, true
		}
	case rankBlob:
		return bytes.Compare(a.BlobValue, b.BlobValue), true
	case rankString:
		return cmp.Compare(*a.StringValue, *b.StringValue), true
	case rankDouble:
		return cmp.Compare(*a.DoubleValue, *b.DoubleValue), true
	case rankGeoPoint:
		// By latitude, then longitude (the documented geo-point sort order).
		if c := cmp.Compare(a.GeoPointValue.Latitude, b.GeoPointValue.Latitude); c != 0 {
			return c, true
		}
		return cmp.Compare(a.GeoPointValue.Longitude, b.GeoPointValue.Longitude), true
	case rankKey:
		// KeyValue holds the key's canonical string form; order it as a key
		// value (fullKeyCompare: partition, then the documented path ordering),
		// not by that encoding — which would sort kinds by length and numeric
		// IDs by digit count.
		return fullKeyCompare(*a.KeyValue, *b.KeyValue), true
	}
	return 0, false
}

func valueEqual(a, b dsstore.Value) bool {
	// Integers and doubles are separate types in Datastore mode (the documented
	// value-type ordering), so `n = 3` does NOT match a stored `3.0` and vice
	// versa. A NaN equals another NaN: NaN is a single distinct value type.
	if a.IntegerValue != nil || b.IntegerValue != nil {
		return a.IntegerValue != nil && b.IntegerValue != nil && *a.IntegerValue == *b.IntegerValue
	}
	if a.DoubleValue != nil || b.DoubleValue != nil {
		if a.DoubleValue == nil || b.DoubleValue == nil {
			return false
		}
		return *a.DoubleValue == *b.DoubleValue ||
			(math.IsNaN(*a.DoubleValue) && math.IsNaN(*b.DoubleValue))
	}
	switch {
	case a.NullValue != nil || b.NullValue != nil:
		return a.NullValue != nil && b.NullValue != nil
	case a.BooleanValue != nil || b.BooleanValue != nil:
		return a.BooleanValue != nil && b.BooleanValue != nil && *a.BooleanValue == *b.BooleanValue
	case a.StringValue != nil || b.StringValue != nil:
		return a.StringValue != nil && b.StringValue != nil && *a.StringValue == *b.StringValue
	case a.TimestampValue != nil || b.TimestampValue != nil:
		if a.TimestampValue == nil || b.TimestampValue == nil {
			return false
		}
		// A timestamp names an instant (proto Timestamp = seconds+nanos since
		// the epoch); the RFC 3339 UTC offset is presentation only, so two
		// strings naming the same instant with different offsets are the same
		// value. Compare parsed instants at Datastore's microsecond resolution
		// (timestampInstant), matching valueCompare's ordering, so two values
		// that differ only below the microsecond are also equal. Both transports
		// canonicalize to UTC and truncate to microseconds on write (REST in
		// transport/rest/datastore/wire.go, gRPC in
		// transport/grpc/datastore/transcode.go), so the comparison is
		// instant-based rather than reliant on that; a raw-string compare would
		// still make EQUAL miss a value the range operators match.
		// Fall back to the literal strings only when a value is unparseable.
		at, errA := timestampInstant(*a.TimestampValue)
		bt, errB := timestampInstant(*b.TimestampValue)
		if errA != nil || errB != nil {
			return *a.TimestampValue == *b.TimestampValue
		}
		return at.Equal(bt)
	case a.KeyValue != nil || b.KeyValue != nil:
		// A key *value* is compared exactly, partition included (canonical keys
		// of the same key are byte-identical). The reserved "__key__" property
		// is the exception: it is partition-agnostic and path-ordered, handled by
		// matchKeyPropertyFilter/keyCompare.
		return a.KeyValue != nil && b.KeyValue != nil && *a.KeyValue == *b.KeyValue
	case a.BlobValue != nil || b.BlobValue != nil:
		return a.BlobValue != nil && b.BlobValue != nil && bytes.Equal(a.BlobValue, b.BlobValue)
	case a.GeoPointValue != nil || b.GeoPointValue != nil:
		return a.GeoPointValue != nil && b.GeoPointValue != nil && *a.GeoPointValue == *b.GeoPointValue
	case a.EntityValue != nil || b.EntityValue != nil:
		if a.EntityValue == nil || b.EntityValue == nil {
			return false
		}
		return entityEqual(*a.EntityValue, *b.EntityValue)
	case a.ArrayValue != nil || b.ArrayValue != nil:
		if a.ArrayValue == nil || b.ArrayValue == nil {
			return false
		}
		if len(a.ArrayValue.Values) != len(b.ArrayValue.Values) {
			return false
		}
		for i := range a.ArrayValue.Values {
			if !valueEqual(a.ArrayValue.Values[i], b.ArrayValue.Values[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func entityEqual(a, b dsstore.Entity) bool {
	if a.Key != b.Key {
		return false
	}
	if len(a.Properties) != len(b.Properties) {
		return false
	}
	for k, av := range a.Properties {
		bv, ok := b.Properties[k]
		if !ok || !valueEqual(av, bv) {
			return false
		}
	}
	return true
}

// aggregate computes one aggregation over the entities the nested query
// returned and returns its neutral Value.
func aggregate(agg Aggregation, entities []dsstore.Entity) (dsstore.Value, error) {
	switch agg.Op {
	case AggCount:
		n := int64(len(entities))
		if agg.UpTo != nil {
			if *agg.UpTo < 0 {
				return dsstore.Value{}, invalidArgument("count up_to must be non-negative")
			}
			if n > *agg.UpTo {
				n = *agg.UpTo
			}
		}
		return dsstore.Value{IntegerValue: &n}, nil
	case AggSum:
		return aggregateSum(agg.Property, entities)
	case AggAvg:
		return aggregateAvg(agg.Property, entities)
	default:
		return dsstore.Value{}, invalidArgument("aggregation has no operator")
	}
}

// aggregateSum sums the named property over entities, following the proto's
// documented Sum behavior:
//   - only integer and double values contribute; a missing property or a
//     non-numeric value (string, bool, null, key, array, ...) is skipped;
//   - an empty contributing set yields integer 0;
//   - the result is a 64-bit integer when every contributing value is an
//     integer and the sum does not overflow int64; otherwise it is a double.
func aggregateSum(name string, entities []dsstore.Entity) (dsstore.Value, error) {
	if name == "" {
		return dsstore.Value{}, invalidArgument("sum aggregation requires a property")
	}
	var (
		isum     int64
		fsum     float64
		allInt   = true
		overflow bool
	)
	for _, e := range entities {
		v, ok := e.Properties[name]
		if !ok {
			continue
		}
		switch {
		case v.IntegerValue != nil:
			iv := *v.IntegerValue
			if (iv > 0 && isum+iv < isum) || (iv < 0 && isum+iv > isum) {
				overflow = true
			}
			isum += iv
			fsum += float64(iv)
		case v.DoubleValue != nil:
			allInt = false
			fsum += *v.DoubleValue
		default:
			continue
		}
	}
	if allInt && !overflow {
		return dsstore.Value{IntegerValue: &isum}, nil
	}
	return dsstore.Value{DoubleValue: &fsum}, nil
}

// aggregateAvg averages the named property over entities, following the
// proto's documented Avg behavior: only integer and double values contribute;
// a missing property or a non-numeric value is skipped; an empty contributing
// set yields NULL; and the result is always a double.
func aggregateAvg(name string, entities []dsstore.Entity) (dsstore.Value, error) {
	if name == "" {
		return dsstore.Value{}, invalidArgument("avg aggregation requires a property")
	}
	var (
		sum float64
		n   int64
	)
	for _, e := range entities {
		v, ok := e.Properties[name]
		if !ok {
			continue
		}
		switch {
		case v.IntegerValue != nil:
			sum += float64(*v.IntegerValue)
			n++
		case v.DoubleValue != nil:
			sum += *v.DoubleValue
			n++
		default:
			continue
		}
	}
	if n == 0 {
		s := "NULL_VALUE"
		return dsstore.Value{NullValue: &s}, nil
	}
	avg := sum / float64(n)
	return dsstore.Value{DoubleValue: &avg}, nil
}

// propertyOpName renders a PropertyOp for error messages.
func propertyOpName(op PropertyOp) string {
	switch op {
	case PropertyEqual:
		return "EQUAL"
	case PropertyNotEqual:
		return "NOT_EQUAL"
	case PropertyIn:
		return "IN"
	case PropertyLessThan:
		return "LESS_THAN"
	case PropertyLessThanOrEqual:
		return "LESS_THAN_OR_EQUAL"
	case PropertyGreaterThan:
		return "GREATER_THAN"
	case PropertyGreaterThanOrEqual:
		return "GREATER_THAN_OR_EQUAL"
	case PropertyHasAncestor:
		return "HAS_ANCESTOR"
	default:
		return "PROPERTY_FILTER_OP_UNSPECIFIED"
	}
}

// compositeOpName renders a CompositeOp for error messages.
func compositeOpName(op CompositeOp) string {
	switch op {
	case CompositeAnd:
		return "AND"
	case CompositeOr:
		return "OR"
	default:
		return "COMPOSITE_FILTER_OP_UNSPECIFIED"
	}
}
