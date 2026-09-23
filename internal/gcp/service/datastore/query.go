package datastore

import (
	"bytes"
	"strings"
	"time"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// matchesFilter evaluates a query filter against an entity. It supports
// PropertyFilter EQUAL, NOT_EQUAL, IN, and the four comparison operators
// (LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL), and
// CompositeFilter AND over those property filters. Unsupported operators
// (HAS_ANCESTOR, NOT_IN, OR, unspecified/unknown) return an InvalidArgument
// error rather than matching everything.
func matchesFilter(e dsstore.Entity, f *Filter) (bool, error) {
	if f == nil {
		return true, nil
	}
	if f.Property != nil {
		return matchesPropertyFilter(e, f.Property)
	}
	if f.Composite != nil {
		cf := f.Composite
		switch cf.Op {
		case CompositeAnd:
			for _, sub := range cf.Filters {
				match, err := matchesFilter(e, sub)
				if err != nil {
					return false, err
				}
				if !match {
					return false, nil
				}
			}
			return true, nil
		default:
			return false, unsupportedQueryOperator(compositeOpName(cf.Op))
		}
	}
	return false, unsupportedQueryOperator("unknown filter")
}

// unsupportedQueryOperator returns an InvalidArgument error for a filter
// operator the emulator does not implement (fail closed, never match-all).
func unsupportedQueryOperator(op string) error {
	return invalidArgument("unsupported query operator: " + op)
}

func matchesPropertyFilter(e dsstore.Entity, pf *PropertyFilter) (bool, error) {
	if pf == nil {
		return false, unsupportedQueryOperator("missing property filter")
	}
	prop := pf.Property
	val, ok := e.Properties[prop]
	filterVal := pf.Value

	switch pf.Op {
	case PropertyEqual:
		return ok && valueEqual(val, filterVal), nil
	case PropertyNotEqual:
		return ok && !valueEqual(val, filterVal), nil
	case PropertyIn:
		if !ok || filterVal.ArrayValue == nil {
			return false, nil
		}
		for _, el := range filterVal.ArrayValue.Values {
			if valueEqual(val, el) {
				return true, nil
			}
		}
		return false, nil
	case PropertyLessThan, PropertyLessThanOrEqual, PropertyGreaterThan, PropertyGreaterThanOrEqual:
		if !ok {
			return false, nil
		}
		c, comparable := valueCompare(val, filterVal)
		if !comparable {
			return false, nil
		}
		switch pf.Op {
		case PropertyLessThan:
			return c < 0, nil
		case PropertyLessThanOrEqual:
			return c <= 0, nil
		case PropertyGreaterThan:
			return c > 0, nil
		default:
			return c >= 0, nil
		}
	default:
		return false, unsupportedQueryOperator(propertyOpName(pf.Op))
	}
}

// numericValue reports the numeric value of v (integer or double) as a float64.
func numericValue(v dsstore.Value) (float64, bool) {
	if v.IntegerValue != nil {
		return float64(*v.IntegerValue), true
	}
	if v.DoubleValue != nil {
		return *v.DoubleValue, true
	}
	return 0, false
}

// valueCompare compares two values for ordering, returning -1, 0, or 1. ok is
// false when the two values are not orderable (different types, or an
// unsupported type such as geo point/entity/array). Integers and doubles
// compare numerically, matching Datastore semantics.
func valueCompare(a, b dsstore.Value) (int, bool) {
	an, aNum := numericValue(a)
	bn, bNum := numericValue(b)
	if aNum || bNum {
		if !aNum || !bNum {
			return 0, false
		}
		switch {
		case an < bn:
			return -1, true
		case an > bn:
			return 1, true
		default:
			return 0, true
		}
	}
	switch {
	case a.StringValue != nil || b.StringValue != nil:
		if a.StringValue == nil || b.StringValue == nil {
			return 0, false
		}
		return strings.Compare(*a.StringValue, *b.StringValue), true
	case a.BooleanValue != nil || b.BooleanValue != nil:
		if a.BooleanValue == nil || b.BooleanValue == nil {
			return 0, false
		}
		switch {
		case *a.BooleanValue == *b.BooleanValue:
			return 0, true
		case !*a.BooleanValue && *b.BooleanValue:
			return -1, true
		default:
			return 1, true
		}
	case a.TimestampValue != nil || b.TimestampValue != nil:
		if a.TimestampValue == nil || b.TimestampValue == nil {
			return 0, false
		}
		at, errA := time.Parse(time.RFC3339Nano, *a.TimestampValue)
		bt, errB := time.Parse(time.RFC3339Nano, *b.TimestampValue)
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
	case a.KeyValue != nil || b.KeyValue != nil:
		if a.KeyValue == nil || b.KeyValue == nil {
			return 0, false
		}
		return strings.Compare(*a.KeyValue, *b.KeyValue), true
	case a.BlobValue != nil || b.BlobValue != nil:
		if a.BlobValue == nil || b.BlobValue == nil {
			return 0, false
		}
		return bytes.Compare(a.BlobValue, b.BlobValue), true
	}
	return 0, false
}

func valueEqual(a, b dsstore.Value) bool {
	an, aNum := numericValue(a)
	bn, bNum := numericValue(b)
	if aNum || bNum {
		return aNum && bNum && an == bn
	}
	switch {
	case a.NullValue != nil || b.NullValue != nil:
		return a.NullValue != nil && b.NullValue != nil
	case a.BooleanValue != nil || b.BooleanValue != nil:
		return a.BooleanValue != nil && b.BooleanValue != nil && *a.BooleanValue == *b.BooleanValue
	case a.StringValue != nil || b.StringValue != nil:
		return a.StringValue != nil && b.StringValue != nil && *a.StringValue == *b.StringValue
	case a.TimestampValue != nil || b.TimestampValue != nil:
		return a.TimestampValue != nil && b.TimestampValue != nil && *a.TimestampValue == *b.TimestampValue
	case a.KeyValue != nil || b.KeyValue != nil:
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
