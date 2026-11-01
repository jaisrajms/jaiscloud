package bigquery

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// ─── Decimal targets ─────────────────────────────────────────────────────────

// decimalTarget is one BigQuery SQL type a Parquet/Avro DECIMAL can load into.
//
// JobConfigurationLoad.decimalTargetTypes (vendored Discovery, rev 2026-10)
// defines how the source (precision, scale) selects the target:
//
//	"In the order of NUMERIC, BIGNUMERIC, and STRING, a type is picked if it is
//	 in the specified list and if it supports the precision and the scale.
//	 STRING supports all precision and scale values. If none of the listed types
//	 supports the precision and the scale, the type supporting the widest range
//	 in the specified list is picked, and if a value exceeds the supported range
//	 when reading the data, an error will be thrown. ... Defaults to
//	 [\"NUMERIC\", \"STRING\"] for ORC and [\"NUMERIC\"] for the other file formats."
//
// The order of the configured list is ignored and duplicate entries are
// rejected ("This field cannot contain duplicate types").
type decimalTarget struct {
	name      string // NUMERIC, BIGNUMERIC or STRING
	precision int    // maximum total digits; 0 means unlimited (STRING)
	scale     int    // maximum fractional digits
}

var (
	decimalNumeric    = decimalTarget{name: "NUMERIC", precision: 38, scale: 9}
	decimalBigNumeric = decimalTarget{name: "BIGNUMERIC", precision: 76, scale: 38}
	decimalString     = decimalTarget{name: "STRING"}
)

// decimalTargetOrder is the documented resolution order: NUMERIC first, then
// BIGNUMERIC, then STRING, regardless of the order the client listed them in.
var decimalTargetOrder = []decimalTarget{decimalNumeric, decimalBigNumeric, decimalString}

// decimalTargetByName maps a JobConfigurationLoad.decimalTargetTypes entry to
// its target. DECIMAL_TARGET_TYPE_UNSPECIFIED is rejected: the Discovery enum
// documents it as "Invalid type."
func decimalTargetByName(name string) (decimalTarget, bool) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "NUMERIC":
		return decimalNumeric, true
	case "BIGNUMERIC":
		return decimalBigNumeric, true
	case "STRING":
		return decimalString, true
	}
	return decimalTarget{}, false
}

// parseDecimalTargetTypes reads and validates configuration.load.decimalTargetTypes.
// Absent/empty means the format default, which for every supported file format
// (Parquet, Avro, CSV, NDJSON — ORC is not supported) is ["NUMERIC"].
func parseDecimalTargetTypes(load map[string]any) ([]decimalTarget, error) {
	raw := stringSlice(load, "decimalTargetTypes")
	if len(raw) == 0 {
		return []decimalTarget{decimalNumeric}, nil
	}
	seen := make(map[string]bool, len(raw))
	listed := make([]decimalTarget, 0, len(raw))
	for _, name := range raw {
		t, ok := decimalTargetByName(name)
		if !ok {
			return nil, invalidArgument("configuration.load.decimalTargetTypes contains the unknown target type " + strconv.Quote(name) +
				` (expected NUMERIC, BIGNUMERIC or STRING)`)
		}
		if seen[t.name] {
			return nil, invalidArgument("configuration.load.decimalTargetTypes contains the duplicate target type " + strconv.Quote(t.name))
		}
		seen[t.name] = true
		listed = append(listed, t)
	}
	// Canonical resolution order, so ["STRING","NUMERIC"] behaves exactly like
	// ["NUMERIC","STRING"] (Discovery: the order of the list is ignored).
	ordered := make([]decimalTarget, 0, len(listed))
	for _, canonical := range decimalTargetOrder {
		if seen[canonical.name] {
			ordered = append(ordered, canonical)
		}
	}
	return ordered, nil
}

// decimalTargetWidest returns the listed target with the widest range: STRING
// covers everything, BIGNUMERIC more than NUMERIC.
func decimalTargetWidest(targets []decimalTarget) decimalTarget {
	widest := targets[0]
	for _, t := range targets[1:] {
		if decimalTargetRank(t) > decimalTargetRank(widest) {
			widest = t
		}
	}
	return widest
}

func decimalTargetRank(t decimalTarget) int {
	switch t.name {
	case "STRING":
		return 3
	case "BIGNUMERIC":
		return 2
	}
	return 1
}

// decimalTargetSupports reports whether target can hold every value of a
// DECIMAL(precision, scale): the fractional part must fit the target's scale and
// the integer part must fit its integer-digit budget (precision - scale).
// STRING supports everything.
func decimalTargetSupports(target decimalTarget, precision, scale int) bool {
	if target.name == "STRING" {
		return true
	}
	if scale < 0 || scale > target.scale || precision < scale {
		return false
	}
	return precision-scale <= target.precision-target.scale
}

// selectDecimalTarget picks the BigQuery type for a DECIMAL(precision, scale):
// the first listed target in NUMERIC/BIGNUMERIC/STRING order that supports the
// source type, else the widest listed target (whose values are range-checked
// when read, per the Discovery text above).
func selectDecimalTarget(targets []decimalTarget, precision, scale int) decimalTarget {
	for _, candidate := range decimalTargetOrder {
		if !decimalTargetListed(targets, candidate.name) {
			continue
		}
		if decimalTargetSupports(candidate, precision, scale) {
			return candidate
		}
	}
	return decimalTargetWidest(targets)
}

func decimalTargetListed(targets []decimalTarget, name string) bool {
	for _, t := range targets {
		if t.name == name {
			return true
		}
	}
	return false
}

// decimalRangeError reports a source decimal whose value does not fit the
// selected target. Load decoding turns it into a request-level
// InvalidArgument, matching the Discovery contract ("if a value exceeds the
// supported range when reading the data, an error will be thrown").
type decimalRangeError struct {
	target    decimalTarget
	scale     int
	unscaled  *big.Int
	precision int
}

func (e *decimalRangeError) Error() string {
	return fmt.Sprintf("decimal value %s (precision %d, scale %d) does not fit %s (max precision %d, max scale %d)",
		formatUnscaledDecimal(e.unscaled, e.scale), e.precision, e.scale, e.target.name, e.target.precision, e.target.scale)
}

// decimalValueFits reports whether an exact decimal (an unscaled integer plus
// its scale) fits target. Values are checked against the actual digits, not the
// schema's declared precision, because a writer may emit a value wider than the
// column's declared precision.
func decimalValueFits(target decimalTarget, unscaled *big.Int, scale int) bool {
	if target.name == "STRING" {
		return true
	}
	if scale < 0 || scale > target.scale {
		return false
	}
	integerDigits := decimalDigits(unscaled) - scale
	if integerDigits < 0 {
		integerDigits = 0
	}
	return integerDigits <= target.precision-target.scale
}

// decimalDigits is the number of digits in |v| (1 for zero).
func decimalDigits(v *big.Int) int {
	if v.Sign() == 0 {
		return 1
	}
	return len(new(big.Int).Abs(v).String())
}

// renderDecimal renders an exact decimal — an unscaled integer plus its scale —
// as the decimal string BigQuery stores for NUMERIC/BIGNUMERIC/STRING. The
// fractional part keeps exactly `scale` digits, so the rendering never loses
// digits (no float conversion happens anywhere on this path).
func renderDecimal(unscaled *big.Int, scale int, target decimalTarget, precision int) (string, error) {
	if !decimalValueFits(target, unscaled, scale) {
		return "", &decimalRangeError{target: target, scale: scale, unscaled: new(big.Int).Set(unscaled), precision: precision}
	}
	return formatUnscaledDecimal(unscaled, scale), nil
}

// formatUnscaledDecimal renders unscaled / 10^scale exactly.
func formatUnscaledDecimal(unscaled *big.Int, scale int) string {
	digits := new(big.Int).Abs(unscaled).String()
	if scale > 0 {
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		digits = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	}
	if unscaled.Sign() < 0 {
		return "-" + digits
	}
	return digits
}

// decimalFromBytes interprets a Parquet/Avro DECIMAL byte array as a
// two's-complement big-endian unscaled integer.
func decimalFromBytes(b []byte) *big.Int {
	v := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		// Negative: subtract 2^(8*len) from the unsigned reading.
		v.Sub(v, new(big.Int).Lsh(big.NewInt(1), uint(len(b))*8))
	}
	return v
}
