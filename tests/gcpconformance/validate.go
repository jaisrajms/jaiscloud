//go:build gcp_conformance

package gcpconformance

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Divergence is one wire-conformance finding. Kind is one of
// missing_required, wrong_type, unknown_field, bad_enum, bad_format,
// unmatched_method, bad_error_envelope.
type Divergence struct {
	Path     string `json:"path"`
	Kind     string `json:"kind"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Severity string `json:"severity"`

	// Service and Method identify the recorded operation that produced the
	// finding; they are additive context for the report.
	Service string `json:"service,omitempty"`
	Method  string `json:"method,omitempty"`
}

const (
	kindMissingRequired  = "missing_required"
	kindWrongType        = "wrong_type"
	kindUnknownField     = "unknown_field"
	kindBadEnum          = "bad_enum"
	kindBadFormat        = "bad_format"
	kindUnmatchedMethod  = "unmatched_method"
	kindBadErrorEnvelope = "bad_error_envelope"
)

// validateOpts tunes validation for a request vs a response context.
type validateOpts struct {
	// requireRequired enforces Discovery `required` fields. Response bodies must
	// have them; request bodies often do not (PATCH/partial updates, and the
	// emulator is deliberately lenient), so request validation leaves it off.
	requireRequired bool
}

// ValidateValue validates a decoded RESPONSE body against a Discovery schema
// and returns divergences. path is the JSON location used in reporting.
func ValidateValue(doc *DiscoveryDoc, schema *Schema, v any, path string) []Divergence {
	return validateValueOpts(doc, schema, v, path, validateOpts{requireRequired: true})
}

// ValidateRequestValue validates a decoded REQUEST body: present-field types and
// unknown fields are checked, but `required` is not enforced (partial bodies are
// legitimate).
func ValidateRequestValue(doc *DiscoveryDoc, schema *Schema, v any, path string) []Divergence {
	return validateValueOpts(doc, schema, v, path, validateOpts{requireRequired: false})
}

func validateValueOpts(doc *DiscoveryDoc, schema *Schema, v any, path string, opts validateOpts) []Divergence {
	if schema == nil {
		return nil
	}
	if schema.Ref != "" {
		if resolved, ok := doc.ResolveRef(schema.Ref); ok {
			return validateValueOpts(doc, resolved, v, path, opts)
		}
		return nil
	}
	typ := schema.Type
	if typ == "" {
		switch {
		case schema.Properties != nil:
			typ = "object"
		case schema.Items != nil:
			typ = "array"
		}
	}
	switch typ {
	case "object":
		obj, ok := v.(map[string]any)
		if !ok {
			return []Divergence{wrongType(path, "object", v)}
		}
		return validateObject(doc, schema, obj, path, opts)
	case "array":
		arr, ok := v.([]any)
		if !ok {
			return []Divergence{wrongType(path, "array", v)}
		}
		var divs []Divergence
		for i, item := range arr {
			divs = append(divs, validateValueOpts(doc, schema.Items, item, fmt.Sprintf("%s[%d]", path, i), opts)...)
		}
		return divs
	case "string":
		s, ok := v.(string)
		if !ok {
			return []Divergence{wrongType(path, "string", v)}
		}
		return validateString(schema, s, path)
	case "integer":
		return validateInteger(schema, v, path)
	case "number":
		if !isNumber(v) {
			return []Divergence{wrongType(path, "number", v)}
		}
		return nil
	case "boolean":
		if _, ok := v.(bool); !ok {
			return []Divergence{wrongType(path, "boolean", v)}
		}
		return nil
	case "any", "":
		return nil
	default:
		return nil
	}
}

func validateObject(doc *DiscoveryDoc, schema *Schema, obj map[string]any, path string, opts validateOpts) []Divergence {
	var divs []Divergence

	if opts.requireRequired {
		for _, name := range schema.Required {
			if _, ok := obj[name]; !ok {
				divs = append(divs, Divergence{
					Path:     joinPath(path, name),
					Kind:     kindMissingRequired,
					Expected: "required field present",
					Actual:   "absent",
					Severity: "high",
				})
			}
		}
	}

	// Deterministic iteration for stable reports.
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sortStrings(keys)

	additionalSchema := schema.additionalSchema()
	for _, name := range keys {
		val := obj[name]
		prop, known := schema.Properties[name]
		if !known {
			if schema.allowsAdditional() {
				if additionalSchema != nil {
					divs = append(divs, validateValueOpts(doc, additionalSchema, val, joinPath(path, name), opts)...)
				}
				continue
			}
			divs = append(divs, Divergence{
				Path:     joinPath(path, name),
				Kind:     kindUnknownField,
				Expected: "field declared in Discovery schema (or additionalProperties allowed)",
				Actual:   "present",
				Severity: "info",
			})
			continue
		}
		divs = append(divs, validateValueOpts(doc, prop, val, joinPath(path, name), opts)...)
	}
	return divs
}

func validateString(schema *Schema, s, path string) []Divergence {
	var divs []Divergence
	if len(schema.Enum) > 0 && !containsString(schema.Enum, s) {
		divs = append(divs, Divergence{
			Path:     path,
			Kind:     kindBadEnum,
			Expected: "one of [" + strings.Join(schema.Enum, ", ") + "]",
			Actual:   s,
			Severity: "medium",
		})
	}
	if schema.Format != "" {
		if !checkFormat(schema.Format, s) {
			divs = append(divs, Divergence{
				Path:     path,
				Kind:     kindBadFormat,
				Expected: "value formatted as " + schema.Format,
				Actual:   s,
				Severity: "medium",
			})
		}
	}
	return divs
}

func validateInteger(schema *Schema, v any, path string) []Divergence {
	switch n := v.(type) {
	case float64:
		if n != math.Trunc(n) {
			return []Divergence{wrongType(path, "integer", v)}
		}
		if schema.Format == "int32" && (n < math.MinInt32 || n > math.MaxInt32) {
			return []Divergence{{
				Path: path, Kind: kindBadFormat,
				Expected: "value within int32 range", Actual: formatNumber(n), Severity: "medium",
			}}
		}
		if schema.Format == "uint32" && (n < 0 || n > math.MaxUint32) {
			return []Divergence{{
				Path: path, Kind: kindBadFormat,
				Expected: "value within uint32 range", Actual: formatNumber(n), Severity: "medium",
			}}
		}
		return nil
	case string:
		// Discovery uses type=string format=int64/uint64 for 64-bit ints.
		var err error
		if schema.Format == "uint64" {
			_, err = strconv.ParseUint(n, 10, 64)
		} else {
			_, err = strconv.ParseInt(n, 10, 64)
		}
		if err != nil {
			return []Divergence{{
				Path: path, Kind: kindBadFormat,
				Expected: "integer-formatted string", Actual: n, Severity: "medium",
			}}
		}
		return nil
	default:
		return []Divergence{wrongType(path, "integer", v)}
	}
}

// checkFormat is best-effort: it validates the formats Discovery actually uses
// for strings on this surface and ignores the rest to avoid false positives.
func checkFormat(format, s string) bool {
	switch format {
	case "date-time", "google-datetime":
		_, err := time.Parse(time.RFC3339, s)
		return err == nil
	case "date":
		_, err := time.Parse("2006-01-02", s)
		return err == nil
	case "byte":
		_, err := base64.StdEncoding.DecodeString(s)
		return err == nil
	case "int64":
		_, err := strconv.ParseInt(s, 10, 64)
		return err == nil
	case "uint64":
		_, err := strconv.ParseUint(s, 10, 64)
		return err == nil
	case "int32", "uint32":
		_, err := strconv.ParseInt(s, 10, 64)
		return err == nil
	default:
		return true
	}
}

func wrongType(path, expected string, v any) Divergence {
	return Divergence{
		Path:     path,
		Kind:     kindWrongType,
		Expected: expected,
		Actual:   typeName(v),
		Severity: "high",
	}
}

func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case float64, int, int64:
		return "number"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return fmt.Sprintf("%T", v)
	}
}

func isNumber(v any) bool {
	switch n := v.(type) {
	case float64:
		return !math.IsNaN(n) && !math.IsInf(n, 0)
	case int, int64:
		return true
	default:
		return false
	}
}

func formatNumber(n float64) string {
	return strconv.FormatFloat(n, 'f', -1, 64)
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func joinPath(base, name string) string {
	if base == "" {
		return name
	}
	return base + "." + name
}

func sortStrings(xs []string) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j] < xs[j-1]; j-- {
			xs[j], xs[j-1] = xs[j-1], xs[j]
		}
	}
}

// ─── GCP error envelope ───────────────────────────────────────────────────────

// httpToRPC maps an HTTP status to the accepted google.rpc.Code status name(s).
var httpToRPC = map[int][]string{
	400: {"INVALID_ARGUMENT", "FAILED_PRECONDITION", "OUT_OF_RANGE"}, // google.rpc.Code: all three map to HTTP 400.
	401: {"UNAUTHENTICATED"},
	403: {"PERMISSION_DENIED"},
	404: {"NOT_FOUND"},
	409: {"ALREADY_EXISTS", "ABORTED"},
	412: {"FAILED_PRECONDITION"},
	429: {"RESOURCE_EXHAUSTED"},
	499: {"CANCELLED"},
	500: {"INTERNAL"},
	501: {"UNIMPLEMENTED"},
	503: {"UNAVAILABLE"},
}

// ValidateErrorEnvelope checks a non-2xx response against GCP's JSON error
// shape {"error":{"code","message","status","errors":[...]}}.
func ValidateErrorEnvelope(status int, body []byte) []Divergence {
	var divs []Divergence

	if len(body) == 0 {
		return []Divergence{{
			Path: "error", Kind: kindBadErrorEnvelope,
			Expected: "GCP error envelope", Actual: "empty body", Severity: "high",
		}}
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return []Divergence{{
			Path: "error", Kind: kindBadErrorEnvelope,
			Expected: "JSON error envelope", Actual: err.Error(), Severity: "high",
		}}
	}

	errAny, ok := raw["error"]
	if !ok {
		return append(divs, Divergence{
			Path: "error", Kind: kindMissingRequired,
			Expected: "top-level error object", Actual: "absent", Severity: "high",
		})
	}
	errObj, ok := errAny.(map[string]any)
	if !ok {
		return append(divs, Divergence{
			Path: "error", Kind: kindWrongType,
			Expected: "object", Actual: typeName(errAny), Severity: "high",
		})
	}

	// code == HTTP status.
	if codeAny, ok := errObj["code"]; !ok {
		divs = append(divs, Divergence{
			Path: "error.code", Kind: kindMissingRequired,
			Expected: fmt.Sprintf("%d (HTTP status)", status), Actual: "absent", Severity: "high",
		})
	} else {
		code, ok := numberToInt(codeAny)
		if !ok {
			divs = append(divs, Divergence{
				Path: "error.code", Kind: kindWrongType,
				Expected: "integer", Actual: typeName(codeAny), Severity: "high",
			})
		} else if code != status {
			divs = append(divs, Divergence{
				Path: "error.code", Kind: kindBadErrorEnvelope,
				Expected: strconv.Itoa(status), Actual: strconv.Itoa(code), Severity: "high",
			})
		}
	}

	// message string.
	if msgAny, ok := errObj["message"]; !ok {
		divs = append(divs, Divergence{
			Path: "error.message", Kind: kindMissingRequired,
			Expected: "non-empty string", Actual: "absent", Severity: "high",
		})
	} else if s, ok := msgAny.(string); !ok || s == "" {
		divs = append(divs, Divergence{
			Path: "error.message", Kind: kindWrongType,
			Expected: "non-empty string", Actual: typeName(msgAny), Severity: "high",
		})
	}

	// GCP's JSON error envelope varies by API generation:
	//   - legacy APIs (Cloud Storage) return errors[] with reason/domain/message
	//     and omit the google.rpc "status";
	//   - modern APIs (Pub/Sub, Secret Manager, KMS, IAM, Cloud DNS, BigQuery)
	//     return "status" (plus "details"/ErrorInfo) and omit errors[].
	//
	// So neither component is individually required: a response must carry at
	// least one of them, and whichever is present must be well-formed. Modeling
	// the variance here (rather than suppressing findings via an allowlist) is
	// the reconciliation that lets the allowlist go to zero.
	statusAny, hasStatus := errObj["status"]
	if hasStatus {
		if s, ok := statusAny.(string); !ok {
			divs = append(divs, Divergence{
				Path: "error.status", Kind: kindWrongType,
				Expected: "string", Actual: typeName(statusAny), Severity: "medium",
			})
		} else if expected := httpToRPC[status]; len(expected) > 0 && !containsString(expected, s) {
			divs = append(divs, Divergence{
				Path: "error.status", Kind: kindBadEnum,
				Expected: "one of [" + strings.Join(expected, ", ") + "]", Actual: s, Severity: "medium",
			})
		}
	}

	errsAny, hasErrors := errObj["errors"]
	if hasErrors {
		errs, ok := errsAny.([]any)
		if !ok {
			divs = append(divs, Divergence{
				Path: "error.errors", Kind: kindWrongType,
				Expected: "array", Actual: typeName(errsAny), Severity: "medium",
			})
		} else {
			for i, e := range errs {
				em, ok := e.(map[string]any)
				if !ok {
					divs = append(divs, Divergence{
						Path: fmt.Sprintf("error.errors[%d]", i), Kind: kindWrongType,
						Expected: "object", Actual: typeName(e), Severity: "medium",
					})
					continue
				}
				for _, field := range []string{"reason", "domain", "message"} {
					v, ok := em[field]
					if !ok {
						divs = append(divs, Divergence{
							Path: fmt.Sprintf("error.errors[%d].%s", i, field), Kind: kindMissingRequired,
							Expected: "string", Actual: "absent", Severity: "medium",
						})
						continue
					}
					if s, ok := v.(string); !ok || s == "" {
						divs = append(divs, Divergence{
							Path: fmt.Sprintf("error.errors[%d].%s", i, field), Kind: kindWrongType,
							Expected: "non-empty string", Actual: typeName(v), Severity: "medium",
						})
					}
				}
			}
		}
	}

	if !hasStatus && !hasErrors {
		divs = append(divs, Divergence{
			Path:     "error",
			Kind:     kindBadErrorEnvelope,
			Expected: "google.rpc status and/or legacy errors[]",
			Actual:   "neither present",
			Severity: "medium",
		})
	}

	return divs
}

func numberToInt(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), n == math.Trunc(n)
	case int:
		return n, true
	case int64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	default:
		return 0, false
	}
}
