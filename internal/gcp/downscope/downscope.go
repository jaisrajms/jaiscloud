// Package downscope implements the Credential Access Boundary (CAB) used by the
// STS token-exchange flow and its enforcement on Cloud Storage operations.
//
// A client (google-auth-library DownscopedCredentials) exchanges a source access
// token for a downscoped one, passing the boundary as the `options` form field
// of a token-exchange request. Real STS returns an opaque token; the emulator
// mints an opaque JWT whose payload carries the parsed rules under the
// "access_boundary" claim. Because the token keeps the identity claims of an
// ordinary access token, internal/gcp/identity still recovers the caller, while
// internal/gcp/downscope recovers the boundary by the presence of that claim and
// enforces it. There is no emulator-specific token prefix.
//
// The accepted boundary is the documented surface: Credential Access Boundaries
// apply to Cloud Storage only, and an access-boundary rule's availableResource is
// a bucket full resource name
// (//storage.googleapis.com/projects/_/buckets/{bucket}); object- and
// folder-level restrictions belong in the availability condition, never in
// availableResource
// (https://cloud.google.com/iam/docs/downscoping-short-lived-credentials).
// Other Google Cloud services do not support the feature, so their resource
// names are rejected instead of being accepted into a boundary the emulator
// would not enforce — as are permissions and condition expressions outside the
// documented subset.
package downscope

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"jaiscloud/internal/gcp/identity"
)

// Supported inRole permissions. Each role maps exactly onto the Cloud Storage
// operations the emulator enforces (read, list, write, delete), so a rule never
// grants more or less than its role says. Broader documented roles (for example
// roles/storage.objectUser or roles/storage.objectCreator) stay rejected rather
// than being approximated; real GCP supports the full IAM role surface.
const (
	PermissionLegacyObjectReader = "inRole:roles/storage.legacyObjectReader"
	PermissionObjectViewer       = "inRole:roles/storage.objectViewer"
	PermissionLegacyBucketWriter = "inRole:roles/storage.legacyBucketWriter"
)

var supportedPermissions = map[string]bool{
	PermissionLegacyObjectReader: true,
	PermissionObjectViewer:       true,
	PermissionLegacyBucketWriter: true,
}

// ErrDenied is returned when a downscoped credential does not permit the
// requested operation.
var ErrDenied = errors.New("downscoped token does not allow this GCS operation")

// Rule is one access-boundary rule: the Cloud Storage bucket it admits, the
// derived object prefix (empty means the whole bucket), the permissions granted
// within it and the availability condition when one was supplied.
//
// The JSON names follow the documented Credential Access Boundary rule shape
// (availableResource / availablePermissions / availabilityCondition), so a
// downscoped token's access_boundary claim round-trips the boundary the client
// sent; Bucket and ObjectPrefix are the derived enforcement view and are
// emulator-internal.
type Rule struct {
	AvailableResource string   `json:"availableResource,omitempty"`
	Bucket            string   `json:"bucket,omitempty"`
	ObjectPrefix      string   `json:"objectPrefix,omitempty"`
	Permissions       []string `json:"availablePermissions,omitempty"`
	// AvailabilityCondition is the documented condition that narrowed the rule
	// to ObjectPrefix. Its title and description carry no enforcement meaning
	// but are preserved so the boundary round-trips.
	AvailabilityCondition *AvailabilityCondition `json:"availabilityCondition,omitempty"`
}

// AvailabilityCondition is the documented availability condition: a CEL
// expression plus an optional title and description.
type AvailabilityCondition struct {
	Expression  string `json:"expression"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
}

// Op classifies a Cloud Storage operation for boundary enforcement.
type Op uint8

const (
	// ReadObject covers object metadata/bytes reads (GCS objects.get,
	// objects.get with alt=media, and the gRPC read RPCs).
	ReadObject Op = iota
	// List covers objects.list (the boundary applies to the list prefix).
	List
	// WriteObject covers object create/update/patch/rewrite/copy/move/compose.
	WriteObject
	// DeleteObject covers objects.delete.
	DeleteObject
	// BucketAdmin covers bucket-level operations (bucket CRUD, IAM, ACL,
	// notifications), which a downscoped token never permits.
	BucketAdmin
)

// optionsDoc is the `options` request JSON emitted by
// CredentialAccessBoundary.toJson(): {"accessBoundary":{"accessBoundaryRules":[...]}}.
type optionsDoc struct {
	AccessBoundary struct {
		AccessBoundaryRules []ruleDoc `json:"accessBoundaryRules"`
	} `json:"accessBoundary"`
}

type ruleDoc struct {
	AvailableResource     string   `json:"availableResource"`
	AvailablePermissions  []string `json:"availablePermissions"`
	AvailabilityCondition *struct {
		Expression  string `json:"expression"`
		Title       string `json:"title"`
		Description string `json:"description"`
	} `json:"availabilityCondition"`
}

const (
	// maxRules is the documented cap: one Credential Access Boundary contains at
	// most 10 access boundary rules.
	maxRules = 10
	// maxExpressionLength is the documented maximum length of an availability
	// condition expression.
	maxExpressionLength = 2048
)

var (
	// bucketResourceRE matches the documented availableResource for a Cloud
	// Storage bucket: //storage.googleapis.com/projects/_/buckets/{bucket}. The
	// project segment is the literal `_`, and object/folder restrictions belong in
	// the availability condition, not in the resource name.
	bucketResourceRE = regexp.MustCompile(`^//storage\.googleapis\.com/projects/_/buckets/([^/]+)$`)
	// resourcePrefixRE matches resource.name.startsWith('<prefix>'), covering the
	// documented object and hierarchical-namespace folder names.
	resourcePrefixRE = regexp.MustCompile(`^resource\.name\.startsWith\((.+)\)$`)
	// listPrefixRE matches
	// api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('<prefix>').
	listPrefixRE = regexp.MustCompile(`^api\.getAttribute\((.+),\s*(.+)\)\.startsWith\((.+)\)$`)
)

// ParseOptions parses the STS `options` form field into access-boundary rules.
// The error is intentionally safe to surface as an OAuth2 invalid_grant
// description.
func ParseOptions(options string) ([]Rule, error) {
	if strings.TrimSpace(options) == "" {
		return nil, errors.New("options is required")
	}
	var doc optionsDoc
	if err := json.Unmarshal([]byte(options), &doc); err != nil {
		return nil, errors.New("options must be valid Credential Access Boundary JSON")
	}
	ruleNodes := doc.AccessBoundary.AccessBoundaryRules
	if len(ruleNodes) == 0 {
		return nil, errors.New("accessBoundaryRules is required")
	}
	if len(ruleNodes) > maxRules {
		return nil, fmt.Errorf("accessBoundary can contain at most %d rules", maxRules)
	}

	rules := make([]Rule, 0, len(ruleNodes))
	for _, node := range ruleNodes {
		bucket, err := parseBucket(node.AvailableResource)
		if err != nil {
			return nil, err
		}
		perms, err := parsePermissions(node.AvailablePermissions)
		if err != nil {
			return nil, err
		}
		rule := Rule{AvailableResource: node.AvailableResource, Bucket: bucket, Permissions: perms}
		if node.AvailabilityCondition != nil {
			condition := &AvailabilityCondition{
				Expression:  strings.TrimSpace(node.AvailabilityCondition.Expression),
				Title:       node.AvailabilityCondition.Title,
				Description: node.AvailabilityCondition.Description,
			}
			if condition.Expression == "" {
				return nil, errors.New("availabilityCondition.expression is required")
			}
			if len(condition.Expression) > maxExpressionLength {
				return nil, fmt.Errorf("availabilityCondition.expression must not exceed %d characters", maxExpressionLength)
			}
			prefix, err := parsePrefixExpression(bucket, condition.Expression)
			if err != nil {
				return nil, err
			}
			rule.ObjectPrefix = prefix
			rule.AvailabilityCondition = condition
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

func parseBucket(availableResource string) (string, error) {
	m := bucketResourceRE.FindStringSubmatch(availableResource)
	if m == nil || m[1] == "" {
		return "", fmt.Errorf("unsupported availableResource %q: Credential Access Boundaries apply to Cloud Storage buckets only (//storage.googleapis.com/projects/_/buckets/{bucket})", availableResource)
	}
	return m[1], nil
}

func parsePermissions(permissions []string) ([]string, error) {
	if len(permissions) == 0 {
		return nil, errors.New("availablePermissions is required")
	}
	out := make([]string, 0, len(permissions))
	for _, p := range permissions {
		if !supportedPermissions[p] {
			return nil, fmt.Errorf("unsupported availablePermission %q", p)
		}
		out = append(out, p)
	}
	return out, nil
}

// parsePrefixExpression resolves a supported availability condition to a single
// object prefix. The expression may be one term, or two terms joined with `||`
// (resource name + list-prefix attribute); all terms must identify the same
// prefix.
func parsePrefixExpression(bucket, expression string) (string, error) {
	parts, err := splitOrExpression(expression)
	if err != nil {
		return "", err
	}
	prefix := ""
	for i, part := range parts {
		p, err := parseSingleExpression(bucket, strings.TrimSpace(part))
		if err != nil {
			return "", err
		}
		if i == 0 {
			prefix = p
			continue
		}
		if p != prefix {
			return "", errors.New("all supported expressions must use the same object prefix")
		}
	}
	return prefix, nil
}

// splitOrExpression splits on top-level `||`, ignoring separators inside string
// literals.
func splitOrExpression(expression string) ([]string, error) {
	var parts []string
	start := 0
	var quote byte
	escaped := false
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if escaped {
			escaped = false
			continue
		}
		if quote != 0 {
			switch ch {
			case '\\':
				escaped = true
			case quote:
				quote = 0
			}
			continue
		}
		switch {
		case ch == '\'' || ch == '"':
			quote = ch
		case ch == '|' && i+1 < len(expression) && expression[i+1] == '|':
			parts = append(parts, expression[start:i])
			start = i + 2
			i++
		}
	}
	if quote != 0 {
		return nil, errors.New("unterminated string literal in expression")
	}
	parts = append(parts, expression[start:])
	return parts, nil
}

func parseSingleExpression(bucket, expression string) (string, error) {
	if m := resourcePrefixRE.FindStringSubmatch(expression); m != nil {
		resourcePrefix, err := parseStringArgument(m[1])
		if err != nil {
			return "", err
		}
		// The documented prefix names an object, or — on a bucket with
		// hierarchical namespace — a folder. Both map onto the object-name prefix
		// the GCS provider enforces.
		for _, collection := range []string{"objects", "folders"} {
			expected := "projects/_/buckets/" + bucket + "/" + collection + "/"
			if strings.HasPrefix(resourcePrefix, expected) {
				return normalizePrefix(resourcePrefix[len(expected):])
			}
		}
		return "", errors.New("resource.name prefix does not match availableResource bucket")
	}
	if m := listPrefixRE.FindStringSubmatch(expression); m != nil {
		attribute, err := parseStringArgument(m[1])
		if err != nil {
			return "", err
		}
		defaultValue, err := parseStringArgument(m[2])
		if err != nil {
			return "", err
		}
		objectPrefix, err := parseStringArgument(m[3])
		if err != nil {
			return "", err
		}
		if attribute != "storage.googleapis.com/objectListPrefix" || defaultValue != "" {
			return "", errors.New("unsupported api.getAttribute expression")
		}
		return normalizePrefix(objectPrefix)
	}
	return "", errors.New("unsupported availabilityCondition expression")
}

// parseStringArgument decodes a single-quoted or double-quoted CEL string
// literal.
func parseStringArgument(argument string) (string, error) {
	trimmed := strings.TrimSpace(argument)
	if len(trimmed) < 2 {
		return "", errors.New("expected string literal")
	}
	quote := trimmed[0]
	if (quote != '\'' && quote != '"') || trimmed[len(trimmed)-1] != quote {
		return "", errors.New("expected string literal")
	}
	return decodeCELString(trimmed[1 : len(trimmed)-1])
}

func decodeCELString(value string) (string, error) {
	var decoded strings.Builder
	decoded.Grow(len(value))
	escaped := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if !escaped {
			if ch == '\\' {
				escaped = true
			} else {
				decoded.WriteByte(ch)
			}
			continue
		}
		switch ch {
		case '\\':
			decoded.WriteByte('\\')
		case '\'':
			decoded.WriteByte('\'')
		case '"':
			decoded.WriteByte('"')
		case 'n':
			decoded.WriteByte('\n')
		case 'r':
			decoded.WriteByte('\r')
		case 't':
			decoded.WriteByte('\t')
		case 'b':
			decoded.WriteByte('\b')
		case 'f':
			decoded.WriteByte('\f')
		default:
			return "", errors.New("unsupported string escape")
		}
		escaped = false
	}
	if escaped {
		return "", errors.New("unterminated string escape")
	}
	return decoded.String(), nil
}

func normalizePrefix(prefix string) (string, error) {
	if strings.TrimSpace(prefix) == "" {
		return "", errors.New("object prefix is required")
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix, nil
}

// RulesFromAuthorization returns the access-boundary rules carried by a
// downscoped bearer credential. Real STS tokens are opaque, so a token is
// identified as downscoped by decoding its JWT payload and finding the
// `access_boundary` claim; a credential without that claim is an ordinary
// access token and is not restricted here. A downscoped token whose boundary
// is present but empty is reported downscoped with no rules and therefore
// denies every operation.
func RulesFromAuthorization(authorization string) (rules []Rule, downscoped bool) {
	return rulesFromToken(bearerToken(authorization))
}

// rulesFromToken decodes the boundary claim from a downscoped token's JWT
// payload. The claim's presence (not its contents) marks the token downscoped.
func rulesFromToken(token string) ([]Rule, bool) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, false
	}
	var payload struct {
		AccessBoundary *[]Rule `json:"access_boundary"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, false
	}
	if payload.AccessBoundary == nil {
		return nil, false
	}
	return *payload.AccessBoundary, true
}

// Allowed reports whether the request's bearer credential permits op on the
// given bucket and object/prefix. It returns nil for credentials that are not
// downscoped (ordinary access tokens are not restricted here) and ErrDenied when
// a downscoped credential excludes the operation.
func Allowed(authorization string, op Op, bucket, name string) error {
	rules, downscoped := RulesFromAuthorization(authorization)
	if !downscoped {
		return nil
	}
	if op == BucketAdmin {
		return ErrDenied
	}
	accepted := acceptedPermissions(op)
	for _, rule := range rules {
		if rule.Bucket != bucket {
			continue
		}
		if !strings.HasPrefix(name, rule.ObjectPrefix) {
			continue
		}
		if hasAnyPermission(rule.Permissions, accepted) {
			return nil
		}
	}
	return ErrDenied
}

func acceptedPermissions(op Op) []string {
	switch op {
	case ReadObject:
		return []string{PermissionLegacyObjectReader, PermissionObjectViewer}
	case List:
		return []string{PermissionObjectViewer, PermissionLegacyBucketWriter}
	case WriteObject, DeleteObject:
		return []string{PermissionLegacyBucketWriter}
	default:
		return nil
	}
}

func hasAnyPermission(granted, accepted []string) bool {
	for _, g := range granted {
		for _, a := range accepted {
			if strings.EqualFold(g, a) {
				return true
			}
		}
	}
	return false
}

func bearerToken(authorization string) string {
	return identity.BearerToken(authorization)
}
