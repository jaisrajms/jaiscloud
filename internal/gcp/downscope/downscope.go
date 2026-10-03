// Package downscope implements the Credential Access Boundary (CAB) used by the
// STS token-exchange flow and its enforcement on Cloud Storage operations.
//
// A client (google-auth-library DownscopedCredentials) exchanges a source access
// token for a downscoped one, passing the boundary as the `options` form field
// of a token-exchange request. The emulator mints a downscoped token that is
// prefixed with TokenPrefix and whose JWT payload carries the parsed rules under
// the "access_boundary" claim. Because the token keeps the identity claims of
// an ordinary access token, internal/gcp/identity still recovers the caller,
// while internal/gcp/downscope recovers the boundary and enforces it.
//
// The accepted boundary is the documented Cloud Storage subset (a bucket plus an
// optional object prefix) rather than arbitrary CEL; unsupported resources,
// permissions and expressions are rejected so a client never believes it has a
// boundary the emulator does not enforce.
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

// TokenPrefix prefixes every downscoped access token. Real STS tokens are
// opaque; the prefix is the emulator convention the compatibility suite asserts.
const TokenPrefix = "floci-gcp-downscoped-"

// Supported inRole permissions (the subset google-auth-library uses for Cloud
// Storage downscoping). Real GCP supports the full IAM role surface.
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

// Rule is one access-boundary rule: a bucket (objectPrefix empty means the whole
// bucket) plus the permissions granted within it.
type Rule struct {
	Bucket       string   `json:"bucket"`
	ObjectPrefix string   `json:"objectPrefix,omitempty"`
	Permissions  []string `json:"permissions"`
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
		Expression string `json:"expression"`
	} `json:"availabilityCondition"`
}

var (
	// resourceRE matches a Cloud Storage bucket resource:
	// //storage.googleapis.com/projects/_/buckets/{bucket}
	resourceRE = regexp.MustCompile(`^//storage\.googleapis\.com/projects/_/buckets/([^/]+)$`)
	// resourcePrefixRE matches resource.name.startsWith('<prefix>').
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
		prefix := ""
		if node.AvailabilityCondition != nil {
			expression := strings.TrimSpace(node.AvailabilityCondition.Expression)
			if expression == "" {
				return nil, errors.New("availabilityCondition.expression is required")
			}
			prefix, err = parsePrefixExpression(bucket, expression)
			if err != nil {
				return nil, err
			}
		}
		rules = append(rules, Rule{Bucket: bucket, ObjectPrefix: prefix, Permissions: perms})
	}
	return rules, nil
}

func parseBucket(availableResource string) (string, error) {
	m := resourceRE.FindStringSubmatch(availableResource)
	if m == nil || m[1] == "" {
		return "", errors.New("unsupported availableResource")
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
		expected := "projects/_/buckets/" + bucket + "/objects/"
		if !strings.HasPrefix(resourcePrefix, expected) {
			return "", errors.New("resource.name prefix does not match availableResource bucket")
		}
		return normalizePrefix(resourcePrefix[len(expected):])
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
// downscoped bearer credential. downscoped is true when the Authorization header
// carries a TokenPrefix credential (even if its boundary cannot be decoded — a
// malformed downscoped token then denies every operation rather than falling
// back to unrestricted access).
func RulesFromAuthorization(authorization string) (rules []Rule, downscoped bool) {
	token := bearerToken(authorization)
	if !strings.HasPrefix(token, TokenPrefix) {
		return nil, false
	}
	return rulesFromToken(token), true
}

// rulesFromToken decodes the boundary claim from a downscoped token's JWT
// payload. Returns nil for a malformed token.
func rulesFromToken(token string) []Rule {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	var payload struct {
		AccessBoundary []Rule `json:"access_boundary"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	return payload.AccessBoundary
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
