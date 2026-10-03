package logging

import (
	"net/url"
	"strings"

	"jaiscloud/internal/model"
)

// logScopes is the set of resource-container collections that may parent a
// Cloud Logging log, per Logging v2: projects, organizations, folders, and
// billingAccounts. A log name uses the form {scope}/{scopeID}/logs/{LOG_ID}.
var logScopes = map[string]struct{}{
	"projects":        {},
	"organizations":   {},
	"folders":         {},
	"billingAccounts": {},
}

// IsLogScope reports whether name is one of the Cloud Logging resource
// containers (projects, organizations, folders, billingAccounts). It is used by
// the REST router to recognize the /v2/{scope}/{id}/logs path family.
func IsLogScope(name string) bool {
	_, ok := logScopes[name]
	return ok
}

func invalidLogName(name string) error {
	return model.NewProviderError("InvalidArgument", "invalid log name: "+name, 400)
}

func invalidParent(name string) error {
	return model.NewProviderError("InvalidArgument", "invalid resource name: "+name, 400)
}

// ParseLogName parses a Cloud Logging log resource name of the form
// {scope}/{scopeID}/logs/{LOG_ID}, where {scope} is one of "projects",
// "organizations", "folders", or "billingAccounts". The route uses non-empty
// segments ([^/]+) and LOG_ID is URL-encoded, so it is URL-decoded here.
//
// It returns the canonical two-segment scope parent ("projects/p",
// "organizations/123", ...) and the decoded log ID. Malformed names, including
// empty segments, an unknown scope, a missing/wrong "logs" segment, a blank
// log ID, and an invalid percent-escape, return an InvalidArgument error.
//
// A leading "/" is stripped for backward compatibility with clients that send
// "/projects/..." (real Cloud Logging processes such names after removing the
// leading slash).
func ParseLogName(name string) (scopeParent, logID string, err error) {
	trimmed := strings.TrimPrefix(name, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 4 || parts[2] != "logs" {
		return "", "", invalidLogName(name)
	}
	if _, ok := logScopes[parts[0]]; !ok || parts[1] == "" || parts[3] == "" {
		return "", "", invalidLogName(name)
	}
	decoded, derr := url.PathUnescape(parts[3])
	if derr != nil || decoded == "" {
		return "", "", invalidLogName(name)
	}
	return parts[0] + "/" + parts[1], decoded, nil
}

// CanonicalLogName builds the canonical log resource name for a scope parent
// and a decoded log ID. The LOG_ID is re-encoded so equivalent spellings
// (for example "%2f" vs "%2F", or a leading slash) collapse to one stored form
// and appear URL-encoded in ListLogs, matching real Cloud Logging.
func CanonicalLogName(scopeParent, logID string) string {
	return scopeParent + "/logs/" + url.PathEscape(logID)
}

// ParseScopeParent normalizes a parent resource name used by ListLogs (parent)
// and ListLogEntries (resource_names) to a canonical two-segment scope parent.
// It accepts a container parent ("projects/p", "organizations/123",
// "folders/f", "billingAccounts/b"), a full log name (the parent portion is
// returned), or a bare project ID (legacy "p" -> "projects/p"). Malformed names
// return an InvalidArgument error.
func ParseScopeParent(name string) (string, error) {
	trimmed := strings.TrimPrefix(name, "/")
	parts := strings.Split(trimmed, "/")
	switch len(parts) {
	case 1:
		if parts[0] == "" {
			return "", invalidParent(name)
		}
		return "projects/" + parts[0], nil
	case 2:
		if _, ok := logScopes[parts[0]]; !ok || parts[1] == "" {
			return "", invalidParent(name)
		}
		return parts[0] + "/" + parts[1], nil
	case 4:
		scopeParent, _, perr := ParseLogName(trimmed)
		if perr != nil {
			return "", perr
		}
		return scopeParent, nil
	default:
		return "", invalidParent(name)
	}
}
