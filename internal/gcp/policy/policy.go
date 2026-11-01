// Package policy provides shared GCP IAM policy helpers used by the Pub/Sub,
// Secret Manager, and IAM providers: getIamPolicy / setIamPolicy (with etag
// optimistic concurrency control) and testIamPermissions.
package policy

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"math"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// Policy is a GCP IAM policy (version + etag + bindings).
type Policy struct {
	Version  int    `json:"version"`
	Etag     string `json:"etag"`
	Bindings []any  `json:"bindings"`
}

// DefaultEtag is the etag assigned to an empty policy.
const DefaultEtag = "ACAB"

// Etag derives a SHA-1 etag from an arbitrary string.
func Etag(s string) string {
	h := sha1.Sum([]byte(s))
	return base64.StdEncoding.EncodeToString(h[:])
}

// EtagFor derives a fresh etag from a policy's bindings.
func EtagFor(bindings []any) string {
	b, _ := json.Marshal(bindings)
	return Etag(string(b))
}

// Load returns the stored policy for a resource, or an empty default policy.
func Load(ctx context.Context, s store.ResourceStore, account, resourceType, id string) Policy {
	p := Policy{Version: 1, Etag: DefaultEtag, Bindings: []any{}}
	if e, err := s.Get(ctx, account, store.GlobalRegion, resourceType, id); err == nil {
		json.Unmarshal(e.Data, &p)
	}
	return p
}

// errEtagMismatch aborts Set's UpsertAtomic mutate callback without writing,
// distinguishing an etag-precondition failure from a genuine storage error.
//
// A stale IAM policy etag is google.rpc.ABORTED: real Cloud IAM's setIamPolicy
// fails a stale-etag write with ABORTED, not a generic 409. Status is set
// explicitly so the gRPC mapping is intentional rather than incidental via
// httpToCode(409) (and the REST envelope reports "ABORTED" instead of the
// generic 409 "ALREADY_EXISTS"). HTTP stays 409 — ABORTED → 409 is correct
// on the REST path. Scoped to the etag-OCC sentinel; other 409s are untouched.
var errEtagMismatch = &model.ProviderError{
	Code:       "Aborted",
	Message:    "etag mismatch: optimistic concurrency control failed",
	HTTPStatus: 409,
	Status:     "ABORTED",
}

// Set stores a policy for a resource, enforcing etag OCC. body is the parsed
// request JSON — either the Policy fields directly, or wrapped in a "policy"
// field per SetIamPolicyRequest. Returns the stored policy.
//
// The etag check and the write happen inside a single UpsertAtomic call so
// two concurrent SetIamPolicy requests that both read the same starting etag
// can't both pass the check and race to overwrite each other — the second to
// acquire the lock sees the first's already-updated etag and is rejected.
func Set(ctx context.Context, s store.ResourceStore, account, resourceType, id string, body map[string]any) (Policy, error) {
	policyBody := body
	if p, ok := body["policy"].(map[string]any); ok {
		policyBody = p
	}
	bindings := []any{}
	if bs, ok := policyBody["bindings"].([]any); ok {
		bindings = bs
	}
	reqEtag, _ := policyBody["etag"].(string)

	version, err := resolveVersion(policyBody["version"], hasCondition(bindings))
	if err != nil {
		return Policy{}, err
	}

	pol := Policy{Version: version, Etag: EtagFor(bindings), Bindings: bindings}

	_, err = s.UpsertAtomic(ctx, account, store.GlobalRegion, resourceType, id, func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
		existing := Policy{Version: 1, Etag: DefaultEtag, Bindings: []any{}}
		if exists {
			json.Unmarshal(current.Data, &existing)
		}
		if reqEtag != "" && reqEtag != existing.Etag {
			return store.ResourceEntry{}, errEtagMismatch
		}
		data, _ := json.Marshal(pol)
		return store.ResourceEntry{Type: resourceType, ID: id, Data: data}, nil
	})
	if err != nil {
		if err == errEtagMismatch {
			return Policy{}, errEtagMismatch
		}
		return Policy{}, err
	}
	return pol, nil
}

// resolveVersion decides the stored policy version and enforces the AIP-149
// version rules. IAM documents versions 0 (unspecified), 1 and 3; 3 is mandatory
// for any policy carrying IAM Conditions.
//
//   - absent/0 with no condition -> 1 (the unconditional default)
//   - absent/0 with a condition -> 3 (Terraform sends no version at all, so
//     rejecting it would make a conditional binding impossible to apply)
//   - explicit 1 -> 1, explicit 1 with a condition -> INVALID_ARGUMENT
//   - explicit 3 -> 3, with or without a condition
//   - anything else (2, negative, non-integral, non-numeric) -> INVALID_ARGUMENT
//
// An unconditional policy is never upgraded to 3 implicitly.
func resolveVersion(raw any, conditional bool) (int, error) {
	version, present, err := parseVersion(raw)
	if err != nil {
		return 0, err
	}
	if present {
		switch version {
		case 0:
			present = false
		case 1, 3:
		default:
			return 0, invalidVersion()
		}
	}
	if conditional {
		if present && version == 1 {
			return 0, model.NewProviderError("InvalidArgument",
				"conditional role bindings require IAM policy version 3", 400)
		}
		return 3, nil
	}
	if !present {
		return 1, nil
	}
	return version, nil
}

// parseVersion decodes a Policy.version from a decoded JSON body or a
// proto-converted map. present is false only for an absent/null field; an
// explicit 0 is returned as present and normalised to "unspecified" by
// resolveVersion.
func parseVersion(raw any) (version int, present bool, err error) {
	switch v := raw.(type) {
	case nil:
		return 0, false, nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || v != math.Trunc(v) {
			return 0, false, invalidVersion()
		}
		return int(v), true, nil
	case int:
		return v, true, nil
	case int32:
		return int(v), true, nil
	case int64:
		return int(v), true, nil
	case json.Number:
		n, convErr := v.Int64()
		if convErr != nil {
			return 0, false, invalidVersion()
		}
		return int(n), true, nil
	default:
		return 0, false, invalidVersion()
	}
}

// invalidVersion is the canonical InvalidArgument for an unsupported policy
// version. IAM (AIP-149) accepts only 0, 1 and 3.
func invalidVersion() error {
	return model.NewProviderError("InvalidArgument",
		"unsupported IAM policy version: valid values are 0, 1, and 3", 400)
}

// hasCondition reports whether any binding carries a non-empty condition
// object. Bindings are otherwise opaque and are stored/rendered verbatim.
func hasCondition(bindings []any) bool {
	for _, b := range bindings {
		m, ok := b.(map[string]any)
		if !ok {
			continue
		}
		if c, ok := m["condition"].(map[string]any); ok && len(c) > 0 {
			return true
		}
	}
	return false
}

// ToMap renders a Policy as a response map. An absent binding list is rendered
// as an empty array, not JSON null: the Discovery Policy schema declares
// "bindings" as an array, so null is a wire-shape violation.
func ToMap(p Policy) map[string]any {
	bindings := p.Bindings
	if bindings == nil {
		bindings = []any{}
	}
	return map[string]any{"version": p.Version, "etag": p.Etag, "bindings": bindings}
}

// TestPermissions returns the permissions the caller is allowed. JaisCloud does
// not enforce IAM (every request to every project succeeds regardless of
// policy), so the emulator treats the caller as project owner and grants all
// requested permissions — consistent with the no-authz-enforcement posture.
func TestPermissions(permissions []string) []string {
	return permissions
}

// Permissions extracts the "permissions" string array from a
// testIamPermissions request body.
func Permissions(body map[string]any) []string {
	items, _ := body["permissions"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
