// Package policy provides shared GCP IAM policy helpers used by the Pub/Sub,
// Secret Manager, and IAM providers: getIamPolicy / setIamPolicy (with etag
// optimistic concurrency control) and testIamPermissions.
package policy

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"

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

	pol := Policy{Version: 1, Etag: EtagFor(bindings), Bindings: bindings}
	if v, ok := policyBody["version"].(float64); ok {
		pol.Version = int(v)
	}

	_, err := s.UpsertAtomic(ctx, account, store.GlobalRegion, resourceType, id, func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
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

// ToMap renders a Policy as a response map.
func ToMap(p Policy) map[string]any {
	return map[string]any{"version": p.Version, "etag": p.Etag, "bindings": p.Bindings}
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
