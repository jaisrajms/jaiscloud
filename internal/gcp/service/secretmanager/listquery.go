// Package secretmanager hosts the transport-neutral list filtering shared by
// the Secret Manager REST provider (internal/gcp/provider/secretmanager) and
// gRPC service (internal/gcp/grpc/secretmanager).
//
// Both transports used to page the store's full secret/version list straight
// into paging.Page, so the documented `filter` parameter of secrets.list and
// secrets.versions.list was silently ignored — a client asking for a subset
// received everything. The AIP-160 grammar (the secret-manager filtering guide)
// is bounded here to the identity, label and timestamp fields the emulator
// stores; anything outside it is INVALID_ARGUMENT rather than silently ignored.
// A shared package keeps REST and gRPC in parity.
package secretmanager

import (
	"strings"
	"time"

	"jaiscloud/internal/gcp/aip160"
	"jaiscloud/internal/gcp/resource"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
)

// FilterSecrets returns the secrets matching the AIP-160 `filter` expression
// and whether a non-empty filter was applied (Secret Manager reports no total
// count once a filter is set). An empty expression matches every secret.
func FilterSecrets(project, expr string, secrets []secretmanagerstore.Secret) ([]secretmanagerstore.Secret, bool, error) {
	filter, err := aip160.Compile("secretmanager secrets.list", expr, secretFilterFields(project))
	if err != nil {
		return nil, false, err
	}
	return retain(secrets, filter), filterApplied(expr), nil
}

// FilterVersions returns the versions of a secret matching the AIP-160 `filter`
// expression and whether a non-empty filter was applied. An empty expression
// matches every version.
func FilterVersions(project, secretID, expr string, versions []secretmanagerstore.Version) ([]secretmanagerstore.Version, bool, error) {
	filter, err := aip160.Compile("secretmanager versions.list", expr, versionFilterFields(project, secretID))
	if err != nil {
		return nil, false, err
	}
	return retain(versions, filter), filterApplied(expr), nil
}

// filterApplied reports whether a request supplied a non-empty filter (a
// whitespace-only one is treated as absent, as Compile does).
func filterApplied(expr string) bool { return strings.TrimSpace(expr) != "" }

// retain keeps the items the filter matches, preserving their order.
func retain[T any](items []T, filter aip160.Filter[T]) []T {
	kept := make([]T, 0, len(items))
	for _, it := range items {
		if filter.Match(it) {
			kept = append(kept, it)
		}
	}
	return kept
}

// secretFilterFields describes secrets.list's filterable fields: the name, the
// labels map and the creation time (the Secret fields the store carries).
func secretFilterFields(project string) aip160.Fields[secretmanagerstore.Secret] {
	return aip160.Fields[secretmanagerstore.Secret]{
		Desc:   "name, labels.<key>, labels and createTime",
		Name:   func(s secretmanagerstore.Secret) string { return resource.ResourceID(project)("secret", s.ID) },
		Labels: func(s secretmanagerstore.Secret) map[string]string { return s.Labels },
		Times: map[string]func(secretmanagerstore.Secret) time.Time{
			"createtime": func(s secretmanagerstore.Secret) time.Time { return s.CreateTime },
		},
	}
}

// versionFilterFields describes versions.list's filterable fields: the name,
// the state and the create/destroy timestamps (the SecretVersion fields the
// store carries).
func versionFilterFields(project, secretID string) aip160.Fields[secretmanagerstore.Version] {
	secretName := resource.ResourceID(project)("secret", secretID)
	return aip160.Fields[secretmanagerstore.Version]{
		Desc: "name, state, createTime and destroyTime",
		Name: func(v secretmanagerstore.Version) string { return secretName + "/versions/" + v.VersionID },
		Strings: map[string]func(secretmanagerstore.Version) string{
			"state": func(v secretmanagerstore.Version) string { return v.State },
		},
		Times: map[string]func(secretmanagerstore.Version) time.Time{
			"createtime":  func(v secretmanagerstore.Version) time.Time { return v.CreateTime },
			"destroytime": func(v secretmanagerstore.Version) time.Time { return v.DestroyTime },
		},
	}
}
