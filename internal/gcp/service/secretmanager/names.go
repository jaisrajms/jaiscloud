// Resource-name and store-key helpers shared by the Secret Manager REST
// provider and gRPC service.
//
// A secret's store key is its ID for a project-level ("global") secret, and its
// location-qualified resource name for a regional secret:
//
//	global   projects/{p}/secrets/{id}                        -> {id}
//	regional projects/{p}/locations/{l}/secrets/{id}          -> locations/{l}/secrets/{id}
//
// The composite form keeps the two namespaces disjoint (a "/" is not a legal
// secret-ID character) without a store schema or interface change, and it gives
// every response enough information to render the documented resource name.
// Both transports go through these helpers so REST and gRPC cannot disagree
// about which store row a request names.
package secretmanager

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
)

// IsRegionalKey reports whether key is the store key of a regional secret
// ("locations/{location}/secrets/{id}").
func IsRegionalKey(key string) bool {
	return strings.HasPrefix(key, "locations/")
}

// SecretID returns the trailing secret ID of a store key.
func SecretID(key string) string {
	if i := strings.Index(key, "/secrets/"); i >= 0 {
		return key[i+len("/secrets/"):]
	}
	return strings.TrimPrefix(key, "secrets/")
}

// SecretName renders a store key as a full resource name.
func SecretName(project, key string) string {
	return resource.ResourceID(project)("secret", key)
}

// SecretKey returns the store key the named secret refers to. It accepts full
// GCP names and the codec's relative names, with or without a trailing version
// component.
func SecretKey(name string) string {
	key, _ := ParseSecretName(name)
	return key
}

// ParseSecretName splits a full or relative secret/version name into the
// secret's store key and its version component ("" for a secret name, the
// version ID or alias otherwise).
func ParseSecretName(name string) (key, version string) {
	secret, version := splitVersion(relativeSecretName(name))
	if IsRegionalKey(secret) {
		return secret, version
	}
	return strings.TrimPrefix(secret, "secrets/"), version
}

// ParentLocation returns the location of a location-scoped parent
// ("projects/{p}/locations/{l}/…" or "locations/{l}/…"), or "" for a
// project-level parent.
func ParentLocation(parent string) string {
	const marker = "/locations/"
	if i := strings.Index(parent, marker); i >= 0 {
		return firstSegment(parent[i+len(marker):])
	}
	if rest := strings.TrimPrefix(parent, "locations/"); rest != parent {
		return firstSegment(rest)
	}
	return ""
}

// ParentKey builds the store key a create under parent assigns to id.
func ParentKey(parent, id string) string {
	if loc := ParentLocation(parent); loc != "" {
		return "locations/" + loc + "/secrets/" + id
	}
	return id
}

// relativeSecretName strips the project prefix from a full resource name while
// preserving the location segment: "projects/{p}/locations/{l}/secrets/{s}" ->
// "locations/{l}/secrets/{s}" and "projects/{p}/secrets/{s}" -> "secrets/{s}".
func relativeSecretName(name string) string {
	if strings.HasPrefix(name, "locations/") {
		return name
	}
	if i := strings.Index(name, "/secrets/"); i >= 0 {
		if j := strings.LastIndex(name[:i], "/locations/"); j >= 0 {
			return name[j+1:]
		}
		return name[i+1:]
	}
	return name
}

// splitVersion separates a version component from a relative secret name. A
// trailing "/versions" is the versions collection only when it closes
// "secrets/{id}/versions" (or its location-qualified form); a secret literally
// named "versions" is not a collection.
func splitVersion(rel string) (secret, version string) {
	if i := strings.Index(rel, "/versions/"); i >= 0 {
		return rel[:i], rel[i+len("/versions/"):]
	}
	parts := strings.Split(rel, "/")
	if n := len(parts); n >= 3 && parts[n-1] == "versions" && parts[n-3] == "secrets" {
		return strings.Join(parts[:n-1], "/"), ""
	}
	return rel, ""
}

func firstSegment(s string) string {
	if i := strings.IndexByte(s, '/'); i >= 0 {
		return s[:i]
	}
	return s
}
