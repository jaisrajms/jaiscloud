package k8shelpers

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"sync"
)

// Own-identity resolution: the ServiceAccount the emulator *itself* runs as.
//
// A per-resource namespace (a Dataproc cluster's placement namespace, a Managed
// Kafka broker's namespace) is created by the emulator, and the executor
// RoleBinding inside it must grant the emulator's own ServiceAccount so the
// emulator can create the driver Job / broker Pod there. Hardcoding
// DefaultExecutorServiceAccount/DefaultExecutorServiceAccountNamespace made
// that binding correct only for the AWS substrate's `jaiscloud`/`jaiscloud`
// pod: a second emulator running as e.g. `jaiscloud-gcp`/`jaiscloud-gcp` bound
// the AWS identity, which the API server accepts at RoleBinding-create time and
// then rejects with Forbidden at driver-pod-create time.
//
// Precedence (first match wins; the result is resolved once per process):
//
//	ServiceAccount name:      JAISCLOUD_K8S_SA            > the pod's projected
//	                          token `sub` claim           > DefaultExecutorServiceAccount
//	ServiceAccount namespace: JAISCLOUD_K8S_SA_NAMESPACE  > the pod's projected
//	                          .../serviceaccount/namespace > the token `sub`
//	                          claim                       > DefaultExecutorServiceAccountNamespace
//
// Both in-cluster sources are what the kubelet already projects into every pod,
// so an in-cluster emulator needs no extra env var: the namespace file is the
// primary namespace source, and the token's `sub` claim
// (`system:serviceaccount:<namespace>:<name>`) supplies the name the token file
// does not expose. The token is the process's own credential and is used here
// only to name ourselves, so its signature is not verified. An out-of-cluster
// (dev, test, bare binary) process has neither file and falls through to the
// constants, which keeps the standalone-binary path unchanged. A missing,
// unreadable or malformed file is not an error: it is simply the next fallback.
const (
	// EnvOwnServiceAccount overrides the resolved ServiceAccount name.
	EnvOwnServiceAccount = "JAISCLOUD_K8S_SA"
	// EnvOwnServiceAccountNamespace overrides the resolved ServiceAccount
	// namespace (it takes precedence over the pod's projected files).
	EnvOwnServiceAccountNamespace = "JAISCLOUD_K8S_SA_NAMESPACE"

	// ownNamespaceFile is where the kubelet projects the pod's own namespace.
	ownNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	// ownTokenFile is the projected ServiceAccount bearer token; its `sub`
	// claim carries both the namespace and the name.
	ownTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	// serviceAccountSubjectPrefix starts a projected token's `sub` claim.
	serviceAccountSubjectPrefix = "system:serviceaccount:"
)

var (
	ownIdentityOnce      sync.Once
	ownIdentityName      string
	ownIdentityNamespace string
)

// ResolveOwnServiceAccount returns the ServiceAccount name and namespace the
// emulator process itself runs as. The pair is resolved once per process and
// cached: the kubelet-projected files (and the environment) do not change under
// a running pod, so re-reading them per cluster would only add syscalls.
// Resolution never fails and always returns non-empty values.
func ResolveOwnServiceAccount() (name, namespace string) {
	ownIdentityOnce.Do(func() {
		ownIdentityName, ownIdentityNamespace = resolveOwnServiceAccount(os.Getenv, os.ReadFile)
	})
	return ownIdentityName, ownIdentityNamespace
}

// resolveOwnServiceAccount is the pure, injectable resolver behind
// ResolveOwnServiceAccount. getenv and readFile are the only side effects, so
// the precedence is testable without touching the process environment or the
// real token paths.
func resolveOwnServiceAccount(getenv func(string) string, readFile func(string) ([]byte, error)) (string, string) {
	tokenNamespace, tokenName := serviceAccountFromToken(readFile)

	name := strings.TrimSpace(getenv(EnvOwnServiceAccount))
	if name == "" {
		name = tokenName
	}
	if name == "" {
		name = DefaultExecutorServiceAccount
	}

	namespace := strings.TrimSpace(getenv(EnvOwnServiceAccountNamespace))
	if namespace == "" {
		if b, err := readFile(ownNamespaceFile); err == nil {
			namespace = strings.TrimSpace(string(b))
		}
	}
	if namespace == "" {
		namespace = tokenNamespace
	}
	if namespace == "" {
		namespace = DefaultExecutorServiceAccountNamespace
	}
	return name, namespace
}

// serviceAccountFromToken extracts (namespace, name) from the projected
// ServiceAccount token's unverified `sub` claim. Any read, decode or shape error
// yields empty strings so the caller falls through to the next source.
func serviceAccountFromToken(readFile func(string) ([]byte, error)) (namespace, name string) {
	raw, err := readFile(ownTokenFile)
	if err != nil {
		return "", ""
	}
	// A JWT is header.payload.signature; only the payload is needed.
	parts := strings.Split(strings.TrimSpace(string(raw)), ".")
	if len(parts) != 3 {
		return "", ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", ""
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return "", ""
	}
	subject := strings.TrimPrefix(claims.Sub, serviceAccountSubjectPrefix)
	if subject == claims.Sub {
		return "", ""
	}
	// <namespace>:<name>; neither component may contain ':' .
	i := strings.LastIndexByte(subject, ':')
	if i <= 0 || i == len(subject)-1 {
		return "", ""
	}
	return subject[:i], subject[i+1:]
}

// resetOwnServiceAccountCacheForTest discards the cached identity so a test can
// resolve again. Test-only: production resolves once and never resets.
func resetOwnServiceAccountCacheForTest() {
	ownIdentityOnce = sync.Once{}
	ownIdentityName = ""
	ownIdentityNamespace = ""
}
