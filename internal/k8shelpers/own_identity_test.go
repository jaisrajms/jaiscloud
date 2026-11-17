package k8shelpers

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
)

// tokenFor builds a syntactically valid (unsigned) projected-token JWT whose
// `sub` claim names the given ServiceAccount.
func tokenFor(namespace, name string) []byte {
	payload, _ := json.Marshal(map[string]string{
		"sub": serviceAccountSubjectPrefix + namespace + ":" + name,
	})
	enc := base64.RawURLEncoding.EncodeToString
	return []byte("eyJhbGciOiJSUzI1NiJ9." + enc(payload) + ".c2ln")
}

// testFiles maps a projected-token path to its contents; a nil value means the
// file does not exist.
type testFiles map[string][]byte

func (f testFiles) reader(t *testing.T) func(string) ([]byte, error) {
	t.Helper()
	return func(path string) ([]byte, error) {
		if b, ok := f[path]; ok && b != nil {
			return b, nil
		}
		return nil, errors.New("no such file")
	}
}

// TestResolveOwnServiceAccountPrecedence pins the resolution contract:
// env > kubelet-projected file > token `sub` claim > exported constants, per
// field, with a missing or unreadable file falling through silently.
func TestResolveOwnServiceAccountPrecedence(t *testing.T) {
	cases := []struct {
		name     string
		env      map[string]string
		files    testFiles
		wantName string
		wantNS   string
	}{
		{
			name:     "nothing set falls back to the constants",
			wantName: DefaultExecutorServiceAccount,
			wantNS:   DefaultExecutorServiceAccountNamespace,
		},
		{
			name: "only the name is overridden",
			env: map[string]string{
				EnvOwnServiceAccount: "jaiscloud-gcp",
			},
			wantName: "jaiscloud-gcp",
			wantNS:   DefaultExecutorServiceAccountNamespace,
		},
		{
			name: "only the namespace is overridden",
			env: map[string]string{
				EnvOwnServiceAccountNamespace: "jaiscloud-gcp",
			},
			wantName: DefaultExecutorServiceAccount,
			wantNS:   "jaiscloud-gcp",
		},
		{
			name: "both overridden by env",
			env: map[string]string{
				EnvOwnServiceAccount:          "emulator-gcp",
				EnvOwnServiceAccountNamespace: "gcp-substrate",
			},
			wantName: "emulator-gcp",
			wantNS:   "gcp-substrate",
		},
		{
			name:     "namespace file supplies the namespace when env does not",
			files:    testFiles{ownNamespaceFile: []byte("jaiscloud-gcp")},
			wantName: DefaultExecutorServiceAccount,
			wantNS:   "jaiscloud-gcp",
		},
		{
			name:     "namespace file whitespace is trimmed",
			files:    testFiles{ownNamespaceFile: []byte("  jaiscloud-gcp\n\n")},
			wantName: DefaultExecutorServiceAccount,
			wantNS:   "jaiscloud-gcp",
		},
		{
			name:     "whitespace-only namespace file falls through to the constant",
			files:    testFiles{ownNamespaceFile: []byte("\n\t \n")},
			wantName: DefaultExecutorServiceAccount,
			wantNS:   DefaultExecutorServiceAccountNamespace,
		},
		{
			name: "env namespace beats the namespace file",
			env: map[string]string{
				EnvOwnServiceAccountNamespace: "from-env",
			},
			files:    testFiles{ownNamespaceFile: []byte("from-file")},
			wantName: DefaultExecutorServiceAccount,
			wantNS:   "from-env",
		},
		{
			name: "env name combines with the namespace file",
			env: map[string]string{
				EnvOwnServiceAccount: "emulator-gcp",
			},
			files:    testFiles{ownNamespaceFile: []byte("jaiscloud-gcp\n")},
			wantName: "emulator-gcp",
			wantNS:   "jaiscloud-gcp",
		},
		{
			name:     "unreadable files (out of cluster) fall through",
			files:    testFiles{},
			wantName: DefaultExecutorServiceAccount,
			wantNS:   DefaultExecutorServiceAccountNamespace,
		},
		{
			name: "blank env values are ignored",
			env: map[string]string{
				EnvOwnServiceAccount:          "  ",
				EnvOwnServiceAccountNamespace: "\t",
			},
			wantName: DefaultExecutorServiceAccount,
			wantNS:   DefaultExecutorServiceAccountNamespace,
		},
		{
			// The token is the only in-cluster source of the *name*: a pod that
			// runs as jaiscloud-gcp must not fall back to the AWS constant.
			name:     "token sub claim supplies both name and namespace",
			files:    testFiles{ownTokenFile: tokenFor("jaiscloud-gcp", "jaiscloud-gcp")},
			wantName: "jaiscloud-gcp",
			wantNS:   "jaiscloud-gcp",
		},
		{
			name:     "namespace file beats the token sub claim",
			files:    testFiles{ownNamespaceFile: []byte("from-file"), ownTokenFile: tokenFor("from-token", "sa-name")},
			wantName: "sa-name",
			wantNS:   "from-file",
		},
		{
			name:     "env name beats the token sub claim name",
			env:      map[string]string{EnvOwnServiceAccount: "from-env"},
			files:    testFiles{ownTokenFile: tokenFor("jaiscloud-gcp", "sa-name")},
			wantName: "from-env",
			wantNS:   "jaiscloud-gcp",
		},
		{
			name:     "a non-JWT token file is ignored",
			files:    testFiles{ownTokenFile: []byte("not-a-jwt")},
			wantName: DefaultExecutorServiceAccount,
			wantNS:   DefaultExecutorServiceAccountNamespace,
		},
		{
			name:     "a JWT with a non-serviceaccount subject is ignored",
			files:    testFiles{ownTokenFile: []byte("e30." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"system:node:n1"}`)) + ".s")},
			wantName: DefaultExecutorServiceAccount,
			wantNS:   DefaultExecutorServiceAccountNamespace,
		},
		{
			name:     "a JWT with a malformed subject is ignored",
			files:    testFiles{ownTokenFile: []byte("e30." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"system:serviceaccount:nons"}`)) + ".s")},
			wantName: DefaultExecutorServiceAccount,
			wantNS:   DefaultExecutorServiceAccountNamespace,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			getenv := func(k string) string { return tc.env[k] }
			files := tc.files
			if files == nil {
				files = testFiles{}
			}
			gotName, gotNS := resolveOwnServiceAccount(getenv, files.reader(t))
			if gotName != tc.wantName {
				t.Fatalf("name = %q, want %q", gotName, tc.wantName)
			}
			if gotNS != tc.wantNS {
				t.Fatalf("namespace = %q, want %q", gotNS, tc.wantNS)
			}
		})
	}
}

// TestResolveOwnServiceAccountCachedOnce verifies the process-wide cache: the
// first call resolves (here from the environment) and a later environment
// change is not observed, so per-cluster provisioning does no repeated reads.
func TestResolveOwnServiceAccountCachedOnce(t *testing.T) {
	t.Setenv(EnvOwnServiceAccount, "jaiscloud-gcp")
	t.Setenv(EnvOwnServiceAccountNamespace, "jaiscloud-gcp")
	resetOwnServiceAccountCacheForTest()
	t.Cleanup(resetOwnServiceAccountCacheForTest)

	name, ns := ResolveOwnServiceAccount()
	if name != "jaiscloud-gcp" || ns != "jaiscloud-gcp" {
		t.Fatalf("ResolveOwnServiceAccount() = (%q, %q), want (jaiscloud-gcp, jaiscloud-gcp)", name, ns)
	}

	t.Setenv(EnvOwnServiceAccount, "changed-later")
	t.Setenv(EnvOwnServiceAccountNamespace, "changed-later")
	name, ns = ResolveOwnServiceAccount()
	if name != "jaiscloud-gcp" || ns != "jaiscloud-gcp" {
		t.Fatalf("cache did not hold: (%q, %q)", name, ns)
	}
}

// TestResolveOwnServiceAccountOutOfClusterDefaults documents the bare-binary
// path: with no env and no projected files, the exported constants apply.
func TestResolveOwnServiceAccountOutOfClusterDefaults(t *testing.T) {
	resetOwnServiceAccountCacheForTest()
	t.Cleanup(resetOwnServiceAccountCacheForTest)

	getenv := func(string) string { return "" }
	readFile := func(string) ([]byte, error) { return nil, errors.New("no such file") }
	name, ns := resolveOwnServiceAccount(getenv, readFile)
	if name != "jaiscloud" || ns != "jaiscloud" {
		t.Fatalf("out-of-cluster identity = (%q, %q), want (jaiscloud, jaiscloud)", name, ns)
	}
}
