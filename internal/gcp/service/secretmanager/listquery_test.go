package secretmanager

import (
	"testing"
	"time"

	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
)

func secretIDs(secrets []secretmanagerstore.Secret) []string {
	out := make([]string, 0, len(secrets))
	for _, s := range secrets {
		out = append(out, s.ID)
	}
	return out
}

func versionIDs(versions []secretmanagerstore.Version) []string {
	out := make([]string, 0, len(versions))
	for _, v := range versions {
		out = append(out, v.VersionID)
	}
	return out
}

func TestFilterSecrets(t *testing.T) {
	day := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	secrets := []secretmanagerstore.Secret{
		{ID: "db-password", Labels: map[string]string{"team": "alpha"}, CreateTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{ID: "api-key", Labels: map[string]string{"team": "beta"}, CreateTime: day},
	}
	cases := []struct {
		expr string
		want []string
	}{
		{``, []string{"db-password", "api-key"}},
		{`name:"password"`, []string{"db-password"}},
		{`name="projects/proj/secrets/api-key"`, []string{"api-key"}},
		{`labels.team=beta`, []string{"api-key"}},
		{`labels.team:*`, []string{"db-password", "api-key"}},
		{`createTime>"2026-02-01"`, []string{"api-key"}},
		{`name:"password" OR name:"key"`, nil}, // OR is rejected below, checked in TestReject
	}
	for _, tc := range cases {
		if tc.want == nil {
			continue
		}
		got, _, err := FilterSecrets("proj", tc.expr, secrets)
		if err != nil {
			t.Fatalf("FilterSecrets(%q): %v", tc.expr, err)
		}
		if ids := secretIDs(got); len(ids) != len(tc.want) {
			t.Fatalf("FilterSecrets(%q) = %v, want %v", tc.expr, ids, tc.want)
		} else {
			for i := range ids {
				if ids[i] != tc.want[i] {
					t.Fatalf("FilterSecrets(%q) = %v, want %v", tc.expr, ids, tc.want)
				}
			}
		}
	}
}

func TestFilterVersions(t *testing.T) {
	versions := []secretmanagerstore.Version{
		{VersionID: "1", State: "ENABLED"},
		{VersionID: "2", State: "DISABLED"},
		{VersionID: "3", State: "DESTROYED", DestroyTime: time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC)},
	}
	cases := []struct {
		expr string
		want []string
	}{
		{``, []string{"1", "2", "3"}},
		{`state:ENABLED`, []string{"1"}},
		{`state:DISABLED`, []string{"2"}},
		{`state:DESTROYED`, []string{"3"}},
		{`name:"versions/3"`, []string{"3"}},
		{`destroyTime:*`, []string{"3"}},
	}
	for _, tc := range cases {
		got, _, err := FilterVersions("proj", "my-secret", tc.expr, versions)
		if err != nil {
			t.Fatalf("FilterVersions(%q): %v", tc.expr, err)
		}
		ids := versionIDs(got)
		if len(ids) != len(tc.want) {
			t.Fatalf("FilterVersions(%q) = %v, want %v", tc.expr, ids, tc.want)
		}
		for i := range ids {
			if ids[i] != tc.want[i] {
				t.Fatalf("FilterVersions(%q) = %v, want %v", tc.expr, ids, tc.want)
			}
		}
	}
}

func TestRejectUnsupportedSecretsFilter(t *testing.T) {
	for _, expr := range []string{
		`name:"a" OR name:"b"`, // OR
		`NOT name:"a"`,         // NOT
		`replication.automatic:*`,
		`expire_time:*`,
		`bogus=1`,
	} {
		_, _, err := FilterSecrets("proj", expr, nil)
		if err == nil {
			t.Errorf("FilterSecrets(%q) succeeded, want InvalidArgument", expr)
			continue
		}
		if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "InvalidArgument" {
			t.Errorf("FilterSecrets(%q) error = %#v, want InvalidArgument", expr, err)
		}
	}
}
