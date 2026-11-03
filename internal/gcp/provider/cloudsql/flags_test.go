package cloudsql

import (
	"context"
	"strings"
	"testing"

	"jaiscloud/internal/model"
)

// flagNames returns the names of the flags in a flags.list response, in the
// order the emulator served them.
func flagNames(t *testing.T, nr *model.NormalizedRequest) ([]string, *model.ProviderResponse) {
	t.Helper()
	resp, err := newProvider().ListFlags(context.Background(), nr)
	if err != nil {
		t.Fatalf("flags.list %v: %v", nr.Params, err)
	}
	items, _ := resp.Data["items"].([]any)
	names := make([]string, 0, len(items))
	for _, item := range items {
		m, _ := item.(map[string]any)
		name, _ := m["name"].(string)
		names = append(names, name)
	}
	return names, resp
}

func flagPage(t *testing.T, params map[string]any) ([]string, *model.ProviderResponse) {
	t.Helper()
	names, resp := flagNames(t, newNR(params))
	if resp.Data["kind"] != kindFlagsList {
		t.Errorf("kind = %v, want %s", resp.Data["kind"], kindFlagsList)
	}
	return names, resp
}

func hasFlag(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

// TestListFlagsDatabaseVersionFilter covers NF16/J56: flags.list honours the
// documented databaseVersion parameter, so `gcloud sql flags list
// --database-version=POSTGRES_15` no longer returns MySQL flags. Real Cloud SQL
// answers 200 with only that engine's flags (probe 2026-11-03).
func TestListFlagsDatabaseVersionFilter(t *testing.T) {
	cases := []struct {
		version string
		want    []string
		mustNot []string
	}{
		{
			version: "POSTGRES_15",
			want:    []string{"max_connections", "cloudsql.iam_authentication", "log_min_duration_statement"},
			mustNot: []string{"character_set_server", "collation_server", "default_time_zone", "log_output", "slow_query_log"},
		},
		{
			version: "MYSQL_8_0",
			want:    []string{"character_set_server", "collation_server", "default_time_zone", "log_output", "max_connections", "slow_query_log"},
			mustNot: []string{"cloudsql.iam_authentication", "log_min_duration_statement"},
		},
		{
			version: "MYSQL_5_7",
			want:    []string{"character_set_server", "collation_server"},
			mustNot: []string{"default_time_zone", "log_output", "slow_query_log", "cloudsql.iam_authentication"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.version, func(t *testing.T) {
			names, _ := flagPage(t, map[string]any{"databaseVersion": tc.version})
			if len(names) == 0 {
				t.Fatalf("%s returned no flags", tc.version)
			}
			for _, want := range tc.want {
				if !hasFlag(names, want) {
					t.Errorf("%s dropped %q: %v", tc.version, want, names)
				}
			}
			for _, unwanted := range tc.mustNot {
				if hasFlag(names, unwanted) {
					t.Errorf("%s returned %q, which does not apply to it: %v", tc.version, unwanted, names)
				}
			}
		})
	}

	// A valid version the catalogue has no flags for is an empty 200, not an
	// error and not the full catalogue. The static catalogue covers the engines
	// it advertises (MySQL 8.0/5.7 and PostgreSQL 15-18), so an unadvertised
	// version — another engine, or an older major — simply has no entries.
	for _, version := range []string{"SQLSERVER_2017_STANDARD", "POSTGRES_9_6", "MYSQL_5_6"} {
		if names, _ := flagPage(t, map[string]any{"databaseVersion": version}); len(names) != 0 {
			t.Errorf("%s returned %v, want an empty list", version, names)
		}
	}
}

// TestListFlagsScopeFilter covers the flagScope parameter: the unspecified (and
// explicit database) scope is the database catalogue, the connection-pool scope
// is its own short catalogue tagged with that scope, and both filters combine.
func TestListFlagsScopeFilter(t *testing.T) {
	all, _ := flagPage(t, nil)

	for _, scope := range []string{"", sqlFlagScopeUnspecified, sqlFlagScopeDatabase} {
		params := map[string]any{}
		if scope != "" {
			params["flagScope"] = scope
		}
		names, resp := flagPage(t, params)
		if len(names) != len(sqlDatabaseFlags) {
			t.Errorf("scope %q returned %d flags, want the %d database flags", scope, len(names), len(sqlDatabaseFlags))
		}
		for _, item := range resp.Data["items"].([]any) {
			if m, _ := item.(map[string]any); m["flagScope"] != nil {
				t.Errorf("scope %q: database flag %v carries flagScope; real Cloud SQL omits it", scope, m["name"])
			}
		}
		if hasFlag(names, "pool_mode") {
			t.Errorf("scope %q returned a connection-pool flag", scope)
		}
	}

	pool, resp := flagPage(t, map[string]any{"flagScope": sqlFlagScopeConnectionPool})
	if len(pool) != len(sqlConnectionPoolFlags) {
		t.Fatalf("connection-pool scope returned %d flags, want %d: %v", len(pool), len(sqlConnectionPoolFlags), pool)
	}
	for _, item := range resp.Data["items"].([]any) {
		m, _ := item.(map[string]any)
		if m["flagScope"] != sqlFlagScopeConnectionPool {
			t.Errorf("connection-pool flag %v flagScope = %v, want %s", m["name"], m["flagScope"], sqlFlagScopeConnectionPool)
		}
		if m["kind"] != "sql#flag" {
			t.Errorf("connection-pool flag %v kind = %v, want sql#flag", m["name"], m["kind"])
		}
	}
	if hasFlag(pool, "max_connections") || hasFlag(pool, "cloudsql.iam_authentication") {
		t.Errorf("connection-pool scope leaked database flags: %v", pool)
	}

	// The two documented filters combine.
	combined, _ := flagPage(t, map[string]any{
		"databaseVersion": "POSTGRES_15",
		"flagScope":       sqlFlagScopeConnectionPool,
	})
	if len(combined) != len(sqlConnectionPoolFlags) {
		t.Errorf("POSTGRES_15 + connection-pool scope = %v, want all %d pool flags", combined, len(sqlConnectionPoolFlags))
	}
	if got, _ := flagPage(t, map[string]any{
		"databaseVersion": "MYSQL_8_0",
		"flagScope":       sqlFlagScopeConnectionPool,
	}); len(got) != 0 {
		t.Errorf("MYSQL_8_0 + connection-pool scope = %v, want none (the pool catalogue is PostgreSQL-only)", got)
	}

	// The database catalogue is unchanged by any of these requests.
	if names, _ := flagPage(t, nil); len(names) != len(all) {
		t.Errorf("unfiltered list = %d flags, want %d (the catalogue must not be filtered in place)", len(names), len(all))
	}
}

// TestListFlagsInvalidParameters pins the documented negatives: an unknown
// databaseVersion is INVALID_ARGUMENT (real Cloud SQL: "Invalid request:
// Unsupported database version: …") rather than a widened list, and an
// explicitly empty or unknown flagScope is INVALID_ARGUMENT too.
func TestListFlagsInvalidParameters(t *testing.T) {
	for _, version := range []string{"POSTGRES_99", "postgres_15", "SQL_DATABASE_VERSION_UNSPECIFIED", "MYSQL_8_0_99"} {
		_, err := newProvider().ListFlags(context.Background(), newNR(map[string]any{"databaseVersion": version}))
		if err == nil {
			t.Errorf("databaseVersion=%q succeeded, want 400", version)
			continue
		}
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.HTTPStatus != 400 {
			t.Errorf("databaseVersion=%q = %v, want 400 INVALID_ARGUMENT", version, err)
			continue
		}
		if !strings.Contains(perr.Message, "Unsupported database version: "+version) {
			t.Errorf("databaseVersion=%q message = %q, want real Cloud SQL's 'Unsupported database version' wording", version, perr.Message)
		}
	}

	for _, scope := range []string{"", "BOGUS", "SQL_FLAG_SCOPE_CONNECTION_POOLS"} {
		_, err := newProvider().ListFlags(context.Background(), newNR(map[string]any{"flagScope": scope}))
		if err == nil {
			t.Errorf("flagScope=%q succeeded, want 400", scope)
			continue
		}
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.HTTPStatus != 400 {
			t.Errorf("flagScope=%q = %v, want 400 INVALID_ARGUMENT", scope, err)
			continue
		}
		if !strings.Contains(perr.Message, "flag_scope") {
			t.Errorf("flagScope=%q message = %q, want the SqlFlagScope binding error", scope, perr.Message)
		}
	}
}

// TestListFlagsPaginationUntouched proves pagination runs over the filtered
// list: a version-filtered request pages the filtered entries only, and its
// cursors are stable across the pages.
func TestListFlagsPaginationUntouched(t *testing.T) {
	filtered, _ := flagPage(t, map[string]any{"databaseVersion": "MYSQL_8_0"})
	if len(filtered) < 3 {
		t.Fatalf("MYSQL_8_0 returned %d flags, need at least 3 to page", len(filtered))
	}

	var paged []string
	params := map[string]any{"databaseVersion": "MYSQL_8_0", "maxResults": "2"}
	for page := 0; page < 10; page++ {
		names, resp := flagPage(t, params)
		paged = append(paged, names...)
		next, _ := resp.Data["nextPageToken"].(string)
		if next == "" {
			break
		}
		params["pageToken"] = next
	}
	if len(paged) != len(filtered) {
		t.Fatalf("paged %d flags over the filtered list, want %d (%v vs %v)", len(paged), len(filtered), paged, filtered)
	}
	for i := range paged {
		if paged[i] != filtered[i] {
			t.Errorf("page %d = %q, want %q", i, paged[i], filtered[i])
		}
	}

	// The unfiltered list still pages too, and a filtered page never carries
	// another engine's flag.
	params = map[string]any{"databaseVersion": "POSTGRES_15", "maxResults": "1"}
	names, resp := flagPage(t, params)
	if len(names) != 1 {
		t.Fatalf("maxResults=1 returned %d flags", len(names))
	}
	if hasFlag(names, "character_set_server") {
		t.Errorf("a PostgreSQL page carried a MySQL flag: %v", names)
	}
	if _, ok := resp.Data["nextPageToken"]; !ok {
		t.Errorf("maxResults=1 over %d PostgreSQL flags must return a nextPageToken", len(filtered))
	}
}
