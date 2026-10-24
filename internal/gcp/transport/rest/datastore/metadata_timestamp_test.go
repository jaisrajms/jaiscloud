package datastore

import (
	"testing"
	"time"

	"jaiscloud/internal/clock"
)

// TestRESTMetadataTimestampSpelling pins TD45: every response-level Datastore
// timestamp — readTime, commitTime, createTime, updateTime — is a proto
// Timestamp in the Discovery (`google-datetime`), so real GCP renders it with
// the one protojson rule TD42 applied to a stored timestampValue: UTC, rounded
// down to Datastore's microsecond resolution, and spelled with a 0/3/6-digit
// fraction (never time.RFC3339Nano's arbitrary trimming, e.g. ".5" for ".500",
// and never the sub-microsecond digits clock.Now() carries).
func TestRESTMetadataTimestampSpelling(t *testing.T) {
	cases := []struct {
		name   string
		frozen time.Time
		want   string
	}{
		// 987 sub-microsecond nanos: RFC3339Nano would print
		// ".500000987"; protojson truncates to the ".500" group.
		{"sub-microsecond dropped, one group kept", time.Date(2026, 1, 1, 12, 0, 0, 500000987, time.UTC), "2026-01-01T12:00:00.500Z"},
		// 789 sub-microsecond nanos below a full six-digit group: the fraction
		// is kept at six digits, not padded or trimmed to RFC3339Nano's ".123456".
		{"microsecond fraction", time.Date(2026, 1, 1, 12, 0, 0, 123456789, time.UTC), "2026-01-01T12:00:00.123456Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, p := newTestProvider(t)
			clock.SetGlobalClock(clock.FixedClock{T: tc.frozen})
			defer clock.SetGlobalClock(clock.RealClock{})

			// CommitResponse.commitTime (transactional) + MutationResult
			// createTime/updateTime.
			begin, err := call(t, c, p, "test", "beginTransaction", map[string]any{})
			if err != nil {
				t.Fatalf("beginTransaction: %v", err)
			}
			txn, _ := begin.Data["transaction"].(string)
			commit, err := call(t, c, p, "test", "commit", map[string]any{
				"mode":        "TRANSACTIONAL",
				"transaction": txn,
				"mutations": []any{map[string]any{"upsert": map[string]any{
					"key":        nameKey("Task", "a"),
					"properties": map[string]any{"n": map[string]any{"integerValue": "1"}},
				}}},
			})
			if err != nil {
				t.Fatalf("commit: %v", err)
			}
			assertTimeField(t, commit.Data, "commitTime", tc.want)
			mr := firstMap(t, commit.Data, "mutationResults")
			assertTimeField(t, mr, "createTime", tc.want)
			assertTimeField(t, mr, "updateTime", tc.want)

			// LookupResponse.readTime + EntityResult createTime/updateTime.
			lookup, err := call(t, c, p, "test", "lookup", map[string]any{"keys": []any{nameKey("Task", "a")}})
			if err != nil {
				t.Fatalf("lookup: %v", err)
			}
			assertTimeField(t, lookup.Data, "readTime", tc.want)
			found := firstMap(t, lookup.Data, "found")
			assertTimeField(t, found, "createTime", tc.want)
			assertTimeField(t, found, "updateTime", tc.want)

			// QueryResultBatch.readTime + AggregationResultBatch.readTime.
			query, err := call(t, c, p, "test", "runQuery", map[string]any{
				"query": map[string]any{"kind": []any{map[string]any{"name": "Task"}}},
			})
			if err != nil {
				t.Fatalf("runQuery: %v", err)
			}
			qbatch := mapField(t, query.Data, "batch")
			assertTimeField(t, qbatch, "readTime", tc.want)
			er := firstMap(t, qbatch, "entityResults")
			assertTimeField(t, er, "createTime", tc.want)
			assertTimeField(t, er, "updateTime", tc.want)

			agg, err := call(t, c, p, "test", "runAggregationQuery", map[string]any{
				"aggregationQuery": map[string]any{
					"nestedQuery": map[string]any{"kind": []any{map[string]any{"name": "Task"}}},
					"aggregations": []any{map[string]any{
						"alias": "count",
						"count": map[string]any{},
					}},
				},
			})
			if err != nil {
				t.Fatalf("runAggregationQuery: %v", err)
			}
			assertTimeField(t, mapField(t, agg.Data, "batch"), "readTime", tc.want)
		})
	}
}

// mapField returns data[key] as an object.
func mapField(t *testing.T, data map[string]any, key string) map[string]any {
	t.Helper()
	m, _ := data[key].(map[string]any)
	if m == nil {
		t.Fatalf("%s is not an object: %v", key, data[key])
	}
	return m
}

// firstMap returns the first element of a data[key] array as a map.
func firstMap(t *testing.T, data map[string]any, key string) map[string]any {
	t.Helper()
	arr, _ := data[key].([]any)
	if len(arr) == 0 {
		t.Fatalf("%s is empty: %v", key, data)
	}
	m, _ := arr[0].(map[string]any)
	if m == nil {
		t.Fatalf("%s[0] is not an object: %v", key, arr[0])
	}
	return m
}

// assertTimeField requires data[key] to equal want exactly (so a spelling real
// GCP would not emit fails, not merely an unparseable value).
func assertTimeField(t *testing.T, data map[string]any, key, want string) {
	t.Helper()
	got, _ := data[key].(string)
	if got != want {
		t.Errorf("%s = %q, want %q", key, got, want)
	}
}
