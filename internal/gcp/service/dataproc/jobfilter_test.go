package dataproc

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
)

// seedListJob creates a job directly in the store so filter/matcher tests can
// pin the state, labels and creation time. StateStartTime is independent of
// CreateTime so a live state does not settle under the advanceJob clock.
func seedListJob(t *testing.T, p *Service, id, cluster, state string, labels map[string]string, createTime, stateStart time.Time) {
	t.Helper()
	j := dpstore.Job{
		ProjectID: "proj", Region: "us-central1",
		JobID: id, PlacementClusterName: cluster,
		Type: "pysparkJob", TypeJob: json.RawMessage(`{"mainPythonFileUri":"gs://b/main.py"}`),
		Labels:     labels,
		Status:     dpstore.JobStatus{State: state, StateStartTime: stateStart},
		CreateTime: createTime,
	}
	require.NoError(t, p.store.CreateJob(context.Background(), "proj", "us-central1", j))
}

// TestParseJobStateMatcher covers the REST jobStateMatcher query parsing.
func TestParseJobStateMatcher(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want JobStateMatcher
	}{
		{"", JobStateMatcherAll},
		{"ALL", JobStateMatcherAll},
		{"ACTIVE", JobStateMatcherActive},
		{"NON_ACTIVE", JobStateMatcherNonActive},
	} {
		got, err := ParseJobStateMatcher(tc.in)
		require.NoError(t, err, tc.in)
		require.Equal(t, tc.want, got, tc.in)
	}
	for _, bad := range []string{"RUNNING", "active", "non_active"} {
		if _, err := ParseJobStateMatcher(bad); err == nil {
			t.Errorf("ParseJobStateMatcher(%q) succeeded, want InvalidArgument", bad)
		}
	}
}

// TestCompileJobFilter_EmptyMatchesAll verifies an empty filter is a no-op.
func TestCompileJobFilter_EmptyMatchesAll(t *testing.T) {
	f, err := compileJobFilter("")
	require.NoError(t, err)
	require.True(t, f.match(dpstore.Job{Status: dpstore.JobStatus{State: jobStateDone}}))
}

// TestCompileJobFilter_Errors verifies the bounded grammar fails loud on
// anything it does not support rather than silently matching everything.
func TestCompileJobFilter_Errors(t *testing.T) {
	for _, filter := range []string{
		`bogus = "x"`,
		`status.state = RUNNING`,
		`status.state != ACTIVE`,
		`labels.env > "x"`,
		`labels. = "x"`,
		`insertTime = 2025-01-01T00:00:00Z`,
		`insertTime = "not-a-time"`,
		`status.state = ACTIVE AND`,
		`status.state = "ACTIVE"`,
		`status.state ! ACTIVE`,
		`labels.env == x`,
		`labels.env = staging bogus`,
		`= ACTIVE`,
		`status.state =`,
		`labels.env = "unterminated`,
	} {
		if _, err := compileJobFilter(filter); err == nil {
			t.Errorf("compileJobFilter(%q) succeeded, want error", filter)
		}
	}
}

// TestJobFilter_Match pins each supported clause and the explicit/implicit AND
// joining against a fixed job set.
func TestJobFilter_Match(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	jobs := map[string]dpstore.Job{
		"pending":   {Status: dpstore.JobStatus{State: jobStatePending}, Labels: map[string]string{"env": "staging", "starred": "yes"}, CreateTime: t0},
		"running":   {Status: dpstore.JobStatus{State: jobStateRunning}, Labels: map[string]string{"env": "prod"}, CreateTime: t0.Add(time.Hour)},
		"done":      {Status: dpstore.JobStatus{State: jobStateDone}, Labels: map[string]string{"env": "", "starred": "no"}, CreateTime: t0.Add(2 * time.Hour)},
		"no-labels": {Status: dpstore.JobStatus{State: jobStateError}, CreateTime: t0.Add(3 * time.Hour)},
	}
	cases := []struct {
		filter string
		want   []string
	}{
		{`status.state = ACTIVE`, []string{"pending", "running"}},
		{`status.state = NON_ACTIVE`, []string{"done", "no-labels"}},
		{`labels.env = "staging"`, []string{"pending"}},
		{`labels.env = staging`, []string{"pending"}},
		{`labels.env = ""`, []string{"done"}},
		{`labels.starred = *`, []string{"pending", "done"}},
		{`labels.env = Staging`, nil},
		{`insertTime < "2025-01-01T01:00:00Z"`, []string{"pending"}},
		{`insertTime <= "2025-01-01T01:00:00Z"`, []string{"pending", "running"}},
		{`insertTime > "2025-01-01T01:00:00Z"`, []string{"done", "no-labels"}},
		{`insertTime >= "2025-01-01T01:00:00Z"`, []string{"running", "done", "no-labels"}},
		{`insertTime != "2025-01-01T00:00:00Z"`, []string{"running", "done", "no-labels"}},
		{`insertTime = "2025-01-01T00:00:00Z"`, []string{"pending"}},
		{`status.state = ACTIVE AND labels.env = staging`, []string{"pending"}},
		{`status.state = ACTIVE labels.env = prod`, []string{"running"}}, // implicit AND
	}
	for _, tc := range cases {
		f, err := compileJobFilter(tc.filter)
		require.NoError(t, err, tc.filter)
		var got []string
		for id, j := range jobs {
			if f.match(j) {
				got = append(got, id)
			}
		}
		sort.Strings(got)
		want := append([]string(nil), tc.want...)
		sort.Strings(want)
		require.Equal(t, want, got, tc.filter)
	}
}

// TestListJobs_FilterAndMatcher exercises the whole ListJobs surface over a
// seeded store: clusterName, filter, jobStateMatcher, and filter overriding the
// matcher.
func TestListJobs_FilterAndMatcher(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	// A one-hour delay keeps the PENDING/RUNNING jobs from settling on read.
	p := newJobStateProvider(t, time.Hour)

	seedListJob(t, p, "j1", "c1", jobStatePending, map[string]string{"env": "staging"}, t0, t0)
	seedListJob(t, p, "j2", "c1", jobStateRunning, map[string]string{"env": "prod"}, t0, t0)
	seedListJob(t, p, "j3", "c2", jobStateDone, map[string]string{"env": "staging"}, t0, t0)
	seedListJob(t, p, "j4", "c2", jobStateError, nil, t0, t0)

	ctx := context.Background()
	list := func(cluster, filter string, m JobStateMatcher) []string {
		page, _, err := p.ListJobs(ctx, "proj", "us-central1", cluster, filter, m, 0, "")
		require.NoError(t, err)
		ids := make([]string, 0, len(page))
		for _, j := range page {
			ids = append(ids, j.JobID)
		}
		sort.Strings(ids)
		return ids
	}

	require.Equal(t, []string{"j1", "j2", "j3", "j4"}, list("", "", JobStateMatcherAll))
	require.Equal(t, []string{"j1", "j2"}, list("c1", "", JobStateMatcherAll))
	require.Equal(t, []string{"j1", "j2"}, list("", "", JobStateMatcherActive))
	require.Equal(t, []string{"j3", "j4"}, list("", "", JobStateMatcherNonActive))
	require.Equal(t, []string{"j1", "j3"}, list("", `labels.env = staging`, JobStateMatcherAll))
	// filter wins over jobStateMatcher (ACTIVE would otherwise drop j3)
	require.Equal(t, []string{"j3"}, list("", `status.state = NON_ACTIVE AND labels.env = staging`, JobStateMatcherActive))
	require.Empty(t, list("", `labels.env = Staging`, JobStateMatcherAll))
}

// TestListJobs_PaginationAfterFilter verifies filtering is applied before
// pagination so the cursor only walks matching jobs, in id order.
func TestListJobs_PaginationAfterFilter(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	p := newJobStateProvider(t, time.Hour)

	for _, id := range []string{"j1", "j2", "j3"} {
		seedListJob(t, p, id, "c1", jobStateDone, map[string]string{"group": "a"}, t0, t0)
	}
	seedListJob(t, p, "j9", "c1", jobStateDone, map[string]string{"group": "b"}, t0, t0)

	ctx := context.Background()
	idsOf := func(page []dpstore.Job) []string {
		out := make([]string, 0, len(page))
		for _, j := range page {
			out = append(out, j.JobID)
		}
		return out
	}

	page1, tok, err := p.ListJobs(ctx, "proj", "us-central1", "", `labels.group = a`, JobStateMatcherAll, 2, "")
	require.NoError(t, err)
	require.Equal(t, []string{"j1", "j2"}, idsOf(page1))
	require.NotEmpty(t, tok)

	page2, tok2, err := p.ListJobs(ctx, "proj", "us-central1", "", `labels.group = a`, JobStateMatcherAll, 2, tok)
	require.NoError(t, err)
	require.Equal(t, []string{"j3"}, idsOf(page2))
	require.Empty(t, tok2)
}

// TestListJobs_MalformedFilterInvalidArgument verifies a bad filter surfaces as
// 400 InvalidArgument from the core.
func TestListJobs_MalformedFilterInvalidArgument(t *testing.T) {
	freezeClock(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	p := newJobStateProvider(t, time.Hour)

	_, _, err := p.ListJobs(context.Background(), "proj", "us-central1", "", "bogus = 1", JobStateMatcherAll, 0, "")
	pe, ok := err.(*model.ProviderError)
	require.True(t, ok, "want *model.ProviderError, got %T", err)
	require.Equal(t, 400, pe.HTTPStatus)
	require.Equal(t, "InvalidArgument", pe.Code)
}
