package dataproc

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
)

// seedListCluster creates a cluster directly in the store so filter/paging tests
// can pin the state and labels. StateStartTime is far enough in the past only
// for settled states; transitional states are pinned with a long ready delay so
// they do not settle under the advanceCluster clock.
func seedListCluster(t *testing.T, p *Service, name, state string, labels map[string]string) {
	t.Helper()
	now := clock.Now().UTC()
	c := dpstore.Cluster{
		ProjectID: "proj", Region: "us-central1", Name: name,
		Status: dpstore.ClusterStatus{State: state, StateStartTime: now},
		Labels: labels,
	}
	require.NoError(t, p.store.CreateCluster(context.Background(), "proj", "us-central1", c))
}

// TestCompileClusterFilter_EmptyMatchesAll verifies an empty filter is a no-op.
func TestCompileClusterFilter_EmptyMatchesAll(t *testing.T) {
	f, err := compileClusterFilter("")
	require.NoError(t, err)
	require.True(t, f.match(dpstore.Cluster{Status: dpstore.ClusterStatus{State: "RUNNING"}}))
}

// TestCompileClusterFilter_Errors verifies the bounded grammar fails loud on
// anything it does not support rather than silently matching everything.
func TestCompileClusterFilter_Errors(t *testing.T) {
	for _, filter := range []string{
		`bogus = "x"`,
		`status.state = STARTING`,   // a real state, but not documented as filterable
		`status.state = RUNNINGX`,   // not a state at all
		`status.state != ACTIVE`,    // only = is supported
		`clusterName > a`,           // only = is supported
		`clusterName = ""`,          // an empty name is invalid, not "any"
		`labels. = "x"`,             // empty label key
		`labels.env != staging`,     // only = is supported
		`status.state = ACTIVE AND`, // dangling AND
		`status.state = "ACTIVE"`,   // quoted enum is not an identifier
		`= ACTIVE`,
		`status.state =`,
		`clusterName = a bogus`,
		`labels.env = "unterminated`,
	} {
		if _, err := compileClusterFilter(filter); err == nil {
			t.Errorf("compileClusterFilter(%q) succeeded, want error", filter)
		}
	}
}

// TestClusterFilter_Match pins each supported clause and the explicit/implicit
// AND joining against a fixed cluster set.
func TestClusterFilter_Match(t *testing.T) {
	clusters := map[string]dpstore.Cluster{
		"creating": {Name: "creating", Status: dpstore.ClusterStatus{State: "CREATING"}, Labels: map[string]string{"env": "staging"}},
		"running":  {Name: "running", Status: dpstore.ClusterStatus{State: "RUNNING"}, Labels: map[string]string{"env": "prod", "starred": "yes"}},
		"stopped":  {Name: "stopped", Status: dpstore.ClusterStatus{State: "STOPPED"}, Labels: map[string]string{"env": ""}},
		"error":    {Name: "error", Status: dpstore.ClusterStatus{State: "ERROR"}},
	}
	cases := []struct {
		filter string
		want   []string
	}{
		{`status.state = ACTIVE`, []string{"creating", "running"}},
		{`status.state = INACTIVE`, []string{"error", "stopped"}},
		{`status.state = RUNNING`, []string{"running"}},
		{`status.state = STOPPED`, []string{"stopped"}},
		{`clusterName = running`, []string{"running"}},
		{`clusterName = "running"`, []string{"running"}},
		{`clusterName = *`, []string{"creating", "error", "running", "stopped"}},
		{`clusterName = nope`, nil},
		{`labels.env = staging`, []string{"creating"}},
		{`labels.env = ""`, []string{"stopped"}},
		{`labels.starred = *`, []string{"running"}},
		{`labels.env = Staging`, nil},
		{`status.state = ACTIVE AND labels.env = staging`, []string{"creating"}},
		{`status.state = INACTIVE clusterName = error`, []string{"error"}}, // implicit AND
	}
	for _, tc := range cases {
		f, err := compileClusterFilter(tc.filter)
		require.NoError(t, err, tc.filter)
		var got []string
		for name, c := range clusters {
			if f.match(c) {
				got = append(got, name)
			}
		}
		sort.Strings(got)
		want := append([]string(nil), tc.want...)
		sort.Strings(want)
		require.Equal(t, want, got, tc.filter)
	}
}

// TestListClusters_FilterAndPaging exercises the whole ListClusters surface over
// a seeded store: filter, and filtering applied before pagination so the cursor
// only walks matching clusters.
func TestListClusters_FilterAndPaging(t *testing.T) {
	// A one-hour ready delay keeps the CREATING cluster from settling on read.
	p := newJobStateProvider(t, time.Hour)

	seedListCluster(t, p, "c1", "RUNNING", map[string]string{"env": "staging"})
	seedListCluster(t, p, "c2", "CREATING", map[string]string{"env": "staging"})
	seedListCluster(t, p, "c3", "STOPPED", map[string]string{"env": "prod"})
	seedListCluster(t, p, "c4", "ERROR", nil)

	ctx := context.Background()
	list := func(filter string, pageSize int, token string) ([]string, string) {
		page, next, err := p.ListClusters(ctx, "proj", "us-central1", filter, pageSize, token)
		require.NoError(t, err)
		names := make([]string, 0, len(page))
		for _, c := range page {
			names = append(names, c.Name)
		}
		sort.Strings(names)
		return names, next
	}

	all, _ := list("", 0, "")
	require.Equal(t, []string{"c1", "c2", "c3", "c4"}, all)

	active, _ := list(`status.state = ACTIVE`, 0, "")
	require.Equal(t, []string{"c1", "c2"}, active)

	inactive, _ := list(`status.state = INACTIVE`, 0, "")
	require.Equal(t, []string{"c3", "c4"}, inactive)

	byName, _ := list(`clusterName = c3`, 0, "")
	require.Equal(t, []string{"c3"}, byName)

	byLabel, _ := list(`labels.env = staging`, 0, "")
	require.Equal(t, []string{"c1", "c2"}, byLabel)

	byStateAndLabel, _ := list(`status.state = ACTIVE AND labels.env = prod`, 0, "")
	require.Empty(t, byStateAndLabel)

	// Filtering applies before pagination: pageSize 1 over the two staging
	// clusters yields exactly two pages, not a page of the full set.
	page1, tok := list(`labels.env = staging`, 1, "")
	require.Equal(t, []string{"c1"}, page1)
	require.NotEmpty(t, tok)
	page2, tok2 := list(`labels.env = staging`, 1, tok)
	require.Equal(t, []string{"c2"}, page2)
	require.Empty(t, tok2)
}

// TestListClusters_MalformedFilterInvalidArgument verifies a bad filter surfaces
// as 400 InvalidArgument from the core.
func TestListClusters_MalformedFilterInvalidArgument(t *testing.T) {
	p := newJobStateProvider(t, time.Hour)
	_, _, err := p.ListClusters(context.Background(), "proj", "us-central1", "bogus = 1", 0, "")
	pe, ok := err.(*model.ProviderError)
	require.True(t, ok, "want *model.ProviderError, got %T", err)
	require.Equal(t, 400, pe.HTTPStatus)
	require.Equal(t, "InvalidArgument", pe.Code)
}

// TestList_NegativePageSizeInvalidArgument verifies the list surfaces reject a
// negative pageSize rather than silently falling back to the default.
func TestList_NegativePageSizeInvalidArgument(t *testing.T) {
	p := newJobStateProvider(t, time.Hour)
	ctx := context.Background()

	_, _, err := p.ListClusters(ctx, "proj", "us-central1", "", -1, "")
	pe, ok := err.(*model.ProviderError)
	require.True(t, ok, "ListClusters: want *model.ProviderError, got %T", err)
	require.Equal(t, 400, pe.HTTPStatus)
	require.Equal(t, "InvalidArgument", pe.Code)

	_, _, err = p.ListJobs(ctx, "proj", "us-central1", "", "", JobStateMatcherAll, -1, "")
	pe, ok = err.(*model.ProviderError)
	require.True(t, ok, "ListJobs: want *model.ProviderError, got %T", err)
	require.Equal(t, 400, pe.HTTPStatus)
	require.Equal(t, "InvalidArgument", pe.Code)

	_, _, err = p.ListWorkflowTemplates(ctx, "proj", "us-central1", -1, "")
	pe, ok = err.(*model.ProviderError)
	require.True(t, ok, "ListWorkflowTemplates: want *model.ProviderError, got %T", err)
	require.Equal(t, 400, pe.HTTPStatus)
	require.Equal(t, "InvalidArgument", pe.Code)
}
