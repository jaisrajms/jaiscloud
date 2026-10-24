// Behavioural depth for the Dataproc Metastore *control plane*
// (metastore.googleapis.com/v1) through the official REST apiary client (TD8):
// `p2` concurrent service create/delete, `p3` error envelopes, and `p4`
// service/backup listing paging. The Hive Metastore Thrift plane's own depth
// lives in hms_depth_test.go.
package sdk_metastore_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	metastore "google.golang.org/api/metastore/v1"
)

// apiErrCode extracts the HTTP status from an apiary error, or 0 for a nil err.
func apiErrCode(err error) int {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	return 0
}

func TestMetastoreDepthREST(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	svc, err := metastore.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := projectID()
	const location = "us-central1"
	parent := fmt.Sprintf("projects/%s/locations/%s", project, location)
	run := unique("depth")

	name := func(suffix string) string { return fmt.Sprintf("%s/services/%s-%s", parent, run, suffix) }
	id := func(suffix string) string { return fmt.Sprintf("%s-%s", run, suffix) }

	// --- p2: concurrent distinct creates all land; a same-id race has one winner.
	const workers = 6
	createDistinct := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.Projects.Locations.Services.Create(parent, &metastore.Service{}).ServiceId(id(fmt.Sprintf("d%d", i))).Do()
			createDistinct[i] = err
		}(i)
	}
	wg.Wait()
	for i, err := range createDistinct {
		require.NoErrorf(t, err, "distinct create %d", i)
	}
	t.Cleanup(func() {
		for i := 0; i < workers; i++ {
			_, _ = svc.Projects.Locations.Services.Delete(name(fmt.Sprintf("d%d", i))).Do()
		}
	})

	// --- p3: duplicate serviceId is 409; get-after-delete is 404.
	_, err = svc.Projects.Locations.Services.Create(parent, &metastore.Service{}).ServiceId(id("d0")).Do()
	require.Equal(t, 409, apiErrCode(err), "duplicate serviceId must be AlreadyExists(409)")

	// --- p4: page over services and backups, each resource seen exactly once.
	pageIDs := []string{"p0", "p1", "p2", "p3", "p4"}
	for _, p := range pageIDs {
		_, err := svc.Projects.Locations.Services.Create(parent, &metastore.Service{}).ServiceId(id(p)).Do()
		require.NoErrorf(t, err, "create %s", p)
	}
	t.Cleanup(func() {
		for _, p := range pageIDs {
			_, _ = svc.Projects.Locations.Services.Delete(name(p)).Do()
		}
	})

	seen := map[string]int{}
	pages := 0
	token := ""
	for {
		page, err := svc.Projects.Locations.Services.List(parent).PageSize(2).PageToken(token).Do()
		require.NoError(t, err)
		pages++
		require.LessOrEqual(t, pages, 50, "listing must terminate")
		for _, s := range page.Services {
			seen[s.Name]++
		}
		token = page.NextPageToken
		if token == "" {
			break
		}
	}
	require.Greater(t, pages, 1, "5 services at pageSize 2 must cross a page boundary")
	for _, p := range pageIDs {
		require.Equalf(t, 1, seen[name(p)], "%s must appear exactly once across pages", p)
	}
	// An over-large pageSize returns everything in one page with no token.
	onePage, err := svc.Projects.Locations.Services.List(parent).PageSize(1000).Do()
	require.NoError(t, err)
	require.Empty(t, onePage.NextPageToken, "a single page must not hand back a token")

	// Backup paging under the first page service.
	host := name("d0")
	bParent := host
	backups := []string{"b0", "b1", "b2", "b3"}
	for _, b := range backups {
		_, err := svc.Projects.Locations.Services.Backups.Create(bParent, &metastore.Backup{Description: "d"}).BackupId(id(b)).Do()
		require.NoErrorf(t, err, "create backup %s", b)
	}
	backupSeen := map[string]int{}
	backupPages := 0
	token = ""
	for {
		page, err := svc.Projects.Locations.Services.Backups.List(bParent).PageSize(2).PageToken(token).Do()
		require.NoError(t, err)
		backupPages++
		require.LessOrEqual(t, backupPages, 50, "backup listing must terminate")
		for _, b := range page.Backups {
			backupSeen[b.Name]++
		}
		token = page.NextPageToken
		if token == "" {
			break
		}
	}
	require.Greater(t, backupPages, 1)
	for _, b := range backups {
		require.Equalf(t, 1, backupSeen[bParent+"/backups/"+id(b)], "backup %s exactly once", b)
	}

	// Delete then confirm the not-found envelope.
	_, err = svc.Projects.Locations.Services.Delete(name("d1")).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Locations.Services.Get(name("d1")).Do()
	require.Equal(t, 404, apiErrCode(err), "get-after-delete must be NotFound(404)")
}
