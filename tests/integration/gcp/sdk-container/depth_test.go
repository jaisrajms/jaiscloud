// depth_test.go closes DC6 (plan_docs/gcp-depth-closure-wave-plan.md W2.4) for
// Google Kubernetes Engine: the p2 and p5 cells its record left exempt —
// control-plane contention and a many-node-pool boundary — asserted through the
// official GKE v1 apiary client and the shared depth-test kit
// (tests/integration/gcp/testdepth).
//
// p4 (cluster/node-pool paging) is deliberately *not* asserted here: the GKE
// Discovery document declares no `pageSize`/`pageToken` on
// `projects.locations.clusters.list` / `...nodePools.list`, neither
// `ListClustersResponse` nor `ListNodePoolsResponse` carries a `nextPageToken`,
// and the generated client has no `PageSize` method at all — so no client pages
// these collections and the class has no client-observable surface (the record
// re-exempts p4 with that evidence). The complete-collection listing that *is*
// observable is the p5 boundary asserted below.
//
// The contention arbiter is the store, not the client: cluster and node-pool
// creates are check-then-insert under the store's cluster lock, so a same-name
// create has exactly one winner and every loser observes ALREADY_EXISTS/409; a
// delete of a cluster that is already gone is NOT_FOUND/404 (not idempotent), so
// a concurrent delete is a one-winner race. Node-pool updates are asserted on
// *distinct* pools: real GKE permits only one operation per node pool to run
// concurrently and rejects a conflicting same-pool request with
// FAILED_PRECONDITION, a lifecycle the metadata-only emulator (operations are
// created DONE, with no in-flight window) does not model (recorded as DC15); on
// distinct pools an operation is allowed, so every concurrent update must land
// and keep its own version. The races run under -race (the module's gate adds
// it); a contention assertion without the race detector is a failure
// (testdepth.AssertRaceEnabled), not a silent downgrade.
package sdk_container_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	container "google.golang.org/api/container/v1"
	testdepth "jaiscloud-gcp-testdepth"
)

// lostWithHTTP returns a contention predicate matching a loser that observed the
// documented HTTP status and google.rpc status token: a duplicate
// cluster/node-pool create is ALREADY_EXISTS/409 and a delete of a missing
// cluster is NOT_FOUND/404.
func lostWithHTTP(status int, token string) func(error) bool {
	return func(err error) bool {
		obs, ok := testdepth.StatusOf(err)
		return ok && obs.Kind == testdepth.KindHTTP && obs.HTTPStatus == status && obs.Status == token
	}
}

// TestSDKContainerDepthConcurrency is container p2: concurrent cluster and
// node-pool mutations under -race.
func TestSDKContainerDepthConcurrency(t *testing.T) {
	resetState(t)
	svc := newService(t)
	testdepth.AssertRaceEnabled(t)

	t.Run("SameNameClusterCreateRace", func(t *testing.T) {
		id := unique("race")
		errs := testdepth.Race(6, func(int) error {
			_, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
				Cluster: &container.Cluster{Name: id, InitialNodeCount: 1},
			}).Do()
			return err
		})
		testdepth.AssertOneWinner(t, errs, lostWithHTTP(http.StatusConflict, "ALREADY_EXISTS"))

		got, err := svc.Projects.Locations.Clusters.Get(clusterName(id)).Do()
		require.NoError(t, err)
		require.Equal(t, "RUNNING", got.Status)
	})

	t.Run("ConcurrentDeleteOneWinner", func(t *testing.T) {
		id := unique("del")
		_, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
			Cluster: &container.Cluster{Name: id},
		}).Do()
		require.NoError(t, err)

		errs := testdepth.Race(6, func(int) error {
			_, err := svc.Projects.Locations.Clusters.Delete(clusterName(id)).Do()
			return err
		})
		testdepth.AssertOneWinner(t, errs, lostWithHTTP(http.StatusNotFound, "NOT_FOUND"))

		_, err = svc.Projects.Locations.Clusters.Get(clusterName(id)).Do()
		requireAPIError(t, err, http.StatusNotFound)
	})

	t.Run("SameNameNodePoolCreateRace", func(t *testing.T) {
		cluster := unique("racepool")
		_, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
			Cluster: &container.Cluster{Name: cluster},
		}).Do()
		require.NoError(t, err)

		pool := unique("p")
		errs := testdepth.Race(6, func(int) error {
			_, err := svc.Projects.Locations.Clusters.NodePools.Create(clusterName(cluster), &container.CreateNodePoolRequest{
				NodePool: &container.NodePool{Name: pool, InitialNodeCount: 1},
			}).Do()
			return err
		})
		testdepth.AssertOneWinner(t, errs, lostWithHTTP(http.StatusConflict, "ALREADY_EXISTS"))

		pools, err := svc.Projects.Locations.Clusters.NodePools.List(clusterName(cluster)).Do()
		require.NoError(t, err)
		require.Len(t, pools.NodePools, 2, "default-pool plus the one winning pool")
	})

	t.Run("ConcurrentDistinctNodePoolUpdates", func(t *testing.T) {
		cluster := unique("updpool")
		_, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
			Cluster: &container.Cluster{Name: cluster},
		}).Do()
		require.NoError(t, err)

		const writers = 6
		pools := make([]string, writers)
		versions := make([]string, writers)
		for i := 0; i < writers; i++ {
			pools[i] = unique("p") + "-" + strconv.Itoa(i)
			versions[i] = "1.31.0-gke." + strconv.Itoa(100+i)
			_, err := svc.Projects.Locations.Clusters.NodePools.Create(clusterName(cluster), &container.CreateNodePoolRequest{
				NodePool: &container.NodePool{Name: pools[i], InitialNodeCount: 1},
			}).Do()
			require.NoError(t, err)
		}

		errs := testdepth.Race(writers, func(i int) error {
			_, err := svc.Projects.Locations.Clusters.NodePools.Update(nodePoolName(cluster, pools[i]), &container.UpdateNodePoolRequest{
				NodeVersion: versions[i],
			}).Do()
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "update %d", i)
		}
		for i := range pools {
			got, err := svc.Projects.Locations.Clusters.NodePools.Get(nodePoolName(cluster, pools[i])).Do()
			require.NoError(t, err)
			require.Equal(t, versions[i], got.Version, "pool %d must keep its own version", i)
		}
	})
}

// TestSDKContainerDepthScale is container p5: the boundary the class names — a
// large node-pool collection (and a large cluster collection) read back in full.
// GKE's metadata lists are unpaginated (the Discovery document declares no
// cursor), so the whole stored collection must come back in one listing.
func TestSDKContainerDepthScale(t *testing.T) {
	resetState(t)
	svc := newService(t)

	cluster := unique("scale")
	_, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
		Cluster: &container.Cluster{Name: cluster},
	}).Do()
	require.NoError(t, err)

	const extraPools = 24
	wantPools := map[string]bool{"default-pool": true}
	for i := 0; i < extraPools; i++ {
		name := unique("sp") + "-" + strconv.Itoa(i)
		_, err := svc.Projects.Locations.Clusters.NodePools.Create(clusterName(cluster), &container.CreateNodePoolRequest{
			NodePool: &container.NodePool{Name: name, InitialNodeCount: 1},
		}).Do()
		require.NoError(t, err)
		wantPools[name] = true
	}

	pools, err := svc.Projects.Locations.Clusters.NodePools.List(clusterName(cluster)).Do()
	require.NoError(t, err)
	require.Len(t, pools.NodePools, len(wantPools), "the full node-pool collection must be returned")
	gotPools := map[string]bool{}
	for _, p := range pools.NodePools {
		gotPools[p.Name] = true
	}
	require.Equal(t, wantPools, gotPools, "every node pool must survive the listing")

	// A dedicated location so the cluster listing is exactly the created set.
	scaleParent := "projects/" + projectID() + "/locations/us-east1"
	const clusters = 25
	wantClusters := map[string]bool{}
	for i := 0; i < clusters; i++ {
		name := unique("sc") + "-" + strconv.Itoa(i)
		_, err := svc.Projects.Locations.Clusters.Create(scaleParent, &container.CreateClusterRequest{
			Cluster: &container.Cluster{Name: name},
		}).Do()
		require.NoError(t, err)
		wantClusters[name] = true
	}

	list, err := svc.Projects.Locations.Clusters.List(scaleParent).Do()
	require.NoError(t, err)
	require.Len(t, list.Clusters, len(wantClusters), "the full cluster collection must be returned")
	gotClusters := map[string]bool{}
	for _, c := range list.Clusters {
		gotClusters[c.Name] = true
	}
	require.Equal(t, wantClusters, gotClusters, "every cluster must survive the listing")
}
