// This file deepens the Memorystore for Redis behavioral coverage: the
// STANDARD_HA tier, per-location isolation, list pagination, label/config
// patch semantics, mask validation, and the NotFound/InvalidArgument contracts.
package sdk_memorystore_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	redis "google.golang.org/api/redis/v1"
)

func baseURL() string {
	return strings.TrimRight(endpoint(), "/")
}

// resetState wipes emulator state between tests (the suite shares one server).
func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(baseURL()+"/_jaiscloud/reset", "", nil) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func requireAPIError(t *testing.T, err error, code int) {
	t.Helper()
	require.Error(t, err)
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, code, apiErr.Code)
}

func newService(t *testing.T) *redis.Service {
	t.Helper()
	svc, err := redis.NewService(context.Background(), opts()...)
	require.NoError(t, err)
	return svc
}

func parentFor(location string) string {
	return "projects/" + projectID() + "/locations/" + location
}

// TestSDKMemorystorePatchAndMasks covers the update-mask semantics (masked,
// unmasked, and unsupported fields) and the label/redis-config overlays.
func TestSDKMemorystorePatchAndMasks(t *testing.T) {
	resetState(t)
	svc := newService(t)
	instances := svc.Projects.Locations.Instances

	location := "us-central1"
	parent := parentFor(location)
	id := unique("inst")
	name := parent + "/instances/" + id

	op, err := instances.Create(parent, &redis.Instance{
		DisplayName:  "original",
		Tier:         "STANDARD_HA",
		MemorySizeGb: 5,
		RedisVersion: "REDIS_7_0",
		Labels:       map[string]string{"env": "test"},
	}).InstanceId(id).Do()
	require.NoError(t, err)
	created := instanceFromOperation(t, op)
	require.Equal(t, "STANDARD_HA", created.Tier)
	require.Equal(t, int64(5), created.MemorySizeGb)
	require.Equal(t, map[string]string{"env": "test"}, created.Labels)

	// Masked label patch replaces labels.
	_, err = instances.Patch(name, &redis.Instance{
		Labels: map[string]string{"env": "prod", "team": "cache"},
	}).UpdateMask("labels").Do()
	require.NoError(t, err)
	got, err := instances.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"env": "prod", "team": "cache"}, got.Labels)
	require.Equal(t, "original", got.DisplayName, "unmasked displayName retained")

	// Masked redisConfigs patch sets the config map.
	_, err = instances.Patch(name, &redis.Instance{
		RedisConfigs: map[string]string{"maxmemory-policy": "allkeys-lru"},
	}).UpdateMask("redisConfigs").Do()
	require.NoError(t, err)
	got, err = instances.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"maxmemory-policy": "allkeys-lru"}, got.RedisConfigs)

	// An empty mask overlays every field present in the body.
	_, err = instances.Patch(name, &redis.Instance{DisplayName: "no-mask"}).Do()
	require.NoError(t, err)
	got, err = instances.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, "no-mask", got.DisplayName)
	require.Equal(t, int64(5), got.MemorySizeGb, "unset fields retained")
	require.Equal(t, map[string]string{"env": "prod", "team": "cache"}, got.Labels)

	// replicaCount is a documented patchable field (instances.patch updateMask
	// allows displayName, labels, memorySizeGb, redisConfig, replica_count), so it
	// must round-trip through GET/LIST rather than be rejected. The instance is
	// STANDARD_HA, where a replica count is meaningful.
	_, err = instances.Patch(name, &redis.Instance{ReplicaCount: 3}).UpdateMask("replicaCount").Do()
	require.NoError(t, err)
	got, err = instances.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, int64(3), got.ReplicaCount)

	list, err := instances.List(parent).Do()
	require.NoError(t, err)
	require.Len(t, list.Instances, 1)
	require.Equal(t, int64(3), list.Instances[0].ReplicaCount)

	// A genuinely unsupported mask field still fails loud.
	_, err = instances.Patch(name, &redis.Instance{DisplayName: "x"}).UpdateMask("bogusField").Do()
	requireAPIError(t, err, 400)

	// An invalid tier is InvalidArgument.
	_, err = instances.Create(parent, &redis.Instance{Tier: "PREMIUM", MemorySizeGb: 1}).InstanceId(unique("bad")).Do()
	requireAPIError(t, err, 400)
}

// TestSDKMemorystoreLocationIsolationAndPagination covers per-location scoping,
// instance list pagination, and location discovery pagination.
func TestSDKMemorystoreLocationIsolationAndPagination(t *testing.T) {
	resetState(t)
	svc := newService(t)
	instances := svc.Projects.Locations.Instances

	central := parentFor("us-central1")
	east := parentFor("us-east1")

	// The same instance id may exist independently in two locations.
	sharedID := unique("shared")
	for _, parent := range []string{central, east} {
		_, err := instances.Create(parent, &redis.Instance{
			Tier: "BASIC", MemorySizeGb: 1,
		}).InstanceId(sharedID).Do()
		require.NoError(t, err)
	}

	centralList, err := instances.List(central).Do()
	require.NoError(t, err)
	require.Len(t, centralList.Instances, 1)
	require.Equal(t, "us-central1", centralList.Instances[0].LocationId)

	eastList, err := instances.List(east).Do()
	require.NoError(t, err)
	require.Len(t, eastList.Instances, 1)
	require.Equal(t, "us-east1", eastList.Instances[0].LocationId)

	// Instance list pagination within one location.
	for i := 0; i < 2; i++ {
		_, err := instances.Create(central, &redis.Instance{
			Tier: "BASIC", MemorySizeGb: 1,
		}).InstanceId(unique("pg")).Do()
		require.NoError(t, err)
	}
	page1, err := instances.List(central).PageSize(2).Do()
	require.NoError(t, err)
	require.Len(t, page1.Instances, 2)
	require.NotEmpty(t, page1.NextPageToken)
	page2, err := instances.List(central).PageSize(2).PageToken(page1.NextPageToken).Do()
	require.NoError(t, err)
	require.Len(t, page2.Instances, 1)
	require.Empty(t, page2.NextPageToken)

	// Location discovery pagination.
	locPage1, err := svc.Projects.Locations.List("projects/" + projectID()).PageSize(2).Do()
	require.NoError(t, err)
	require.Len(t, locPage1.Locations, 2)
	require.NotEmpty(t, locPage1.NextPageToken)
	locPage2, err := svc.Projects.Locations.List("projects/" + projectID()).PageSize(2).PageToken(locPage1.NextPageToken).Do()
	require.NoError(t, err)
	require.NotEmpty(t, locPage2.Locations)

	// A missing instance is NotFound across get/patch/upgrade/delete.
	missing := central + "/instances/inst-missing"
	_, err = instances.Get(missing).Do()
	requireAPIError(t, err, 404)
	_, err = instances.Patch(missing, &redis.Instance{DisplayName: "x"}).UpdateMask("displayName").Do()
	requireAPIError(t, err, 404)
	_, err = instances.Upgrade(missing, &redis.UpgradeInstanceRequest{RedisVersion: "REDIS_7_2"}).Do()
	requireAPIError(t, err, 404)
	_, err = instances.Delete(missing).Do()
	requireAPIError(t, err, 404)
}

// TestSDKMemorystoreUpgrade covers the redis-version upgrade path, including
// the no-op form when the request omits a version.
func TestSDKMemorystoreUpgrade(t *testing.T) {
	resetState(t)
	svc := newService(t)
	instances := svc.Projects.Locations.Instances

	location := "europe-west1"
	parent := parentFor(location)
	id := unique("inst")
	name := parent + "/instances/" + id

	_, err := instances.Create(parent, &redis.Instance{
		Tier: "BASIC", MemorySizeGb: 1, RedisVersion: "REDIS_7_0",
	}).InstanceId(id).Do()
	require.NoError(t, err)

	upgradeOp, err := instances.Upgrade(name, &redis.UpgradeInstanceRequest{RedisVersion: "REDIS_7_2"}).Do()
	require.NoError(t, err)
	upgraded := instanceFromOperation(t, upgradeOp)
	require.Equal(t, "REDIS_7_2", upgraded.RedisVersion)
	require.Equal(t, "READY", upgraded.State)

	// An upgrade with no version retains the current one.
	noopOp, err := instances.Upgrade(name, &redis.UpgradeInstanceRequest{}).Do()
	require.NoError(t, err)
	noop := instanceFromOperation(t, noopOp)
	require.Equal(t, "REDIS_7_2", noop.RedisVersion)
}
