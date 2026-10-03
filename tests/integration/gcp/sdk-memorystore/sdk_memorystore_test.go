// Package sdk_memorystore_test exercises the jaiscloud-gcp emulator's
// Memorystore for Redis control plane (redis.googleapis.com/v1) through the
// official Google REST apiary client. This validates wire-level parity with the
// real SDK: instance create/get/list/update-mask/delete round-trips, the inline
// done:true long-running Operation shape, region-scoped listing, location
// discovery, and the AlreadyExists/NotFound error codes.
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_memorystore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	redis "google.golang.org/api/redis/v1"
)

func endpoint() string {
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8080/"
}

func projectID() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "proj"
}

func opts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(endpoint()), option.WithoutAuthentication()}
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// instanceFromOperation decodes a done long-running Operation's response into
// an Instance, asserting the inline done shape on the way.
func instanceFromOperation(t *testing.T, op *redis.Operation) *redis.Instance {
	t.Helper()
	require.NotNil(t, op)
	require.True(t, op.Done, "operation must complete inline (done:true)")
	require.NotEmpty(t, op.Name)
	require.NotEmpty(t, op.Response, "done operation must carry a response")

	var inst redis.Instance
	require.NoError(t, json.Unmarshal(op.Response, &inst))
	return &inst
}

func TestSDKMemorystore(t *testing.T) {
	ctx := context.Background()
	svc, err := redis.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := projectID()
	location := "us-central1"
	parent := fmt.Sprintf("projects/%s/locations/%s", project, location)
	id := unique("inst")
	name := parent + "/instances/" + id

	instances := svc.Projects.Locations.Instances

	// --- create ---
	op, err := instances.Create(parent, &redis.Instance{
		DisplayName:  "sdk test instance",
		Tier:         "BASIC",
		MemorySizeGb: 1,
		RedisVersion: "REDIS_7_0",
		Labels:       map[string]string{"env": "test"},
	}).InstanceId(id).Do()
	require.NoError(t, err)
	created := instanceFromOperation(t, op)
	require.Equal(t, name, created.Name)
	require.Equal(t, "READY", created.State)
	require.Equal(t, "BASIC", created.Tier)
	require.Equal(t, int64(1), created.MemorySizeGb)
	require.Equal(t, "REDIS_7_0", created.RedisVersion)
	require.NotEmpty(t, created.Host)
	require.Equal(t, int64(6379), created.Port)
	require.Equal(t, location, created.LocationId)
	require.Equal(t, location, created.CurrentLocationId)
	require.NotEmpty(t, created.CreateTime)

	// --- get ---
	got, err := instances.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, name, got.Name)
	require.Equal(t, "READY", got.State)
	require.Equal(t, created.Host, got.Host)

	// --- list (location-scoped) ---
	list, err := instances.List(parent).Do()
	require.NoError(t, err)
	require.NotEmpty(t, list.Instances)
	found := false
	for _, i := range list.Instances {
		if i.Name == name {
			found = true
		}
	}
	require.True(t, found, "created instance must appear in its location list")

	// --- duplicate create is rejected ---
	_, err = instances.Create(parent, &redis.Instance{
		Tier: "BASIC", MemorySizeGb: 1,
	}).InstanceId(id).Do()
	require.Error(t, err, "duplicate instance must be rejected")
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 409, apiErr.Code)

	// --- update applies the mask ---
	updateOp, err := instances.Patch(name, &redis.Instance{
		DisplayName:  "renamed",
		MemorySizeGb: 2,
	}).UpdateMask("displayName,memorySizeGb").Do()
	require.NoError(t, err)
	updated := instanceFromOperation(t, updateOp)
	require.Equal(t, "renamed", updated.DisplayName)
	require.Equal(t, int64(2), updated.MemorySizeGb)
	require.Equal(t, "BASIC", updated.Tier, "unmasked tier must be retained")

	// --- upgrade ---
	upgradeOp, err := instances.Upgrade(name, &redis.UpgradeInstanceRequest{
		RedisVersion: "REDIS_7_2",
	}).Do()
	require.NoError(t, err)
	upgraded := instanceFromOperation(t, upgradeOp)
	require.Equal(t, "REDIS_7_2", upgraded.RedisVersion)

	// --- locations ---
	loc, err := svc.Projects.Locations.Get(parent).Do()
	require.NoError(t, err)
	require.Equal(t, parent, loc.Name)
	require.Equal(t, location, loc.LocationId)

	locations, err := svc.Projects.Locations.List(fmt.Sprintf("projects/%s", project)).Do()
	require.NoError(t, err)
	require.NotEmpty(t, locations.Locations)

	// --- delete ---
	deleteOp, err := instances.Delete(name).Do()
	require.NoError(t, err)
	require.NotNil(t, deleteOp)
	require.True(t, deleteOp.Done)

	_, err = instances.Get(name).Do()
	require.Error(t, err, "a deleted instance must not be readable")
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code)
}
