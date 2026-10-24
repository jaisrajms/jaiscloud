package sdk_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	gcsapi "google.golang.org/api/storage/v1"
)

// TestSDKVersioningGenerations covers GCS object versioning through the SDK:
// enable versioning, overwrite an object, read the archived generation, and
// list versions.
func TestSDKVersioningGenerations(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-ver-%d", time.Now().UnixNano())
	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))

	// Enable versioning.
	_, err := client.Bucket(bucket).Update(ctx, storage.BucketAttrsToUpdate{VersioningEnabled: true})
	require.NoError(t, err)
	attrs, err := client.Bucket(bucket).Attrs(ctx)
	require.NoError(t, err)
	require.True(t, attrs.VersioningEnabled, "versioning should be enabled")

	// First generation.
	writeObject(t, client, bucket, "v.txt", "one")
	a1, err := client.Bucket(bucket).Object("v.txt").Attrs(ctx)
	require.NoError(t, err)
	gen1 := a1.Generation

	// Overwrite → second generation.
	writeObject(t, client, bucket, "v.txt", "two")
	a2, err := client.Bucket(bucket).Object("v.txt").Attrs(ctx)
	require.NoError(t, err)
	require.NotEqual(t, gen1, a2.Generation, "overwrite must produce a new generation")

	// Read the archived generation's media.
	r, err := client.Bucket(bucket).Object("v.txt").Generation(gen1).NewReader(ctx)
	require.NoError(t, err)
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	r.Close()
	require.Equal(t, "one", string(b), "archived generation should retain its bytes")

	// Read the archived generation's metadata.
	oldAttrs, err := client.Bucket(bucket).Object("v.txt").Generation(gen1).Attrs(ctx)
	require.NoError(t, err)
	require.Equal(t, gen1, oldAttrs.Generation)

	// List all versions; the archived generation reports Deleted.
	var gens []int64
	deletedSeen := false
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{Versions: true})
	for {
		oa, err := it.Next()
		if err != nil {
			break
		}
		gens = append(gens, oa.Generation)
		if oa.Generation == gen1 && !oa.Deleted.IsZero() {
			deletedSeen = true
		}
	}
	require.Len(t, gens, 2, "versions list should contain both generations")
	require.True(t, deletedSeen, "archived generation should report Deleted")
}

// TestSDKRetentionBlocksDelete covers bucket retention: set a retention policy,
// insert an object, then a delete while retention is active must fail.
func TestSDKRetentionBlocksDelete(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-ret-%d", time.Now().UnixNano())
	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))

	_, err := client.Bucket(bucket).Update(ctx, storage.BucketAttrsToUpdate{
		RetentionPolicy: &storage.RetentionPolicy{RetentionPeriod: 24 * time.Hour},
	})
	require.NoError(t, err)

	writeObject(t, client, bucket, "locked.txt", "secret")

	err = client.Bucket(bucket).Object("locked.txt").Delete(ctx)
	require.Error(t, err, "delete must fail while the object is under retention")
}

// TestSDKLifecycleConfigReadBack covers bucket lifecycle: set a Delete rule and
// read it back.
func TestSDKLifecycleConfigReadBack(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-lc-%d", time.Now().UnixNano())
	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))

	_, err := client.Bucket(bucket).Update(ctx, storage.BucketAttrsToUpdate{
		Lifecycle: &storage.Lifecycle{Rules: []storage.LifecycleRule{
			{Action: storage.LifecycleAction{Type: "Delete"}, Condition: storage.LifecycleCondition{AgeInDays: 30}},
		}},
	})
	require.NoError(t, err)

	attrs, err := client.Bucket(bucket).Attrs(ctx)
	require.NoError(t, err)
	require.Len(t, attrs.Lifecycle.Rules, 1, "lifecycle rule should round-trip")
	require.Equal(t, "Delete", attrs.Lifecycle.Rules[0].Action.Type)
	require.Equal(t, int64(30), attrs.Lifecycle.Rules[0].Condition.AgeInDays)
}

// TestSDKListNegativeMaxResultsRejected sends maxResults=-1 through the
// generated storage/v1 client (the high-level iterator guards pageSize > 0 and
// can never emit it) and requires the AIP-158 INVALID_ARGUMENT answer on both
// objects.list and buckets.list (DC39).
func TestSDKListNegativeMaxResultsRejected(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	bucket := fmt.Sprintf("sdk-neg-max-%d", time.Now().UnixNano())
	require.NoError(t, client.Bucket(bucket).Create(ctx, "proj", nil))

	svc, err := gcsapi.NewService(ctx,
		option.WithEndpoint(emulatorHost()+"/storage/v1/"),
		option.WithoutAuthentication())
	require.NoError(t, err)

	_, err = svc.Objects.List(bucket).MaxResults(-1).Do()
	requireAPIError(t, err, "objects.list maxResults=-1")

	_, err = svc.Buckets.List("proj").MaxResults(-1).Do()
	requireAPIError(t, err, "buckets.list maxResults=-1")
}

func requireAPIError(t *testing.T, err error, what string) {
	t.Helper()
	apiErr, ok := err.(*googleapi.Error)
	require.Truef(t, ok, "%s: expected a *googleapi.Error, got %v", what, err)
	require.Equalf(t, http.StatusBadRequest, apiErr.Code, "%s: %s", what, apiErr.Body)
	require.NotEmptyf(t, apiErr.Errors, "%s: %s", what, apiErr.Body)
	require.Equalf(t, "invalidParameter", apiErr.Errors[0].Reason, "%s: %s", what, apiErr.Body)
}
