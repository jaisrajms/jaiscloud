// This file deepens the Cloud SQL behavioral coverage: instance PUT-update and
// settings deep-merge, list pagination, database patch, multi-host users, and
// the fail-loud (501) deferred surfaces.
package sdk_cloudsql_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	sqladmin "google.golang.org/api/sqladmin/v1beta4"
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

func newService(t *testing.T) *sqladmin.Service {
	t.Helper()
	svc, err := sqladmin.NewService(context.Background(), []option.ClientOption{
		option.WithEndpoint(endpoint()), option.WithoutAuthentication(),
	}...)
	require.NoError(t, err)
	return svc
}

// TestSDKCloudSQLInstanceUpdateAndPagination covers the PUT update path, the
// settings deep-merge, and instances/operations list pagination.
func TestSDKCloudSQLInstanceUpdateAndPagination(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()

	first := unique("inst")
	second := unique("inst")
	for _, name := range []string{first, second} {
		_, err := svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
			Name:            name,
			Region:          "us-central1",
			DatabaseVersion: "MYSQL_8_0",
			Settings:        &sqladmin.Settings{Tier: "db-n1-standard-1"},
		}).Do()
		require.NoError(t, err)
	}

	// List pagination: one instance per page.
	page1, err := svc.Instances.List(project).MaxResults(1).Do()
	require.NoError(t, err)
	require.Len(t, page1.Items, 1)
	require.NotEmpty(t, page1.NextPageToken)
	page2, err := svc.Instances.List(project).MaxResults(1).PageToken(page1.NextPageToken).Do()
	require.NoError(t, err)
	require.Len(t, page2.Items, 1)
	require.Empty(t, page2.NextPageToken)

	// PUT update merges settings: a tier-only update retains the normalized
	// defaults (availabilityType/dataDiskType/ipConfiguration).
	updOp, err := svc.Instances.Update(project, first, &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{Tier: "db-n1-standard-2"},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "UPDATE", updOp.OperationType)
	require.Equal(t, "DONE", updOp.Status)

	got, err := svc.Instances.Get(project, first).Do()
	require.NoError(t, err)
	require.Equal(t, "db-n1-standard-2", got.Settings.Tier)
	require.Equal(t, "ZONAL", got.Settings.AvailabilityType, "unset settings retained")
	require.Equal(t, "PD_SSD", got.Settings.DataDiskType, "unset settings retained")
	require.Equal(t, "PER_USE", got.Settings.PricingPlan, "unset settings retained")
	require.NotNil(t, got.Settings.IpConfiguration)
	require.True(t, got.Settings.IpConfiguration.Ipv4Enabled)
	require.NotNil(t, got.Settings.LocationPreference, "locationPreference retained")
	require.Equal(t, "us-central1-a", got.Settings.LocationPreference.Zone)

	// A second update changes availabilityType and retains the tier.
	_, err = svc.Instances.Update(project, first, &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{AvailabilityType: "REGIONAL"},
	}).Do()
	require.NoError(t, err)
	got, err = svc.Instances.Get(project, first).Do()
	require.NoError(t, err)
	require.Equal(t, "REGIONAL", got.Settings.AvailabilityType)
	require.Equal(t, "db-n1-standard-2", got.Settings.Tier, "tier retained across updates")

	// Operations list pagination.
	ops, err := svc.Operations.List(project).MaxResults(1).Do()
	require.NoError(t, err)
	require.Len(t, ops.Items, 1)
	require.NotEmpty(t, ops.NextPageToken)

	// Missing instances are NotFound across get/update/patch/restart/delete.
	_, err = svc.Instances.Get(project, "inst-missing").Do()
	requireAPIError(t, err, 404)
	_, err = svc.Instances.Update(project, "inst-missing", &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{Tier: "db-n1-standard-1"}}).Do()
	requireAPIError(t, err, 404)
	_, err = svc.Instances.Patch(project, "inst-missing", &sqladmin.DatabaseInstance{Settings: &sqladmin.Settings{Tier: "db-n1-standard-1"}}).Do()
	requireAPIError(t, err, 404)
	_, err = svc.Instances.Restart(project, "inst-missing").Do()
	requireAPIError(t, err, 404)
	_, err = svc.Instances.Delete(project, "inst-missing").Do()
	requireAPIError(t, err, 404)
	_, err = svc.Connect.Get(project, "inst-missing").Do()
	requireAPIError(t, err, 404)
}

// TestSDKCloudSQLDatabaseAndUserDepth covers database patch and multi-host user
// identity (the name/host key), plus nested-resource scoping errors.
func TestSDKCloudSQLDatabaseAndUserDepth(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()

	instance := unique("inst")
	_, err := svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: instance, Region: "us-central1", Settings: &sqladmin.Settings{Tier: "db-f1-micro"},
	}).Do()
	require.NoError(t, err)

	for _, name := range []string{"db1", "db2"} {
		_, err := svc.Databases.Insert(project, instance, &sqladmin.Database{
			Name: name, Charset: "utf8", Collation: "utf8_general_ci",
		}).Do()
		require.NoError(t, err)
	}

	// Databases.patch updates collation while retaining charset.
	patchOp, err := svc.Databases.Patch(project, instance, "db1", &sqladmin.Database{Collation: "utf8_bin"}).Do()
	require.NoError(t, err)
	require.Equal(t, "UPDATE_DATABASE", patchOp.OperationType)
	db, err := svc.Databases.Get(project, instance, "db1").Do()
	require.NoError(t, err)
	require.Equal(t, "utf8_bin", db.Collation)
	require.Equal(t, "utf8", db.Charset, "unmasked charset retained")

	dbs, err := svc.Databases.List(project, instance).Do()
	require.NoError(t, err)
	require.Len(t, dbs.Items, 2)

	// Users are keyed by name+host: two users may share a name with different
	// hosts.
	for _, u := range []*sqladmin.User{
		{Name: "alice", Host: "%", Password: "hunter2"},
		{Name: "alice", Host: "10.0.0.1"},
	} {
		_, err := svc.Users.Insert(project, instance, u).Do()
		require.NoError(t, err)
	}
	users, err := svc.Users.List(project, instance).Do()
	require.NoError(t, err)
	require.Len(t, users.Items, 2)

	remote, err := svc.Users.Get(project, instance, "alice").Host("10.0.0.1").Do()
	require.NoError(t, err)
	require.Equal(t, "alice", remote.Name)
	require.Equal(t, "10.0.0.1", remote.Host)
	require.Equal(t, "BUILT_IN", remote.Type)

	// Updating the remote user changes only that (name, host) record.
	updOp, err := svc.Users.Update(project, instance, &sqladmin.User{
		Name: "alice", Host: "10.0.0.1", Type: "CLOUD_IAM_USER",
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "UPDATE_USER", updOp.OperationType)
	remote, err = svc.Users.Get(project, instance, "alice").Host("10.0.0.1").Do()
	require.NoError(t, err)
	require.Equal(t, "CLOUD_IAM_USER", remote.Type)
	anyHost, err := svc.Users.Get(project, instance, "alice").Host("%").Do()
	require.NoError(t, err)
	require.Equal(t, "BUILT_IN", anyHost.Type, "the other host keeps its own type")

	// Delete one host identity; the other survives.
	delOp, err := svc.Users.Delete(project, instance).Name("alice").Host("10.0.0.1").Do()
	require.NoError(t, err)
	require.Equal(t, "DELETE_USER", delOp.OperationType)
	users, err = svc.Users.List(project, instance).Do()
	require.NoError(t, err)
	require.Len(t, users.Items, 1)
	require.Equal(t, "%", users.Items[0].Host)

	// Nested resources under a missing instance are NotFound.
	_, err = svc.Databases.List(project, "inst-missing").Do()
	requireAPIError(t, err, 404)
	_, err = svc.Databases.Get(project, "inst-missing", "db1").Do()
	requireAPIError(t, err, 404)
	_, err = svc.Users.List(project, "inst-missing").Do()
	requireAPIError(t, err, 404)
	_, err = svc.Databases.Get(project, instance, "db-missing").Do()
	requireAPIError(t, err, 404)
}

// TestSDKCloudSQLUnimplementedDeviation pins the fail-loud contract for the
// deferred data-plane/HA surfaces: they return 501 Unimplemented rather than
// silently succeeding.
func TestSDKCloudSQLUnimplementedDeviation(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()

	instance := unique("inst")
	_, err := svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: instance, Region: "us-central1", Settings: &sqladmin.Settings{Tier: "db-f1-micro"},
	}).Do()
	require.NoError(t, err)

	_, err = svc.Instances.Clone(project, instance, &sqladmin.InstancesCloneRequest{}).Do()
	requireAPIError(t, err, 501)
	_, err = svc.Instances.Failover(project, instance, &sqladmin.InstancesFailoverRequest{}).Do()
	requireAPIError(t, err, 501)
	_, err = svc.Instances.PromoteReplica(project, instance).Do()
	requireAPIError(t, err, 501)
	_, err = svc.Instances.ResetSslConfig(project, instance).Do()
	requireAPIError(t, err, 501)
}
