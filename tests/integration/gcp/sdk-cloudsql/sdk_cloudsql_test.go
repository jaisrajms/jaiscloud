// Package sdk_cloudsql_test exercises the jaiscloud-gcp emulator's Cloud SQL
// Admin control plane (sqladmin.googleapis.com/sql/v1beta4) through the
// official Google REST apiary client. This validates wire-level parity with the
// real SDK: instance insert/get/list/patch/restart/delete round-trips, the
// sql#operation envelope with status DONE, databases and users nested under an
// instance, the list envelopes, and the AlreadyExists/NotFound error codes.
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_cloudsql_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	sqladmin "google.golang.org/api/sqladmin/v1beta4"
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

func TestSDKCloudSQL(t *testing.T) {
	ctx := context.Background()
	svc, err := sqladmin.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := projectID()
	instance := unique("inst")

	// --- instance insert -> get -> list ---
	op, err := svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name:            instance,
		Region:          "us-central1",
		DatabaseVersion: "MYSQL_8_0",
		Settings:        &sqladmin.Settings{Tier: "db-n1-standard-1"},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "sql#operation", op.Kind)
	require.Equal(t, "DONE", op.Status)
	require.Equal(t, "CREATE", op.OperationType)
	require.Equal(t, instance, op.TargetId)
	require.NotEmpty(t, op.Name)
	require.NotEmpty(t, op.SelfLink)

	got, err := svc.Instances.Get(project, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "sql#instance", got.Kind)
	require.Equal(t, "RUNNABLE", got.State)
	require.Equal(t, "us-central1", got.Region)
	require.Equal(t, "SECOND_GEN", got.BackendType)
	require.Equal(t, project+":us-central1:"+instance, got.ConnectionName)
	require.NotEmpty(t, got.SelfLink)
	require.NotEmpty(t, got.ServiceAccountEmailAddress)
	require.NotNil(t, got.Settings)
	require.Equal(t, "db-n1-standard-1", got.Settings.Tier)
	require.NotEmpty(t, got.IpAddresses)

	list, err := svc.Instances.List(project).Do()
	require.NoError(t, err)
	require.Equal(t, "sql#instancesList", list.Kind)
	require.NotEmpty(t, list.Items)

	// A duplicate instance is rejected.
	_, err = svc.Instances.Insert(project, &sqladmin.DatabaseInstance{Name: instance, Region: "us-central1"}).Do()
	require.Error(t, err, "duplicate instance must be rejected")
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 409, apiErr.Code)

	// --- patch settings ---
	patchOp, err := svc.Instances.Patch(project, instance, &sqladmin.DatabaseInstance{
		Settings: &sqladmin.Settings{Tier: "db-n1-standard-2"},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "UPDATE", patchOp.OperationType)
	require.Equal(t, "DONE", patchOp.Status)

	patched, err := svc.Instances.Get(project, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "db-n1-standard-2", patched.Settings.Tier)

	// --- restart ---
	restartOp, err := svc.Instances.Restart(project, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "RESTART", restartOp.OperationType)
	require.Equal(t, "DONE", restartOp.Status)

	// --- operations get/list ---
	fetchedOp, err := svc.Operations.Get(project, op.Name).Do()
	require.NoError(t, err)
	require.Equal(t, op.Name, fetchedOp.Name)
	require.Equal(t, "DONE", fetchedOp.Status)

	ops, err := svc.Operations.List(project).Do()
	require.NoError(t, err)
	require.Equal(t, "sql#operationsList", ops.Kind)
	require.NotEmpty(t, ops.Items)

	// --- databases nested under the instance ---
	dbOp, err := svc.Databases.Insert(project, instance, &sqladmin.Database{
		Name:      "appdb",
		Charset:   "utf8",
		Collation: "utf8_general_ci",
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "CREATE_DATABASE", dbOp.OperationType)
	require.Equal(t, "DONE", dbOp.Status)

	db, err := svc.Databases.Get(project, instance, "appdb").Do()
	require.NoError(t, err)
	require.Equal(t, "sql#database", db.Kind)
	require.Equal(t, "appdb", db.Name)
	require.Equal(t, instance, db.Instance)
	require.Equal(t, "utf8", db.Charset)

	dbs, err := svc.Databases.List(project, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "sql#databasesList", dbs.Kind)
	require.Len(t, dbs.Items, 1)

	_, err = svc.Databases.Insert(project, instance, &sqladmin.Database{Name: "appdb"}).Do()
	require.Error(t, err, "duplicate database must be rejected")
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 409, apiErr.Code)

	updOp, err := svc.Databases.Update(project, instance, "appdb", &sqladmin.Database{Collation: "utf8_unicode_ci"}).Do()
	require.NoError(t, err)
	require.Equal(t, "UPDATE_DATABASE", updOp.OperationType)

	paDB, err := svc.Databases.Get(project, instance, "appdb").Do()
	require.NoError(t, err)
	require.Equal(t, "utf8_unicode_ci", paDB.Collation)
	require.Equal(t, "utf8", paDB.Charset, "unmasked charset must be retained")

	delDBOp, err := svc.Databases.Delete(project, instance, "appdb").Do()
	require.NoError(t, err)
	require.Equal(t, "DELETE_DATABASE", delDBOp.OperationType)
	_, err = svc.Databases.Get(project, instance, "appdb").Do()
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code)

	// --- users nested under the instance ---
	userOp, err := svc.Users.Insert(project, instance, &sqladmin.User{
		Name:     "alice",
		Host:     "%",
		Password: "hunter2",
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "CREATE_USER", userOp.OperationType)
	require.Equal(t, "DONE", userOp.Status)

	user, err := svc.Users.Get(project, instance, "alice").Do()
	require.NoError(t, err)
	require.Equal(t, "sql#user", user.Kind)
	require.Equal(t, "alice", user.Name)
	require.Equal(t, "BUILT_IN", user.Type)

	users, err := svc.Users.List(project, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "sql#usersList", users.Kind)
	require.Len(t, users.Items, 1)

	_, err = svc.Users.Insert(project, instance, &sqladmin.User{Name: "alice", Host: "%"}).Do()
	require.Error(t, err, "duplicate user must be rejected")
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 409, apiErr.Code)

	usrUpdOp, err := svc.Users.Update(project, instance, &sqladmin.User{
		Name: "alice",
		Host: "%",
		Type: "CLOUD_IAM_USER",
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "UPDATE_USER", usrUpdOp.OperationType)

	updUser, err := svc.Users.Get(project, instance, "alice").Do()
	require.NoError(t, err)
	require.Equal(t, "CLOUD_IAM_USER", updUser.Type)

	usrDelOp, err := svc.Users.Delete(project, instance).Name("alice").Host("%").Do()
	require.NoError(t, err)
	require.Equal(t, "DELETE_USER", usrDelOp.OperationType)
	_, err = svc.Users.Get(project, instance, "alice").Do()
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code)

	// --- discovery surfaces ---
	flags, err := svc.Flags.List().Do()
	require.NoError(t, err)
	require.Equal(t, "sql#flagsList", flags.Kind)
	require.NotEmpty(t, flags.Items)

	tiers, err := svc.Tiers.List(project).Do()
	require.NoError(t, err)
	require.Equal(t, "sql#tiersList", tiers.Kind)
	require.NotEmpty(t, tiers.Items)

	cs, err := svc.Connect.Get(project, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "sql#connectSettings", cs.Kind)
	require.Equal(t, "us-central1", cs.Region)

	// --- instance delete ---
	delOp, err := svc.Instances.Delete(project, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "DELETE", delOp.OperationType)
	require.Equal(t, "DONE", delOp.Status)
	_, err = svc.Instances.Get(project, instance).Do()
	require.Error(t, err, "a deleted instance must not be readable")
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code)
}
