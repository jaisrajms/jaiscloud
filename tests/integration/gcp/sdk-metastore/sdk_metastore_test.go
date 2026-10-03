// Package sdk_metastore_test exercises the jaiscloud-gcp emulator's Dataproc
// Metastore control plane (metastore.googleapis.com/v1) through the official
// Google REST apiary client. This validates wire-level parity with the real SDK
// (Dataproc-shaped long-running operations).
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_metastore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metastore "google.golang.org/api/metastore/v1"
	"google.golang.org/api/option"
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

func TestSDKMetastore(t *testing.T) {
	ctx := context.Background()
	svc, err := metastore.NewService(ctx, opts()...)
	require.NoError(t, err)

	project := projectID()
	const location = "us-central1"
	parent := fmt.Sprintf("projects/%s/locations/%s", project, location)
	serviceID := unique("ms")

	// CreateService returns a done LRO (Dataproc shape).
	op, err := svc.Projects.Locations.Services.Create(parent, &metastore.Service{
		Labels: map[string]string{"env": "test"},
		HiveMetastoreConfig: &metastore.HiveMetastoreConfig{
			Version: "3.1.2",
		},
	}).ServiceId(serviceID).Do()
	require.NoError(t, err)
	require.True(t, op.Done, "operation should be done")
	require.NotEmpty(t, op.Name)

	var created metastore.Service
	require.NoError(t, json.Unmarshal(op.Response, &created))
	wantName := fmt.Sprintf("projects/%s/locations/%s/services/%s", project, location, serviceID)
	require.Equal(t, wantName, created.Name)
	require.Equal(t, "ACTIVE", created.State)
	require.Equal(t, "DEVELOPER", created.Tier)
	require.NotEmpty(t, created.EndpointUri)
	require.Equal(t, "test", created.Labels["env"])

	// GetService returns the logical service (created synchronously).
	got, err := svc.Projects.Locations.Services.Get(wantName).Do()
	require.NoError(t, err)
	require.Equal(t, wantName, got.Name)
	require.Equal(t, "ACTIVE", got.State)

	// ListServices includes it.
	list, err := svc.Projects.Locations.Services.List(parent).Do()
	require.NoError(t, err)
	require.NotEmpty(t, list.Services)

	// CreateBackup returns a done LRO.
	backupID := unique("bk")
	backupParent := fmt.Sprintf("%s/services/%s", parent, serviceID)
	backupOp, err := svc.Projects.Locations.Services.Backups.Create(backupParent, &metastore.Backup{
		Description: "nightly",
	}).BackupId(backupID).Do()
	require.NoError(t, err)
	require.True(t, backupOp.Done)

	var createdBackup metastore.Backup
	require.NoError(t, json.Unmarshal(backupOp.Response, &createdBackup))
	require.Equal(t, fmt.Sprintf("%s/backups/%s", backupParent, backupID), createdBackup.Name)
	require.Equal(t, "ACTIVE", createdBackup.State)

	// GetBackup + ListBackups.
	gotBackup, err := svc.Projects.Locations.Services.Backups.Get(createdBackup.Name).Do()
	require.NoError(t, err)
	require.Equal(t, "ACTIVE", gotBackup.State)
	backupList, err := svc.Projects.Locations.Services.Backups.List(backupParent).Do()
	require.NoError(t, err)
	require.NotEmpty(t, backupList.Backups)

	// CreateMetadataImport returns a done LRO.
	importID := unique("imp")
	importOp, err := svc.Projects.Locations.Services.MetadataImports.Create(backupParent, &metastore.MetadataImport{
		Description: "initial",
		DatabaseDump: &metastore.DatabaseDump{GcsUri: "gs://bucket/dump.sql"},
	}).MetadataImportId(importID).Do()
	require.NoError(t, err)
	require.True(t, importOp.Done)

	var createdImport metastore.MetadataImport
	require.NoError(t, json.Unmarshal(importOp.Response, &createdImport))
	require.Equal(t, fmt.Sprintf("%s/metadataImports/%s", backupParent, importID), createdImport.Name)
	require.Equal(t, "SUCCEEDED", createdImport.State)

	// GetMetadataImport + ListMetadataImports.
	gotImport, err := svc.Projects.Locations.Services.MetadataImports.Get(createdImport.Name).Do()
	require.NoError(t, err)
	require.Equal(t, "SUCCEEDED", gotImport.State)
	importList, err := svc.Projects.Locations.Services.MetadataImports.List(backupParent).Do()
	require.NoError(t, err)
	require.NotEmpty(t, importList.MetadataImports)

	// DeleteService returns a done LRO.
	del, err := svc.Projects.Locations.Services.Delete(wantName).Do()
	require.NoError(t, err)
	require.True(t, del.Done)

	// The service is gone.
	_, err = svc.Projects.Locations.Services.Get(wantName).Do()
	require.Error(t, err)
}
