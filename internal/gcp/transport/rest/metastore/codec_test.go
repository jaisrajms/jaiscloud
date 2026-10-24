package metastore

import (
	"net/http/httptest"
	"testing"
)

func TestMetastoreCodecDecode(t *testing.T) {
	cases := []struct {
		method, path, action string
	}{
		{"POST", "/v1/projects/p/locations/us/services?serviceId=s", "CreateService"},
		{"GET", "/v1/projects/p/locations/us/services", "ListServices"},
		{"GET", "/v1/projects/p/locations/us/services/s", "GetService"},
		{"PATCH", "/v1/projects/p/locations/us/services/s", "UpdateService"},
		{"DELETE", "/v1/projects/p/locations/us/services/s", "DeleteService"},
		{"POST", "/v1/projects/p/locations/us/services/s/backups?backupId=b", "CreateBackup"},
		{"GET", "/v1/projects/p/locations/us/services/s/backups", "ListBackups"},
		{"GET", "/v1/projects/p/locations/us/services/s/backups/b", "GetBackup"},
		{"DELETE", "/v1/projects/p/locations/us/services/s/backups/b", "DeleteBackup"},
		{"POST", "/v1/projects/p/locations/us/services/s/metadataImports?metadataImportId=m", "CreateMetadataImport"},
		{"GET", "/v1/projects/p/locations/us/services/s/metadataImports", "ListMetadataImports"},
		{"GET", "/v1/projects/p/locations/us/services/s/metadataImports/m", "GetMetadataImport"},
		{"PATCH", "/v1/projects/p/locations/us/services/s/metadataImports/m", "UpdateMetadataImport"},
		{"GET", "/v1/projects/p/locations/us/operations", "ListOperations"},
		{"GET", "/v1/projects/p/locations/us/operations/op", "GetOperation"},
		{"POST", "/v1/projects/p/locations/us/services/s:exportMetadata", "ExportMetadata"},
		{"POST", "/v1/projects/p/locations/us/services/s:restore", "RestoreService"},
		{"POST", "/v1/projects/p/locations/us/services/s:queryMetadata", "QueryMetadata"},
		{"POST", "/v1/projects/p/locations/us/services/s:moveTableToDatabase", "MoveTableToDatabase"},
		{"POST", "/v1/projects/p/locations/us/services/s:alterLocation", "AlterMetadataResourceLocation"},
		{"POST", "/v1/projects/p/locations/us/federations?federationId=f", "CreateFederation"},
		{"GET", "/v1/projects/p/locations/us/federations", "ListFederations"},
		{"GET", "/v1/projects/p/locations/us/federations/f", "GetFederation"},
		{"PATCH", "/v1/projects/p/locations/us/federations/f", "UpdateFederation"},
		{"DELETE", "/v1/projects/p/locations/us/federations/f", "DeleteFederation"},
		{"GET", "/v1/projects/p/locations/us/services/s:getIamPolicy", "ServiceGetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/services/s:setIamPolicy", "ServiceSetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/services/s:testIamPermissions", "ServiceTestIamPermissions"},
		{"GET", "/v1/projects/p/locations/us/services/s/backups/b:getIamPolicy", "BackupGetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/services/s/backups/b:setIamPolicy", "BackupSetIamPolicy"},
		{"GET", "/v1/projects/p/locations/us/services/s/databases/d:getIamPolicy", "DatabaseGetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/services/s/databases/d:setIamPolicy", "DatabaseSetIamPolicy"},
		{"GET", "/v1/projects/p/locations/us/services/s/databases/d/tables/t:getIamPolicy", "TableGetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/services/s/databases/d/tables/t:setIamPolicy", "TableSetIamPolicy"},
		{"GET", "/v1/projects/p/locations/us/federations/f:getIamPolicy", "FederationGetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/federations/f:setIamPolicy", "FederationSetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/federations/f:testIamPermissions", "FederationTestIamPermissions"},
	}
	for _, tc := range cases {
		codec := NewCodec()
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := codec.Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
	}
}
