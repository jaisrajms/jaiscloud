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
