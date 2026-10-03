package gcp

import (
	"net/http/httptest"
	"testing"
)

func TestCloudSQLCodecDecode(t *testing.T) {
	cases := []struct {
		method, path, action string
	}{
		{"GET", "/sql/v1beta4/projects/p/instances", "InstancesList"},
		{"POST", "/sql/v1beta4/projects/p/instances", "InstancesInsert"},
		{"GET", "/sql/v1beta4/projects/p/instances/i", "InstancesGet"},
		{"PUT", "/sql/v1beta4/projects/p/instances/i", "InstancesUpdate"},
		{"PATCH", "/sql/v1beta4/projects/p/instances/i", "InstancesPatch"},
		{"DELETE", "/sql/v1beta4/projects/p/instances/i", "InstancesDelete"},
		{"POST", "/sql/v1beta4/projects/p/instances/i/restart", "InstancesRestart"},
		{"GET", "/sql/v1beta4/projects/p/instances/i/databases", "DatabasesList"},
		{"POST", "/sql/v1beta4/projects/p/instances/i/databases", "DatabasesInsert"},
		{"GET", "/sql/v1beta4/projects/p/instances/i/databases/d", "DatabasesGet"},
		{"PATCH", "/sql/v1beta4/projects/p/instances/i/databases/d", "DatabasesPatch"},
		{"PUT", "/sql/v1beta4/projects/p/instances/i/databases/d", "DatabasesUpdate"},
		{"DELETE", "/sql/v1beta4/projects/p/instances/i/databases/d", "DatabasesDelete"},
		{"GET", "/sql/v1beta4/projects/p/instances/i/users", "UsersList"},
		{"POST", "/sql/v1beta4/projects/p/instances/i/users", "UsersInsert"},
		{"PUT", "/sql/v1beta4/projects/p/instances/i/users", "UsersUpdate"},
		{"DELETE", "/sql/v1beta4/projects/p/instances/i/users?name=alice&host=%25", "UsersDelete"},
		{"GET", "/sql/v1beta4/projects/p/instances/i/users/alice", "UsersGet"},
		{"GET", "/sql/v1beta4/projects/p/instances/i/connectSettings", "ConnectGet"},
		{"GET", "/sql/v1beta4/projects/p/operations", "OperationsList"},
		{"GET", "/sql/v1beta4/projects/p/operations/op1", "OperationsGet"},
		{"GET", "/sql/v1beta4/projects/p/tiers", "TiersList"},
		{"GET", "/sql/v1beta4/flags", "FlagsList"},
		// Deferred surfaces fail loud rather than 404-ing.
		{"GET", "/sql/v1beta4/projects/p/instances/i/sslCerts", "Unimplemented"},
		{"POST", "/sql/v1beta4/projects/p/instances/i/clone", "Unimplemented"},
		{"POST", "/sql/v1beta4/projects/p/instances/i:generateEphemeralCert", "Unimplemented"},
		{"POST", "/sql/v1beta4/projects/p/operations/op1/cancel", "Unimplemented"},
	}
	for _, tc := range cases {
		codec := &CloudSQLCodec{Service: "sqladmin"}
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := codec.Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
		if nr.Service != "sqladmin" {
			t.Errorf("%s %s: service = %q, want sqladmin", tc.method, tc.path, nr.Service)
		}
	}
}

func TestCloudSQLCodecParams(t *testing.T) {
	codec := &CloudSQLCodec{Service: "sqladmin"}

	nr, err := codec.Decode(httptest.NewRequest("PATCH", "/sql/v1beta4/projects/p/instances/i/databases/d", nil), nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Params["project"] != "p" || nr.Params["instance"] != "i" || nr.Params["database"] != "d" {
		t.Errorf("database params = %v", nr.Params)
	}

	nr, err = codec.Decode(httptest.NewRequest("GET", "/sql/v1beta4/projects/p/instances/i/users/alice", nil), nil)
	if err != nil {
		t.Fatalf("decode user: %v", err)
	}
	if nr.Params["instance"] != "i" || nr.Params["user"] != "alice" {
		t.Errorf("user params = %v", nr.Params)
	}

	nr, err = codec.Decode(httptest.NewRequest("DELETE", "/sql/v1beta4/projects/p/instances/i/users?name=alice&host=%25", nil), nil)
	if err != nil {
		t.Fatalf("decode user delete: %v", err)
	}
	if nr.Params["name"] != "alice" || nr.Params["host"] != "%" {
		t.Errorf("user delete params = %v", nr.Params)
	}

	nr, err = codec.Decode(httptest.NewRequest("GET", "/sql/v1beta4/projects/p/operations/op1", nil), nil)
	if err != nil {
		t.Fatalf("decode operation: %v", err)
	}
	if nr.Params["operation"] != "op1" {
		t.Errorf("operation param = %v", nr.Params["operation"])
	}

	// flags.list has no project segment.
	nr, err = codec.Decode(httptest.NewRequest("GET", "/sql/v1beta4/flags", nil), nil)
	if err != nil {
		t.Fatalf("decode flags: %v", err)
	}
	if _, ok := nr.Params["project"]; ok {
		t.Errorf("flags must not carry a project, got %v", nr.Params["project"])
	}
}

func TestDetectCloudSQLService(t *testing.T) {
	r := httptest.NewRequest("GET", "/sql/v1beta4/projects/p/instances", nil)
	svc, src := DetectService(r)
	if svc != "sqladmin" {
		t.Fatalf("DetectService = %q, want sqladmin", svc)
	}
	if src != SourcePath {
		t.Fatalf("detection source = %v, want SourcePath", src)
	}
}
