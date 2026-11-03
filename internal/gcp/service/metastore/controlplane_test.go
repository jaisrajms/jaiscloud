package metastore

import (
	"context"
	"errors"
	"strings"
	"testing"

	hmsstore "jaiscloud/internal/gcp/store/hms"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
	"jaiscloud/internal/model"
)

// fakeCatalogAdmin records the catalog mutations the control plane applies. The
// Hive-struct rewriting itself is covered against a real store in
// internal/gcp/hms (see catalog_admin_test.go); this fake pins what the control
// plane asks the catalog to do.
type fakeCatalogAdmin struct {
	moves      [][3]string
	dbLoc      [][2]string
	tableLoc   [][3]string
	partLoc    [][4]string
	err        error
	partValues []string
}

func (f *fakeCatalogAdmin) MoveTable(_ context.Context, db, tableName, destDB string) error {
	f.moves = append(f.moves, [3]string{db, tableName, destDB})
	return f.err
}

func (f *fakeCatalogAdmin) SetDatabaseLocation(_ context.Context, db, locationURI string) error {
	f.dbLoc = append(f.dbLoc, [2]string{db, locationURI})
	return f.err
}

func (f *fakeCatalogAdmin) SetTableLocation(_ context.Context, db, tableName, locationURI string) error {
	f.tableLoc = append(f.tableLoc, [3]string{db, tableName, locationURI})
	return f.err
}

func (f *fakeCatalogAdmin) SetPartitionLocation(_ context.Context, db, tableName string, values []string, locationURI string) error {
	f.partValues = values
	f.partLoc = append(f.partLoc, [4]string{db, tableName, locationURI, ""})
	return f.err
}

// newAdminFixture builds a core with one ACTIVE service and the given catalog
// admin behind the host seam.
func newAdminFixture(t *testing.T, admin CatalogAdmin) (*Service, *fakeCatalogHost) {
	t.Helper()
	host := newFakeCatalogHost()
	host.admin = admin
	s := NewService(metastorestore.NewMemoryStore(), WithCatalogHost(host), WithHMSPort(9083))
	if _, _, err := s.CreateService(context.Background(), "proj", "us-central1", "hms", nil); err != nil {
		t.Fatalf("create service: %v", err)
	}
	return s, host
}

// TestMoveTableToDatabase covers the control-plane half of the move: it asks the
// Service's own catalog to move the table, and returns a done operation whose
// response is the method's (empty) MoveTableToDatabaseResponse.
func TestMoveTableToDatabase(t *testing.T) {
	ctx := context.Background()
	admin := &fakeCatalogAdmin{}
	s, _ := newAdminFixture(t, admin)

	op, err := s.MoveTableToDatabase(ctx, "proj", "us-central1", "hms", "db1", "t1", "db2")
	if err != nil {
		t.Fatalf("MoveTableToDatabase: %v", err)
	}
	if len(admin.moves) != 1 || admin.moves[0] != [3]string{"db1", "t1", "db2"} {
		t.Fatalf("catalog moves = %v", admin.moves)
	}
	if !op.Done || op.Verb != "moveTableToDatabase" {
		t.Fatalf("operation = %+v", op)
	}
	if want := ServiceName("proj", "us-central1", "hms"); op.Target != want {
		t.Fatalf("operation target = %q, want %q", op.Target, want)
	}
	if !containsTypeURL(op.Response, moveTableResponseTypeURL) {
		t.Fatalf("operation response = %s, want @type %s", op.Response, moveTableResponseTypeURL)
	}
	// The operation is retrievable from the store, as a client poll requires.
	got, err := s.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil || !got.Done {
		t.Fatalf("GetOperation = %+v, %v", got, err)
	}
}

// TestAlterMetadataResourceLocation covers all three addressed resources: a
// database (its root directory), a table and a partition (their storage
// descriptors), including the Hive partition-name form of a partition id.
func TestAlterMetadataResourceLocation(t *testing.T) {
	ctx := context.Background()
	admin := &fakeCatalogAdmin{}
	s, _ := newAdminFixture(t, admin)

	cases := []struct {
		name     string
		resource string
		want     string // "db" | "table" | "part"
		values   []string
	}{
		{"database", "databases/db1", "db", nil},
		{"table", "databases/db1/tables/t1", "table", nil},
		{"partition name", "databases/db1/tables/t1/partitions/dt=2026-01-01", "part", []string{"2026-01-01"}},
		{"partition tuple", "databases/db1/tables/t1/partitions/year=2026/month=02/hour=03", "part", []string{"2026", "02", "03"}},
		{"partition bare value", "databases/db1/tables/t1/partitions/2026-01-01", "part", []string{"2026-01-01"}},
	}
	for _, tc := range cases {
		admin.dbLoc, admin.tableLoc, admin.partLoc = nil, nil, nil
		location := "gs://wh/new-" + tc.name
		op, err := s.AlterMetadataResourceLocation(ctx, "proj", "us-central1", "hms", tc.resource, location)
		if err != nil {
			t.Fatalf("%s: AlterMetadataResourceLocation: %v", tc.name, err)
		}
		if !op.Done || op.Verb != "alterLocation" {
			t.Fatalf("%s: operation = %+v", tc.name, op)
		}
		if !containsTypeURL(op.Response, alterLocationResponseTypeURL) {
			t.Fatalf("%s: operation response = %s", tc.name, op.Response)
		}
		switch tc.want {
		case "db":
			if len(admin.dbLoc) != 1 || admin.dbLoc[0] != [2]string{"db1", location} {
				t.Fatalf("%s: database mutation = %v", tc.name, admin.dbLoc)
			}
			if len(admin.tableLoc)+len(admin.partLoc) != 0 {
				t.Fatalf("%s: database arm also touched a table/partition", tc.name)
			}
		case "table":
			if len(admin.tableLoc) != 1 || admin.tableLoc[0] != [3]string{"db1", "t1", location} {
				t.Fatalf("%s: table mutation = %v", tc.name, admin.tableLoc)
			}
			if len(admin.dbLoc)+len(admin.partLoc) != 0 {
				t.Fatalf("%s: table arm also touched a database/partition", tc.name)
			}
		case "part":
			if len(admin.partLoc) != 1 || admin.partLoc[0] != [4]string{"db1", "t1", location, ""} {
				t.Fatalf("%s: partition mutation = %v", tc.name, admin.partLoc)
			}
			if len(admin.partValues) != len(tc.values) {
				t.Fatalf("%s: partition values = %v, want %v", tc.name, admin.partValues, tc.values)
			}
			for j := range tc.values {
				if admin.partValues[j] != tc.values[j] {
					t.Fatalf("%s: partition values = %v, want %v", tc.name, admin.partValues, tc.values)
				}
			}
		}
	}
}

// TestAdminOperationsFailLoud pins every failure the admin/DR surface reports:
// a malformed request is InvalidArgument, an unknown Service is NotFound, a
// missing catalog resource is NotFound, and a deployment that serves no catalog
// is a failed precondition — never a fabricated success.
func TestAdminOperationsFailLoud(t *testing.T) {
	ctx := context.Background()
	admin := &fakeCatalogAdmin{}
	s, host := newAdminFixture(t, admin)

	invalidArgs := []struct {
		name string
		run  func() error
	}{
		{"move: missing table", func() error {
			_, err := s.MoveTableToDatabase(ctx, "proj", "us-central1", "hms", "db1", "", "db2")
			return err
		}},
		{"move: missing destination", func() error {
			_, err := s.MoveTableToDatabase(ctx, "proj", "us-central1", "hms", "db1", "t1", "")
			return err
		}},
		{"alter: missing location", func() error {
			_, err := s.AlterMetadataResourceLocation(ctx, "proj", "us-central1", "hms", "databases/db1", "")
			return err
		}},
		{"alter: malformed resource", func() error {
			_, err := s.AlterMetadataResourceLocation(ctx, "proj", "us-central1", "hms", "db1", "gs://wh/x")
			return err
		}},
		{"alter: unknown resource kind", func() error {
			_, err := s.AlterMetadataResourceLocation(ctx, "proj", "us-central1", "hms", "databases/db1/views/v1", "gs://wh/x")
			return err
		}},
		{"alter: empty partition id", func() error {
			_, err := s.AlterMetadataResourceLocation(ctx, "proj", "us-central1", "hms", "databases/db1/tables/t1/partitions/", "gs://wh/x")
			return err
		}},
	}
	for _, tc := range invalidArgs {
		var perr *model.ProviderError
		if err := tc.run(); !errors.As(err, &perr) || perr.Code != "InvalidArgument" {
			t.Fatalf("%s: err = %v, want InvalidArgument", tc.name, err)
		}
	}
	if len(admin.moves)+len(admin.dbLoc)+len(admin.tableLoc)+len(admin.partLoc) != 0 {
		t.Fatal("an invalid request mutated the catalog")
	}

	// Unknown Service.
	if _, err := s.MoveTableToDatabase(ctx, "proj", "us-central1", "nope", "db1", "t1", "db2"); !IsNotFound(err) {
		t.Fatalf("unknown service: err = %v, want NotFound", err)
	}
	if _, err := s.AlterMetadataResourceLocation(ctx, "proj", "us-central1", "nope", "databases/db1", "gs://wh/x"); !IsNotFound(err) {
		t.Fatalf("unknown service: err = %v, want NotFound", err)
	}

	// A missing catalog resource is the serving plane's NotFound, mapped onto the
	// control plane's wire error.
	for _, storeErr := range []error{hmsstore.ErrDatabaseNotFound, hmsstore.ErrTableNotFound, hmsstore.ErrPartitionNotFound} {
		admin.err = storeErr
		if _, err := s.MoveTableToDatabase(ctx, "proj", "us-central1", "hms", "db1", "t1", "db2"); !IsNotFound(err) {
			t.Fatalf("%v: err = %v, want NotFound", storeErr, err)
		}
	}
	admin.err = hmsstore.ErrTableExists
	var perr *model.ProviderError
	if _, err := s.MoveTableToDatabase(ctx, "proj", "us-central1", "hms", "db1", "t1", "db2"); !errors.As(err, &perr) || perr.Code != "AlreadyExists" || perr.HTTPStatus != 409 {
		t.Fatalf("destination conflict: err = %v, want AlreadyExists/409", err)
	}
	admin.err = nil

	// Two ways a deployment serves no catalog for the Service.
	host.admin = nil
	if _, err := s.MoveTableToDatabase(ctx, "proj", "us-central1", "hms", "db1", "t1", "db2"); !isFailedPrecondition(err) {
		t.Fatalf("no catalog: err = %v, want FailedPrecondition", err)
	}
	host.catalogErr = errors.New("port unavailable")
	if _, err := s.AlterMetadataResourceLocation(ctx, "proj", "us-central1", "hms", "databases/db1", "gs://wh/x"); !isFailedPrecondition(err) {
		t.Fatalf("catalog error: err = %v, want FailedPrecondition", err)
	}

	// Without a host at all (metastore disabled), the mutation is still refused.
	plain := NewService(metastorestore.NewMemoryStore())
	if _, _, err := plain.CreateService(ctx, "proj", "us-central1", "hms", nil); err != nil {
		t.Fatalf("create service: %v", err)
	}
	if _, err := plain.MoveTableToDatabase(ctx, "proj", "us-central1", "hms", "db1", "t1", "db2"); !isFailedPrecondition(err) {
		t.Fatalf("hostless: err = %v, want FailedPrecondition", err)
	}
}

// isFailedPrecondition reports whether err is the FAILED_PRECONDITION the admin
// surface reports for a Service whose catalog is not served.
func isFailedPrecondition(err error) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.Code == "FailedPrecondition" && perr.HTTPStatus == 412
}

func containsTypeURL(response, typeURL string) bool {
	return strings.Contains(response, `"@type":"`+typeURL+`"`)
}
