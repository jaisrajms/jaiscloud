//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// firestoreAdminScenario compares the Firestore Admin control plane over REST and
// gRPC (AUD5-1): databases, collection-group fields, user creds and backup
// schedules. The REST surface is served under the `firestore` wire service
// (firestore.googleapis.com/v1/projects/{p}/databases/...) while the gRPC twin is
// google.firestore.admin.v1.FirestoreAdmin; both adapt the same transport-neutral
// provider, so a field one adapter's transcode drops or invents is caught the
// same way the document scenario catches a Value-union divergence.
//
// State is staged once over gRPC (the reads then observe identical resources on
// both transports); the database update is applied over REST and read back over
// both, so a REST-only transcode gap cannot hide. The backups surface is
// read-only (no create path) and is not exercised here.
func firestoreAdminScenario() Scenario {
	project := func(e *Env) string { return e.Cfg.Project }
	// dbID is a valid database id (lowercase letter start, letters/digits/hyphens)
	// and unique per run, so concurrent runs cannot collide.
	dbID := func(e *Env) string { return e.Resource("fsadb") }
	cg := func(e *Env) string { return "cg" + e.Cfg.Suffix }
	fieldPath := func(e *Env) string { return "fp" + e.Cfg.Suffix }
	ucID := func(e *Env) string { return e.Resource("fsuc") }

	adminRoot := func(e *Env) string { return "projects/" + project(e) + "/databases/" }
	dbName := func(e *Env) string { return adminRoot(e) + dbID(e) }
	fieldName := func(e *Env) string {
		return dbName(e) + "/collectionGroups/" + cg(e) + "/fields/" + fieldPath(e)
	}
	ucName := func(e *Env) string { return dbName(e) + "/userCreds/" + ucID(e) }
	// Backup schedules have no client-supplied id; the emulator (like the index
	// surface) assigns "1". The schedule is scoped by its unique database id.
	scheduleName := func(e *Env) string { return dbName(e) + "/backupSchedules/1" }

	// ─── gRPC side ────────────────────────────────────────────────────────────
	createDBGRPC := func(ctx context.Context, e *Env) error {
		return fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			_, err := c.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
				Parent:     "projects/" + project(e),
				DatabaseId: dbID(e),
				Database:   &adminpb.Database{LocationId: "nam5"},
			})
			return err
		})
	}
	updateFieldGRPC := func(ctx context.Context, e *Env) error {
		return fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			_, err := c.UpdateField(ctx, &adminpb.UpdateFieldRequest{
				Field: &adminpb.Field{
					Name: fieldName(e),
					IndexConfig: &adminpb.Field_IndexConfig{
						Indexes: []*adminpb.Index{{
							QueryScope: adminpb.Index_COLLECTION,
							Fields: []*adminpb.Index_IndexField{{
								FieldPath: fieldPath(e),
								ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_ASCENDING},
							}},
						}},
					},
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"index_config"}},
			})
			return err
		})
	}
	createUCGRPC := func(ctx context.Context, e *Env) error {
		return fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			_, err := c.CreateUserCreds(ctx, &adminpb.CreateUserCredsRequest{
				Parent:      dbName(e),
				UserCredsId: ucID(e),
			})
			return err
		})
	}
	createScheduleGRPC := func(ctx context.Context, e *Env) error {
		return fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			_, err := c.CreateBackupSchedule(ctx, &adminpb.CreateBackupScheduleRequest{
				Parent: dbName(e),
				BackupSchedule: &adminpb.BackupSchedule{
					Retention: durationpb.New(time.Hour),
				},
			})
			return err
		})
	}

	seed := func(ctx context.Context, e *Env) error {
		if err := createDBGRPC(ctx, e); err != nil {
			return err
		}
		if err := updateFieldGRPC(ctx, e); err != nil {
			return err
		}
		if err := createUCGRPC(ctx, e); err != nil {
			return err
		}
		return createScheduleGRPC(ctx, e)
	}

	// ─── REST side ────────────────────────────────────────────────────────────
	restPrefix := func(e *Env) string { return "/v1/projects/" + project(e) + "/databases/" + dbID(e) }
	updateDBREST := func(ctx context.Context, e *Env) error {
		_, err := e.Rest(ctx, http.MethodPatch, restPrefix(e)+"?updateMask=concurrencyMode",
			`{"concurrencyMode":"PESSIMISTIC"}`)
		return err
	}

	// ─── read steps (same resource over both transports) ──────────────────────
	getDatabase := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *adminpb.Database
		err := fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			var cerr error
			out, cerr = c.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: dbName(e)})
			return cerr
		})
		return out, err
	}
	listDatabases := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *adminpb.ListDatabasesResponse
		err := fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			var cerr error
			out, cerr = c.ListDatabases(ctx, &adminpb.ListDatabasesRequest{Parent: "projects/" + project(e)})
			return cerr
		})
		return out, err
	}
	getField := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *adminpb.Field
		err := fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			var cerr error
			out, cerr = c.GetField(ctx, &adminpb.GetFieldRequest{Name: fieldName(e)})
			return cerr
		})
		return out, err
	}
	listFields := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *adminpb.ListFieldsResponse
		err := fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			var cerr error
			out, cerr = c.ListFields(ctx, &adminpb.ListFieldsRequest{
				Parent: dbName(e) + "/collectionGroups/" + cg(e),
			})
			return cerr
		})
		return out, err
	}
	getUC := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *adminpb.UserCreds
		err := fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			var cerr error
			out, cerr = c.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: ucName(e)})
			return cerr
		})
		return out, err
	}
	listUC := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *adminpb.ListUserCredsResponse
		err := fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			var cerr error
			out, cerr = c.ListUserCreds(ctx, &adminpb.ListUserCredsRequest{Parent: dbName(e)})
			return cerr
		})
		return out, err
	}
	getSchedule := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *adminpb.BackupSchedule
		err := fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			var cerr error
			out, cerr = c.GetBackupSchedule(ctx, &adminpb.GetBackupScheduleRequest{Name: scheduleName(e)})
			return cerr
		})
		return out, err
	}
	listSchedules := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *adminpb.ListBackupSchedulesResponse
		err := fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			var cerr error
			out, cerr = c.ListBackupSchedules(ctx, &adminpb.ListBackupSchedulesRequest{Parent: dbName(e)})
			return cerr
		})
		return out, err
	}

	// ─── cleanup (best-effort; runs as the final Mutate) ──────────────────────
	cleanup := func(ctx context.Context, e *Env) error {
		_ = e.RestDelete(ctx, restPrefix(e)+"/userCreds/"+ucID(e))
		_ = e.RestDelete(ctx, restPrefix(e)+"/backupSchedules/1")
		return e.RestDelete(ctx, restPrefix(e))
	}

	// ─── LRO head-to-head parity (AUD5-3) ────────────────────────────────────
	// The control-plane mutations return a terminal google.longrunning.Operation
	// whose typed response must transcode identically over each transport. The
	// gRPC side unwraps the operation's Any response; the REST side uses
	// RestOperationResource (which unwraps "response" and strips "@type"), so the
	// two Database bodies diff directly.
	createPath := func(e *Env) string {
		return "/v1/projects/" + project(e) + "/databases?databaseId=" + dbID(e)
	}
	cleanupDatabase := func(ctx context.Context, e *Env) error {
		return e.RestDelete(ctx, restPrefix(e))
	}
	createDatabaseGRPCResource := func(ctx context.Context, e *Env) (protoMessage, error) {
		var out *adminpb.Database
		err := fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			op, cerr := c.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
				Parent:     "projects/" + project(e),
				DatabaseId: dbID(e),
				Database:   &adminpb.Database{LocationId: "nam5"},
			})
			if cerr != nil {
				return cerr
			}
			out = &adminpb.Database{}
			return op.GetResponse().UnmarshalTo(out)
		})
		return out, err
	}
	createDatabaseRESTResource := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.RestOperationResource(ctx, http.MethodPost, createPath(e), `{"locationId":"nam5"}`)
	}
	updateDatabaseGRPCResource := func(ctx context.Context, e *Env) (protoMessage, error) {
		if err := createDBGRPC(ctx, e); err != nil {
			return nil, err
		}
		var out *adminpb.Database
		err := fsAdminDial(ctx, e, func(c adminpb.FirestoreAdminClient) error {
			op, cerr := c.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
				Database: &adminpb.Database{
					Name:            dbName(e),
					ConcurrencyMode: adminpb.Database_PESSIMISTIC,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"concurrency_mode"}},
			})
			if cerr != nil {
				return cerr
			}
			out = &adminpb.Database{}
			return op.GetResponse().UnmarshalTo(out)
		})
		return out, err
	}
	updateDatabaseRESTResource := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		if _, err := e.Rest(ctx, http.MethodPost, createPath(e), `{"locationId":"nam5"}`); err != nil {
			return nil, err
		}
		return e.RestOperationResource(ctx, http.MethodPatch, restPrefix(e)+"?updateMask=concurrencyMode",
			`{"concurrencyMode":"PESSIMISTIC"}`)
	}

	return Scenario{Service: "firestoreadmin", Steps: []Step{
		{
			Op: "CreateDatabase (LRO parity)",
			Mutation: &MutationParity{
				GRPC: createDatabaseGRPCResource, REST: createDatabaseRESTResource, Cleanup: cleanupDatabase,
			},
		},
		{
			Op: "UpdateDatabase (LRO parity)",
			Mutation: &MutationParity{
				GRPC: updateDatabaseGRPCResource, REST: updateDatabaseRESTResource, Cleanup: cleanupDatabase,
			},
		},
		{Op: "SeedDatabase+Field+UserCreds+Schedule", Mutate: seed},
		{
			Op:   "GetDatabase",
			GRPC: getDatabase, REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, http.MethodGet, restPrefix(e), "")
			},
		},
		{
			Op: "ListDatabases", Scope: true,
			GRPC: listDatabases, REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, http.MethodGet, "/v1/projects/"+project(e)+"/databases", "")
			},
		},
		{Op: "UpdateDatabase", Mutate: updateDBREST},
		{
			Op:   "GetDatabase(updated)",
			GRPC: getDatabase, REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, http.MethodGet, restPrefix(e), "")
			},
		},
		{
			Op:   "GetField",
			GRPC: getField, REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, http.MethodGet, restPrefix(e)+"/collectionGroups/"+cg(e)+"/fields/"+fieldPath(e), "")
			},
		},
		{
			Op: "ListFields", Scope: true,
			GRPC: listFields, REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, http.MethodGet, restPrefix(e)+"/collectionGroups/"+cg(e)+"/fields", "")
			},
		},
		{
			Op:   "GetUserCreds",
			GRPC: getUC, REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, http.MethodGet, restPrefix(e)+"/userCreds/"+ucID(e), "")
			},
		},
		{
			Op: "ListUserCreds", Scope: true,
			GRPC: listUC, REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, http.MethodGet, restPrefix(e)+"/userCreds", "")
			},
		},
		{
			Op:   "GetBackupSchedule",
			GRPC: getSchedule, REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, http.MethodGet, restPrefix(e)+"/backupSchedules/1", "")
			},
		},
		{
			Op: "ListBackupSchedules", Scope: true,
			GRPC: listSchedules, REST: func(ctx context.Context, e *Env) (json.RawMessage, error) {
				return e.Rest(ctx, http.MethodGet, restPrefix(e)+"/backupSchedules", "")
			},
		},
		{Op: "Cleanup", Mutate: cleanup},
	}}
}

// fsAdminDial opens a one-shot Firestore Admin gRPC connection and runs fn with
// the generated stub client, mirroring fsDial for the data plane.
func fsAdminDial(ctx context.Context, e *Env, fn func(adminpb.FirestoreAdminClient) error) error {
	conn, err := grpc.NewClient(e.Cfg.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(adminpb.NewFirestoreAdminClient(conn))
}
