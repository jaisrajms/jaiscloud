package grpcconformance

import (
	"context"
	"fmt"
	"strings"

	admin "cloud.google.com/go/firestore/apiv1/admin"
	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// firestoreAdminChecks covers the Firestore Admin gRPC surface
// (google.firestore.admin.v1.FirestoreAdmin) composite-index CRUD via the
// official admin client. Create/Get/Delete drive the GAPIC
// cloud.google.com/go/firestore/apiv1/admin client (so CreateIndex exercises
// the long-running-operation wrapper); ListIndexes drives the generated
// adminpb stub directly because the GAPIC iterator hides next_page_token.
//
// Checks run in registry order and share one run-unique collection group: the
// Create probe leaves two indexes in place for Get/List/Delete. Index IDs are
// server-generated (there is no client-supplied id), so the later probes
// re-discover names by listing. Names are run-unique via cfg.Suffix, so a
// long-lived emulator never sees cross-run collisions, and the Delete probe
// cleans its fixtures up.
func firestoreAdminChecks() []Check {
	return []Check{
		{Service: "firestoreadmin", RPC: "CreateIndex", Method: "CreateIndex", KeyField: "operation done=true; index state READY", Run: checkFirestoreAdminCreateIndex},
		{Service: "firestoreadmin", RPC: "GetIndex", Method: "GetIndex", KeyField: "index fields/state", Run: checkFirestoreAdminGetIndex},
		{Service: "firestoreadmin", RPC: "ListIndexes", Method: "ListIndexes", KeyField: "pageSize/pageToken pages over two indexes", Run: checkFirestoreAdminListIndexesPaged},
		{Service: "firestoreadmin", RPC: "DeleteIndex", Method: "DeleteIndex", KeyField: "index absent after delete", Run: checkFirestoreAdminDeleteIndex},
	}
}

// firestoreAdminCollectionGroup is the run-unique collection group all index
// fixtures live under.
func firestoreAdminCollectionGroup(cfg Config) string { return cfg.ResourceName("gcpc_fsadmin") }

// firestoreAdminParent is the index parent resource name.
func firestoreAdminParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/databases/(default)/collectionGroups/%s", cfg.Project, firestoreAdminCollectionGroup(cfg))
}

// firestoreAdminField returns a run-unique field path. Field paths must not
// collide with fixtures from other runs, so both indexes are identified by the
// run suffix.
func firestoreAdminField(cfg Config, name string) string { return cfg.ResourceName("fsadmin_" + name) }

// firestoreAdminAsc builds an ascending order field.
func firestoreAdminAsc(path string) *adminpb.Index_IndexField {
	return &adminpb.Index_IndexField{
		FieldPath: path,
		ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_ASCENDING},
	}
}

// firestoreAdminDesc builds a descending order field.
func firestoreAdminDesc(path string) *adminpb.Index_IndexField {
	return &adminpb.Index_IndexField{
		FieldPath: path,
		ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_DESCENDING},
	}
}

// newFirestoreAdminClient dials the emulator and returns the official GAPIC
// admin client (insecure endpoint, authentication disabled), mirroring the
// operations/KMS probes.
func newFirestoreAdminClient(ctx context.Context, cfg Config) (*admin.FirestoreAdminClient, error) {
	return admin.NewFirestoreAdminClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

// newFirestoreAdminStub dials the emulator and returns the generated adminpb
// client for the paginated ListIndexes probe. The returned conn must be closed
// by the caller.
func newFirestoreAdminStub(cfg Config) (adminpb.FirestoreAdminClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(cfg.GRPCAddr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", cfg.GRPCAddr(), err)
	}
	return adminpb.NewFirestoreAdminClient(conn), conn, nil
}

// firestoreAdminListAll returns every index under the parent, walking pages of
// size pageSize.
func firestoreAdminListAll(ctx context.Context, stub adminpb.FirestoreAdminClient, cfg Config, pageSize int32) ([]*adminpb.Index, error) {
	var all []*adminpb.Index
	token := ""
	for {
		resp, err := stub.ListIndexes(ctx, &adminpb.ListIndexesRequest{
			Parent:    firestoreAdminParent(cfg),
			PageSize:  pageSize,
			PageToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("ListIndexes: %w", err)
		}
		all = append(all, resp.GetIndexes()...)
		token = resp.GetNextPageToken()
		if token == "" {
			return all, nil
		}
	}
}

// firestoreAdminFindIndex lists the collection group and returns the index
// whose first field path is fieldPath.
func firestoreAdminFindIndex(ctx context.Context, stub adminpb.FirestoreAdminClient, cfg Config, fieldPath string) (*adminpb.Index, error) {
	idxs, err := firestoreAdminListAll(ctx, stub, cfg, 0)
	if err != nil {
		return nil, err
	}
	for _, idx := range idxs {
		if len(idx.GetFields()) > 0 && idx.GetFields()[0].GetFieldPath() == fieldPath {
			return idx, nil
		}
	}
	return nil, fmt.Errorf("index with first field %q not found among %d indexes", fieldPath, len(idxs))
}

// Check 1: CreateIndex returns a terminal operation wrapping a READY index. Two
// indexes are created so the later Get/List/Delete probes have fixtures.
func checkFirestoreAdminCreateIndex(ctx context.Context, cfg Config) error {
	client, err := newFirestoreAdminClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	fa, fb := firestoreAdminField(cfg, "a"), firestoreAdminField(cfg, "b")
	fc, fd := firestoreAdminField(cfg, "c"), firestoreAdminField(cfg, "d")

	op, err := client.CreateIndex(ctx, &adminpb.CreateIndexRequest{
		Parent: firestoreAdminParent(cfg),
		Index: &adminpb.Index{
			QueryScope: adminpb.Index_COLLECTION,
			Fields:     []*adminpb.Index_IndexField{firestoreAdminAsc(fa), firestoreAdminAsc(fb)},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateIndex: %w", err)
	}
	if !op.Done() {
		return fmt.Errorf("CreateIndex operation done = false, want true")
	}
	first, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("CreateIndex Wait: %w", err)
	}
	if wantPrefix := firestoreAdminParent(cfg) + "/indexes/"; !strings.HasPrefix(first.GetName(), wantPrefix) {
		return fmt.Errorf("CreateIndex name = %q, want prefix %q", first.GetName(), wantPrefix)
	}
	if first.GetState() != adminpb.Index_READY {
		return fmt.Errorf("CreateIndex state = %v, want READY", first.GetState())
	}

	second, err := client.CreateIndex(ctx, &adminpb.CreateIndexRequest{
		Parent: firestoreAdminParent(cfg),
		Index: &adminpb.Index{
			QueryScope: adminpb.Index_COLLECTION,
			Fields:     []*adminpb.Index_IndexField{firestoreAdminAsc(fc), firestoreAdminDesc(fd)},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateIndex (second): %w", err)
	}
	if _, err := second.Wait(ctx); err != nil {
		return fmt.Errorf("CreateIndex (second) Wait: %w", err)
	}
	return nil
}

// Check 2: GetIndex returns the created index with its fields and READY state.
func checkFirestoreAdminGetIndex(ctx context.Context, cfg Config) error {
	client, err := newFirestoreAdminClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	want := firestoreAdminField(cfg, "a")
	found, err := firestoreAdminFindIndex(ctx, stub, cfg, want)
	if err != nil {
		return err
	}
	got, err := client.GetIndex(ctx, &adminpb.GetIndexRequest{Name: found.GetName()})
	if err != nil {
		return fmt.Errorf("GetIndex: %w", err)
	}
	if got.GetName() != found.GetName() {
		return fmt.Errorf("GetIndex name = %q, want %q", got.GetName(), found.GetName())
	}
	if got.GetState() != adminpb.Index_READY {
		return fmt.Errorf("GetIndex state = %v, want READY", got.GetState())
	}
	if len(got.GetFields()) != 2 || got.GetFields()[0].GetFieldPath() != want {
		return fmt.Errorf("GetIndex fields = %v, want first field %q", got.GetFields(), want)
	}
	return nil
}

// Check 3: ListIndexes honors page_size/page_token. With two run-unique
// indexes and pageSize=1, a full walk must visit each exactly once and must
// have produced at least one non-empty token.
func checkFirestoreAdminListIndexesPaged(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	a, c := firestoreAdminField(cfg, "a"), firestoreAdminField(cfg, "c")
	seen := map[string]bool{}
	pages, sawToken := 0, false
	token := ""
	for {
		resp, err := stub.ListIndexes(ctx, &adminpb.ListIndexesRequest{
			Parent:    firestoreAdminParent(cfg),
			PageSize:  1,
			PageToken: token,
		})
		if err != nil {
			return fmt.Errorf("ListIndexes: %w", err)
		}
		pages++
		if len(resp.GetIndexes()) > 1 {
			return fmt.Errorf("ListIndexes page returned %d indexes, want <= pageSize 1", len(resp.GetIndexes()))
		}
		for _, idx := range resp.GetIndexes() {
			if len(idx.GetFields()) == 0 {
				continue
			}
			fp := idx.GetFields()[0].GetFieldPath()
			if fp != a && fp != c {
				continue
			}
			if seen[fp] {
				return fmt.Errorf("ListIndexes returned %q twice across pages", fp)
			}
			seen[fp] = true
		}
		token = resp.GetNextPageToken()
		if token == "" {
			break
		}
		sawToken = true
		if pages > 20 {
			return fmt.Errorf("ListIndexes did not terminate after 20 pages")
		}
	}
	if !seen[a] || !seen[c] {
		return fmt.Errorf("ListIndexes pages did not cover both fixtures (a=%t c=%t)", seen[a], seen[c])
	}
	if !sawToken {
		return fmt.Errorf("ListIndexes pageSize=1 never returned a next_page_token for two indexes")
	}
	return nil
}

// Check 4: DeleteIndex removes the created indexes; a follow-up GetIndex must
// report NotFound.
func checkFirestoreAdminDeleteIndex(ctx context.Context, cfg Config) error {
	client, err := newFirestoreAdminClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	target, err := firestoreAdminFindIndex(ctx, stub, cfg, firestoreAdminField(cfg, "a"))
	if err != nil {
		return err
	}
	other, err := firestoreAdminFindIndex(ctx, stub, cfg, firestoreAdminField(cfg, "c"))
	if err != nil {
		return err
	}
	for _, name := range []string{target.GetName(), other.GetName()} {
		if err := client.DeleteIndex(ctx, &adminpb.DeleteIndexRequest{Name: name}); err != nil {
			return fmt.Errorf("DeleteIndex %q: %w", name, err)
		}
	}
	if _, err := client.GetIndex(ctx, &adminpb.GetIndexRequest{Name: target.GetName()}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetIndex after delete = %v, want NotFound", err)
	}
	return nil
}
