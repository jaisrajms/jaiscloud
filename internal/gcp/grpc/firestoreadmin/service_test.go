package firestoreadmin

import (
	"context"
	"net"
	"testing"

	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const testParent = "projects/proj/databases/(default)/collectionGroups/cities"

// newAdminClient starts an in-process gRPC server hosting the FirestoreAdmin
// service over a memory store and returns a raw generated client. resources
// may be nil to exercise the provider's nil-store fast path.
func newAdminClient(t *testing.T, resources store.ResourceStore) (adminpb.FirestoreAdminClient, func()) {
	t.Helper()
	providerSvc := firestoreprovider.New(firestorestore.NewMemoryStore(), resources).Service
	svc := NewService(providerSvc, "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	adminpb.RegisterFirestoreAdminServer(srv, svc)
	go func() { _ = srv.Serve(ln) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return adminpb.NewFirestoreAdminClient(conn), cleanup
}

// asc and desc build ordered index fields.
func asc(path string) *adminpb.Index_IndexField {
	return &adminpb.Index_IndexField{FieldPath: path, ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_ASCENDING}}
}

func desc(path string) *adminpb.Index_IndexField {
	return &adminpb.Index_IndexField{FieldPath: path, ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_DESCENDING}}
}

// createIndex creates a COLLECTION-scoped index and returns the terminal
// operation's Index.
func createIndex(t *testing.T, client adminpb.FirestoreAdminClient, parent string, fields ...*adminpb.Index_IndexField) *adminpb.Index {
	t.Helper()
	return createIndexScoped(t, client, parent, adminpb.Index_COLLECTION, fields...)
}

// createIndexScoped creates an index with an explicit query scope and returns
// the terminal operation's Index.
func createIndexScoped(t *testing.T, client adminpb.FirestoreAdminClient, parent string, scope adminpb.Index_QueryScope, fields ...*adminpb.Index_IndexField) *adminpb.Index {
	t.Helper()
	op, err := client.CreateIndex(context.Background(), &adminpb.CreateIndexRequest{
		Parent: parent,
		Index:  &adminpb.Index{QueryScope: scope, Fields: fields},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("CreateIndex done = false, want true")
	}
	if op.GetResponse() == nil {
		t.Fatalf("CreateIndex result = %T, want Operation_Response", op.GetResult())
	}
	idx := &adminpb.Index{}
	if err := op.GetResponse().UnmarshalTo(idx); err != nil {
		t.Fatalf("CreateIndex response unmarshal: %v", err)
	}
	return idx
}

func TestCreateGetListDeleteRoundTrip(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	created := createIndex(t, client, testParent, asc("a"), asc("b"))
	if created.GetName() == "" || created.GetState() != adminpb.Index_READY {
		t.Fatalf("created index = %+v, want non-empty name and READY", created)
	}

	got, err := client.GetIndex(ctx, &adminpb.GetIndexRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("GetIndex: %v", err)
	}
	if got.GetName() != created.GetName() || len(got.GetFields()) != 3 {
		t.Fatalf("GetIndex = %+v, want the created index plus the appended __name__", got)
	}
	if last := got.GetFields()[2]; last.GetFieldPath() != "__name__" || last.GetOrder() != adminpb.Index_IndexField_ASCENDING {
		t.Fatalf("trailing field = %+v, want __name__ ASCENDING (the last declared field's direction)", last)
	}

	list, err := client.ListIndexes(ctx, &adminpb.ListIndexesRequest{Parent: testParent})
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	if len(list.GetIndexes()) != 1 || list.GetIndexes()[0].GetName() != created.GetName() {
		t.Fatalf("ListIndexes = %+v, want the created index", list.GetIndexes())
	}

	if _, err := client.DeleteIndex(ctx, &adminpb.DeleteIndexRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("DeleteIndex: %v", err)
	}
	if _, err := client.GetIndex(ctx, &adminpb.GetIndexRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetIndex after delete = %v, want NotFound", err)
	}
}

func TestCreateIndexLROShape(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()

	op, err := client.CreateIndex(context.Background(), &adminpb.CreateIndexRequest{
		Parent: testParent,
		Index:  &adminpb.Index{QueryScope: adminpb.Index_COLLECTION, Fields: []*adminpb.Index_IndexField{asc("a"), desc("b")}},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if !op.GetDone() || op.GetError() != nil {
		t.Fatalf("operation done=%v error=%v, want done and no error", op.GetDone(), op.GetError())
	}
	if op.GetName() == "" {
		t.Fatalf("operation name is empty")
	}
	var idx adminpb.Index
	if err := op.GetResponse().UnmarshalTo(&idx); err != nil {
		t.Fatalf("operation response is not an Index: %v", err)
	}
	if idx.GetState() != adminpb.Index_READY {
		t.Fatalf("operation Index state = %v, want READY", idx.GetState())
	}
	if got := idx.GetFields()[1].GetOrder(); got != adminpb.Index_IndexField_DESCENDING {
		t.Fatalf("second field order = %v, want DESCENDING", got)
	}
}

func TestListIndexesPagination(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	for _, fp := range []string{"a", "b", "c"} {
		createIndex(t, client, testParent, asc(fp), asc("x"))
	}

	page1, err := client.ListIndexes(ctx, &adminpb.ListIndexesRequest{Parent: testParent, PageSize: 2})
	if err != nil {
		t.Fatalf("ListIndexes page 1: %v", err)
	}
	if len(page1.GetIndexes()) != 2 {
		t.Fatalf("page 1 returned %d indexes, want 2", len(page1.GetIndexes()))
	}
	if page1.GetNextPageToken() == "" {
		t.Fatalf("page 1 next_page_token empty, want a cursor")
	}
	page2, err := client.ListIndexes(ctx, &adminpb.ListIndexesRequest{Parent: testParent, PageSize: 2, PageToken: page1.GetNextPageToken()})
	if err != nil {
		t.Fatalf("ListIndexes page 2: %v", err)
	}
	if len(page2.GetIndexes()) != 1 {
		t.Fatalf("page 2 returned %d indexes, want 1", len(page2.GetIndexes()))
	}
	if page2.GetNextPageToken() != "" {
		t.Fatalf("page 2 next_page_token = %q, want empty", page2.GetNextPageToken())
	}
	if page1.GetIndexes()[0].GetName() == page2.GetIndexes()[0].GetName() {
		t.Fatalf("pages returned the same index")
	}
}

func TestGetAndDeleteMissingNotFound(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()
	missing := testParent + "/indexes/does-not-exist"

	if _, err := client.GetIndex(ctx, &adminpb.GetIndexRequest{Name: missing}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetIndex missing = %v, want NotFound", err)
	}
	if _, err := client.DeleteIndex(ctx, &adminpb.DeleteIndexRequest{Name: missing}); status.Code(err) != codes.NotFound {
		t.Fatalf("DeleteIndex missing = %v, want NotFound", err)
	}
}

// alreadyExistsResourceStore fails every Create with store.ErrAlreadyExists so
// the provider's duplicate mapping can be exercised (index IDs are otherwise
// server-generated, so a natural collision cannot be provoked).
type alreadyExistsResourceStore struct{ store.ResourceStore }

func (alreadyExistsResourceStore) Create(context.Context, string, string, store.ResourceEntry) error {
	return store.ErrAlreadyExists
}

func TestCreateDuplicateAlreadyExists(t *testing.T) {
	client, cleanup := newAdminClient(t, alreadyExistsResourceStore{store.NewMemoryResourceStore()})
	defer cleanup()

	_, err := client.CreateIndex(context.Background(), &adminpb.CreateIndexRequest{
		Parent: testParent,
		Index:  &adminpb.Index{QueryScope: adminpb.Index_COLLECTION, Fields: []*adminpb.Index_IndexField{asc("a"), asc("b")}},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateIndex duplicate = %v, want AlreadyExists", err)
	}
}

func TestIndexEnumRoundTrip(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	created := createIndexScoped(t, client, testParent, adminpb.Index_COLLECTION_GROUP,
		&adminpb.Index_IndexField{
			FieldPath: "tags",
			ValueMode: &adminpb.Index_IndexField_ArrayConfig_{ArrayConfig: adminpb.Index_IndexField_CONTAINS},
		},
		desc("rank"),
	)
	if got := created.GetQueryScope(); got != adminpb.Index_COLLECTION_GROUP {
		t.Fatalf("created query_scope = %v, want COLLECTION_GROUP", got)
	}
	if got := created.GetFields()[0].GetArrayConfig(); got != adminpb.Index_IndexField_CONTAINS {
		t.Fatalf("created field array_config = %v, want CONTAINS", got)
	}
	if got := created.GetFields()[1].GetOrder(); got != adminpb.Index_IndexField_DESCENDING {
		t.Fatalf("created field order = %v, want DESCENDING", got)
	}

	got, err := client.GetIndex(ctx, &adminpb.GetIndexRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("GetIndex: %v", err)
	}
	if got.GetQueryScope() != adminpb.Index_COLLECTION_GROUP {
		t.Fatalf("GetIndex query_scope = %v, want COLLECTION_GROUP", got.GetQueryScope())
	}
	if got.GetFields()[0].GetArrayConfig() != adminpb.Index_IndexField_CONTAINS {
		t.Fatalf("GetIndex array_config = %v, want CONTAINS", got.GetFields()[0].GetArrayConfig())
	}
	if got.GetFields()[1].GetOrder() != adminpb.Index_IndexField_DESCENDING {
		t.Fatalf("GetIndex order = %v, want DESCENDING", got.GetFields()[1].GetOrder())
	}
}

func TestCreateIndexMissingDefinitionInvalidArgument(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()

	_, err := client.CreateIndex(context.Background(), &adminpb.CreateIndexRequest{Parent: testParent})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateIndex without an index = %v, want InvalidArgument", err)
	}
}

func TestInvalidResourceNames(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	if _, err := client.CreateIndex(ctx, &adminpb.CreateIndexRequest{
		Parent: "projects/proj/databases/(default)",
		Index:  &adminpb.Index{QueryScope: adminpb.Index_COLLECTION, Fields: []*adminpb.Index_IndexField{asc("a"), asc("b")}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateIndex bad parent = %v, want InvalidArgument", err)
	}
	if _, err := client.ListIndexes(ctx, &adminpb.ListIndexesRequest{Parent: "projects/proj"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListIndexes bad parent = %v, want InvalidArgument", err)
	}
	if _, err := client.GetIndex(ctx, &adminpb.GetIndexRequest{Name: testParent}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetIndex bad name = %v, want InvalidArgument", err)
	}
	if _, err := client.DeleteIndex(ctx, &adminpb.DeleteIndexRequest{Name: testParent}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("DeleteIndex bad name = %v, want InvalidArgument", err)
	}
}

// TestEnterpriseIndexOptionsRoundTrip covers the Enterprise edition index
// surface over gRPC (FE1): a single-field index is accepted on an Enterprise
// database, and the index options (api_scope, density, multikey, unique,
// shard_count) survive create -> get -> list. A Standard database keeps the
// documented composite-only contract.
func TestEnterpriseIndexOptionsRoundTrip(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	op, err := client.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:     testDBParent,
		DatabaseId: "ent-db",
		Database:   &adminpb.Database{LocationId: "nam5", DatabaseEdition: adminpb.Database_ENTERPRISE},
	})
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	created := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(created); err != nil {
		t.Fatalf("CreateDatabase response unmarshal: %v", err)
	}
	if created.GetDatabaseEdition() != adminpb.Database_ENTERPRISE {
		t.Fatalf("database edition = %v, want ENTERPRISE", created.GetDatabaseEdition())
	}
	parent := "projects/proj/databases/ent-db/collectionGroups/orders"

	createOp, err := client.CreateIndex(ctx, &adminpb.CreateIndexRequest{
		Parent: parent,
		Index: &adminpb.Index{
			QueryScope: adminpb.Index_COLLECTION_GROUP,
			ApiScope:   adminpb.Index_MONGODB_COMPATIBLE_API,
			Density:    adminpb.Index_SPARSE_ANY,
			Multikey:   true,
			Unique:     true,
			ShardCount: 4,
			Fields:     []*adminpb.Index_IndexField{asc("status")},
		},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	single := &adminpb.Index{}
	if err := createOp.GetResponse().UnmarshalTo(single); err != nil {
		t.Fatalf("CreateIndex response unmarshal: %v", err)
	}
	if len(single.GetFields()) != 1 || single.GetFields()[0].GetFieldPath() != "status" {
		t.Fatalf("single-field index fields = %+v", single.GetFields())
	}
	for _, tc := range []struct {
		name string
		got  any
		want any
	}{
		{"api_scope", single.GetApiScope(), adminpb.Index_MONGODB_COMPATIBLE_API},
		{"density", single.GetDensity(), adminpb.Index_SPARSE_ANY},
		{"multikey", single.GetMultikey(), true},
		{"unique", single.GetUnique(), true},
		{"shard_count", single.GetShardCount(), int32(4)},
	} {
		if tc.got != tc.want {
			t.Errorf("created %s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}

	got, err := client.GetIndex(ctx, &adminpb.GetIndexRequest{Name: single.GetName()})
	if err != nil {
		t.Fatalf("GetIndex: %v", err)
	}
	if got.GetDensity() != adminpb.Index_SPARSE_ANY || got.GetApiScope() != adminpb.Index_MONGODB_COMPATIBLE_API ||
		!got.GetMultikey() || !got.GetUnique() || got.GetShardCount() != 4 {
		t.Fatalf("GetIndex = %+v, want the Enterprise options echoed", got)
	}
	list, err := client.ListIndexes(ctx, &adminpb.ListIndexesRequest{Parent: parent})
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	if len(list.GetIndexes()) != 1 || list.GetIndexes()[0].GetDensity() != adminpb.Index_SPARSE_ANY {
		t.Fatalf("ListIndexes = %+v, want the stored index with its density", list.GetIndexes())
	}

	// A create that omits density serves the ENTERPRISE default, DENSE.
	defaulted := createIndex(t, client, parent, asc("a"), desc("b"))
	if defaulted.GetDensity() != adminpb.Index_DENSE {
		t.Errorf("default density = %v, want DENSE", defaulted.GetDensity())
	}
	if defaulted.GetApiScope() != adminpb.Index_ANY_API {
		t.Errorf("default api_scope = %v, want ANY_API", defaulted.GetApiScope())
	}

	// The STANDARD database (default) still requires two fields.
	if _, err := client.CreateIndex(ctx, &adminpb.CreateIndexRequest{
		Parent: testParent,
		Index:  &adminpb.Index{QueryScope: adminpb.Index_COLLECTION, Fields: []*adminpb.Index_IndexField{asc("status")}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("Standard single-field index error = %v, want InvalidArgument", err)
	}
}
