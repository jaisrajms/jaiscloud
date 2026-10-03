package cloudsql

import (
	"context"
	"errors"
	"sync"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     params,
		ResourceID: resource.ResourceID("proj"),
	}
}

func newProvider() *Provider { return New(store.NewMemoryResourceStore()) }

func instanceBody(name string) map[string]any {
	return map[string]any{
		"name":            name,
		"region":          "us-central1",
		"databaseVersion": "MYSQL_8_0",
		"settings":        map[string]any{"tier": "db-n1-standard-1"},
	}
}

func mustCreateInstance(t *testing.T, p *Provider, name string) map[string]any {
	t.Helper()
	resp, err := p.InstancesInsert(context.Background(), newNR(map[string]any{"body": instanceBody(name)}))
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	return resp.Data
}

func TestInstanceRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	op := mustCreateInstance(t, p, "inst-a")
	if op["kind"] != kindOperation {
		t.Fatalf("insert must return a %s operation, got %v", kindOperation, op["kind"])
	}
	if op["status"] != "DONE" {
		t.Fatalf("operation status = %v, want DONE", op["status"])
	}
	if op["operationType"] != "CREATE" {
		t.Errorf("operationType = %v, want CREATE", op["operationType"])
	}

	got, err := p.InstancesGet(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if got.Data["state"] != "RUNNABLE" {
		t.Errorf("state = %v, want RUNNABLE", got.Data["state"])
	}
	if got.Data["connectionName"] != "proj:us-central1:inst-a" {
		t.Errorf("connectionName = %v", got.Data["connectionName"])
	}
	settings, _ := got.Data["settings"].(map[string]any)
	if settings["tier"] != "db-n1-standard-1" {
		t.Errorf("settings.tier = %v", settings["tier"])
	}
	if settings["dataDiskSizeGb"] != "10" {
		t.Errorf("settings.dataDiskSizeGb = %v (%T), want \"10\"", settings["dataDiskSizeGb"], settings["dataDiskSizeGb"])
	}

	list, err := p.InstancesList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	if list.Data["kind"] != kindInstancesList {
		t.Errorf("list kind = %v", list.Data["kind"])
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(items))
	}

	// Patch the tier; the other settings survive.
	patchOp, err := p.InstancesPatch(ctx, newNR(map[string]any{
		"instance": "inst-a",
		"body":     map[string]any{"settings": map[string]any{"tier": "db-n1-standard-2"}},
	}))
	if err != nil {
		t.Fatalf("patch instance: %v", err)
	}
	if patchOp.Data["operationType"] != "UPDATE" || patchOp.Data["status"] != "DONE" {
		t.Errorf("unexpected patch operation: %v", patchOp.Data)
	}
	patched, _ := p.InstancesGet(ctx, newNR(map[string]any{"instance": "inst-a"}))
	patchedSettings, _ := patched.Data["settings"].(map[string]any)
	if patchedSettings["tier"] != "db-n1-standard-2" {
		t.Errorf("patched tier = %v", patchedSettings["tier"])
	}
	if patchedSettings["availabilityType"] != "ZONAL" {
		t.Errorf("unmasked settings must be retained, got availabilityType=%v", patchedSettings["availabilityType"])
	}

	if _, err := p.InstancesRestart(ctx, newNR(map[string]any{"instance": "inst-a"})); err != nil {
		t.Fatalf("restart instance: %v", err)
	}

	delOp, err := p.InstancesDelete(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("delete instance: %v", err)
	}
	if delOp.Data["operationType"] != "DELETE" || delOp.Data["status"] != "DONE" {
		t.Errorf("unexpected delete operation: %v", delOp.Data)
	}
	if _, err := p.InstancesGet(ctx, newNR(map[string]any{"instance": "inst-a"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

func TestInstanceAlreadyExistsAndNotFound(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")

	if _, err := p.InstancesInsert(ctx, newNR(map[string]any{"body": instanceBody("inst-a")})); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
	if _, err := p.InstancesGet(ctx, newNR(map[string]any{"instance": "nope"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound, got %v", err)
	}
	if _, err := p.InstancesPatch(ctx, newNR(map[string]any{"instance": "nope", "body": map[string]any{}})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound on patch, got %v", err)
	}
	if _, err := p.InstancesDelete(ctx, newNR(map[string]any{"instance": "nope"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound on delete, got %v", err)
	}

	_, err := p.InstancesInsert(ctx, newNR(map[string]any{"body": map[string]any{"region": "us-central1"}}))
	if !isCode(err, "InvalidArgument") {
		t.Fatalf("expected InvalidArgument for missing name, got %v", err)
	}
}

func TestOperationShapeAndLookup(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	op := mustCreateInstance(t, p, "inst-a")

	opName, _ := op["name"].(string)
	if opName == "" {
		t.Fatal("operation name is empty")
	}
	if op["targetId"] != "inst-a" {
		t.Errorf("targetId = %v", op["targetId"])
	}
	if op["targetLink"] == "" || op["selfLink"] == "" {
		t.Errorf("operation links must be populated: %v", op)
	}
	if op["insertTime"] == "" || op["endTime"] == "" {
		t.Errorf("operation times must be populated: %v", op)
	}

	got, err := p.OperationsGet(ctx, newNR(map[string]any{"operation": opName}))
	if err != nil {
		t.Fatalf("operations.get: %v", err)
	}
	if got.Data["name"] != opName || got.Data["status"] != "DONE" {
		t.Errorf("operation readback = %v", got.Data)
	}

	list, err := p.OperationsList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("operations.list: %v", err)
	}
	if list.Data["kind"] != kindOperationsList {
		t.Errorf("operations list kind = %v", list.Data["kind"])
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(items))
	}

	if _, err := p.OperationsGet(ctx, newNR(map[string]any{"operation": "missing"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestDatabaseCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")

	op, err := p.DatabasesInsert(ctx, newNR(map[string]any{
		"instance": "inst-a",
		"body":     map[string]any{"name": "appdb", "charset": "utf8", "collation": "utf8_general_ci"},
	}))
	if err != nil {
		t.Fatalf("create database: %v", err)
	}
	if op.Data["operationType"] != "CREATE_DATABASE" || op.Data["status"] != "DONE" {
		t.Errorf("unexpected database operation: %v", op.Data)
	}

	if _, err := p.DatabasesInsert(ctx, newNR(map[string]any{
		"instance": "inst-a",
		"body":     map[string]any{"name": "appdb"},
	})); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}

	got, err := p.DatabasesGet(ctx, newNR(map[string]any{"instance": "inst-a", "database": "appdb"}))
	if err != nil {
		t.Fatalf("get database: %v", err)
	}
	if got.Data["kind"] != kindDatabase || got.Data["charset"] != "utf8" {
		t.Errorf("database = %v", got.Data)
	}

	list, err := p.DatabasesList(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("list databases: %v", err)
	}
	if list.Data["kind"] != kindDatabasesList {
		t.Errorf("database list kind = %v", list.Data["kind"])
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 database, got %d", len(items))
	}

	// Databases are scoped to their instance.
	if _, err := p.DatabasesGet(ctx, newNR(map[string]any{"instance": "inst-b", "database": "appdb"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound for unknown instance, got %v", err)
	}

	if _, err := p.DatabasesPatch(ctx, newNR(map[string]any{
		"instance": "inst-a",
		"database": "appdb",
		"body":     map[string]any{"collation": "utf8_unicode_ci"},
	})); err != nil {
		t.Fatalf("patch database: %v", err)
	}
	patched, _ := p.DatabasesGet(ctx, newNR(map[string]any{"instance": "inst-a", "database": "appdb"}))
	if patched.Data["collation"] != "utf8_unicode_ci" || patched.Data["charset"] != "utf8" {
		t.Errorf("patched database = %v", patched.Data)
	}

	delOp, err := p.DatabasesDelete(ctx, newNR(map[string]any{"instance": "inst-a", "database": "appdb"}))
	if err != nil {
		t.Fatalf("delete database: %v", err)
	}
	if delOp.Data["operationType"] != "DELETE_DATABASE" {
		t.Errorf("unexpected delete operation: %v", delOp.Data)
	}
	if _, err := p.DatabasesGet(ctx, newNR(map[string]any{"instance": "inst-a", "database": "appdb"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

func TestUserCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")

	op, err := p.UsersInsert(ctx, newNR(map[string]any{
		"instance": "inst-a",
		"body":     map[string]any{"name": "alice", "host": "%", "password": "hunter2"},
	}))
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if op.Data["operationType"] != "CREATE_USER" || op.Data["status"] != "DONE" {
		t.Errorf("unexpected user operation: %v", op.Data)
	}

	got, err := p.UsersGet(ctx, newNR(map[string]any{"instance": "inst-a", "user": "alice"}))
	if err != nil {
		t.Fatalf("get user: %v", err)
	}
	if got.Data["kind"] != kindUser || got.Data["type"] != "BUILT_IN" {
		t.Errorf("user = %v", got.Data)
	}
	if _, ok := got.Data["password"]; ok {
		t.Errorf("password must not be echoed: %v", got.Data["password"])
	}

	if _, err := p.UsersInsert(ctx, newNR(map[string]any{
		"instance": "inst-a",
		"body":     map[string]any{"name": "alice", "host": "%"},
	})); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}

	list, err := p.UsersList(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("list users: %v", err)
	}
	if list.Data["kind"] != kindUsersList {
		t.Errorf("user list kind = %v", list.Data["kind"])
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 user, got %d", len(items))
	}

	if _, err := p.UsersUpdate(ctx, newNR(map[string]any{
		"instance": "inst-a",
		"body":     map[string]any{"name": "alice", "host": "%", "type": "CLOUD_IAM_USER"},
	})); err != nil {
		t.Fatalf("update user: %v", err)
	}
	updated, _ := p.UsersGet(ctx, newNR(map[string]any{"instance": "inst-a", "user": "alice"}))
	if updated.Data["type"] != "CLOUD_IAM_USER" {
		t.Errorf("updated user type = %v", updated.Data["type"])
	}

	// Delete uses the query-parameter name/host form.
	if _, err := p.UsersDelete(ctx, newNR(map[string]any{"instance": "inst-a", "name": "alice", "host": "%"})); err != nil {
		t.Fatalf("delete user: %v", err)
	}
	if _, err := p.UsersGet(ctx, newNR(map[string]any{"instance": "inst-a", "user": "alice"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after user delete, got %v", err)
	}
}

func TestDeleteInstancePurgesChildren(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")
	if _, err := p.DatabasesInsert(ctx, newNR(map[string]any{
		"instance": "inst-a", "body": map[string]any{"name": "appdb"},
	})); err != nil {
		t.Fatalf("seed database: %v", err)
	}
	if _, err := p.UsersInsert(ctx, newNR(map[string]any{
		"instance": "inst-a", "body": map[string]any{"name": "alice"},
	})); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	if _, err := p.InstancesDelete(ctx, newNR(map[string]any{"instance": "inst-a"})); err != nil {
		t.Fatalf("delete instance: %v", err)
	}
	mustCreateInstance(t, p, "inst-a")
	dbs, _ := p.DatabasesList(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if items, _ := dbs.Data["items"].([]any); len(items) != 0 {
		t.Fatalf("expected no databases after instance recreate, got %d", len(items))
	}
	users, _ := p.UsersList(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if items, _ := users.Data["items"].([]any); len(items) != 0 {
		t.Fatalf("expected no users after instance recreate, got %d", len(items))
	}
}

func TestFlagsTiersConnect(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")

	flags, err := p.ListFlags(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("flags list: %v", err)
	}
	if flags.Data["kind"] != kindFlagsList {
		t.Errorf("flags kind = %v", flags.Data["kind"])
	}
	flagItems, _ := flags.Data["items"].([]any)
	if len(flagItems) == 0 {
		t.Error("expected at least one flag")
	}
	if !hasItemField(flagItems, "name", "max_connections") {
		t.Error("flags.list must include max_connections")
	}

	tiers, err := p.ListTiers(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("tiers list: %v", err)
	}
	if tiers.Data["kind"] != kindTiersList {
		t.Errorf("tiers kind = %v", tiers.Data["kind"])
	}
	tierItems, _ := tiers.Data["items"].([]any)
	for _, want := range []string{"db-f1-micro", "db-n1-standard-1", "db-custom-1-3840"} {
		if !hasItemField(tierItems, "tier", want) {
			t.Errorf("tiers.list must include %s", want)
		}
	}

	cs, err := p.GetConnectSettings(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("connectSettings: %v", err)
	}
	if cs.Data["kind"] != kindConnectSettings || cs.Data["region"] != "us-central1" {
		t.Errorf("connectSettings = %v", cs.Data)
	}

	if _, err := p.Unimplemented(ctx, newNR(nil)); !isCode(err, "Unimplemented") {
		t.Fatalf("expected Unimplemented, got %v", err)
	}
}

func TestInstanceListPagination(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, n := range []string{"i1", "i2", "i3"} {
		mustCreateInstance(t, p, n)
	}
	first, err := p.InstancesList(ctx, newNR(map[string]any{"maxResults": "2"}))
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	items, _ := first.Data["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("page 1 size = %d, want 2", len(items))
	}
	token, _ := first.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected a nextPageToken")
	}
	second, err := p.InstancesList(ctx, newNR(map[string]any{"maxResults": "2", "pageToken": token}))
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	items2, _ := second.Data["items"].([]any)
	if len(items2) != 1 {
		t.Fatalf("page 2 size = %d, want 1", len(items2))
	}
	if _, ok := second.Data["nextPageToken"]; ok {
		t.Error("last page must not carry a nextPageToken")
	}
}

func isCode(err error, code string) bool {
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		return false
	}
	return perr.Code == code
}

// hasItemField reports whether any item in a list response has field == value.
func hasItemField(items []any, field, value string) bool {
	for _, it := range items {
		if m, ok := it.(map[string]any); ok && stringField(m, field) == value {
			return true
		}
	}
	return false
}

// TestListFlagsConcurrent guards against in-place sorting of the shared
// package-level flag catalogue: paginate must only sort a per-request copy.
func TestListFlagsConcurrent(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.ListFlags(ctx, newNR(nil)); err != nil {
				t.Errorf("ListFlags: %v", err)
			}
		}()
	}
	wg.Wait()
}
