package cloudsql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

// boundValue renders the offending query value the way the frontend's binding
// error echoes it: a string value is re-quoted verbatim.
func boundValue(v any) string {
	if s, ok := v.(string); ok {
		return strconv.Quote(s)
	}
	return fmt.Sprint(v)
}

// TestInstancesListMaxResultsDefaultAndCeiling pins instances.list's
// documented maxResults contract (DC61): "If unspecified, at most 500 instances
// are returned. The maximum value is 1000; values above 1000 are coerced to
// 1000." The 1,200 seeded instances make both halves observable — the default
// page is 500 (not the pre-fix 100) and a larger request is clamped to 1,000
// rather than served whole. Only instances.list documents either half, so
// operations.list keeps the emulator's 100 (asserted below).
func TestInstancesListMaxResultsDefaultAndCeiling(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	data := func(name string) []byte {
		b, err := json.Marshal(instanceBody(name))
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		return b
	}
	for i := 0; i < 1200; i++ {
		name := fmt.Sprintf("inst-%04d", i)
		if err := p.resources.Create(ctx, "proj", store.GlobalRegion, store.ResourceEntry{
			Type: rtInstance, ID: name, Data: data(name),
		}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	for _, tc := range []struct {
		label string
		param *string
		want  int
	}{
		{"unspecified", nil, instancesDefaultMaxResults},
		{"zero", ptr("0"), instancesDefaultMaxResults},
		{"at ceiling", ptr("1000"), 1000},
		{"above ceiling", ptr("1001"), 1000},
		{"far above ceiling", ptr("5000"), 1000},
		{"legal uint32", ptr("3000000000"), 1000},
		{"alias above ceiling", ptr("pageSize=5000"), 1000},
	} {
		params := map[string]any{}
		if tc.param != nil {
			if strings.HasPrefix(*tc.param, "pageSize=") {
				params["pageSize"] = strings.TrimPrefix(*tc.param, "pageSize=")
			} else {
				params["maxResults"] = *tc.param
			}
		}
		resp, err := p.InstancesList(ctx, newNR(params))
		if err != nil {
			t.Errorf("%s: InstancesList = %v", tc.label, err)
			continue
		}
		items, _ := resp.Data["items"].([]any)
		if len(items) != tc.want {
			t.Errorf("%s: page size = %d, want %d", tc.label, len(items), tc.want)
		}
		if _, ok := resp.Data["nextPageToken"]; !ok {
			t.Errorf("%s: expected a nextPageToken for a truncated page", tc.label)
		}
	}
	// operations.list documents no default, so it keeps the emulator's 100 —
	// proof that the 500 default is scoped to instances.list.
	for i := 0; i < 150; i++ {
		name := fmt.Sprintf("op-%04d", i)
		if err := p.resources.Create(ctx, "proj", store.GlobalRegion, store.ResourceEntry{
			Type: rtOperation, ID: name, Data: []byte(`{"name":"` + name + `"}`),
		}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	// operations.list keeps the emulator's 100 default and none of instances'
	// contract: an explicit 5,000 is served whole (150 of 150), not clamped to
	// the instances ceiling.
	resp, err := p.OperationsList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("operations.list: %v", err)
	}
	if ops, _ := resp.Data["items"].([]any); len(ops) != defaultMaxResults {
		t.Errorf("operations.list default page = %d, want %d", len(ops), defaultMaxResults)
	}
	resp, err = p.OperationsList(ctx, newNR(map[string]any{"maxResults": "5000"}))
	if err != nil {
		t.Fatalf("operations.list(maxResults=5000): %v", err)
	}
	if ops, _ := resp.Data["items"].([]any); len(ops) != 150 {
		t.Errorf("operations.list(maxResults=5000) page = %d, want all 150 (no instances ceiling)", len(ops))
	}
}

func ptr(s string) *string { return &s }

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

// TestListMaxResultsBinding pins Cloud SQL's maxResults query binding on the
// two list methods whose Discovery document declares it: instances.list and
// operations.list declare a uint32, so an unparseable value ("abc", "1.5"), an
// explicitly empty one (DC63), a negative one or one above uint32 is
// INVALID_ARGUMENT/400 with the binding message, never a silent fall back to
// the default page. A value above int32 is still a legal uint32.
func TestListMaxResultsBinding(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "i1")
	handlers := []struct {
		name string
		call func(map[string]any) (*model.ProviderResponse, error)
	}{
		{"InstancesList", func(extra map[string]any) (*model.ProviderResponse, error) {
			return p.InstancesList(ctx, newNR(extra))
		}},
		{"OperationsList", func(extra map[string]any) (*model.ProviderResponse, error) {
			return p.OperationsList(ctx, newNR(extra))
		}},
	}
	for _, h := range handlers {
		for _, v := range []any{"", "abc", "1.5", "-1", "-0.5", "4294967296", float64(1.5)} {
			_, err := h.call(map[string]any{"maxResults": v})
			var perr *model.ProviderError
			if !errors.As(err, &perr) {
				t.Errorf("%s(maxResults=%#v) error = %v, want a *model.ProviderError", h.name, v, err)
				continue
			}
			if perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
				t.Errorf("%s(maxResults=%#v) = %+v, want InvalidArgument/400", h.name, v, perr)
			}
			if want := `Invalid value at 'max_results' (TYPE_UINT32), ` + boundValue(v); perr.Message != want {
				t.Errorf("%s(maxResults=%#v) message = %q, want %q", h.name, v, perr.Message, want)
			}
		}
		if resp, err := h.call(map[string]any{"maxResults": "2"}); err != nil || resp == nil {
			t.Errorf("%s(maxResults=2) = %v, %v; want a page", h.name, resp, err)
		}
		// The pageSize alias the emulator accepts is validated the same way.
		if _, err := h.call(map[string]any{"pageSize": "abc"}); !isCode(err, "InvalidArgument") {
			t.Errorf("%s(pageSize=abc) = %v, want InvalidArgument", h.name, err)
		}
	}
	// A value above int32 is a legal uint32, not a parse error.
	if _, err := p.InstancesList(ctx, newNR(map[string]any{"maxResults": "3000000000"})); err != nil {
		t.Errorf("InstancesList(maxResults=3000000000) = %v, want a page (legal uint32)", err)
	}
}

// TestListEmulatorPaginationStaysLenient pins the methods whose Discovery
// document declares neither maxResults nor pageToken (databases.list,
// users.list, flags.list, tiers.list): pagination there is an emulator
// extension, and real GCP ignores an unknown query parameter, so a malformed or
// explicitly empty value keeps selecting the default page instead of failing a
// request the real API would accept. (DC63's empty-is-invalid rule applies only
// to the declared bindings above.)
func TestListEmulatorPaginationStaysLenient(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "i1")
	handlers := []struct {
		name string
		call func(map[string]any) (*model.ProviderResponse, error)
	}{
		{"DatabasesList", func(extra map[string]any) (*model.ProviderResponse, error) {
			extra["instance"] = "i1"
			return p.DatabasesList(ctx, newNR(extra))
		}},
		{"UsersList", func(extra map[string]any) (*model.ProviderResponse, error) {
			extra["instance"] = "i1"
			return p.UsersList(ctx, newNR(extra))
		}},
		{"ListFlags", func(extra map[string]any) (*model.ProviderResponse, error) {
			return p.ListFlags(ctx, newNR(extra))
		}},
		{"ListTiers", func(extra map[string]any) (*model.ProviderResponse, error) {
			return p.ListTiers(ctx, newNR(extra))
		}},
	}
	for _, h := range handlers {
		for _, v := range []any{"", "abc", "1.5", "-1", float64(1.5)} {
			if resp, err := h.call(map[string]any{"maxResults": v}); err != nil || resp == nil {
				t.Errorf("%s(maxResults=%#v) = %v, %v; want the default page (extension stays lenient)", h.name, v, resp, err)
			}
		}
	}
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
