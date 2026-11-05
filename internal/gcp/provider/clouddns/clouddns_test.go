package clouddns

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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

func zoneBody(name string) map[string]any {
	return map[string]any{
		"name":        name,
		"dnsName":     "example.com.",
		"description": "test zone",
		"labels":      map[string]any{"env": "test"},
	}
}

func mustCreateZone(t *testing.T, p *Provider, name string) map[string]any {
	t.Helper()
	resp, err := p.CreateManagedZone(context.Background(), newNR(map[string]any{"body": zoneBody(name)}))
	if err != nil {
		t.Fatalf("create zone: %v", err)
	}
	return resp.Data
}

func TestManagedZoneRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	created := mustCreateZone(t, p, "zone-a")
	if created["kind"] != kindManagedZone {
		t.Errorf("kind = %v, want %v", created["kind"], kindManagedZone)
	}
	if created["name"] != "zone-a" {
		t.Errorf("name = %v", created["name"])
	}
	if created["dnsName"] != "example.com." {
		t.Errorf("dnsName = %v", created["dnsName"])
	}
	if created["visibility"] != "public" {
		t.Errorf("visibility = %v, want public", created["visibility"])
	}
	if created["id"] == "" || created["id"] == nil {
		t.Errorf("expected a synthesized numeric id, got %v", created["id"])
	}
	servers, _ := created["nameServers"].([]any)
	if len(servers) != 4 {
		t.Errorf("nameServers = %v, want 4 entries", created["nameServers"])
	}

	got, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "zone-a"}))
	if err != nil {
		t.Fatalf("get zone: %v", err)
	}
	if got.Data["id"] != created["id"] {
		t.Errorf("id not stable across reads: %v vs %v", got.Data["id"], created["id"])
	}

	list, err := p.ListManagedZones(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list zones: %v", err)
	}
	if list.Data["kind"] != kindManagedZoneList {
		t.Errorf("list kind = %v", list.Data["kind"])
	}
	zones, _ := list.Data["managedZones"].([]any)
	if len(zones) != 1 {
		t.Fatalf("expected 1 zone, got %d", len(zones))
	}

	if _, err := p.DeleteManagedZone(ctx, newNR(map[string]any{"managedZone": "zone-a"})); err != nil {
		t.Fatalf("delete zone: %v", err)
	}
	if _, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "zone-a"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

func TestManagedZoneAlreadyExistsAndNotFound(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateZone(t, p, "zone-a")

	_, err := p.CreateManagedZone(ctx, newNR(map[string]any{"body": zoneBody("zone-a")}))
	if !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}

	_, err = p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "nope"}))
	if !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound, got %v", err)
	}

	_, err = p.DeleteManagedZone(ctx, newNR(map[string]any{"managedZone": "nope"}))
	if !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound on delete, got %v", err)
	}
}

// TestManagedZonePatchReturnsOperation covers the dns/v1 contract for
// managedZones.patch: it returns a dns#operation (not the zone), the operation
// is already done, and the next managedZones.get shows the change.
func TestManagedZonePatchReturnsOperation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateZone(t, p, "zone-a")

	resp, err := p.PatchManagedZone(ctx, newNR(map[string]any{
		"managedZone": "zone-a",
		"body":        map[string]any{"description": "patched", "labels": map[string]any{"tier": "gold"}},
	}))
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	if resp.Data["kind"] != kindOperation {
		t.Errorf("kind = %v, want %v", resp.Data["kind"], kindOperation)
	}
	opID, _ := resp.Data["id"].(string)
	if opID == "" {
		t.Fatal("operation id is empty")
	}
	if resp.Data["type"] != "update" {
		t.Errorf("type = %v, want update", resp.Data["type"])
	}
	if resp.Data["status"] != "done" {
		t.Errorf("status = %v, want done", resp.Data["status"])
	}
	if ts, _ := resp.Data["startTime"].(string); ts == "" {
		t.Error("startTime is empty")
	}
	zc, _ := resp.Data["zoneContext"].(map[string]any)
	oldVal, _ := zc["oldValue"].(map[string]any)
	newVal, _ := zc["newValue"].(map[string]any)
	if oldVal["description"] != "test zone" {
		t.Errorf("zoneContext.oldValue.description = %v, want test zone", oldVal["description"])
	}
	if newVal["description"] != "patched" {
		t.Errorf("zoneContext.newValue.description = %v, want patched", newVal["description"])
	}
	if newVal["dnsName"] != "example.com." {
		t.Errorf("dnsName must be preserved, got %v", newVal["dnsName"])
	}
	labels, _ := newVal["labels"].(map[string]any)
	if labels["tier"] != "gold" {
		t.Errorf("labels = %v, want tier=gold", newVal["labels"])
	}

	// The operation polls as done, and the zone is updated once it is.
	got, err := p.GetManagedZoneOperation(ctx, newNR(map[string]any{"managedZone": "zone-a", "operation": opID}))
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if got.Data["status"] != "done" || got.Data["id"] != opID {
		t.Errorf("polled operation = %v, want done %s", got.Data, opID)
	}
	zone, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "zone-a"}))
	if err != nil {
		t.Fatalf("get zone: %v", err)
	}
	if zone.Data["description"] != "patched" {
		t.Errorf("zone description = %v, want patched", zone.Data["description"])
	}
	if zl, _ := zone.Data["labels"].(map[string]any); zl["tier"] != "gold" {
		t.Errorf("zone labels = %v, want tier=gold", zone.Data["labels"])
	}
}

// TestManagedZoneUpdateReturnsOperation covers managedZones.update (PUT): same
// merge semantics, also a dns#operation.
func TestManagedZoneUpdateReturnsOperation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateZone(t, p, "zone-a")

	resp, err := p.UpdateManagedZone(ctx, newNR(map[string]any{
		"managedZone": "zone-a",
		"body":        map[string]any{"description": "replaced", "visibility": "private"},
	}))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if resp.Data["kind"] != kindOperation || resp.Data["type"] != "update" {
		t.Fatalf("update response = %v, want a dns#operation", resp.Data)
	}
	zone, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "zone-a"}))
	if err != nil {
		t.Fatalf("get zone: %v", err)
	}
	if zone.Data["description"] != "replaced" || zone.Data["visibility"] != "private" {
		t.Errorf("zone = %v, want description=replaced visibility=private", zone.Data)
	}
}

// TestManagedZoneOperationGetList covers managedZoneOperations.get/list, the
// clientOperationId binding, and the unknown-operation NotFound.
func TestManagedZoneOperationGetList(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateZone(t, p, "zone-a")

	for _, id := range []string{"op-b", "op-a"} {
		if _, err := p.PatchManagedZone(ctx, newNR(map[string]any{
			"managedZone":       "zone-a",
			"clientOperationId": id,
			"body":              map[string]any{"description": "patch " + id},
		})); err != nil {
			t.Fatalf("patch(%s): %v", id, err)
		}
	}
	// A clientOperationId is unique within the zone's operations collection.
	if _, err := p.PatchManagedZone(ctx, newNR(map[string]any{
		"managedZone":       "zone-a",
		"clientOperationId": "op-a",
		"body":              map[string]any{"description": "dup"},
	})); !isCode(err, "AlreadyExists") {
		t.Fatalf("duplicate clientOperationId err = %v, want AlreadyExists", err)
	}

	// Get by the client-supplied id.
	got, err := p.GetManagedZoneOperation(ctx, newNR(map[string]any{"managedZone": "zone-a", "operation": "op-a"}))
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if got.Data["id"] != "op-a" || got.Data["status"] != "done" {
		t.Errorf("operation = %v, want done op-a", got.Data)
	}
	if _, err := p.GetManagedZoneOperation(ctx, newNR(map[string]any{"managedZone": "zone-a", "operation": "missing"})); !isCode(err, "NotFound") {
		t.Fatalf("unknown operation err = %v, want NotFound", err)
	}

	// List, sorted by id.
	list, err := p.ListManagedZoneOperations(ctx, newNR(map[string]any{"managedZone": "zone-a", "sortBy": "id"}))
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if list.Data["kind"] != kindOperationList {
		t.Errorf("list kind = %v, want %v", list.Data["kind"], kindOperationList)
	}
	ops, _ := list.Data["operations"].([]any)
	if len(ops) != 2 {
		t.Fatalf("operations = %v, want 2", ops)
	}
	for i, want := range []string{"op-a", "op-b"} {
		op, _ := ops[i].(map[string]any)
		if op["id"] != want {
			t.Errorf("operations[%d].id = %v, want %s", i, op["id"], want)
		}
	}

	// An unsupported sortBy is InvalidArgument rather than a mis-sorted page.
	if _, err := p.ListManagedZoneOperations(ctx, newNR(map[string]any{"managedZone": "zone-a", "sortBy": "description"})); !isCode(err, "InvalidArgument") {
		t.Fatalf("bad sortBy err = %v, want InvalidArgument", err)
	}
	// The operations collection of a missing zone is NotFound.
	if _, err := p.ListManagedZoneOperations(ctx, newNR(map[string]any{"managedZone": "missing"})); !isCode(err, "NotFound") {
		t.Fatalf("missing zone err = %v, want NotFound", err)
	}
}

func TestResourceRecordSetRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateZone(t, p, "zone-a")

	create, err := p.CreateResourceRecordSet(ctx, newNR(map[string]any{
		"managedZone": "zone-a",
		"body": map[string]any{
			"name":    "www.example.com.",
			"type":    "A",
			"ttl":     300,
			"rrdatas": []any{"1.2.3.4"},
		},
	}))
	if err != nil {
		t.Fatalf("create rrset: %v", err)
	}
	if create.Data["kind"] != kindResourceRecordSet {
		t.Errorf("kind = %v", create.Data["kind"])
	}

	// A duplicate rrset is rejected.
	_, err = p.CreateResourceRecordSet(ctx, newNR(map[string]any{
		"managedZone": "zone-a",
		"body":        map[string]any{"name": "www.example.com.", "type": "A", "ttl": 300, "rrdatas": []any{"1.2.3.4"}},
	}))
	if !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists for duplicate rrset, got %v", err)
	}

	list, err := p.ListResourceRecordSets(ctx, newNR(map[string]any{"managedZone": "zone-a"}))
	if err != nil {
		t.Fatalf("list rrsets: %v", err)
	}
	if list.Data["kind"] != kindResourceRecordList {
		t.Errorf("list kind = %v", list.Data["kind"])
	}
	sets, _ := list.Data["rrsets"].([]any)
	if len(sets) != 1 {
		t.Fatalf("expected 1 rrset, got %d", len(sets))
	}

	// The path-segment form (name/type) drives delete.
	if _, err := p.DeleteResourceRecordSet(ctx, newNR(map[string]any{
		"managedZone": "zone-a", "rrsetName": "www.example.com.", "rrsetType": "A",
	})); err != nil {
		t.Fatalf("delete rrset: %v", err)
	}
	if _, err := p.GetResourceRecordSet(ctx, newNR(map[string]any{
		"managedZone": "zone-a", "rrsetName": "www.example.com.", "rrsetType": "A",
	})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after rrset delete, got %v", err)
	}
}

func TestResourceRecordSetUnknownZone(t *testing.T) {
	p := newProvider()
	_, err := p.CreateResourceRecordSet(context.Background(), newNR(map[string]any{
		"managedZone": "missing",
		"body":        map[string]any{"name": "www.example.com.", "type": "A"},
	}))
	if !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound for unknown zone, got %v", err)
	}
}

func TestChangeAppliesAdditionsAndDeletions(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateZone(t, p, "zone-a")
	if _, err := p.CreateResourceRecordSet(ctx, newNR(map[string]any{
		"managedZone": "zone-a",
		"body":        map[string]any{"name": "old.example.com.", "type": "A", "ttl": 60, "rrdatas": []any{"9.9.9.9"}},
	})); err != nil {
		t.Fatalf("seed rrset: %v", err)
	}

	resp, err := p.CreateChange(ctx, newNR(map[string]any{
		"managedZone": "zone-a",
		"body": map[string]any{
			"additions": []any{
				map[string]any{"name": "new.example.com.", "type": "A", "ttl": 120, "rrdatas": []any{"5.6.7.8"}},
			},
			"deletions": []any{
				map[string]any{"name": "old.example.com.", "type": "A", "ttl": 60, "rrdatas": []any{"9.9.9.9"}},
			},
		},
	}))
	if err != nil {
		t.Fatalf("create change: %v", err)
	}
	if resp.Data["kind"] != kindChange || resp.Data["status"] != "done" {
		t.Errorf("unexpected change envelope: %v", resp.Data)
	}
	changeID, _ := resp.Data["id"].(string)
	if changeID == "" {
		t.Fatal("change id is empty")
	}

	got, err := p.GetChange(ctx, newNR(map[string]any{"managedZone": "zone-a", "changeId": changeID}))
	if err != nil {
		t.Fatalf("get change: %v", err)
	}
	if got.Data["id"] != changeID {
		t.Errorf("change id = %v, want %v", got.Data["id"], changeID)
	}

	list, err := p.ListChanges(ctx, newNR(map[string]any{"managedZone": "zone-a"}))
	if err != nil {
		t.Fatalf("list changes: %v", err)
	}
	if list.Data["kind"] != kindChangeList {
		t.Errorf("change list kind = %v", list.Data["kind"])
	}
	if changes, _ := list.Data["changes"].([]any); len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}

	rrsets, err := p.ListResourceRecordSets(ctx, newNR(map[string]any{"managedZone": "zone-a"}))
	if err != nil {
		t.Fatalf("list rrsets: %v", err)
	}
	sets, _ := rrsets.Data["rrsets"].([]any)
	if len(sets) != 1 {
		t.Fatalf("expected the old rrset deleted and new one added, got %d", len(sets))
	}
	if sets[0].(map[string]any)["name"] != "new.example.com." {
		t.Errorf("unexpected rrset after change: %v", sets[0])
	}
}

func TestListPagination(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, n := range []string{"z1", "z2", "z3"} {
		mustCreateZone(t, p, n)
	}

	first, err := p.ListManagedZones(ctx, newNR(map[string]any{"maxResults": "2"}))
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if zones, _ := first.Data["managedZones"].([]any); len(zones) != 2 {
		t.Fatalf("page 1 size = %d, want 2", len(zones))
	}
	token, _ := first.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected a nextPageToken")
	}

	second, err := p.ListManagedZones(ctx, newNR(map[string]any{"maxResults": "2", "pageToken": token}))
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if zones, _ := second.Data["managedZones"].([]any); len(zones) != 1 {
		t.Fatalf("page 2 size = %d, want 1", len(zones))
	}
	if _, ok := second.Data["nextPageToken"]; ok {
		t.Error("last page must not carry a nextPageToken")
	}
}

func TestDeleteZonePurgesChildren(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateZone(t, p, "zone-a")
	if _, err := p.CreateResourceRecordSet(ctx, newNR(map[string]any{
		"managedZone": "zone-a",
		"body":        map[string]any{"name": "www.example.com.", "type": "A", "ttl": 300, "rrdatas": []any{"1.2.3.4"}},
	})); err != nil {
		t.Fatalf("seed rrset: %v", err)
	}

	if _, err := p.DeleteManagedZone(ctx, newNR(map[string]any{"managedZone": "zone-a"})); err != nil {
		t.Fatalf("delete zone: %v", err)
	}
	// Recreating the zone must not surface the old child rrsets.
	mustCreateZone(t, p, "zone-a")
	list, err := p.ListResourceRecordSets(ctx, newNR(map[string]any{"managedZone": "zone-a"}))
	if err != nil {
		t.Fatalf("list rrsets: %v", err)
	}
	if sets, _ := list.Data["rrsets"].([]any); len(sets) != 0 {
		t.Fatalf("expected no rrsets after zone recreate, got %d", len(sets))
	}
}

func TestProjectGetAndUnimplemented(t *testing.T) {
	p := newProvider()
	resp, err := p.GetProject(context.Background(), newNR(nil))
	if err != nil {
		t.Fatalf("project get: %v", err)
	}
	if resp.Data["kind"] != "dns#project" {
		t.Errorf("kind = %v", resp.Data["kind"])
	}
	if resp.Data["number"] == "" || resp.Data["number"] == nil {
		t.Errorf("expected numeric project number, got %v", resp.Data["number"])
	}
	quota, ok := resp.Data["quota"].(map[string]any)
	if !ok {
		t.Errorf("expected a quota block, got %v", resp.Data["quota"])
	} else {
		if _, has := quota["recordsPerRrset"]; has {
			t.Errorf("quota must not carry the non-Discovery recordsPerRrset field")
		}
		if _, has := quota["resourceRecordsPerRrset"]; !has {
			t.Errorf("quota must carry resourceRecordsPerRrset, got %v", quota)
		}
	}

	if _, err := p.Unimplemented(context.Background(), newNR(nil)); !isCode(err, "Unimplemented") {
		t.Fatalf("expected Unimplemented, got %v", err)
	}
}

func TestNumericIDStableAndFQDN(t *testing.T) {
	if numericID("proj/zone-a") != numericID("proj/zone-a") {
		t.Error("numericID must be stable")
	}
	if numericID("a") == numericID("b") {
		t.Error("numericID should differ for distinct inputs")
	}
	// Cloud DNS ids are uint64 on the wire, but the official provider reads
	// managed_zone_id as a signed int; a value above MaxInt64 fails the read
	// ("expected type 'int', got unconvertible type 'string'"), so the
	// synthesized id must stay in the positive int64 range for every name.
	for _, name := range []string{"proj/jc-module-private", "proj/prefix-private", "proj/prefix-public", "proj/a", "proj/b"} {
		id := numericID(name)
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil || n <= 0 {
			t.Errorf("numericID(%q) = %q, want a positive int64", name, id)
		}
	}
	if fqdn("example.com") != "example.com." {
		t.Errorf("fqdn = %q", fqdn("example.com"))
	}
	if fqdn("example.com.") != "example.com." {
		t.Errorf("fqdn must not double the dot: %q", fqdn("example.com."))
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

func isCode(err error, code string) bool {
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		return false
	}
	return perr.Code == code
}

// TestListMaxResultsBinding pins Cloud DNS's maxResults int32 query binding on
// every paged listing: an unparseable value ("abc", "1.5"), a fraction, an
// explicitly empty one (DC63) or an out-of-int32 one is INVALID_ARGUMENT/400
// with real GCP's binding envelope ("Invalid value at 'max_results'
// (TYPE_INT32), \"abc\""), never a silently absent parameter served from the
// default 100, while a legal integer still pages. Found in the DC41 review
// (DC55); the message was corrected in DC67. The binding is resolved before the
// parent-zone lookup, so a bad maxResults beats the missing-zone 404.
func TestListMaxResultsBinding(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateZone(t, p, "zone-a")
	handlers := []struct {
		name string
		call func(map[string]any) (*model.ProviderResponse, error)
	}{
		{"ListManagedZones", func(extra map[string]any) (*model.ProviderResponse, error) {
			return p.ListManagedZones(ctx, newNR(extra))
		}},
		{"ListResourceRecordSets", func(extra map[string]any) (*model.ProviderResponse, error) {
			if _, ok := extra["managedZone"]; !ok {
				extra["managedZone"] = "zone-a"
			}
			return p.ListResourceRecordSets(ctx, newNR(extra))
		}},
		{"ListChanges", func(extra map[string]any) (*model.ProviderResponse, error) {
			if _, ok := extra["managedZone"]; !ok {
				extra["managedZone"] = "zone-a"
			}
			return p.ListChanges(ctx, newNR(extra))
		}},
	}
	for _, h := range handlers {
		for _, v := range []any{"", "abc", "1.5", "-0.5", "2147483648", float64(1.5)} {
			_, err := h.call(map[string]any{"maxResults": v})
			var perr *model.ProviderError
			if !errors.As(err, &perr) {
				t.Errorf("%s(maxResults=%#v) error = %v, want a *model.ProviderError", h.name, v, err)
				continue
			}
			if perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
				t.Errorf("%s(maxResults=%#v) = %+v, want InvalidArgument/400", h.name, v, perr)
			}
			if want := `Invalid value at 'max_results' (TYPE_INT32), ` + boundValue(v); perr.Message != want {
				t.Errorf("%s(maxResults=%#v) message = %q, want %q", h.name, v, perr.Message, want)
			}
		}
		if resp, err := h.call(map[string]any{"maxResults": "2"}); err != nil || resp == nil {
			t.Errorf("%s(maxResults=2) = %v, %v; want a page", h.name, resp, err)
		}
		// A negative int32 parses but is outside the 1..1000 range real GCP's
		// list backend accepts, so it is the parameter-validation 400, not the
		// default page (DC62).
		for _, v := range []string{"-1", "0", "1001", "2147483647"} {
			_, err := h.call(map[string]any{"maxResults": v})
			var perr *model.ProviderError
			if !errors.As(err, &perr) {
				t.Errorf("%s(maxResults=%s) error = %v, want a *model.ProviderError", h.name, v, err)
				continue
			}
			if perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
				t.Errorf("%s(maxResults=%s) = %+v, want InvalidArgument/400", h.name, v, perr)
			}
			if want := "Invalid value for 'parameters.maxResults': '" + v + "'"; perr.Message != want {
				t.Errorf("%s(maxResults=%s) message = %q, want %q", h.name, v, perr.Message, want)
			}
		}
		// The documented range's edges are accepted and still page.
		for _, v := range []string{"1", "1000"} {
			if resp, err := h.call(map[string]any{"maxResults": v}); err != nil || resp == nil {
				t.Errorf("%s(maxResults=%s) = %v, %v; want a page", h.name, v, resp, err)
			}
		}
		// The pageSize alias the emulator accepts resolves through the same
		// contract, since the emulator cannot ignore it the way real GCP
		// ignores an unknown query parameter.
		if _, err := h.call(map[string]any{"pageSize": "abc"}); !isCode(err, "InvalidArgument") {
			t.Errorf("%s(pageSize=abc) = %v, want InvalidArgument", h.name, err)
		}
		if _, err := h.call(map[string]any{"pageSize": "0"}); !isCode(err, "InvalidArgument") {
			t.Errorf("%s(pageSize=0) = %v, want InvalidArgument", h.name, err)
		}
	}
	// Real GCP validates the query binding in the frontend, before routing, but
	// the backend's 1..1000 range after resolving the parent zone — so an
	// unparseable value beats the missing-zone 404 while an out-of-range one
	// does not (probed: rrsets?maxResults=abc on a missing zone is 400,
	// ?maxResults=0 is the zone's 404).
	resp, err := p.ListResourceRecordSets(ctx, newNR(map[string]any{"managedZone": "missing", "maxResults": "abc"}))
	if !isCode(err, "InvalidArgument") {
		t.Errorf("ListResourceRecordSets(missing zone, maxResults=abc) = %v, %v; want InvalidArgument", resp, err)
	}
	// An explicitly empty value is the same frontend binding rejection, so it
	// too precedes the missing-zone lookup (DC63).
	if _, err := p.ListResourceRecordSets(ctx, newNR(map[string]any{"managedZone": "missing", "maxResults": ""})); !isCode(err, "InvalidArgument") {
		t.Errorf("ListResourceRecordSets(missing zone, maxResults=) = %v, want InvalidArgument", err)
	}
	for _, h := range handlers[1:] { // the two lists with a parent zone
		for _, v := range []string{"0", "-1", "1001"} {
			if resp, err := h.call(map[string]any{"managedZone": "missing", "maxResults": v}); !isCode(err, "NotFound") {
				t.Errorf("%s(missing zone, maxResults=%s) = %v, %v; want the zone's NotFound", h.name, v, resp, err)
			}
		}
	}
	if _, err := p.ListResourceRecordSets(ctx, newNR(map[string]any{"managedZone": "missing"})); !isCode(err, "NotFound") {
		t.Errorf("ListResourceRecordSets(missing zone) = %v, want NotFound", err)
	}
	// changes.list verifies its parent zone like rrsets.list does: real GCP
	// answers 404 for a missing zone rather than an empty page (probed).
	if _, err := p.ListChanges(ctx, newNR(map[string]any{"managedZone": "missing"})); !isCode(err, "NotFound") {
		t.Errorf("ListChanges(missing zone) = %v, want NotFound", err)
	}
}

// ─── Managed-zone configuration blocks (TGM4) ─────────────────────────────────

// nestedMap returns a nested object from a wire map, failing the test when it is
// absent or not an object.
func nestedMap(t *testing.T, m map[string]any, key string) map[string]any {
	t.Helper()
	v, ok := m[key].(map[string]any)
	if !ok {
		t.Fatalf("%s = %#v, want an object", key, m[key])
	}
	return v
}

// nestedList returns a nested array from a wire map.
func nestedList(t *testing.T, m map[string]any, key string) []any {
	t.Helper()
	v, ok := m[key].([]any)
	if !ok {
		t.Fatalf("%s = %#v, want an array", key, m[key])
	}
	return v
}

// TestManagedZoneDNSSECConfigRoundTrip covers the public-zone DNSSEC block the
// Terraform module emits when dnssec_config is set: kind, state, nonExistence
// and both default key specs come back exactly as accepted, and a minimal
// block is not padded with defaults the client did not send.
func TestManagedZoneDNSSECConfigRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	body := zoneBody("public-a")
	body["visibility"] = "public"
	body["dnssecConfig"] = map[string]any{
		"kind":         "dns#managedZoneDnsSecConfig",
		"state":        "on",
		"nonExistence": "nsec3",
		"defaultKeySpecs": []any{
			map[string]any{"kind": "dns#dnsKeySpec", "algorithm": "rsasha256", "keyLength": 2048, "keyType": "keySigning"},
			map[string]any{"kind": "dns#dnsKeySpec", "algorithm": "rsasha256", "keyLength": 1024, "keyType": "zoneSigning"},
		},
	}
	created, err := p.CreateManagedZone(ctx, newNR(map[string]any{"body": body}))
	if err != nil {
		t.Fatalf("create zone: %v", err)
	}
	dc := nestedMap(t, created.Data, "dnssecConfig")
	if dc["kind"] != "dns#managedZoneDnsSecConfig" || dc["state"] != "on" || dc["nonExistence"] != "nsec3" {
		t.Errorf("dnssecConfig = %v, want the accepted block", dc)
	}
	specs := nestedList(t, dc, "defaultKeySpecs")
	if len(specs) != 2 {
		t.Fatalf("defaultKeySpecs = %v, want 2 specs", specs)
	}
	if first := specs[0].(map[string]any); first["keyType"] != "keySigning" || first["keyLength"] != float64(2048) {
		t.Errorf("defaultKeySpecs[0] = %v, want keySigning/2048", first)
	}
	if second := specs[1].(map[string]any); second["keyType"] != "zoneSigning" || second["keyLength"] != float64(1024) {
		t.Errorf("defaultKeySpecs[1] = %v, want zoneSigning/1024", second)
	}

	got, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "public-a"}))
	if err != nil {
		t.Fatalf("get zone: %v", err)
	}
	if gotDC := nestedMap(t, got.Data, "dnssecConfig"); gotDC["state"] != "on" {
		t.Errorf("GET dnssecConfig = %v, want state on", gotDC)
	}

	// A block with only the state stays minimal: the emulator stores what it
	// accepted and synthesizes nothing.
	minimal := zoneBody("public-b")
	minimal["visibility"] = "public"
	minimal["dnssecConfig"] = map[string]any{"state": "off"}
	if _, err := p.CreateManagedZone(ctx, newNR(map[string]any{"body": minimal})); err != nil {
		t.Fatalf("create minimal dnssec zone: %v", err)
	}
	min, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "public-b"}))
	if err != nil {
		t.Fatalf("get minimal zone: %v", err)
	}
	dc = nestedMap(t, min.Data, "dnssecConfig")
	if _, ok := dc["kind"]; ok {
		t.Errorf("dnssecConfig = %v, must not synthesize kind", dc)
	}
	if _, ok := dc["nonExistence"]; ok {
		t.Errorf("dnssecConfig = %v, must not synthesize nonExistence", dc)
	}

	// The list render carries the same block.
	list, err := p.ListManagedZones(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list zones: %v", err)
	}
	for _, raw := range nestedList(t, list.Data, "managedZones") {
		if zone := raw.(map[string]any); zone["name"] == "public-a" {
			if nestedMap(t, zone, "dnssecConfig")["state"] != "on" {
				t.Errorf("list dnssecConfig = %v, want state on", zone["dnssecConfig"])
			}
		}
	}
}

// TestManagedZonePrivateVisibilityConfigRoundTrip covers the private zone the
// module creates by default: visibility private with VPC networks and GKE
// clusters, both of which must read back.
func TestManagedZonePrivateVisibilityConfigRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	body := zoneBody("private-a")
	body["visibility"] = "private"
	body["privateVisibilityConfig"] = map[string]any{
		"kind": "dns#managedZonePrivateVisibilityConfig",
		"networks": []any{
			map[string]any{"kind": "dns#managedZonePrivateVisibilityConfigNetwork", "networkUrl": "projects/proj/global/networks/default"},
			map[string]any{"networkUrl": "projects/proj/global/networks/secondary"},
		},
		"gkeClusters": []any{
			map[string]any{"kind": "dns#managedZonePrivateVisibilityConfigGKECluster", "gkeClusterName": "projects/proj/locations/us-central1/clusters/gke-a"},
		},
	}
	created, err := p.CreateManagedZone(ctx, newNR(map[string]any{"body": body}))
	if err != nil {
		t.Fatalf("create zone: %v", err)
	}
	if created.Data["visibility"] != "private" {
		t.Errorf("visibility = %v, want private", created.Data["visibility"])
	}
	got, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "private-a"}))
	if err != nil {
		t.Fatalf("get zone: %v", err)
	}
	pvc := nestedMap(t, got.Data, "privateVisibilityConfig")
	networks := nestedList(t, pvc, "networks")
	if len(networks) != 2 {
		t.Fatalf("networks = %v, want 2", networks)
	}
	if url := networks[0].(map[string]any)["networkUrl"]; url != "projects/proj/global/networks/default" {
		t.Errorf("networks[0].networkUrl = %v", url)
	}
	clusters := nestedList(t, pvc, "gkeClusters")
	if len(clusters) != 1 {
		t.Fatalf("gkeClusters = %v, want 1", clusters)
	}
	if name := clusters[0].(map[string]any)["gkeClusterName"]; name != "projects/proj/locations/us-central1/clusters/gke-a" {
		t.Errorf("gkeClusters[0].gkeClusterName = %v", name)
	}
}

// TestManagedZoneForwardingConfigRoundTrip covers a forwarding zone, including
// the optional forwardingPath and the untouched private visibility networks the
// module attaches to it.
func TestManagedZoneForwardingConfigRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	body := zoneBody("forwarding-a")
	body["visibility"] = "private"
	body["forwardingConfig"] = map[string]any{
		"kind": "dns#managedZoneForwardingConfig",
		"targetNameServers": []any{
			map[string]any{"ipv4Address": "10.128.0.10", "forwardingPath": "private"},
			map[string]any{"ipv4Address": "10.128.0.11", "forwardingPath": "default"},
		},
	}
	body["privateVisibilityConfig"] = map[string]any{
		"networks": []any{map[string]any{"networkUrl": "projects/proj/global/networks/default"}},
	}
	if _, err := p.CreateManagedZone(ctx, newNR(map[string]any{"body": body})); err != nil {
		t.Fatalf("create zone: %v", err)
	}
	got, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "forwarding-a"}))
	if err != nil {
		t.Fatalf("get zone: %v", err)
	}
	fc := nestedMap(t, got.Data, "forwardingConfig")
	targets := nestedList(t, fc, "targetNameServers")
	if len(targets) != 2 {
		t.Fatalf("targetNameServers = %v, want 2", targets)
	}
	first := targets[0].(map[string]any)
	if first["ipv4Address"] != "10.128.0.10" || first["forwardingPath"] != "private" {
		t.Errorf("targetNameServers[0] = %v", first)
	}
	if _, ok := got.Data["peeringConfig"]; ok {
		t.Errorf("a forwarding zone must not render a peeringConfig: %v", got.Data)
	}
}

// TestManagedZonePeeringConfigRoundTrip covers a peering zone.
func TestManagedZonePeeringConfigRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	body := zoneBody("peering-a")
	body["visibility"] = "private"
	body["peeringConfig"] = map[string]any{
		"kind": "dns#managedZonePeeringConfig",
		"targetNetwork": map[string]any{
			"kind":       "dns#managedZonePeeringConfigTargetNetwork",
			"networkUrl": "projects/proj/global/networks/peer",
		},
	}
	body["privateVisibilityConfig"] = map[string]any{
		"networks": []any{map[string]any{"networkUrl": "projects/proj/global/networks/default"}},
	}
	if _, err := p.CreateManagedZone(ctx, newNR(map[string]any{"body": body})); err != nil {
		t.Fatalf("create zone: %v", err)
	}
	got, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "peering-a"}))
	if err != nil {
		t.Fatalf("get zone: %v", err)
	}
	pc := nestedMap(t, got.Data, "peeringConfig")
	target := nestedMap(t, pc, "targetNetwork")
	if target["networkUrl"] != "projects/proj/global/networks/peer" {
		t.Errorf("targetNetwork = %v", target)
	}
	if _, ok := got.Data["forwardingConfig"]; ok {
		t.Errorf("a peering zone must not render a forwardingConfig: %v", got.Data)
	}
}

// TestManagedZoneUpdateReplacesPrivateVisibilityConfig covers the provider's
// full-object PATCH: description/labels change and the network list is replaced,
// then a PATCH that carries a JSON null privateVisibilityConfig must not clear
// the stored block (real GCP treats null as unset, and the provider emits it
// whenever the caller has no block).
func TestManagedZoneUpdateReplacesPrivateVisibilityConfig(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	body := zoneBody("private-a")
	body["visibility"] = "private"
	body["privateVisibilityConfig"] = map[string]any{
		"networks": []any{
			map[string]any{"networkUrl": "projects/proj/global/networks/a"},
			map[string]any{"networkUrl": "projects/proj/global/networks/b"},
		},
	}
	if _, err := p.CreateManagedZone(ctx, newNR(map[string]any{"body": body})); err != nil {
		t.Fatalf("create zone: %v", err)
	}

	resp, err := p.PatchManagedZone(ctx, newNR(map[string]any{
		"managedZone": "private-a",
		"body": map[string]any{
			"description": "updated",
			"labels":      map[string]any{"tier": "gold"},
			"privateVisibilityConfig": map[string]any{
				"networks": []any{
					map[string]any{"networkUrl": "projects/proj/global/networks/b"},
					map[string]any{"networkUrl": "projects/proj/global/networks/c"},
				},
				"gkeClusters": []any{
					map[string]any{"gkeClusterName": "projects/proj/locations/us-central1/clusters/gke-a"},
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	// The operation's zoneContext reports the pre- and post-operation blocks.
	zc := nestedMap(t, resp.Data, "zoneContext")
	oldPVC := nestedMap(t, nestedMap(t, zc, "oldValue"), "privateVisibilityConfig")
	if got := len(nestedList(t, oldPVC, "networks")); got != 2 {
		t.Errorf("zoneContext.oldValue networks = %d, want 2", got)
	}

	got, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "private-a"}))
	if err != nil {
		t.Fatalf("get zone: %v", err)
	}
	if got.Data["description"] != "updated" {
		t.Errorf("description = %v, want updated", got.Data["description"])
	}
	if labels := nestedMap(t, got.Data, "labels"); labels["tier"] != "gold" {
		t.Errorf("labels = %v, want tier=gold", labels)
	}
	pvc := nestedMap(t, got.Data, "privateVisibilityConfig")
	if networks := nestedList(t, pvc, "networks"); len(networks) != 2 {
		t.Fatalf("networks = %v, want 2", networks)
	} else if url := networks[1].(map[string]any)["networkUrl"]; url != "projects/proj/global/networks/c" {
		t.Errorf("networks[1].networkUrl = %v, want the new network", url)
	}
	if clusters := nestedList(t, pvc, "gkeClusters"); len(clusters) != 1 {
		t.Errorf("gkeClusters = %v, want the accepted cluster", clusters)
	}

	// A later PATCH without a configuration block (or with an explicit null)
	// leaves the stored block alone.
	if _, err := p.PatchManagedZone(ctx, newNR(map[string]any{
		"managedZone": "private-a",
		"body": map[string]any{
			"description":             "updated again",
			"privateVisibilityConfig": nil,
			"dnssecConfig":            nil,
		},
	})); err != nil {
		t.Fatalf("patch without config: %v", err)
	}
	got, err = p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "private-a"}))
	if err != nil {
		t.Fatalf("get zone: %v", err)
	}
	if got.Data["description"] != "updated again" {
		t.Errorf("description = %v, want updated again", got.Data["description"])
	}
	if networks := nestedList(t, nestedMap(t, got.Data, "privateVisibilityConfig"), "networks"); len(networks) != 2 {
		t.Errorf("networks after null patch = %v, want the stored 2", networks)
	}
}

// TestManagedZoneUpdateReplacesDNSSECConfig covers turning DNSSEC on through the
// PATCH: the provider sends the whole dnssecConfig, so the default key specs
// survive the state change.
func TestManagedZoneUpdateReplacesDNSSECConfig(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	body := zoneBody("public-a")
	body["visibility"] = "public"
	body["cloudLoggingConfig"] = map[string]any{"kind": "dns#managedZoneCloudLoggingConfig", "enableLogging": true}
	body["dnssecConfig"] = map[string]any{
		"kind":         "dns#managedZoneDnsSecConfig",
		"state":        "off",
		"nonExistence": "nsec3",
		"defaultKeySpecs": []any{
			map[string]any{"algorithm": "rsasha256", "keyLength": 2048, "keyType": "keySigning"},
			map[string]any{"algorithm": "rsasha256", "keyLength": 1024, "keyType": "zoneSigning"},
		},
	}
	if _, err := p.CreateManagedZone(ctx, newNR(map[string]any{"body": body})); err != nil {
		t.Fatalf("create zone: %v", err)
	}

	if _, err := p.PatchManagedZone(ctx, newNR(map[string]any{
		"managedZone": "public-a",
		"body": map[string]any{
			"description":        "dnssec on",
			"cloudLoggingConfig": map[string]any{"kind": "dns#managedZoneCloudLoggingConfig", "enableLogging": false},
			"dnssecConfig": map[string]any{
				"kind":         "dns#managedZoneDnsSecConfig",
				"state":        "on",
				"nonExistence": "nsec3",
				"defaultKeySpecs": []any{
					map[string]any{"algorithm": "rsasha256", "keyLength": 2048, "keyType": "keySigning"},
					map[string]any{"algorithm": "rsasha256", "keyLength": 1024, "keyType": "zoneSigning"},
				},
			},
		},
	})); err != nil {
		t.Fatalf("patch dnssec: %v", err)
	}
	got, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "public-a"}))
	if err != nil {
		t.Fatalf("get zone: %v", err)
	}
	if logging := nestedMap(t, got.Data, "cloudLoggingConfig"); logging["enableLogging"] != false {
		t.Errorf("cloudLoggingConfig.enableLogging = %v, want false", logging["enableLogging"])
	}
	dc := nestedMap(t, got.Data, "dnssecConfig")
	if dc["state"] != "on" {
		t.Errorf("dnssecConfig.state = %v, want on", dc["state"])
	}
	specs := nestedList(t, dc, "defaultKeySpecs")
	if len(specs) != 2 {
		t.Errorf("defaultKeySpecs = %v, want the accepted 2", specs)
	}
}

// TestManagedZoneConfigValidation pins the combinations the Discovery document
// and the API declare: configuration blocks live on private zones, a zone is
// either forwarding or peering, and a forwarding target names its IPv4 address.
func TestManagedZoneConfigValidation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	base := func(name string) map[string]any {
		body := zoneBody(name)
		return body
	}

	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "forwarding target without ipv4Address",
			mutate: func(b map[string]any) {
				b["visibility"] = "private"
				b["forwardingConfig"] = map[string]any{
					"targetNameServers": []any{map[string]any{"forwardingPath": "default"}},
				}
			},
		},
		{
			name: "forwarding on a public zone",
			mutate: func(b map[string]any) {
				b["visibility"] = "public"
				b["forwardingConfig"] = map[string]any{
					"targetNameServers": []any{map[string]any{"ipv4Address": "10.128.0.10"}},
				}
			},
		},
		{
			name: "forwarding and peering together",
			mutate: func(b map[string]any) {
				b["visibility"] = "private"
				b["forwardingConfig"] = map[string]any{
					"targetNameServers": []any{map[string]any{"ipv4Address": "10.128.0.10"}},
				}
				b["peeringConfig"] = map[string]any{
					"targetNetwork": map[string]any{"networkUrl": "projects/proj/global/networks/peer"},
				}
			},
		},
		{
			name: "peering on a public zone",
			mutate: func(b map[string]any) {
				b["visibility"] = "public"
				b["peeringConfig"] = map[string]any{
					"targetNetwork": map[string]any{"networkUrl": "projects/proj/global/networks/peer"},
				}
			},
		},
		{
			name: "peering without a target network",
			mutate: func(b map[string]any) {
				b["visibility"] = "private"
				b["peeringConfig"] = map[string]any{"kind": "dns#managedZonePeeringConfig"}
			},
		},
		{
			name: "private visibility config on a public zone",
			mutate: func(b map[string]any) {
				b["visibility"] = "public"
				b["privateVisibilityConfig"] = map[string]any{
					"networks": []any{map[string]any{"networkUrl": "projects/proj/global/networks/default"}},
				}
			},
		},
	}
	for _, tc := range cases {
		body := base("bad-" + strconv.Itoa(len(tc.name)))
		tc.mutate(body)
		_, err := p.CreateManagedZone(ctx, newNR(map[string]any{"body": body}))
		if !isCode(err, "InvalidArgument") {
			t.Errorf("%s: err = %v, want InvalidArgument/400", tc.name, err)
		}
	}

	// The same combination rules hold on the update path: a zone with a private
	// visibility config cannot be made public without clearing it.
	body := zoneBody("private-a")
	body["visibility"] = "private"
	body["privateVisibilityConfig"] = map[string]any{
		"networks": []any{map[string]any{"networkUrl": "projects/proj/global/networks/default"}},
	}
	if _, err := p.CreateManagedZone(ctx, newNR(map[string]any{"body": body})); err != nil {
		t.Fatalf("create zone: %v", err)
	}
	if _, err := p.PatchManagedZone(ctx, newNR(map[string]any{
		"managedZone": "private-a",
		"body":        map[string]any{"visibility": "public"},
	})); !isCode(err, "InvalidArgument") {
		t.Errorf("patch visibility public over privateVisibilityConfig = %v, want InvalidArgument", err)
	}
	// The rejected patch must not have been applied.
	got, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "private-a"}))
	if err != nil {
		t.Fatalf("get zone: %v", err)
	}
	if got.Data["visibility"] != "private" {
		t.Errorf("visibility = %v, want the stored private", got.Data["visibility"])
	}
}

// TestManagedZoneEmptyPrivateVisibilityConfig covers the clearing form
// `hashicorp/google` always sends: an empty `{"networks": []}` block is accepted
// even on a public zone (the provider emits it for every zone with no
// private_visibility_config) and is not reported back, so the next plan sees no
// block to re-add. The same form clears a private zone's bindings.
func TestManagedZoneEmptyPrivateVisibilityConfig(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	public := zoneBody("public-empty")
	public["visibility"] = "public"
	public["privateVisibilityConfig"] = map[string]any{"networks": []any{}}
	created, err := p.CreateManagedZone(ctx, newNR(map[string]any{"body": public}))
	if err != nil {
		t.Fatalf("create public zone with an empty privateVisibilityConfig: %v", err)
	}
	if _, ok := created.Data["privateVisibilityConfig"]; ok {
		t.Errorf("created public zone renders %v, want no privateVisibilityConfig", created.Data["privateVisibilityConfig"])
	}

	private := zoneBody("private-clear")
	private["visibility"] = "private"
	private["privateVisibilityConfig"] = map[string]any{
		"networks": []any{map[string]any{"networkUrl": "projects/proj/global/networks/default"}},
	}
	if _, err := p.CreateManagedZone(ctx, newNR(map[string]any{"body": private})); err != nil {
		t.Fatalf("create private zone: %v", err)
	}
	if _, err := p.PatchManagedZone(ctx, newNR(map[string]any{
		"managedZone": "private-clear",
		"body":        map[string]any{"privateVisibilityConfig": map[string]any{"networks": []any{}}},
	})); err != nil {
		t.Fatalf("clear private visibility: %v", err)
	}
	got, err := p.GetManagedZone(ctx, newNR(map[string]any{"managedZone": "private-clear"}))
	if err != nil {
		t.Fatalf("get zone: %v", err)
	}
	if _, ok := got.Data["privateVisibilityConfig"]; ok {
		t.Errorf("cleared zone renders %v, want no privateVisibilityConfig", got.Data["privateVisibilityConfig"])
	}
}
