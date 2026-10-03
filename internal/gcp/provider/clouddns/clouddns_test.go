package clouddns

import (
	"context"
	"errors"
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

func TestManagedZonePatch(t *testing.T) {
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
	if resp.Data["description"] != "patched" {
		t.Errorf("description = %v", resp.Data["description"])
	}
	labels, _ := resp.Data["labels"].(map[string]any)
	if labels["tier"] != "gold" {
		t.Errorf("labels = %v", resp.Data["labels"])
	}
	if resp.Data["dnsName"] != "example.com." {
		t.Errorf("dnsName must be preserved, got %v", resp.Data["dnsName"])
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
	if fqdn("example.com") != "example.com." {
		t.Errorf("fqdn = %q", fqdn("example.com"))
	}
	if fqdn("example.com.") != "example.com." {
		t.Errorf("fqdn must not double the dot: %q", fqdn("example.com."))
	}
}

func isCode(err error, code string) bool {
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		return false
	}
	return perr.Code == code
}
