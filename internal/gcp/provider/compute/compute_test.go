package compute

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/gcp/serviceaccount"
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
		"name":        name,
		"machineType": "e2-micro",
		"disks": []any{map[string]any{
			"boot": true,
			"initializeParams": map[string]any{
				"sourceImage": "projects/debian-cloud/global/images/debian-12-bookworm-v20240101",
			},
		}},
		"networkInterfaces": []any{map[string]any{
			"network": "global/networks/default",
		}},
		"labels": map[string]any{"env": "test"},
	}
}

func zonal(zone string, extra map[string]any) map[string]any {
	p := map[string]any{"scope": zone, "scopeType": "zones", "zone": zone}
	for k, v := range extra {
		p[k] = v
	}
	return p
}

func regional(region string, extra map[string]any) map[string]any {
	p := map[string]any{"scope": region, "scopeType": "regions", "region": region}
	for k, v := range extra {
		p[k] = v
	}
	return p
}

func mustCreateInstance(t *testing.T, p *Provider, zone, name string) map[string]any {
	t.Helper()
	resp, err := p.InstancesInsert(context.Background(), newNR(zonal(zone, map[string]any{"body": instanceBody(name)})))
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	return resp.Data
}

func TestInstanceRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	op := mustCreateInstance(t, p, "us-central1-a", "vm-a")
	if op["kind"] != kindOperation {
		t.Fatalf("insert must return a %s operation, got %v", kindOperation, op["kind"])
	}
	if op["status"] != "DONE" {
		t.Fatalf("operation status = %v, want DONE", op["status"])
	}
	if op["operationType"] != "insert" {
		t.Errorf("operationType = %v, want insert", op["operationType"])
	}
	if op["progress"] != int64(100) {
		t.Errorf("progress = %v, want 100", op["progress"])
	}
	if _, ok := op["targetId"].(string); !ok {
		t.Errorf("targetId must be a numeric string, got %T", op["targetId"])
	}

	got, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"})))
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if got.Data["status"] != "RUNNING" {
		t.Errorf("status = %v, want RUNNING", got.Data["status"])
	}
	if got.Data["kind"] != kindInstance {
		t.Errorf("kind = %v, want %s", got.Data["kind"], kindInstance)
	}
	if !strings.Contains(str(got.Data["zone"]), "zones/us-central1-a") {
		t.Errorf("zone = %v", got.Data["zone"])
	}
	if !strings.Contains(str(got.Data["machineType"]), "machineTypes/e2-micro") {
		t.Errorf("machineType = %v", got.Data["machineType"])
	}
	if _, ok := got.Data["id"].(string); !ok {
		t.Errorf("id must be a numeric string, got %T", got.Data["id"])
	}

	list, err := p.InstancesList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	if list.Data["kind"] != kindInstanceList {
		t.Errorf("list kind = %v", list.Data["kind"])
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(items))
	}

	// stop -> TERMINATED
	stopOp, err := p.InstancesStop(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"})))
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
	if stopOp.Data["operationType"] != "stop" || stopOp.Data["status"] != "DONE" {
		t.Errorf("unexpected stop operation: %v", stopOp.Data)
	}
	stopped, _ := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"})))
	if stopped.Data["status"] != "TERMINATED" {
		t.Errorf("status after stop = %v, want TERMINATED", stopped.Data["status"])
	}

	// start -> RUNNING
	startOp, err := p.InstancesStart(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"})))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if startOp.Data["operationType"] != "start" {
		t.Errorf("unexpected start operation: %v", startOp.Data)
	}
	started, _ := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"})))
	if started.Data["status"] != "RUNNING" {
		t.Errorf("status after start = %v, want RUNNING", started.Data["status"])
	}

	if _, err := p.InstancesReset(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"}))); err != nil {
		t.Fatalf("reset: %v", err)
	}

	delOp, err := p.InstancesDelete(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"})))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if delOp.Data["operationType"] != "delete" {
		t.Errorf("unexpected delete operation: %v", delOp.Data)
	}
	if _, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-a"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

// TestInstanceMetadataFingerprintIsBase64 pins the wire format: Discovery
// declares Metadata.fingerprint as format:byte, so the emulator must emit
// base64-encoded bytes, not a variable-length hex digest.
func TestInstanceMetadataFingerprintIsBase64(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "us-central1-a", "vm-fp")

	got, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-fp"})))
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	md, ok := got.Data["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("metadata = %#v, want an object", got.Data["metadata"])
	}
	fp, _ := md["fingerprint"].(string)
	if fp == "" {
		t.Fatal("metadata.fingerprint is empty")
	}
	if _, err := base64.StdEncoding.DecodeString(fp); err != nil {
		t.Fatalf("metadata.fingerprint %q is not valid base64 (format:byte): %v", fp, err)
	}
}

func TestZonalScopingDoesNotCollide(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "us-central1-a", "vm")
	mustCreateInstance(t, p, "us-central1-b", "vm")

	a, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm"})))
	if err != nil {
		t.Fatalf("get zone a: %v", err)
	}
	if !strings.Contains(str(a.Data["zone"]), "us-central1-a") {
		t.Errorf("zone a instance zone = %v", a.Data["zone"])
	}
	b, err := p.InstancesGet(ctx, newNR(zonal("us-central1-b", map[string]any{"instance": "vm"})))
	if err != nil {
		t.Fatalf("get zone b: %v", err)
	}
	if !strings.Contains(str(b.Data["zone"]), "us-central1-b") {
		t.Errorf("zone b instance zone = %v", b.Data["zone"])
	}

	listA, _ := p.InstancesList(ctx, newNR(zonal("us-central1-a", nil)))
	if items, _ := listA.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("zone a list = %d, want 1", len(items))
	}
	listB, _ := p.InstancesList(ctx, newNR(zonal("us-central1-b", nil)))
	if items, _ := listB.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("zone b list = %d, want 1", len(items))
	}
	listC, _ := p.InstancesList(ctx, newNR(zonal("us-central1-c", nil)))
	if items, _ := listC.Data["items"].([]any); len(items) != 0 {
		t.Fatalf("zone c list = %d, want 0", len(items))
	}
}

func TestInstanceAlreadyExistsAndNotFound(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "us-central1-a", "vm")

	if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": instanceBody("vm")}))); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
	if _, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "nope"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound, got %v", err)
	}
	if _, err := p.InstancesDelete(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "nope"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound on delete, got %v", err)
	}
	if _, err := p.InstancesStop(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "nope"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound on stop, got %v", err)
	}
	if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{"machineType": "e2-micro"}}))); !isCode(err, "InvalidArgument") {
		t.Fatalf("expected InvalidArgument for missing name, got %v", err)
	}
}

func TestAggregatedList(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "us-central1-a", "vm-a")
	mustCreateInstance(t, p, "us-east1-b", "vm-b")

	resp, err := p.InstancesAggregatedList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("aggregated list: %v", err)
	}
	if resp.Data["kind"] != kindInstanceAggregatedList {
		t.Errorf("kind = %v", resp.Data["kind"])
	}
	items, _ := resp.Data["items"].(map[string]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 scoped groups, got %d: %v", len(items), items)
	}
	if _, ok := items["zones/us-central1-a"]; !ok {
		t.Errorf("missing zone a group: %v", items)
	}
}

func TestDiskCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	op, err := p.DisksInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{
		"name": "disk-a", "sizeGb": 20, "type": "pd-ssd",
	}})))
	if err != nil {
		t.Fatalf("create disk: %v", err)
	}
	if op.Data["operationType"] != "insert" || op.Data["status"] != "DONE" {
		t.Errorf("unexpected disk operation: %v", op.Data)
	}

	if _, err := p.DisksInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{"name": "disk-a"}}))); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}

	got, err := p.DisksGet(ctx, newNR(zonal("us-central1-a", map[string]any{"disk": "disk-a"})))
	if err != nil {
		t.Fatalf("get disk: %v", err)
	}
	if got.Data["kind"] != kindDisk || got.Data["status"] != "READY" {
		t.Errorf("disk = %v", got.Data)
	}
	if got.Data["sizeGb"] != "20" {
		t.Errorf("sizeGb = %v (%T), want \"20\"", got.Data["sizeGb"], got.Data["sizeGb"])
	}
	// physicalBlockSizeBytes is Optional+Computed in the provider: a disk that
	// omitted it must read the documented server default back, or the ForceNew
	// field permadiffs (0 in state vs the configured value).
	if got.Data["physicalBlockSizeBytes"] != defaultDiskPhysicalBlockSizeBytes {
		t.Errorf("physicalBlockSizeBytes = %v, want the %s default", got.Data["physicalBlockSizeBytes"], defaultDiskPhysicalBlockSizeBytes)
	}
	if !strings.Contains(str(got.Data["type"]), "diskTypes/pd-ssd") {
		t.Errorf("type = %v", got.Data["type"])
	}

	list, err := p.DisksList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("list disks: %v", err)
	}
	if list.Data["kind"] != kindDiskList {
		t.Errorf("disk list kind = %v", list.Data["kind"])
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 disk, got %d", len(items))
	}

	// Disks are zonal: another zone must not see it.
	if _, err := p.DisksGet(ctx, newNR(zonal("us-central1-b", map[string]any{"disk": "disk-a"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound in other zone, got %v", err)
	}

	if _, err := p.DisksDelete(ctx, newNR(zonal("us-central1-a", map[string]any{"disk": "disk-a"}))); err != nil {
		t.Fatalf("delete disk: %v", err)
	}
	if _, err := p.DisksGet(ctx, newNR(zonal("us-central1-a", map[string]any{"disk": "disk-a"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

// TestDiskInsertEchoesPersistedInputs pins the disk-insert field echo
// (TF2-11-3): every persisted caller input the provider reads back must survive
// the round trip, because a dropped value permadiffs and — for the ForceNew
// attributes — replaces the disk on every plan. Fields with no documented
// server default must stay absent when the request omits them: synthesizing one
// would invent state the caller never sent (architecture, storagePool) and
// permadiff a config that omits it.
func TestDiskInsertEchoesPersistedInputs(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	zone := "us-central1-a"

	guestFeatures := []any{
		map[string]any{"type": "SECURE_BOOT"},
		map[string]any{"type": "WINDOWS"},
	}
	licenses := []any{"https://www.googleapis.com/compute/v1/projects/windows-cloud/global/licenses/windows-server-core"}
	resourcePolicies := []any{"projects/proj/regions/us-central1/resourcePolicies/rp"}
	replicaZones := []any{"https://www.googleapis.com/compute/v1/projects/proj/zones/us-central1-a"}
	asyncPrimary := map[string]any{"disk": "projects/proj/zones/us-central1-b/disks/primary"}

	if _, err := p.DisksInsert(ctx, newNR(zonal(zone, map[string]any{"body": map[string]any{
		"name":                      "disk-features",
		"guestOsFeatures":           guestFeatures,
		"licenses":                  licenses,
		"provisionedIops":           30000,  // a raw JSON number
		"provisionedThroughput":     "1200", // already the wire's decimal string
		"accessMode":                "READ_WRITE_SINGLE",
		"resourcePolicies":          resourcePolicies,
		"storagePool":               "projects/proj/zones/us-central1-a/storagePools/pool",
		"architecture":              "X86_64",
		"replicaZones":              replicaZones,
		"enableConfidentialCompute": true,
		"asyncPrimaryDisk":          asyncPrimary,
		"sourceDisk":                "projects/proj/zones/us-central1-a/disks/source",
		"sourceSnapshot":            "projects/proj/global/snapshots/snap",
		"sourceInstantSnapshot":     "projects/proj/zones/us-central1-a/instantSnapshots/instant",
		"sourceStorageObject":       "gs://bucket/disk.tar.gz",
		// Input-only material: real Compute does not persist `params` as part of
		// the resource payload, so the record must not either. The encryption-key
		// blocks are covered by TestDiskInsertEncryptionKeyShapes.
		"params": map[string]any{"resourceManagerTags": map[string]any{"tagKeys/1": "tagValues/2"}},
	}}))); err != nil {
		t.Fatalf("create disk: %v", err)
	}
	got, err := p.DisksGet(ctx, newNR(zonal(zone, map[string]any{"disk": "disk-features"})))
	if err != nil {
		t.Fatalf("get disk: %v", err)
	}
	for field, want := range map[string]any{
		"guestOsFeatures":           guestFeatures,
		"licenses":                  licenses,
		"provisionedIops":           "30000",
		"provisionedThroughput":     "1200",
		"accessMode":                "READ_WRITE_SINGLE",
		"resourcePolicies":          resourcePolicies,
		"storagePool":               "projects/proj/zones/us-central1-a/storagePools/pool",
		"architecture":              "X86_64",
		"replicaZones":              replicaZones,
		"enableConfidentialCompute": true,
		"asyncPrimaryDisk":          asyncPrimary,
		"sourceDisk":                "projects/proj/zones/us-central1-a/disks/source",
		"sourceSnapshot":            "projects/proj/global/snapshots/snap",
		"sourceInstantSnapshot":     "projects/proj/zones/us-central1-a/instantSnapshots/instant",
		"sourceStorageObject":       "gs://bucket/disk.tar.gz",
	} {
		if !reflect.DeepEqual(got.Data[field], want) {
			t.Errorf("%s = %v (%T), want %v", field, got.Data[field], got.Data[field], want)
		}
	}
	if v, ok := got.Data["params"]; ok {
		t.Errorf("params must not be recorded, got %v", v)
	}

	// A create that omits the optional inputs keeps them absent, except the one
	// field with a documented server default.
	if _, err := p.DisksInsert(ctx, newNR(zonal(zone, map[string]any{"body": map[string]any{"name": "disk-bare"}}))); err != nil {
		t.Fatalf("create bare disk: %v", err)
	}
	bare, err := p.DisksGet(ctx, newNR(zonal(zone, map[string]any{"disk": "disk-bare"})))
	if err != nil {
		t.Fatalf("get bare disk: %v", err)
	}
	for _, field := range []string{
		"accessMode", "architecture", "asyncPrimaryDisk", "enableConfidentialCompute",
		"guestOsFeatures", "licenses", "provisionedIops", "provisionedThroughput",
		"replicaZones", "resourcePolicies", "sourceDisk", "sourceInstantSnapshot",
		"sourceSnapshot", "sourceStorageObject", "storagePool",
		"diskEncryptionKey", "sourceImageEncryptionKey", "sourceSnapshotEncryptionKey",
	} {
		if v, ok := bare.Data[field]; ok {
			t.Errorf("bare disk must omit %s, got %v", field, v)
		}
	}
	if bare.Data["physicalBlockSizeBytes"] != defaultDiskPhysicalBlockSizeBytes {
		t.Errorf("physicalBlockSizeBytes = %v, want the %s default", bare.Data["physicalBlockSizeBytes"], defaultDiskPhysicalBlockSizeBytes)
	}
}

// TestDiskInsertEncryptionKeyShapes pins the CustomerEncryptionKey echo
// (TF2-11-4). Discovery types the disk's diskEncryptionKey,
// sourceImageEncryptionKey and sourceSnapshotEncryptionKey as
// CustomerEncryptionKey blocks whose rawKey/rsaEncryptedKey are request-only
// material and whose sha256 is "[Output only] The RFC 4648 base64 encoded
// SHA-256 hash of the customer-supplied encryption key that protects this
// resource" — so the record keeps the server-visible shape alone. The provider
// reads the block back into its ForceNew disk_encryption_key (and source_*)
// block, which is what stopped a CMEK disk re-planning "forces replacement".
func TestDiskInsertEncryptionKeyShapes(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	zone := "us-central1-a"
	kmsKey := "projects/proj/locations/us-central1/keyRings/ring/cryptoKeys/key"
	kmsSA := "service-123@compute-system.iam.gserviceaccount.com"
	keyBytes := []byte("0123456789abcdef0123456789abcdef")
	rawKey := base64.StdEncoding.EncodeToString(keyBytes)
	sum := sha256.Sum256(keyBytes)
	wantSHA := base64.StdEncoding.EncodeToString(sum[:])

	insert := func(name string, body map[string]any) map[string]any {
		t.Helper()
		if body == nil {
			body = map[string]any{}
		}
		body["name"] = name
		if _, err := p.DisksInsert(ctx, newNR(zonal(zone, map[string]any{"body": body}))); err != nil {
			t.Fatalf("create disk %s: %v", name, err)
		}
		got, err := p.DisksGet(ctx, newNR(zonal(zone, map[string]any{"disk": name})))
		if err != nil {
			t.Fatalf("get disk %s: %v", name, err)
		}
		return got.Data
	}

	// CMEK: the key name and its service account echo. There is no key material
	// to leak; real Compute may additionally qualify the name with a key
	// version once the disk exists, which the emulator does not invent.
	cmek := insert("disk-cmek", map[string]any{"diskEncryptionKey": map[string]any{
		"kmsKeyName":           kmsKey,
		"kmsKeyServiceAccount": kmsSA,
	}})
	wantCMEK := map[string]any{"kmsKeyName": kmsKey, "kmsKeyServiceAccount": kmsSA}
	if !reflect.DeepEqual(cmek["diskEncryptionKey"], wantCMEK) {
		t.Errorf("diskEncryptionKey = %v, want %v", cmek["diskEncryptionKey"], wantCMEK)
	}

	// CSEK: only the documented sha256 is echoed — a caller-supplied sha256 is
	// ignored (output-only) and the raw key never comes back.
	csek := insert("disk-csek", map[string]any{"diskEncryptionKey": map[string]any{
		"rawKey": rawKey,
		"sha256": base64.StdEncoding.EncodeToString([]byte("caller-supplied")),
	}})
	wantCSEK := map[string]any{"sha256": wantSHA}
	if !reflect.DeepEqual(csek["diskEncryptionKey"], wantCSEK) {
		t.Errorf("diskEncryptionKey = %v, want %v", csek["diskEncryptionKey"], wantCSEK)
	}

	// The source-image and source-snapshot keys follow the same rule.
	source := insert("disk-source-keys", map[string]any{
		"sourceImageEncryptionKey":    map[string]any{"kmsKeyName": kmsKey},
		"sourceSnapshotEncryptionKey": map[string]any{"rawKey": rawKey},
	})
	if want := map[string]any{"kmsKeyName": kmsKey}; !reflect.DeepEqual(source["sourceImageEncryptionKey"], want) {
		t.Errorf("sourceImageEncryptionKey = %v, want %v", source["sourceImageEncryptionKey"], want)
	}
	if want := map[string]any{"sha256": wantSHA}; !reflect.DeepEqual(source["sourceSnapshotEncryptionKey"], want) {
		t.Errorf("sourceSnapshotEncryptionKey = %v, want %v", source["sourceSnapshotEncryptionKey"], want)
	}

	// An rsa-wrapped key alone carries no server-derivable member: the wrapped
	// material is never echoed, and its sha256 needs the private key real
	// Compute holds. No block is rendered rather than an empty one.
	rsa := insert("disk-rsa", map[string]any{"diskEncryptionKey": map[string]any{
		"rsaEncryptedKey": base64.StdEncoding.EncodeToString([]byte("wrapped")),
	}})
	if v, ok := rsa["diskEncryptionKey"]; ok {
		t.Errorf("rsa-only create must render no block, got %v", v)
	}

	// A plain create renders no block, not an empty one.
	if v, ok := insert("disk-no-key", nil)["diskEncryptionKey"]; ok {
		t.Errorf("plain create must render no block, got %v", v)
	}

	// A rawKey that is not a base64-encoded 256-bit key is the documented 400.
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"disk-bad-base64", map[string]any{"diskEncryptionKey": map[string]any{"rawKey": "not base64!"}}},
		{"disk-short-key", map[string]any{"diskEncryptionKey": map[string]any{"rawKey": base64.StdEncoding.EncodeToString([]byte("short"))}}},
	} {
		tc.body["name"] = tc.name
		_, err := p.DisksInsert(ctx, newNR(zonal(zone, map[string]any{"body": tc.body})))
		if !isCode(err, "InvalidArgument") {
			t.Errorf("%s: expected InvalidArgument, got %v", tc.name, err)
		}
	}
}

// TestDiskEncryptionKeyStoreRoundTrip checks the persisted record itself (the
// JSON the ResourceStore keeps) rather than only the rendered read: a CSEK
// key's raw material must not be persisted anywhere, a CMEK key name must
// survive the store round trip, and disks.list renders the same block as
// disks.get.
func TestDiskEncryptionKeyStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	zone := "us-central1-a"
	kmsKey := "projects/proj/locations/us-central1/keyRings/ring/cryptoKeys/key"
	keyBytes := []byte("0123456789abcdef0123456789abcdef")
	rawKey := base64.StdEncoding.EncodeToString(keyBytes)
	sum := sha256.Sum256(keyBytes)
	wantSHA := base64.StdEncoding.EncodeToString(sum[:])

	for name, body := range map[string]map[string]any{
		// A CMEK request never carries raw material in practice; the extra
		// rsaEncryptedKey here is the leak check (it must be dropped, not
		// sanitized into an invented sha256).
		"disk-store-cmek": {"diskEncryptionKey": map[string]any{
			"kmsKeyName": kmsKey, "rsaEncryptedKey": "d3JhcHBlZA==",
		}},
		"disk-store-csek": {"diskEncryptionKey": map[string]any{"rawKey": rawKey}},
	} {
		body["name"] = name
		if _, err := p.DisksInsert(ctx, newNR(zonal(zone, map[string]any{"body": body}))); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		entry, err := p.resources.Get(ctx, "proj", zone, rtDisk, name)
		if err != nil {
			t.Fatalf("store get %s: %v", name, err)
		}
		record := string(entry.Data)
		for _, secret := range []string{rawKey, "d3JhcHBlZA==", "rawKey", "rsaEncryptedKey"} {
			if strings.Contains(record, secret) {
				t.Errorf("%s: stored record leaks key material %q: %s", name, secret, record)
			}
		}
		var stored map[string]any
		if err := json.Unmarshal(entry.Data, &stored); err != nil {
			t.Fatalf("unmarshal %s: %v", name, err)
		}
		block, _ := stored["diskEncryptionKey"].(map[string]any)
		if name == "disk-store-cmek" {
			if block["kmsKeyName"] != kmsKey {
				t.Errorf("%s: stored kmsKeyName = %v, want %v", name, block["kmsKeyName"], kmsKey)
			}
			if _, ok := block["sha256"]; ok {
				t.Errorf("%s: stored block has an invented sha256: %v", name, block["sha256"])
			}
			continue
		}
		if block["sha256"] != wantSHA {
			t.Errorf("%s: stored sha256 = %v, want %v", name, block["sha256"], wantSHA)
		}
	}

	// The list path renders the stored block exactly as get does.
	list, err := p.DisksList(ctx, newNR(zonal(zone, map[string]any{})))
	if err != nil {
		t.Fatalf("list disks: %v", err)
	}
	items, _ := list.Data["items"].([]any)
	var listed map[string]any
	for _, it := range items {
		if m, ok := it.(map[string]any); ok && m["name"] == "disk-store-csek" {
			listed = m
		}
	}
	if listed == nil {
		t.Fatal("disk-store-csek missing from disks.list")
	}
	if want := map[string]any{"sha256": wantSHA}; !reflect.DeepEqual(listed["diskEncryptionKey"], want) {
		t.Errorf("disks.list diskEncryptionKey = %v, want %v", listed["diskEncryptionKey"], want)
	}
}

// TestDiskSourceImageResolution pins the disk-insert image normalization: a
// caller's image reference is recorded the way real Compute records it (the
// resolved image's absolute selfLink plus its id), so a read is canonical and
// the provider's `image` diff suppression sees a link it understands. A family
// reference resolves to the catalog's concrete build (TF2-11).
func TestDiskSourceImageResolution(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	zone := "us-central1-a"

	cases := []struct {
		name, ref, wantLink, wantID string
	}{
		{
			name:     "family",
			ref:      "projects/debian-cloud/global/images/family/debian-12",
			wantLink: "https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/debian-12-" + familyImageBuild,
			wantID:   numericID("image/debian-cloud/debian-12-" + familyImageBuild),
		},
		{
			name:     "project-relative",
			ref:      "projects/debian-cloud/global/images/debian-11-bullseye-v20251111",
			wantLink: "https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/debian-11-bullseye-v20251111",
			wantID:   numericID("image/debian-cloud/debian-11-bullseye-v20251111"),
		},
		{
			name:     "absolute",
			ref:      "https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/debian-12-bookworm-v20241009",
			wantLink: "https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/debian-12-bookworm-v20241009",
			wantID:   numericID("image/debian-cloud/debian-12-bookworm-v20241009"),
		},
		{
			name:     "universe-url",
			ref:      "https://compute.example.com/compute/v1/projects/debian-cloud/global/images/debian-12-bookworm-v20241009",
			wantLink: "https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/debian-12-bookworm-v20241009",
			wantID:   numericID("image/debian-cloud/debian-12-bookworm-v20241009"),
		},
	}
	for _, tc := range cases {
		if _, err := p.DisksInsert(ctx, newNR(zonal(zone, map[string]any{"body": map[string]any{
			"name": "disk-" + tc.name, "sourceImage": tc.ref,
		}}))); err != nil {
			t.Fatalf("%s: create disk: %v", tc.name, err)
		}
		got, err := p.DisksGet(ctx, newNR(zonal(zone, map[string]any{"disk": "disk-" + tc.name})))
		if err != nil {
			t.Fatalf("%s: get disk: %v", tc.name, err)
		}
		if got.Data["sourceImage"] != tc.wantLink {
			t.Errorf("%s: sourceImage = %v, want %v", tc.name, got.Data["sourceImage"], tc.wantLink)
		}
		if got.Data["sourceImageId"] != tc.wantID {
			t.Errorf("%s: sourceImageId = %v, want %v", tc.name, got.Data["sourceImageId"], tc.wantID)
		}
	}

	if _, err := p.DisksInsert(ctx, newNR(zonal(zone, map[string]any{"body": map[string]any{
		"name": "disk-requested-block", "physicalBlockSizeBytes": 16384,
	}}))); err != nil {
		t.Fatalf("create disk with a requested block size: %v", err)
	}
	block, err := p.DisksGet(ctx, newNR(zonal(zone, map[string]any{"disk": "disk-requested-block"})))
	if err != nil {
		t.Fatalf("get disk with a requested block size: %v", err)
	}
	if block.Data["physicalBlockSizeBytes"] != "16384" {
		t.Errorf("physicalBlockSizeBytes = %v, want the requested 16384", block.Data["physicalBlockSizeBytes"])
	}

	// A reference that resolves to no image fails the create rather than storing
	// a dangling one: a family name asked for as an image, and a project that
	// publishes no public images.
	for _, ref := range []string{
		"projects/debian-cloud/global/images/debian-12",
		"projects/proj/global/images/debian-12-bookworm-v20241009",
	} {
		if _, err := p.DisksInsert(ctx, newNR(zonal(zone, map[string]any{"body": map[string]any{
			"name": "disk-bad", "sourceImage": ref,
		}}))); !isCode(err, "NotFound") {
			t.Fatalf("%s: expected NotFound, got %v", ref, err)
		}
	}
	// An unrecognized spelling is INVALID_ARGUMENT (the API accepts only the
	// documented project/global image and image-family forms).
	if _, err := p.DisksInsert(ctx, newNR(zonal(zone, map[string]any{"body": map[string]any{
		"name": "disk-bad", "sourceImage": "debian-cloud/debian-12",
	}}))); !isCode(err, "InvalidArgument") {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
	if _, err := p.DisksInsert(ctx, newNR(zonal(zone, map[string]any{"body": map[string]any{
		"name": "disk-bad", "sourceImage": 42,
	}}))); !isCode(err, "InvalidArgument") {
		t.Fatalf("non-string sourceImage = %v, want InvalidArgument", err)
	}
}

func TestNetworkAndFirewallCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	netOp, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{
		"name": "vpc-a", "autoCreateSubnetworks": false,
	}}))
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	if netOp.Data["operationType"] != "insert" || netOp.Data["status"] != "DONE" {
		t.Errorf("unexpected network operation: %v", netOp.Data)
	}
	if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{"name": "vpc-a"}})); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
	net, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-a"}))
	if err != nil {
		t.Fatalf("get network: %v", err)
	}
	if net.Data["kind"] != kindNetwork || net.Data["autoCreateSubnetworks"] != false {
		t.Errorf("network = %v", net.Data)
	}
	// networkFirewallPolicyEnforcementOrder is Optional with a provider Default
	// (AFTER_CLASSIC_FIREWALL); Discovery documents the same server default, so
	// a create that omits it must read the default back (TF2-17).
	if net.Data["networkFirewallPolicyEnforcementOrder"] != defaultNetworkFirewallPolicyEnforcementOrder {
		t.Errorf("networkFirewallPolicyEnforcementOrder = %v, want the %s default",
			net.Data["networkFirewallPolicyEnforcementOrder"], defaultNetworkFirewallPolicyEnforcementOrder)
	}
	// mtu is Optional+Computed with a documented server default; a network that
	// omits it must read 1460 back, and one that sets it must echo the caller's
	// value (an absent mtu reads as 0 and permadiffs mtu = 0 -> <configured>).
	if mtu, ok := net.Data["mtu"].(float64); !ok || int(mtu) != defaultNetworkMtu {
		t.Errorf("mtu = %v, want the %d default", net.Data["mtu"], defaultNetworkMtu)
	}
	// A caller-supplied order and mtu are echoed verbatim.
	if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{
		"name": "vpc-b", "autoCreateSubnetworks": false,
		"networkFirewallPolicyEnforcementOrder": "BEFORE_CLASSIC_FIREWALL",
		"mtu":                                   1500,
	}})); err != nil {
		t.Fatalf("create network with explicit order: %v", err)
	}
	explicit, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-b"}))
	if err != nil {
		t.Fatalf("get network with explicit order: %v", err)
	}
	if explicit.Data["networkFirewallPolicyEnforcementOrder"] != "BEFORE_CLASSIC_FIREWALL" {
		t.Errorf("networkFirewallPolicyEnforcementOrder = %v, want the requested BEFORE_CLASSIC_FIREWALL",
			explicit.Data["networkFirewallPolicyEnforcementOrder"])
	}
	if mtu, ok := explicit.Data["mtu"].(float64); !ok || int(mtu) != 1500 {
		t.Errorf("mtu = %v, want the requested 1500", explicit.Data["mtu"])
	}
	if _, err := p.NetworksDelete(ctx, newNR(map[string]any{"network": "vpc-b"})); err != nil {
		t.Fatalf("delete network with explicit order: %v", err)
	}
	nets, err := p.NetworksList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list networks: %v", err)
	}
	if nets.Data["kind"] != kindNetworkList {
		t.Errorf("network list kind = %v", nets.Data["kind"])
	}
	if _, err := p.NetworksDelete(ctx, newNR(map[string]any{"network": "vpc-a"})); err != nil {
		t.Fatalf("delete network: %v", err)
	}
	if _, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-a"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}

	fwOp, err := p.FirewallsInsert(ctx, newNR(map[string]any{"body": map[string]any{
		"name":         "allow-ssh",
		"network":      "vpc-a",
		"sourceRanges": []any{"0.0.0.0/0"},
		"allowed":      []any{map[string]any{"IPProtocol": "tcp", "ports": []any{"22"}}},
	}}))
	if err != nil {
		t.Fatalf("create firewall: %v", err)
	}
	if fwOp.Data["operationType"] != "insert" {
		t.Errorf("unexpected firewall operation: %v", fwOp.Data)
	}
	if _, err := p.FirewallsInsert(ctx, newNR(map[string]any{"body": map[string]any{"name": "allow-ssh"}})); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
	fw, err := p.FirewallsGet(ctx, newNR(map[string]any{"firewall": "allow-ssh"}))
	if err != nil {
		t.Fatalf("get firewall: %v", err)
	}
	if fw.Data["kind"] != kindFirewall || fw.Data["direction"] != "INGRESS" {
		t.Errorf("firewall = %v", fw.Data)
	}
	if !strings.Contains(str(fw.Data["network"]), "global/networks/vpc-a") {
		t.Errorf("firewall network = %v", fw.Data["network"])
	}
	fws, err := p.FirewallsList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list firewalls: %v", err)
	}
	if fws.Data["kind"] != kindFirewallList {
		t.Errorf("firewall list kind = %v", fws.Data["kind"])
	}
	if _, err := p.FirewallsDelete(ctx, newNR(map[string]any{"firewall": "allow-ssh"})); err != nil {
		t.Fatalf("delete firewall: %v", err)
	}
	if _, err := p.FirewallsGet(ctx, newNR(map[string]any{"firewall": "allow-ssh"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

// TestNetworksPatchAppliesMutableFields pins compute.networks.patch (TF2-17-1)
// and its read shape (TF2-17-2). A Terraform google_compute_network update path
// PATCHes the network (with a fingerprinted body for mtu) whenever mtu,
// network_firewall_policy_enforcement_order, routing_mode or a bgp_* field
// changes, then polls the operation it returns — before TF2-17-1, every such
// apply failed with a 501 after the resource was already created. The stored
// fields are applied in place, an absent field keeps its stored value (merge
// semantics, including member-wise for routingConfig), the identity of the
// record is untouched, and the response is the Operation envelope Discovery
// declares.
func TestNetworksPatchAppliesMutableFields(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{
		"name": "vpc-patch", "autoCreateSubnetworks": false, "description": "original",
	}})); err != nil {
		t.Fatalf("create network: %v", err)
	}
	before, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-patch"}))
	if err != nil {
		t.Fatalf("get network: %v", err)
	}

	// The provider's mtu update sends the fingerprint it read plus the new mtu;
	// a network has no fingerprint field, so only mtu must land.
	op, err := p.NetworksPatch(ctx, newNR(map[string]any{
		"network": "vpc-patch",
		"body":    map[string]any{"mtu": 1500, "fingerprint": nil},
	}))
	if err != nil {
		t.Fatalf("patch mtu: %v", err)
	}
	if op.Data["operationType"] != "patch" || op.Data["status"] != "DONE" {
		t.Errorf("patch operation = %v, want operationType patch and status DONE", op.Data)
	}
	if op.Data["targetLink"] != before.Data["selfLink"] {
		t.Errorf("operation targetLink = %v, want the network selfLink %v", op.Data["targetLink"], before.Data["selfLink"])
	}
	if op.Data["name"] == "" || op.Data["id"] == "" {
		t.Errorf("operation envelope missing name/id: %v", op.Data)
	}

	afterMtu, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-patch"}))
	if err != nil {
		t.Fatalf("get network after mtu patch: %v", err)
	}
	if mtu, ok := afterMtu.Data["mtu"].(float64); !ok || int(mtu) != 1500 {
		t.Errorf("mtu = %v, want 1500 after the patch", afterMtu.Data["mtu"])
	}
	// A merge patch that names only mtu must not reset the enforcement order.
	if afterMtu.Data["networkFirewallPolicyEnforcementOrder"] != defaultNetworkFirewallPolicyEnforcementOrder {
		t.Errorf("networkFirewallPolicyEnforcementOrder = %v, want the unchanged %s default",
			afterMtu.Data["networkFirewallPolicyEnforcementOrder"], defaultNetworkFirewallPolicyEnforcementOrder)
	}
	// Identity and non-patchable fields survive the mutation.
	for _, field := range []string{"kind", "id", "name", "selfLink", "targetLink", "creationTimestamp", "description", "autoCreateSubnetworks"} {
		if afterMtu.Data[field] != before.Data[field] {
			t.Errorf("%s changed across a patch: %v -> %v", field, before.Data[field], afterMtu.Data[field])
		}
	}

	// The enforcement order is patchable on its own, and does not reset mtu.
	if _, err := p.NetworksPatch(ctx, newNR(map[string]any{
		"network": "vpc-patch",
		"body":    map[string]any{"networkFirewallPolicyEnforcementOrder": "BEFORE_CLASSIC_FIREWALL"},
	})); err != nil {
		t.Fatalf("patch enforcement order: %v", err)
	}
	afterOrder, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-patch"}))
	if err != nil {
		t.Fatalf("get network after order patch: %v", err)
	}
	if afterOrder.Data["networkFirewallPolicyEnforcementOrder"] != "BEFORE_CLASSIC_FIREWALL" {
		t.Errorf("networkFirewallPolicyEnforcementOrder = %v, want BEFORE_CLASSIC_FIREWALL", afterOrder.Data["networkFirewallPolicyEnforcementOrder"])
	}
	if mtu, ok := afterOrder.Data["mtu"].(float64); !ok || int(mtu) != 1500 {
		t.Errorf("mtu = %v, want the patched 1500 to survive an order-only patch", afterOrder.Data["mtu"])
	}

	// The provider's routing_mode/BGP path sends a collapsed routingConfig and
	// the ULA pair when those change. Both are part of the record's stored shape
	// now (TF2-17-2): the members the body carries are applied, ones it does not
	// carry keep their stored value, and a raw reader sees them.
	if _, err := p.NetworksPatch(ctx, newNR(map[string]any{
		"network": "vpc-patch",
		"body": map[string]any{
			"routingConfig":         map[string]any{"routingMode": "REGIONAL"},
			"enableUlaInternalIpv6": true,
			"internalIpv6Range":     "fd20:0:0::/48",
		},
	})); err != nil {
		t.Fatalf("patch routingConfig: %v", err)
	}
	afterRouting, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-patch"}))
	if err != nil {
		t.Fatalf("get network after routingConfig patch: %v", err)
	}
	routing, ok := afterRouting.Data["routingConfig"].(map[string]any)
	if !ok || routing["routingMode"] != "REGIONAL" {
		t.Errorf("routingConfig = %v, want routingMode REGIONAL", afterRouting.Data["routingConfig"])
	}
	if afterRouting.Data["enableUlaInternalIpv6"] != true || afterRouting.Data["internalIpv6Range"] != "fd20:0:0::/48" {
		t.Errorf("ULA IPv6 pair = %v / %v, want true / fd20:0:0::/48",
			afterRouting.Data["enableUlaInternalIpv6"], afterRouting.Data["internalIpv6Range"])
	}

	// A second routingConfig patch merges member-wise: the BGP members land and
	// the routingMode a previous patch set survives.
	if _, err := p.NetworksPatch(ctx, newNR(map[string]any{
		"network": "vpc-patch",
		"body": map[string]any{"routingConfig": map[string]any{
			"bgpBestPathSelectionMode": "STANDARD",
			"bgpInterRegionCost":       "ADD_COST_TO_MED",
			"bgpAlwaysCompareMed":      true,
		}},
	})); err != nil {
		t.Fatalf("patch BGP members: %v", err)
	}
	afterBGP, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-patch"}))
	if err != nil {
		t.Fatalf("get network after BGP patch: %v", err)
	}
	routing, _ = afterBGP.Data["routingConfig"].(map[string]any)
	wantRouting := map[string]any{
		"routingMode":              "REGIONAL",
		"bgpBestPathSelectionMode": "STANDARD",
		"bgpInterRegionCost":       "ADD_COST_TO_MED",
		"bgpAlwaysCompareMed":      true,
	}
	for field, want := range wantRouting {
		if routing[field] != want {
			t.Errorf("routingConfig.%s = %v, want %v (member-wise merge)", field, routing[field], want)
		}
	}
	// The earlier mtu/order patches are untouched by the routing patch.
	if mtu, ok := afterBGP.Data["mtu"].(float64); !ok || int(mtu) != 1500 {
		t.Errorf("mtu = %v, want the patched 1500 to survive a routing patch", afterBGP.Data["mtu"])
	}
	if afterBGP.Data["networkFirewallPolicyEnforcementOrder"] != "BEFORE_CLASSIC_FIREWALL" {
		t.Errorf("networkFirewallPolicyEnforcementOrder = %v, want the previously patched BEFORE_CLASSIC_FIREWALL",
			afterBGP.Data["networkFirewallPolicyEnforcementOrder"])
	}

	// networkProfile is creation-time only (Discovery) and ForceNew in the
	// provider, so a patch that carries it is ignored.
	if _, err := p.NetworksPatch(ctx, newNR(map[string]any{
		"network": "vpc-patch",
		"body":    map[string]any{"networkProfile": "projects/proj/global/networkProfiles/p"},
	})); err != nil {
		t.Fatalf("patch networkProfile: %v", err)
	}
	afterProfile, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-patch"}))
	if err != nil {
		t.Fatalf("get network after networkProfile patch: %v", err)
	}
	if _, ok := afterProfile.Data["networkProfile"]; ok {
		t.Errorf("networkProfile = %v, want it not patchable", afterProfile.Data["networkProfile"])
	}

	// A retried patch replays its operation instead of mutating twice.
	reqID := "0f8fad5b-d9cb-469f-a165-70867728950e"
	retryParams := map[string]any{
		"network":   "vpc-patch",
		"requestId": reqID,
		"body":      map[string]any{"mtu": 1400},
	}
	retryFirst, err := p.NetworksPatch(ctx, newNR(retryParams))
	if err != nil {
		t.Fatalf("patch with requestId: %v", err)
	}
	retrySecond, err := p.NetworksPatch(ctx, newNR(retryParams))
	if err != nil {
		t.Fatalf("retry patch with requestId: %v", err)
	}
	if retrySecond.Data["name"] != retryFirst.Data["name"] {
		t.Errorf("patch retry replayed %v, want %v", retrySecond.Data["name"], retryFirst.Data["name"])
	}

	// Failures are loud and leave the record untouched.
	if _, err := p.NetworksPatch(ctx, newNR(map[string]any{"network": "no-such-network", "body": map[string]any{"mtu": 1500}})); !isCode(err, "NotFound") {
		t.Errorf("patch of an unknown network = %v, want NotFound", err)
	}
	for _, mtu := range []int{minNetworkMtu - 1, maxNetworkMtu + 1} {
		if _, err := p.NetworksPatch(ctx, newNR(map[string]any{"network": "vpc-patch", "body": map[string]any{"mtu": mtu}})); !isCode(err, "InvalidArgument") {
			t.Errorf("patch mtu=%d = %v, want InvalidArgument", mtu, err)
		}
	}
	if _, err := p.NetworksPatch(ctx, newNR(map[string]any{
		"network": "vpc-patch",
		"body":    map[string]any{"networkFirewallPolicyEnforcementOrder": "SIDEWAYS"},
	})); !isCode(err, "InvalidArgument") {
		t.Errorf("patch bad enforcement order = %v, want InvalidArgument", err)
	}
	// The routingConfig members and the ULA range are validated on patch the same
	// way they are on create.
	for _, bad := range []map[string]any{
		{"routingConfig": map[string]any{"routingMode": "SIDEWAYS"}},
		{"routingConfig": map[string]any{"bgpBestPathSelectionMode": "BEST"}},
		{"routingConfig": map[string]any{"bgpInterRegionCost": "CHEAP"}},
		{"routingConfig": map[string]any{"bgpAlwaysCompareMed": "yes"}},
		{"routingConfig": "REGIONAL"},
		{"internalIpv6Range": "fd30:0:0::/48"},
		{"internalIpv6Range": "fd20:0:0::/56"},
		{"internalIpv6Range": "not-a-range"},
	} {
		if _, err := p.NetworksPatch(ctx, newNR(map[string]any{"network": "vpc-patch", "body": bad})); !isCode(err, "InvalidArgument") {
			t.Errorf("patch %v = %v, want InvalidArgument", bad, err)
		}
	}
	final, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-patch"}))
	if err != nil {
		t.Fatalf("get network after rejected patches: %v", err)
	}
	if mtu, ok := final.Data["mtu"].(float64); !ok || int(mtu) != 1400 {
		t.Errorf("mtu = %v, want the last accepted 1400 (a rejected patch must not mutate)", final.Data["mtu"])
	}
	if routing, _ := final.Data["routingConfig"].(map[string]any); routing["routingMode"] != "REGIONAL" || routing["bgpAlwaysCompareMed"] != true {
		t.Errorf("routingConfig = %v, want the accepted REGIONAL/true pair after the rejected patches", final.Data["routingConfig"])
	}
	if final.Data["internalIpv6Range"] != "fd20:0:0::/48" {
		t.Errorf("internalIpv6Range = %v, want the accepted fd20:0:0::/48 after the rejected patches", final.Data["internalIpv6Range"])
	}

	// delete_bgp_always_compare_med: the hashicorp provider clears the member by
	// sending it as JSON null (plus its undocumented deleteBgpAlwaysCompareMed
	// flag); the other routingConfig members stay.
	if _, err := p.NetworksPatch(ctx, newNR(map[string]any{
		"network": "vpc-patch",
		"body": map[string]any{"routingConfig": map[string]any{
			"bgpAlwaysCompareMed":       nil,
			"deleteBgpAlwaysCompareMed": true,
		}},
	})); err != nil {
		t.Fatalf("clear bgpAlwaysCompareMed: %v", err)
	}
	cleared, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-patch"}))
	if err != nil {
		t.Fatalf("get network after clearing bgpAlwaysCompareMed: %v", err)
	}
	clearedRouting, _ := cleared.Data["routingConfig"].(map[string]any)
	if v, ok := clearedRouting["bgpAlwaysCompareMed"]; ok {
		t.Errorf("bgpAlwaysCompareMed = %v, want the member cleared", v)
	}
	if clearedRouting["routingMode"] != "REGIONAL" || clearedRouting["bgpBestPathSelectionMode"] != "STANDARD" {
		t.Errorf("routingConfig = %v, want the other members kept across the clear", clearedRouting)
	}
}

// TestNetworkCreateValidatesMutableFieldRanges pins that the create path rejects
// the same out-of-range mtu and unknown enforcement order a patch does: real
// Compute validates the field whichever method carries it, and accepting a bad
// value at create only to reject it at the first update is a worse contract.
func TestNetworkCreateValidatesMutableFieldRanges(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, mtu := range []int{minNetworkMtu - 1, maxNetworkMtu + 1} {
		if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{"name": "bad-mtu", "mtu": mtu}})); !isCode(err, "InvalidArgument") {
			t.Errorf("create mtu=%d = %v, want InvalidArgument", mtu, err)
		}
	}
	if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{
		"name": "bad-order", "networkFirewallPolicyEnforcementOrder": "SIDEWAYS",
	}})); !isCode(err, "InvalidArgument") {
		t.Errorf("create with bad enforcement order = %v, want InvalidArgument", err)
	}
	if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{"name": "jumbo", "mtu": 8896}})); err != nil {
		t.Errorf("create mtu=8896 = %v, want success (documented maximum)", err)
	}
}

// TestNetworkReadShapeEchoesRoutingConfigAndUla pins TF2-17-2: the network
// record now keeps the documented read shape a create request carries —
// routingConfig (routingMode and the BGP members), networkProfile, and the ULA
// IPv6 pair — so a raw reader (`gcloud compute networks describe`, which showed
// no routing mode or BGP state) sees what was set. Only supplied fields are
// echoed: Network.routingConfig documents no server default, and while
// Discovery says real Compute allocates a /48 when ULA is enabled without a
// range, the emulator has no ULA allocator and does not invent one.
func TestNetworkReadShapeEchoesRoutingConfigAndUla(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	const profile = "projects/proj/global/networkProfiles/global-profiles"
	if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{
		"name":                  "vpc-shape",
		"autoCreateSubnetworks": false,
		"networkProfile":        profile,
		"enableUlaInternalIpv6": true,
		"internalIpv6Range":     "fd20:0:1234::/48",
		"routingConfig": map[string]any{
			"routingMode":              "GLOBAL",
			"bgpBestPathSelectionMode": "STANDARD",
			"bgpInterRegionCost":       "ADD_COST_TO_MED",
			"bgpAlwaysCompareMed":      true,
			// Output-only members are never persisted or echoed.
			"effectiveBgpAlwaysCompareMed": true,
			"effectiveBgpInterRegionCost":  "ADD_COST_TO_MED",
		},
	}})); err != nil {
		t.Fatalf("create network with the full read shape: %v", err)
	}
	got, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-shape"}))
	if err != nil {
		t.Fatalf("get network: %v", err)
	}
	routing, ok := got.Data["routingConfig"].(map[string]any)
	if !ok {
		t.Fatalf("routingConfig = %v, want the persisted message", got.Data["routingConfig"])
	}
	for field, want := range map[string]any{
		"routingMode":              "GLOBAL",
		"bgpBestPathSelectionMode": "STANDARD",
		"bgpInterRegionCost":       "ADD_COST_TO_MED",
		"bgpAlwaysCompareMed":      true,
	} {
		if routing[field] != want {
			t.Errorf("routingConfig.%s = %v, want %v", field, routing[field], want)
		}
	}
	for _, outputOnly := range []string{"effectiveBgpAlwaysCompareMed", "effectiveBgpInterRegionCost"} {
		if _, ok := routing[outputOnly]; ok {
			t.Errorf("routingConfig.%s = %v, want the output-only member dropped", outputOnly, routing[outputOnly])
		}
	}
	if got.Data["networkProfile"] != profile {
		t.Errorf("networkProfile = %v, want the supplied %s", got.Data["networkProfile"], profile)
	}
	if got.Data["enableUlaInternalIpv6"] != true {
		t.Errorf("enableUlaInternalIpv6 = %v, want true", got.Data["enableUlaInternalIpv6"])
	}
	if got.Data["internalIpv6Range"] != "fd20:0:1234::/48" {
		t.Errorf("internalIpv6Range = %v, want the supplied fd20:0:1234::/48", got.Data["internalIpv6Range"])
	}

	// networks.list renders the same record.
	list, err := p.NetworksList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list networks: %v", err)
	}
	items, _ := list.Data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("network list items = %v, want 1", list.Data["items"])
	}
	listed, _ := items[0].(map[string]any)
	if listed["networkProfile"] != profile || listed["enableUlaInternalIpv6"] != true {
		t.Errorf("listed network = %v, want the same networkProfile/ULA pair as get", listed)
	}

	// A network created without any of them reads none of them back — no
	// fabricated routingConfig and no invented /48.
	if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{
		"name": "vpc-bare", "autoCreateSubnetworks": false,
	}})); err != nil {
		t.Fatalf("create bare network: %v", err)
	}
	bare, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-bare"}))
	if err != nil {
		t.Fatalf("get bare network: %v", err)
	}
	for _, field := range []string{"routingConfig", "networkProfile", "enableUlaInternalIpv6", "internalIpv6Range"} {
		if v, ok := bare.Data[field]; ok {
			t.Errorf("%s = %v on a network created without it, want absent", field, v)
		}
	}

	// ULA enabled without a range keeps enableUlaInternalIpv6 but invents no
	// range (Discovery documents a server allocation the emulator does not do).
	if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{
		"name": "vpc-ula", "enableUlaInternalIpv6": true,
	}})); err != nil {
		t.Fatalf("create ULA network without a range: %v", err)
	}
	ula, err := p.NetworksGet(ctx, newNR(map[string]any{"network": "vpc-ula"}))
	if err != nil {
		t.Fatalf("get ULA network: %v", err)
	}
	if ula.Data["enableUlaInternalIpv6"] != true {
		t.Errorf("enableUlaInternalIpv6 = %v, want true", ula.Data["enableUlaInternalIpv6"])
	}
	if v, ok := ula.Data["internalIpv6Range"]; ok {
		t.Errorf("internalIpv6Range = %v, want absent (the emulator allocates no /48)", v)
	}
}

// TestNetworkCreateValidatesReadShapeFields pins that create rejects the invalid
// enum/range a patch rejects, because real Compute validates a field whichever
// method carries it.
func TestNetworkCreateValidatesReadShapeFields(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, bad := range []map[string]any{
		{"routingConfig": map[string]any{"routingMode": "SIDEWAYS"}},
		{"routingConfig": map[string]any{"bgpBestPathSelectionMode": "BEST"}},
		{"routingConfig": map[string]any{"bgpInterRegionCost": "CHEAP"}},
		{"routingConfig": map[string]any{"bgpAlwaysCompareMed": "yes"}},
		{"routingConfig": []any{"REGIONAL"}},
		{"networkProfile": 7},
		{"enableUlaInternalIpv6": "yes"},
		{"internalIpv6Range": "fd30:0:0::/48"},
		{"internalIpv6Range": "fd20:0:0::/56"},
		{"internalIpv6Range": "fd20:1234:5678:9::/48"},
	} {
		body := map[string]any{"name": "bad-shape"}
		for k, v := range bad {
			body[k] = v
		}
		if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": body})); !isCode(err, "InvalidArgument") {
			t.Errorf("create %v = %v, want InvalidArgument", bad, err)
		}
	}
	// The documented boundaries are accepted: a /48 at the start of fd20::/20
	// and the enum members in any combination.
	if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{
		"name":              "good-shape",
		"internalIpv6Range": "fd20::/48",
		"routingConfig": map[string]any{
			"routingMode":              "REGIONAL",
			"bgpBestPathSelectionMode": "LEGACY",
			"bgpInterRegionCost":       "DEFAULT",
			"bgpAlwaysCompareMed":      false,
		},
	}})); err != nil {
		t.Errorf("create with documented boundary values = %v, want success", err)
	}
}

func TestSubnetworkCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	op, err := p.SubnetworksInsert(ctx, newNR(regional("us-central1", map[string]any{"body": map[string]any{
		"name": "sub-a", "network": "vpc-a", "ipCidrRange": "10.1.0.0/20",
	}})))
	if err != nil {
		t.Fatalf("create subnetwork: %v", err)
	}
	if op.Data["operationType"] != "insert" {
		t.Errorf("unexpected subnetwork operation: %v", op.Data)
	}
	if _, err := p.SubnetworksInsert(ctx, newNR(regional("us-central1", map[string]any{"body": map[string]any{"name": "sub-a"}}))); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
	sub, err := p.SubnetworksGet(ctx, newNR(regional("us-central1", map[string]any{"subnetwork": "sub-a"})))
	if err != nil {
		t.Fatalf("get subnetwork: %v", err)
	}
	if sub.Data["kind"] != kindSubnetwork || sub.Data["ipCidrRange"] != "10.1.0.0/20" {
		t.Errorf("subnetwork = %v", sub.Data)
	}
	if !strings.Contains(str(sub.Data["region"]), "regions/us-central1") {
		t.Errorf("subnetwork region = %v", sub.Data["region"])
	}
	// Regional scoping: a different region must not see it.
	if _, err := p.SubnetworksGet(ctx, newNR(regional("us-east1", map[string]any{"subnetwork": "sub-a"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound in other region, got %v", err)
	}
	list, err := p.SubnetworksList(ctx, newNR(regional("us-central1", nil)))
	if err != nil {
		t.Fatalf("list subnetworks: %v", err)
	}
	if list.Data["kind"] != kindSubnetworkList {
		t.Errorf("subnetwork list kind = %v", list.Data["kind"])
	}
	if _, err := p.SubnetworksDelete(ctx, newNR(regional("us-central1", map[string]any{"subnetwork": "sub-a"}))); err != nil {
		t.Fatalf("delete subnetwork: %v", err)
	}
}

func TestOperationsPersistedAndLookup(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	op := mustCreateInstance(t, p, "us-central1-a", "vm-a")
	opName, _ := op["name"].(string)
	if opName == "" {
		t.Fatal("operation name is empty")
	}
	if op["targetId"] == "" || op["targetLink"] == "" || op["selfLink"] == "" {
		t.Errorf("operation links must be populated: %v", op)
	}
	if op["insertTime"] == "" || op["endTime"] == "" {
		t.Errorf("operation times must be populated: %v", op)
	}

	got, err := p.OperationsGet(ctx, newNR(zonal("us-central1-a", map[string]any{"operation": opName})))
	if err != nil {
		t.Fatalf("operations.get: %v", err)
	}
	if got.Data["name"] != opName || got.Data["status"] != "DONE" {
		t.Errorf("operation readback = %v", got.Data)
	}
	if !strings.Contains(str(got.Data["selfLink"]), "zones/us-central1-a/operations/") {
		t.Errorf("operation selfLink = %v", got.Data["selfLink"])
	}

	list, err := p.OperationsList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("operations.list: %v", err)
	}
	if list.Data["kind"] != kindOperationList {
		t.Errorf("operations list kind = %v", list.Data["kind"])
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(items))
	}

	// An operation is scoped: another zone must not see it.
	if _, err := p.OperationsGet(ctx, newNR(zonal("us-central1-b", map[string]any{"operation": opName}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound in other zone, got %v", err)
	}
	if _, err := p.OperationsGet(ctx, newNR(zonal("us-central1-a", map[string]any{"operation": "missing"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound, got %v", err)
	}

	// A global mutation persists its operation at global scope.
	netOp, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{"name": "global-net"}}))
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	globalName, _ := netOp.Data["name"].(string)
	globalGot, err := p.OperationsGet(ctx, newNR(map[string]any{"operation": globalName}))
	if err != nil {
		t.Fatalf("global operations.get: %v", err)
	}
	if !strings.Contains(str(globalGot.Data["selfLink"]), "global/operations/") {
		t.Errorf("global operation selfLink = %v", globalGot.Data["selfLink"])
	}
}

func TestSynthesizedDiscovery(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	zones, err := p.ZonesList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("zones list: %v", err)
	}
	if zones.Data["kind"] != kindZoneList {
		t.Errorf("zones kind = %v", zones.Data["kind"])
	}
	if items, _ := zones.Data["items"].([]any); len(items) == 0 {
		t.Fatal("expected zones")
	}
	zone, err := p.ZonesGet(ctx, newNR(map[string]any{"zone": "us-central1-a"}))
	if err != nil {
		t.Fatalf("zone get: %v", err)
	}
	if zone.Data["kind"] != kindZone || zone.Data["status"] != "UP" {
		t.Errorf("zone = %v", zone.Data)
	}
	if _, err := p.ZonesGet(ctx, newNR(map[string]any{"zone": "us-moon-1a"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound for unknown zone, got %v", err)
	}

	regions, err := p.RegionsList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("regions list: %v", err)
	}
	if regions.Data["kind"] != kindRegionList {
		t.Errorf("regions kind = %v", regions.Data["kind"])
	}
	region, err := p.RegionsGet(ctx, newNR(map[string]any{"region": "us-central1"}))
	if err != nil {
		t.Fatalf("region get: %v", err)
	}
	if zoneURLs, _ := region.Data["zones"].([]any); len(zoneURLs) == 0 {
		t.Error("region must list its zones")
	}
	if _, err := p.RegionsGet(ctx, newNR(map[string]any{"region": "mars-central1"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound for unknown region, got %v", err)
	}

	mts, err := p.MachineTypesList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("machine types list: %v", err)
	}
	if mts.Data["kind"] != kindMachineTypeList {
		t.Errorf("machine types kind = %v", mts.Data["kind"])
	}
	if items, _ := mts.Data["items"].([]any); len(items) == 0 {
		t.Fatal("expected machine types")
	}
	mt, err := p.MachineTypesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"machineType": "e2-micro"})))
	if err != nil {
		t.Fatalf("machine type get: %v", err)
	}
	if mt.Data["kind"] != kindMachineType || mt.Data["guestCpus"] != int64(2) {
		t.Errorf("machine type = %v", mt.Data)
	}
	if _, err := p.MachineTypesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"machineType": "nope"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

// projectNR is newNR for a request addressed at another project (the compute
// image catalog is only served from the public image projects).
func projectNR(project string, params map[string]any) *model.NormalizedRequest {
	nr := newNR(params)
	nr.AccountID = project
	nr.ResourceID = resource.ResourceID(project)
	nr.Params["project"] = project
	return nr
}

// TestImagesGet pins compute.images.get: the emulator serves a synthesized READY
// public image for a concrete build name in a public image project, 404s a
// family name (so the hashicorp/google provider falls back to the family
// lookup) and 404s any project that does not publish public images (TF2-11).
func TestImagesGet(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	resp, err := p.ImagesGet(ctx, projectNR("debian-cloud", map[string]any{"image": "debian-12-bookworm-v20241009"}))
	if err != nil {
		t.Fatalf("images get: %v", err)
	}
	img := resp.Data
	if img["kind"] != kindImage {
		t.Errorf("kind = %v, want %s", img["kind"], kindImage)
	}
	if img["name"] != "debian-12-bookworm-v20241009" {
		t.Errorf("name = %v", img["name"])
	}
	if img["family"] != "debian-12-bookworm" {
		t.Errorf("family = %v, want debian-12-bookworm", img["family"])
	}
	if img["status"] != "READY" || img["sourceType"] != "RAW" || img["architecture"] != "X86_64" {
		t.Errorf("image status/sourceType/architecture = %v/%v/%v", img["status"], img["sourceType"], img["architecture"])
	}
	if img["diskSizeGb"] != imageDiskSizeGB {
		t.Errorf("diskSizeGb = %v, want %v", img["diskSizeGb"], imageDiskSizeGB)
	}
	wantSelf := "https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/debian-12-bookworm-v20241009"
	if img["selfLink"] != wantSelf {
		t.Errorf("selfLink = %v, want %v", img["selfLink"], wantSelf)
	}
	if img["creationTimestamp"] != "2024-10-09T00:00:00.000-07:00" {
		t.Errorf("creationTimestamp = %v, want the build date", img["creationTimestamp"])
	}
	licenses, _ := img["licenses"].([]string)
	if len(licenses) != 1 || licenses[0] != "https://www.googleapis.com/compute/v1/projects/debian-cloud/global/licenses/debian-12-bookworm" {
		t.Errorf("licenses = %v", img["licenses"])
	}
	if feats, _ := img["guestOsFeatures"].([]any); len(feats) == 0 {
		t.Error("expected guestOsFeatures")
	}

	// Two reads of the same name are identical: nothing is stored, so a read
	// after a restart (or from another gateway) reports the same resource.
	again, err := p.ImagesGet(ctx, projectNR("debian-cloud", map[string]any{"image": "debian-12-bookworm-v20241009"}))
	if err != nil {
		t.Fatalf("images get again: %v", err)
	}
	if !reflect.DeepEqual(resp.Data, again.Data) {
		t.Errorf("repeated read differs:\n%v\n%v", resp.Data, again.Data)
	}

	// Cloud COS images carry a milestone/build/patch/revision label instead of a
	// -vYYYYMMDD date; the family is still derived.
	cos, err := p.ImagesGet(ctx, projectNR("cos-cloud", map[string]any{"image": "cos-stable-109-17800-147-54"}))
	if err != nil {
		t.Fatalf("cos image get: %v", err)
	}
	if cos.Data["family"] != "cos-stable" {
		t.Errorf("cos family = %v, want cos-stable", cos.Data["family"])
	}

	// A family name is not an image: the provider's resolver probes images.get
	// first and relies on this 404 to read the name as a family instead.
	if _, err := p.ImagesGet(ctx, projectNR("debian-cloud", map[string]any{"image": "debian-12"})); !isCode(err, "NotFound") {
		t.Fatalf("family name through images.get = %v, want NotFound", err)
	}

	// No image registry outside the public image projects: a project's own
	// images cannot exist (images.insert is not served), so a read is a 404.
	if _, err := p.ImagesGet(ctx, projectNR("proj", map[string]any{"image": "debian-12-bookworm-v20241009"})); !isCode(err, "NotFound") {
		t.Fatalf("image in a non-public project = %v, want NotFound", err)
	}

	// Malformed names are rejected before any lookup, like every other compute
	// resource name.
	if _, err := p.ImagesGet(ctx, projectNR("debian-cloud", map[string]any{"image": "Debian_12"})); !isCode(err, "InvalidArgument") {
		t.Fatalf("malformed image name = %v, want InvalidArgument", err)
	}
	if _, err := p.ImagesGet(ctx, projectNR("debian-cloud", nil)); !isCode(err, "InvalidArgument") {
		t.Fatalf("missing image = %v, want InvalidArgument", err)
	}
}

// TestImagesGetFromFamily pins compute.images.getFromFamily: any family name in
// a public image project answers one stable synthesized build, and that build
// reads back through images.get as the same family.
func TestImagesGetFromFamily(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	resp, err := p.ImagesGetFromFamily(ctx, projectNR("debian-cloud", map[string]any{"family": "debian-12"}))
	if err != nil {
		t.Fatalf("images get from family: %v", err)
	}
	img := resp.Data
	if img["kind"] != kindImage || img["status"] != "READY" {
		t.Errorf("image = %v", img)
	}
	if img["name"] != "debian-12-"+familyImageBuild {
		t.Errorf("name = %v, want debian-12-%s", img["name"], familyImageBuild)
	}
	if img["family"] != "debian-12" {
		t.Errorf("family = %v, want debian-12", img["family"])
	}
	if img["creationTimestamp"] != "2024-01-01T00:00:00.000-07:00" {
		t.Errorf("creationTimestamp = %v, want the synthesized build date", img["creationTimestamp"])
	}

	// The name a family read reports is a concrete build the same catalog
	// serves, and it derives the same family.
	back, err := p.ImagesGet(ctx, projectNR("debian-cloud", map[string]any{"image": img["name"]}))
	if err != nil {
		t.Fatalf("read the family build: %v", err)
	}
	if back.Data["family"] != "debian-12" || back.Data["selfLink"] != img["selfLink"] {
		t.Errorf("family build reads back as %v / %v", back.Data["family"], back.Data["selfLink"])
	}

	if _, err := p.ImagesGetFromFamily(ctx, projectNR("proj", map[string]any{"family": "debian-12"})); !isCode(err, "NotFound") {
		t.Fatalf("family in a non-public project = %v, want NotFound", err)
	}
	if _, err := p.ImagesGetFromFamily(ctx, projectNR("debian-cloud", map[string]any{"family": "Debian_12"})); !isCode(err, "InvalidArgument") {
		t.Fatalf("malformed family = %v, want InvalidArgument", err)
	}
	if _, err := p.ImagesGetFromFamily(ctx, projectNR("debian-cloud", nil)); !isCode(err, "InvalidArgument") {
		t.Fatalf("missing family = %v, want InvalidArgument", err)
	}
}

func TestUnimplemented(t *testing.T) {
	if _, err := newProvider().Unimplemented(context.Background(), newNR(nil)); !isCode(err, "Unimplemented") {
		t.Fatalf("expected Unimplemented, got %v", err)
	}
}

// TestProjectsGet pins compute.projects.get: the synthesized compute#project
// answers for the project id in the request path (arbitrary ids resolve, like
// Resource Manager), carries the schema's required shape, and reports the
// project's default service account — the field the hashicorp/google
// `google_compute_default_service_account` data source reads (TF2-1).
func TestProjectsGet(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	resp, err := p.ProjectsGet(ctx, newNR(map[string]any{"project": "proj"}))
	if err != nil {
		t.Fatalf("projects get: %v", err)
	}
	if resp.Data["kind"] != kindProject {
		t.Errorf("kind = %v, want %s", resp.Data["kind"], kindProject)
	}
	if resp.Data["name"] != "proj" {
		t.Errorf("name = %v, want proj", resp.Data["name"])
	}
	number := resource.ProjectNumber("proj")
	if resp.Data["id"] != number {
		t.Errorf("id = %v, want the project number %s", resp.Data["id"], number)
	}
	wantSA := number + "-compute@developer.gserviceaccount.com"
	if resp.Data["defaultServiceAccount"] != wantSA {
		t.Errorf("defaultServiceAccount = %v, want %s", resp.Data["defaultServiceAccount"], wantSA)
	}
	if got, want := resp.Data["selfLink"], computeBasePath+"projects/proj"; got != want {
		t.Errorf("selfLink = %v, want %s", got, want)
	}
	if got, want := resp.Data["defaultNetworkTier"], "PREMIUM"; got != want {
		t.Errorf("defaultNetworkTier = %v, want %s", got, want)
	}
	if got, want := resp.Data["vmDnsSetting"], "ZONAL_ONLY"; got != want {
		t.Errorf("vmDnsSetting = %v, want %s", got, want)
	}

	// Quotas are the Discovery Quota shape (metric/limit/usage) over the same
	// four metrics the region surface advertises, with JSON-number limits.
	wantMetrics := []string{"CPUS", "DISKS_TOTAL_GB", "STATIC_ADDRESSES", "IN_USE_ADDRESSES"}
	quotas, ok := resp.Data["quotas"].([]any)
	if !ok || len(quotas) != len(wantMetrics) {
		t.Fatalf("quotas = %v, want %d entries", resp.Data["quotas"], len(wantMetrics))
	}
	for i, q := range quotas {
		qm, ok := q.(map[string]any)
		if !ok {
			t.Fatalf("quota[%d] = %v, want an object", i, q)
		}
		if qm["metric"] != wantMetrics[i] {
			t.Errorf("quota[%d].metric = %v, want %s", i, qm["metric"], wantMetrics[i])
		}
		if _, ok := qm["limit"].(float64); !ok {
			t.Errorf("quota[%d].limit = %T, want a JSON number", i, qm["limit"])
		}
		if _, ok := qm["usage"].(float64); !ok {
			t.Errorf("quota[%d].usage = %T, want a JSON number", i, qm["usage"])
		}
	}

	// A project that is not a Shared VPC host reports the unspecified member of
	// the output-only xpnProjectStatus enum.
	if got, want := resp.Data["xpnProjectStatus"], "UNSPECIFIED_XPN_PROJECT_STATUS"; got != want {
		t.Errorf("xpnProjectStatus = %v, want %s", got, want)
	}

	// commonInstanceMetadata is the empty default envelope with a stable
	// fingerprint: a second read must not churn (Terraform would see drift).
	md, _ := resp.Data["commonInstanceMetadata"].(map[string]any)
	if md["kind"] != "compute#metadata" || md["fingerprint"] == "" {
		t.Errorf("commonInstanceMetadata = %v, want kind+fingerprint", md)
	}
	again, err := p.ProjectsGet(ctx, newNR(map[string]any{"project": "proj"}))
	if err != nil {
		t.Fatalf("projects get (again): %v", err)
	}
	md2, _ := again.Data["commonInstanceMetadata"].(map[string]any)
	if md2["fingerprint"] != md["fingerprint"] {
		t.Errorf("fingerprint churned between reads: %v then %v", md["fingerprint"], md2["fingerprint"])
	}

	// The reported account is the platform-wide implicit default that
	// serviceaccount.ImplicitDefault classifies as live (the classifier
	// Eventarc's trigger validation uses), and it is the operation envelope's
	// user: one identity, one derivation. Note the read path is a separate
	// gap — serviceAccounts.get does not render the managed default (TF2-1-1).
	if !serviceaccount.ImplicitDefault("proj", wantSA) {
		t.Errorf("serviceaccount.ImplicitDefault does not recognize %q", wantSA)
	}
	if got := serviceAccountEmail("proj"); got != wantSA {
		t.Errorf("operation user = %s, want the project default account %s", got, wantSA)
	}

	// The response describes the requested project, not a fixed default: the
	// path project wins over the request's bound ResourceID, for every field
	// that names the project (name, selfLink, id, defaultServiceAccount).
	nr := newNR(map[string]any{"project": "other"})
	other, err := p.ProjectsGet(ctx, nr)
	if err != nil {
		t.Fatalf("projects get (other): %v", err)
	}
	if other.Data["name"] != "other" {
		t.Errorf("name = %v, want the path project other", other.Data["name"])
	}
	if got, want := other.Data["selfLink"], computeBasePath+"projects/other"; got != want {
		t.Errorf("selfLink = %v, want %s", got, want)
	}
	otherNumber := resource.ProjectNumber("other")
	if other.Data["id"] != otherNumber {
		t.Errorf("id = %v, want the project number %s", other.Data["id"], otherNumber)
	}
	if got, want := other.Data["defaultServiceAccount"], otherNumber+"-compute@developer.gserviceaccount.com"; got != want {
		t.Errorf("defaultServiceAccount = %v, want %s", got, want)
	}
	if otherNumber == number {
		t.Error("project number must differ between project ids")
	}

	// Discovery's project parameter also accepts a project number. The
	// synthesized number is not invertible, so a numeric reference is taken at
	// face value: it is the number, and the id equals the request.
	byNumber, err := p.ProjectsGet(ctx, newNR(map[string]any{"project": otherNumber}))
	if err != nil {
		t.Fatalf("projects get (by number): %v", err)
	}
	if byNumber.Data["id"] != otherNumber {
		t.Errorf("id = %v, want the requested number %s", byNumber.Data["id"], otherNumber)
	}
	if got, want := byNumber.Data["defaultServiceAccount"], otherNumber+"-compute@developer.gserviceaccount.com"; got != want {
		t.Errorf("defaultServiceAccount = %v, want %s", got, want)
	}
	if got, want := byNumber.Data["selfLink"], computeBasePath+"projects/"+otherNumber; got != want {
		t.Errorf("selfLink = %v, want %s", got, want)
	}
	// The same identity rule must hold for the operation envelope on that
	// reference, or a mutation through a numeric path would report a different
	// account than projects.get.
	if got, want := serviceAccountEmail(otherNumber), byNumber.Data["defaultServiceAccount"]; got != want {
		t.Errorf("operation user = %s, want the projects.get account %v", got, want)
	}

	// A missing project is INVALID_ARGUMENT, like Resource Manager's get.
	if _, err := p.ProjectsGet(ctx, newNR(nil)); !isCode(err, "InvalidArgument") {
		t.Fatalf("expected InvalidArgument for an empty project, got %v", err)
	}
}

// TestResourceNameValidation pins Compute Engine's documented name grammar
// (Discovery, Instance.name): 1-63 chars matching [a-z]([-a-z0-9]*[a-z0-9])?.
func TestResourceNameValidation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	bad := []string{"Upper", "under_score", "-leading", "trailing-", "with space", strings.Repeat("a", 64)}

	for _, name := range bad {
		if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": instanceBody(name)}))); !isCode(err, "InvalidArgument") {
			t.Errorf("instance name %q: expected InvalidArgument, got %v", name, err)
		}
		if _, err := p.DisksInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{"name": name}}))); !isCode(err, "InvalidArgument") {
			t.Errorf("disk name %q: expected InvalidArgument, got %v", name, err)
		}
		if _, err := p.NetworksInsert(ctx, newNR(map[string]any{"body": map[string]any{"name": name}})); !isCode(err, "InvalidArgument") {
			t.Errorf("network name %q: expected InvalidArgument, got %v", name, err)
		}
		if _, err := p.FirewallsInsert(ctx, newNR(map[string]any{"body": map[string]any{"name": name}})); !isCode(err, "InvalidArgument") {
			t.Errorf("firewall name %q: expected InvalidArgument, got %v", name, err)
		}
		if _, err := p.SubnetworksInsert(ctx, newNR(regional("us-central1", map[string]any{"body": map[string]any{"name": name}}))); !isCode(err, "InvalidArgument") {
			t.Errorf("subnetwork name %q: expected InvalidArgument, got %v", name, err)
		}
	}

	// A single-character name and an all-digit tail are valid.
	for _, name := range []string{"a", "vm-1", "web-01"} {
		if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": instanceBody(name)}))); err != nil {
			t.Errorf("instance name %q: expected success, got %v", name, err)
		}
	}
}

// TestMachineTypeValidationOnInsert pins that an unknown machine type is
// rejected with 400 (real GCP) instead of silently accepted.
func TestMachineTypeValidationOnInsert(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	// Unknown machine type.
	if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{
		"name": "vm-bogus", "machineType": "bogus-type",
	}}))); !isCode(err, "InvalidArgument") {
		t.Fatalf("expected InvalidArgument for unknown machine type, got %v", err)
	}

	// A full/partial machine-type URL resolves to its last segment.
	if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{
		"name": "vm-url", "machineType": "projects/proj/zones/us-central1-a/machineTypes/e2-micro",
	}}))); err != nil {
		t.Fatalf("full machine-type URL must be accepted, got %v", err)
	}

	// Real machine types missing from the advertised catalog must still be
	// accepted (the catalog is a subset, not the universe).
	for i, mt := range []string{"e2-standard-4", "n2-highmem-8", "c3-highcpu-4", "custom-2-4096", "n2-custom-2-4096", "n2-custom-2-4096-ext"} {
		if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{
			"name": fmt.Sprintf("vm-real-%d", i), "machineType": mt,
		}}))); err != nil {
			t.Errorf("real machine type %q must be accepted, got %v", mt, err)
		}
	}

	// An absent machine type defaults to e2-micro.
	if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{"body": map[string]any{
		"name": "vm-default",
	}}))); err != nil {
		t.Fatalf("insert without machineType: %v", err)
	}
	inst, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-default"})))
	if err != nil {
		t.Fatalf("get default instance: %v", err)
	}
	if !strings.Contains(str(inst.Data["machineType"]), "machineTypes/"+defaultMachineType) {
		t.Errorf("default machineType = %v, want %s", inst.Data["machineType"], defaultMachineType)
	}
}

// TestListFilterThroughHandlers exercises the documented filter subset through
// the provider handlers (not just the parser).
func TestListFilterThroughHandlers(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, n := range []string{"web-a", "web-b", "db-a"} {
		mustCreateInstance(t, p, "us-central1-a", n)
	}

	// Filter by name equality.
	resp, err := p.InstancesList(ctx, newNR(zonal("us-central1-a", map[string]any{"filter": "name = web-a"})))
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	if len(items) != 1 || stringField(items[0].(map[string]any), "name") != "web-a" {
		t.Fatalf("name filter = %v", items)
	}

	// Filter by status and by labels (instances are RUNNING with env=test).
	for _, expr := range []string{"status = RUNNING", "labels.env = test"} {
		resp, err := p.InstancesList(ctx, newNR(zonal("us-central1-a", map[string]any{"filter": expr})))
		if err != nil {
			t.Fatalf("filter %q: %v", expr, err)
		}
		if items, _ := resp.Data["items"].([]any); len(items) != 3 {
			t.Errorf("filter %q = %d items, want 3", expr, len(items))
		}
	}

	// Negation.
	resp, err = p.InstancesList(ctx, newNR(zonal("us-central1-a", map[string]any{"filter": "name != web-a"})))
	if err != nil {
		t.Fatalf("negated filter: %v", err)
	}
	if items, _ := resp.Data["items"].([]any); len(items) != 2 {
		t.Errorf("name != web-a = %d items, want 2", len(items))
	}

	// A filter outside the subset fails loud rather than returning everything.
	if _, err := p.InstancesList(ctx, newNR(zonal("us-central1-a", map[string]any{"filter": "cpuPlatform eq .*"}))); !isCode(err, "InvalidArgument") {
		t.Fatalf("unsupported filter must be InvalidArgument, got %v", err)
	}
}

// TestAggregatedListPaging covers the documented maxResults/pageToken cursor
// over the flattened instance set, re-grouped by zone.
func TestAggregatedListPaging(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "us-central1-a", "vm-1")
	mustCreateInstance(t, p, "us-central1-a", "vm-2")
	mustCreateInstance(t, p, "us-east1-b", "vm-3")

	seen := map[string]bool{}
	token := ""
	pages := 0
	for {
		params := map[string]any{"maxResults": 1}
		if token != "" {
			params["pageToken"] = token
		}
		resp, err := p.InstancesAggregatedList(ctx, newNR(params))
		if err != nil {
			t.Fatalf("aggregated page: %v", err)
		}
		if resp.Data["kind"] != kindInstanceAggregatedList {
			t.Errorf("kind = %v", resp.Data["kind"])
		}
		items, _ := resp.Data["items"].(map[string]any)
		for _, g := range items {
			group, _ := g.(map[string]any)
			insts, _ := group["instances"].([]any)
			for _, i := range insts {
				name := stringField(i.(map[string]any), "name")
				if seen[name] {
					t.Errorf("instance %q returned on more than one page", name)
				}
				seen[name] = true
			}
		}
		token, _ = resp.Data["nextPageToken"].(string)
		pages++
		if token == "" {
			break
		}
		if pages > 10 {
			t.Fatal("paging did not terminate")
		}
	}
	if len(seen) != 3 {
		t.Fatalf("paged aggregated list returned %d instances, want 3: %v", len(seen), seen)
	}
	if pages != 3 {
		t.Errorf("pages = %d, want 3", pages)
	}

	// The aggregated list also honours the filter.
	resp, err := p.InstancesAggregatedList(ctx, newNR(map[string]any{"filter": "name = vm-3"}))
	if err != nil {
		t.Fatalf("filtered aggregated list: %v", err)
	}
	items, _ := resp.Data["items"].(map[string]any)
	if _, ok := items["zones/us-east1-b"]; !ok {
		t.Errorf("filtered aggregated list = %v", items)
	}
	if _, ok := items["zones/us-central1-a"]; ok {
		t.Errorf("zone a must be filtered out: %v", items)
	}
}

// TestListParamsPageSizeDefaults pins Compute Engine's documented 500-result
// default and 0-500 range (the shared paging helper defaults to 1000).
func TestListParamsPageSizeDefaults(t *testing.T) {
	cases := []struct {
		label  string
		params map[string]any
		want   int
	}{
		{"absent", nil, defaultComputePageSize},
		{"maxResults int", map[string]any{"maxResults": 2}, 2},
		{"maxResults string", map[string]any{"maxResults": "3"}, 3},
		{"zero means default", map[string]any{"maxResults": 0}, defaultComputePageSize},
		{"clamped to max", map[string]any{"maxResults": 501}, maxComputePageSize},
		{"at max", map[string]any{"maxResults": 500}, 500},
		// maxResults is a uint32 in Compute's Discovery document, so a value
		// above int32 is a legal binding; the documented 0-500 range clamps it.
		{"above int32 but valid uint32", map[string]any{"maxResults": "2147483648"}, maxComputePageSize},
		// Compute Engine has no pageSize parameter; it must not override maxResults.
		{"pageSize ignored", map[string]any{"pageSize": 7}, defaultComputePageSize},
	}
	for _, tc := range cases {
		params, err := listParams(newNR(tc.params))
		if err != nil {
			t.Errorf("%s: listParams error = %v", tc.label, err)
			continue
		}
		if got := params["pageSize"]; got != tc.want {
			t.Errorf("%s: pageSize = %v, want %d", tc.label, got, tc.want)
		}
	}
}

// TestListParamsRejectsBadMaxResults pins the uint32 query binding Compute's
// Discovery document declares for maxResults: an unparseable value, a fraction,
// an explicitly empty one (DC63) or a negative one is INVALID_ARGUMENT, never a
// silent fall back to the 500 default page — the same answer real GCP's binding
// gives. Compute's frontend binds its query parameters on the `header` message,
// so the message names 'header.max_results' (DC68).
func TestListParamsRejectsBadMaxResults(t *testing.T) {
	for _, v := range []any{"", "abc", "1.5", "-1", "-0.5", "4294967296", float64(1.5), int64(-1)} {
		_, err := listParams(newNR(map[string]any{"maxResults": v}))
		var perr *model.ProviderError
		if !errors.As(err, &perr) {
			t.Errorf("listParams(maxResults=%#v) error = %v, want a *model.ProviderError", v, err)
			continue
		}
		if perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Errorf("listParams(maxResults=%#v) = %+v, want InvalidArgument/400", v, perr)
		}
		if want := `Invalid value at 'header.max_results' (TYPE_UINT32), ` + boundValue(v); perr.Message != want {
			t.Errorf("listParams(maxResults=%#v) message = %q, want %q", v, perr.Message, want)
		}
	}
}

// ─── requestId idempotency (TD18) ─────────────────────────────────────────────

// boundValue renders the offending query value the way the frontend's binding
// error echoes it: a string value is re-quoted verbatim.
func boundValue(v any) string {
	if s, ok := v.(string); ok {
		return strconv.Quote(s)
	}
	return fmt.Sprint(v)
}

// testRequestID is a well-formed UUID used by the requestId tests.
const testRequestID = "3b241101-e2bb-4255-8caf-4136c566a962"

// TestInstanceInsertRequestIDIsIdempotent covers the documented contract on
// instances.insert: a retry carrying the same requestId is ignored once the
// first attempt finished and replays the original operation, so no duplicate
// instance is created.
func TestInstanceInsertRequestIDIsIdempotent(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	params := zonal("us-central1-a", map[string]any{"body": instanceBody("vm-idem"), "requestId": testRequestID})

	first, err := p.InstancesInsert(ctx, newNR(params))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := str(first.Data["clientOperationId"]); got != testRequestID {
		t.Errorf("clientOperationId = %q, want %q", got, testRequestID)
	}

	// A retry with the same token replays the original operation.
	second, err := p.InstancesInsert(ctx, newNR(params))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if second.Data["name"] != first.Data["name"] {
		t.Errorf("retry replayed operation %v, want %v", second.Data["name"], first.Data["name"])
	}
	if second.Data["selfLink"] != first.Data["selfLink"] {
		t.Errorf("retry selfLink = %v, want %v", second.Data["selfLink"], first.Data["selfLink"])
	}
	if second.Data["clientOperationId"] != testRequestID {
		t.Errorf("retry clientOperationId = %v, want %s", second.Data["clientOperationId"], testRequestID)
	}

	// The retry must not have created a second instance.
	list, err := p.InstancesList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 instance after retry, got %d", len(items))
	}

	// operations.get resolves the replayed operation, token echoed.
	opName := str(first.Data["name"])
	got, err := p.OperationsGet(ctx, newNR(zonal("us-central1-a", map[string]any{"operation": opName})))
	if err != nil {
		t.Fatalf("operations.get: %v", err)
	}
	if got.Data["clientOperationId"] != testRequestID {
		t.Errorf("persisted clientOperationId = %v, want %s", got.Data["clientOperationId"], testRequestID)
	}

	// A fresh token on the same name is a distinct request: the name is taken.
	if _, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{
		"body": instanceBody("vm-idem"), "requestId": "0f8fad5b-d9cb-469f-a165-70867728950e"}))); !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists for a new token on an existing name, got %v", err)
	}
}

// TestRequestIDReuseAcrossResourcesReplaysOriginal pins the documented
// "if original operation with the same request ID was received ... ignore the
// second request": the token, not the body, decides — a different body with
// the same token replays the original operation and creates nothing.
func TestRequestIDReuseAcrossResourcesReplaysOriginal(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	first, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{
		"body": instanceBody("vm-first"), "requestId": testRequestID})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	second, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{
		"body": instanceBody("vm-second"), "requestId": testRequestID})))
	if err != nil {
		t.Fatalf("reuse: %v", err)
	}
	if second.Data["name"] != first.Data["name"] {
		t.Errorf("reused token replayed operation %v, want %v", second.Data["name"], first.Data["name"])
	}
	if _, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-second"}))); !isCode(err, "NotFound") {
		t.Fatalf("vm-second must not have been created, got %v", err)
	}
}

// TestDiskInsertRequestIDIsIdempotent covers the same contract on disks.insert.
func TestDiskInsertRequestIDIsIdempotent(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	body := map[string]any{"name": "disk-idem", "sizeGb": 20, "type": "pd-ssd"}
	params := zonal("us-central1-a", map[string]any{"body": body, "requestId": testRequestID})

	first, err := p.DisksInsert(ctx, newNR(params))
	if err != nil {
		t.Fatalf("insert disk: %v", err)
	}
	second, err := p.DisksInsert(ctx, newNR(params))
	if err != nil {
		t.Fatalf("retry disk: %v", err)
	}
	if second.Data["name"] != first.Data["name"] {
		t.Errorf("retry replayed operation %v, want %v", second.Data["name"], first.Data["name"])
	}
	if second.Data["clientOperationId"] != testRequestID {
		t.Errorf("clientOperationId = %v, want %s", second.Data["clientOperationId"], testRequestID)
	}
	list, err := p.DisksList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("list disks: %v", err)
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 disk after retry, got %d", len(items))
	}
}

// TestDeleteRequestIDIsIdempotent covers the delete side: retrying a completed
// delete with the same token replays the original operation rather than
// 404-ing on the already-deleted instance.
func TestDeleteRequestIDIsIdempotent(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "us-central1-a", "vm-del")
	params := zonal("us-central1-a", map[string]any{"instance": "vm-del", "requestId": testRequestID})

	first, err := p.InstancesDelete(ctx, newNR(params))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	second, err := p.InstancesDelete(ctx, newNR(params))
	if err != nil {
		t.Fatalf("retry delete: %v", err)
	}
	if second.Data["name"] != first.Data["name"] {
		t.Errorf("retry replayed operation %v, want %v", second.Data["name"], first.Data["name"])
	}
	if second.Data["operationType"] != "delete" {
		t.Errorf("operationType = %v, want delete", second.Data["operationType"])
	}
	// Without a token the resource is gone, so a plain retry is 404.
	if _, err := p.InstancesDelete(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-del"}))); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound for an untokened retry, got %v", err)
	}
}

// TestRequestIDValidation asserts the documented UUID contract: a malformed
// token or the unsupported zero UUID fails loud with 400, and an absent token
// leaves clientOperationId unset.
func TestRequestIDValidation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, bad := range []string{
		"not-a-uuid",
		"3b241101e2bb42558caf4136c566a962",
		"00000000-0000-0000-0000-000000000000",
	} {
		_, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{
			"body": instanceBody("vm-bad"), "requestId": bad})))
		if !isCode(err, "InvalidArgument") {
			t.Errorf("requestId %q: expected InvalidArgument, got %v", bad, err)
		}
	}

	op := mustCreateInstance(t, p, "us-central1-a", "vm-plain")
	if _, ok := op["clientOperationId"]; ok {
		t.Errorf("clientOperationId must be absent without a requestId: %v", op)
	}
}

// TestRequestIDSameTokenConcurrentInsertsReplayOneOperation asserts that a
// burst of same-token inserts (including ones that overlap an in-flight
// original) all replay the single original operation, so no duplicate resource
// is created and no retry is answered with a conflict. Run under -race it also
// exercises the idempotency mapping and its per-token lock.
func TestRequestIDSameTokenConcurrentInsertsReplayOneOperation(t *testing.T) {
	const workers = 8
	ctx := context.Background()
	p := newProvider()
	names := make([]string, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := p.InstancesInsert(ctx, newNR(zonal("us-central1-a", map[string]any{
				"body": instanceBody("vm-race"), "requestId": testRequestID})))
			errs[i] = err
			if err == nil {
				names[i] = str(resp.Data["name"])
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("same-token insert %d failed: %v (a retry must replay, not conflict)", i, err)
		}
		if names[i] != names[0] {
			t.Fatalf("worker %d replayed operation %q, want %q", i, names[i], names[0])
		}
	}

	list, err := p.InstancesList(ctx, newNR(zonal("us-central1-a", nil)))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected exactly 1 instance from concurrent same-token inserts, got %d", len(items))
	}
}

// TestRequestIDIdempotentForGlobalScopeAndTransitions covers the global-scope
// (network) and instance-transition paths: a retried start replays the original
// operation and leaves the instance RUNNING.
func TestRequestIDIdempotentForGlobalScopeAndTransitions(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	// Global scope: a network insert retry replays.
	nparams := map[string]any{"body": map[string]any{"name": "net-idem"}, "requestId": testRequestID}
	firstNet, err := p.NetworksInsert(ctx, newNR(nparams))
	if err != nil {
		t.Fatalf("network insert: %v", err)
	}
	secondNet, err := p.NetworksInsert(ctx, newNR(nparams))
	if err != nil {
		t.Fatalf("network retry: %v", err)
	}
	if secondNet.Data["name"] != firstNet.Data["name"] {
		t.Errorf("network retry replayed %v, want %v", secondNet.Data["name"], firstNet.Data["name"])
	}
	if secondNet.Data["clientOperationId"] != testRequestID {
		t.Errorf("network clientOperationId = %v, want %s", secondNet.Data["clientOperationId"], testRequestID)
	}
	nets, err := p.NetworksList(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list networks: %v", err)
	}
	if items, _ := nets.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 network after retry, got %d", len(items))
	}

	// A state transition: a retried start replays its operation.
	mustCreateInstance(t, p, "us-central1-a", "vm-start")
	sparams := zonal("us-central1-a", map[string]any{
		"instance": "vm-start", "requestId": "0f8fad5b-d9cb-469f-a165-70867728950e"})
	firstStart, err := p.InstancesStart(ctx, newNR(sparams))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	secondStart, err := p.InstancesStart(ctx, newNR(sparams))
	if err != nil {
		t.Fatalf("start retry: %v", err)
	}
	if secondStart.Data["name"] != firstStart.Data["name"] {
		t.Errorf("start retry replayed %v, want %v", secondStart.Data["name"], firstStart.Data["name"])
	}
	if secondStart.Data["operationType"] != "start" {
		t.Errorf("operationType = %v, want start", secondStart.Data["operationType"])
	}
	got, err := p.InstancesGet(ctx, newNR(zonal("us-central1-a", map[string]any{"instance": "vm-start"})))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Data["status"] != "RUNNING" {
		t.Errorf("status = %v, want RUNNING", got.Data["status"])
	}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func isCode(err error, code string) bool {
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		return false
	}
	return perr.Code == code
}

// TestListHandlersRejectBadMaxResults pins the uint32 query binding on
// Compute's list surfaces: a negative, fractional or unparseable maxResults is
// INVALID_ARGUMENT (the documented range is 0 to 500 inclusive and the
// parameter is unsigned), never a silent fall-back to the 500 default. Every
// list handler that pages through listParams is exercised so the shared reject
// is covered on each cursor walk.
func TestListHandlersRejectBadMaxResults(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	handlers := []struct {
		name string
		call func(*model.NormalizedRequest) (*model.ProviderResponse, error)
	}{
		{"InstancesList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.InstancesList(ctx, nr) }},
		{"InstancesAggregatedList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return p.InstancesAggregatedList(ctx, nr)
		}},
		{"DisksList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.DisksList(ctx, nr) }},
		{"NetworksList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.NetworksList(ctx, nr) }},
		{"FirewallsList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.FirewallsList(ctx, nr) }},
		{"SubnetworksList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.SubnetworksList(ctx, nr) }},
		{"MachineTypesList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.MachineTypesList(ctx, nr) }},
		{"ZonesList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.ZonesList(ctx, nr) }},
		{"RegionsList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.RegionsList(ctx, nr) }},
		{"OperationsList", func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) { return p.OperationsList(ctx, nr) }},
	}
	for _, h := range handlers {
		for _, v := range []any{"-1", "abc", "1.5", "4294967296"} {
			_, err := h.call(newNR(map[string]any{"maxResults": v}))
			if !isCode(err, "InvalidArgument") {
				t.Errorf("%s(maxResults=%v) = %v, want InvalidArgument", h.name, v, err)
			}
		}
	}
}

// ─── Project common instance metadata (TF1-5) ────────────────────────────────

// projectMetadata calls compute.projects.get and returns the
// commonInstanceMetadata envelope it reports.
func projectMetadata(t *testing.T, p *Provider, project string) map[string]any {
	t.Helper()
	resp, err := p.ProjectsGet(context.Background(), newNR(map[string]any{"project": project}))
	if err != nil {
		t.Fatalf("projects get %s: %v", project, err)
	}
	md, ok := resp.Data["commonInstanceMetadata"].(map[string]any)
	if !ok {
		t.Fatalf("commonInstanceMetadata = %v, want an object", resp.Data["commonInstanceMetadata"])
	}
	return md
}

// setProjectMetadata calls compute.projects.setCommonInstanceMetadata.
func setProjectMetadata(t *testing.T, p *Provider, project string, body map[string]any) (map[string]any, error) {
	t.Helper()
	resp, err := p.ProjectsSetCommonInstanceMetadata(context.Background(),
		newNR(map[string]any{"project": project, "body": body}))
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// metadataItems renders the metadata envelope's items as key=value pairs so a
// test can compare the stored set without depending on map iteration order.
func metadataItems(md map[string]any) map[string]string {
	out := map[string]string{}
	list, _ := md["items"].([]any)
	for _, raw := range list {
		item, _ := raw.(map[string]any)
		key, _ := item["key"].(string)
		value, _ := item["value"].(string)
		out[key] = value
	}
	return out
}

// TestProjectMetadataSetGetRoundTrip pins compute.projects.setCommonInstanceMetadata
// against compute.projects.get: the metadata the write stores is what the
// project resource reports, and the fingerprint advances on every write (even a
// write that repeats the previous items), which is what makes the provider's
// read-modify-write converge instead of permadiffing (TF1-5).
func TestProjectMetadataSetGetRoundTrip(t *testing.T) {
	p := newProvider()

	before := projectMetadata(t, p, "proj")
	if before["kind"] != kindMetadata {
		t.Errorf("commonInstanceMetadata.kind = %v, want %s", before["kind"], kindMetadata)
	}
	if items := metadataItems(before); len(items) != 0 {
		t.Errorf("a never-written project reports items %v, want none", items)
	}
	fp0 := str(before["fingerprint"])
	if fp0 == "" {
		t.Fatal("a never-written project must still report a fingerprint")
	}

	// The provider's create path: GET the project, then POST items + the
	// fingerprint it just read.
	op, err := setProjectMetadata(t, p, "proj", map[string]any{
		"items": []any{
			map[string]any{"key": "foo", "value": "bar"},
			map[string]any{"key": "fizz", "value": "buzz"},
		},
		"fingerprint": fp0,
	})
	if err != nil {
		t.Fatalf("setCommonInstanceMetadata: %v", err)
	}
	// The response is Compute Engine's own DONE Operation envelope, not a
	// google.longrunning.Operation: the provider polls it with
	// ComputeOperationWaitTime.
	if op["kind"] != kindOperation || op["status"] != "DONE" {
		t.Errorf("operation = %v, want a DONE %s", op, kindOperation)
	}
	if op["operationType"] != "setCommonInstanceMetadata" {
		t.Errorf("operationType = %v, want setCommonInstanceMetadata", op["operationType"])
	}
	if got, want := op["targetLink"], computeBasePath+"projects/proj"; got != want {
		t.Errorf("targetLink = %v, want %s", got, want)
	}
	if got, want := op["progress"], int64(100); got != want {
		t.Errorf("progress = %v, want %v", got, want)
	}
	if got, want := op["selfLink"], computeBasePath+"projects/proj/global/operations/"+str(op["name"]); got != want {
		t.Errorf("selfLink = %v, want %s", got, want)
	}
	if op["name"] == "" {
		t.Error("operation name is empty")
	}

	after := projectMetadata(t, p, "proj")
	fp1 := str(after["fingerprint"])
	if fp1 == "" || fp1 == fp0 {
		t.Errorf("fingerprint after write = %q, want a fresh value (was %q)", fp1, fp0)
	}
	if got, want := metadataItems(after), map[string]string{"foo": "bar", "fizz": "buzz"}; !reflect.DeepEqual(got, want) {
		t.Errorf("items after write = %v, want %v", got, want)
	}

	// A write that repeats the same items still advances the fingerprint
	// (Discovery: it "changes after every request to modify or update metadata").
	if _, err := setProjectMetadata(t, p, "proj", map[string]any{
		"items":       after["items"],
		"fingerprint": fp1,
	}); err != nil {
		t.Fatalf("repeated write: %v", err)
	}
	again := projectMetadata(t, p, "proj")
	fp2 := str(again["fingerprint"])
	if fp2 == fp1 {
		t.Errorf("fingerprint did not advance on a repeated write: %q", fp2)
	}
	if got, want := metadataItems(again), map[string]string{"foo": "bar", "fizz": "buzz"}; !reflect.DeepEqual(got, want) {
		t.Errorf("items after repeated write = %v, want %v", got, want)
	}

	// A stale fingerprint is the documented 412 conditionNotMet; nothing is
	// stored.
	_, err = setProjectMetadata(t, p, "proj", map[string]any{
		"items":       []any{map[string]any{"key": "nope", "value": "1"}},
		"fingerprint": fp1,
	})
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.Code != "PreconditionFailed" || perr.HTTPStatus != 412 {
		t.Fatalf("stale fingerprint = %v, want PreconditionFailed/412", err)
	}
	if got := metadataItems(projectMetadata(t, p, "proj")); got["nope"] != "" || len(got) != 2 {
		t.Errorf("a rejected write changed the metadata: %v", got)
	}

	// The fingerprint is optional in Discovery (the schema marks no required
	// annotation for it): a body without one is accepted, which is also what a
	// bare client call and the conformance probe send.
	if _, err := setProjectMetadata(t, p, "proj", map[string]any{
		"items": []any{map[string]any{"key": "solo", "value": "1"}},
	}); err != nil {
		t.Fatalf("write without fingerprint: %v", err)
	}
	if got := metadataItems(projectMetadata(t, p, "proj")); !reflect.DeepEqual(got, map[string]string{"solo": "1"}) {
		t.Errorf("items after fingerprintless write = %v, want the replacement set", got)
	}

	// The provider's destroy path sends the fingerprint and no items, which
	// clears the set.
	current := str(projectMetadata(t, p, "proj")["fingerprint"])
	if _, err := setProjectMetadata(t, p, "proj", map[string]any{"fingerprint": current}); err != nil {
		t.Fatalf("clear: %v", err)
	}
	cleared := projectMetadata(t, p, "proj")
	if items := metadataItems(cleared); len(items) != 0 {
		t.Errorf("items after clear = %v, want none", items)
	}
	if str(cleared["fingerprint"]) == current {
		t.Error("clearing did not advance the fingerprint")
	}

	// Metadata is per project: a write to one project must not leak into
	// another (projects.get answers any id).
	if items := metadataItems(projectMetadata(t, p, "other")); len(items) != 0 {
		t.Errorf("project other reports items %v, want none", items)
	}
}

// TestProjectMetadataValidation pins the 400 paths Discovery documents for the
// write: a fingerprint that is not a base64 byte string, a metadata key outside
// the documented grammar, a missing value, and over-size values.
func TestProjectMetadataValidation(t *testing.T) {
	p := newProvider()
	ctx := context.Background()

	cases := []struct {
		name string
		body map[string]any
	}{
		{"malformed fingerprint", map[string]any{"fingerprint": "not base64!"}},
		{"key outside the grammar", map[string]any{
			"items": []any{map[string]any{"key": "has space", "value": "v"}},
		}},
		{"over-long key", map[string]any{
			"items": []any{map[string]any{"key": strings.Repeat("k", 129), "value": "v"}},
		}},
		{"missing value", map[string]any{
			"items": []any{map[string]any{"key": "foo"}},
		}},
		{"missing key", map[string]any{
			"items": []any{map[string]any{"value": "v"}},
		}},
		{"over-size value", map[string]any{
			"items": []any{map[string]any{"key": "big", "value": strings.Repeat("v", maxMetadataValueBytes+1)}},
		}},
		{"items not a list", map[string]any{"items": "nope"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.ProjectsSetCommonInstanceMetadata(ctx, newNR(map[string]any{"project": "proj", "body": tc.body}))
			if !isCode(err, "InvalidArgument") {
				t.Fatalf("err = %v, want InvalidArgument", err)
			}
			if items := metadataItems(projectMetadata(t, p, "proj")); len(items) != 0 {
				t.Errorf("a rejected write stored %v", items)
			}
		})
	}

	// The documented total-size ceiling (less than 512 KiB across all keys and
	// values) is enforced even when each value is individually legal.
	items := make([]any, 0, 3)
	for _, key := range []string{"a", "b", "c"} {
		items = append(items, map[string]any{"key": key, "value": strings.Repeat("v", maxMetadataValueBytes)})
	}
	if _, err := p.ProjectsSetCommonInstanceMetadata(ctx, newNR(map[string]any{
		"project": "proj", "body": map[string]any{"items": items},
	})); !isCode(err, "InvalidArgument") {
		t.Fatalf("over-size total = %v, want InvalidArgument", err)
	}
}
