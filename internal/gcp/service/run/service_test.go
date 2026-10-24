package run

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/events"
	"jaiscloud/internal/gcp/lro"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newTestService() *Service {
	return NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore())
}

// A wired event bus receives one cloud-neutral status event per service
// mutation so the console's live stream can invalidate the GCP/run query tree.
func TestMutationsPublishStatusEvents(t *testing.T) {
	ctx := context.Background()
	bus := events.NewEventBus()
	s := NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithEventBus(bus))

	var got []events.StatusEvent
	bus.Subscribe(events.EventStatus, func(e events.Event) {
		if se, ok := e.Payload.(events.StatusEvent); ok {
			got = append(got, se)
		}
	})

	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", createRequest(), "template", false, false); err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	if _, err := s.DeleteService(ctx, "proj", "us-central1", "svc", false, ""); err != nil {
		t.Fatalf("DeleteService: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("status events = %d, want 3: %+v", len(got), got)
	}
	if got[0].State != "READY" || got[1].State != "READY" || got[2].State != "DELETED" {
		t.Fatalf("states = %q,%q,%q", got[0].State, got[1].State, got[2].State)
	}
	first := got[0]
	if first.Cloud != model.CloudGCP || first.ID != "svc" || first.Resource != "gcp-run-service" {
		t.Fatalf("first event = %+v", first)
	}
	if len(first.Keys) != 2 || first.Keys[0] != "gcp" || first.Keys[1] != "run" {
		t.Fatalf("first keys = %v", first.Keys)
	}
}

func createRequest() map[string]any {
	return map[string]any{
		"template": map[string]any{
			"containers": []any{
				map[string]any{
					"image": "nginx:latest",
					"ports": []any{map[string]any{"containerPort": float64(80)}},
				},
			},
		},
	}
}

func TestCreateGetListDeleteService(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	op, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false)
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if !op.Done {
		t.Fatalf("operation not done synchronously")
	}
	if got := op.ID; !strings.HasPrefix(got, "operation-run-") {
		t.Errorf("operation id = %q, want operation-run- prefix", got)
	}
	if op.Target != "projects/proj/locations/us-central1/services/svc" {
		t.Errorf("operation target = %q", op.Target)
	}

	svc, err := s.GetService(ctx, "proj", "us-central1", "svc")
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	js := ServiceJSON(svc)
	if js["name"] != "projects/proj/locations/us-central1/services/svc" {
		t.Errorf("name = %v", js["name"])
	}
	uri, _ := js["uri"].(string)
	if !strings.HasPrefix(uri, "https://svc-") || !strings.HasSuffix(uri, ".us-central1.run.app") {
		t.Errorf("uri = %q", uri)
	}
	if lrr, _ := js["latestReadyRevision"].(string); !strings.Contains(lrr, "/revisions/") {
		t.Errorf("latestReadyRevision = %q", lrr)
	}
	cond, _ := js["terminalCondition"].(map[string]any)
	if cond["type"] != "Ready" || cond["state"] != "CONDITION_SUCCEEDED" {
		t.Errorf("terminalCondition = %v", cond)
	}
	statuses, _ := js["trafficStatuses"].([]any)
	if len(statuses) != 1 || statuses[0].(map[string]any)["percent"] != 100 {
		t.Errorf("trafficStatuses = %v", statuses)
	}
	urls, _ := js["urls"].([]any)
	if len(urls) != 1 || urls[0] != uri {
		t.Errorf("urls = %v", urls)
	}

	// Revisions.
	revs, err := s.ListRevisions(ctx, "proj", "us-central1", "svc")
	if err != nil {
		t.Fatalf("ListRevisions: %v", err)
	}
	if len(revs) != 1 || revs[0].ID != "svc-00001" {
		t.Fatalf("revisions = %+v", revs)
	}
	rev, err := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00001")
	if err != nil {
		t.Fatalf("GetRevision: %v", err)
	}
	rj := RevisionJSON(rev)
	if rj["service"] != "projects/proj/locations/us-central1/services/svc" {
		t.Errorf("revision service = %v", rj["service"])
	}
	containers, _ := rj["containers"].([]any)
	if len(containers) != 1 || containers[0].(map[string]any)["image"] != "nginx:latest" {
		t.Errorf("revision containers = %v", containers)
	}

	// Delete.
	delOp, err := s.DeleteService(ctx, "proj", "us-central1", "svc", false, "")
	if err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
	if !delOp.Done || delOp.Service == nil || delOp.Service.DeleteTime.IsZero() {
		t.Errorf("delete op = %+v", delOp)
	}
	if _, err := s.GetService(ctx, "proj", "us-central1", "svc"); !isNotFound(err) {
		t.Errorf("GetService after delete err = %v, want NotFound", err)
	}
	if _, err := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00001"); !isNotFound(err) {
		t.Errorf("GetRevision after delete err = %v, want NotFound", err)
	}
}

func TestCreateDuplicate(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "p", "l", "svc", nil, false); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err := s.CreateService(ctx, "p", "l", "svc", nil, false)
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.HTTPStatus != 409 {
		t.Fatalf("duplicate create err = %v, want 409", err)
	}
}

func TestCreateInvalidID(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	_, err := s.CreateService(ctx, "p", "l", "Bad_ID", nil, false)
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.HTTPStatus != 400 {
		t.Fatalf("invalid id err = %v, want 400", err)
	}
}

func TestUpdateMintsRevision(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "p", "l", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	newBody := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "httpd:latest"}}}}
	op, err := s.UpdateService(ctx, "p", "l", "svc", newBody, "template", false, false)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if op.Verb != "update" {
		t.Errorf("op verb = %q", op.Verb)
	}
	if got := op.Service.LatestCreatedRevision; !strings.HasSuffix(got, "/revisions/svc-00002") {
		t.Errorf("latestCreatedRevision = %q, want svc-00002", got)
	}
	if op.Service.LatestReadyRevision != op.Service.LatestCreatedRevision {
		t.Errorf("mock mode should make the new revision ready")
	}
	revs, _ := s.ListRevisions(ctx, "p", "l", "svc")
	if len(revs) != 2 {
		t.Fatalf("revisions = %d, want 2", len(revs))
	}
}

func TestIAMPolicy(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "p", "l", "svc", nil, false); err != nil {
		t.Fatalf("create: %v", err)
	}
	pol, err := s.ServiceSetIamPolicy(ctx, "p", "l", "svc", map[string]any{
		"policy": map[string]any{"bindings": []any{
			map[string]any{"role": "roles/run.invoker", "members": []any{"allUsers"}},
		}},
	})
	if err != nil {
		t.Fatalf("set iam: %v", err)
	}
	if len(pol.Bindings) != 1 {
		t.Fatalf("bindings = %v", pol.Bindings)
	}
	got, err := s.ServiceGetIamPolicy(ctx, "p", "l", "svc")
	if err != nil {
		t.Fatalf("get iam: %v", err)
	}
	if len(got.Bindings) != 1 {
		t.Errorf("get bindings = %v", got.Bindings)
	}
	perms, err := s.ServiceTestIamPermissions(ctx, "p", "l", "svc", []string{"run.services.get"})
	if err != nil {
		t.Fatalf("test iam: %v", err)
	}
	if len(perms) != 1 || perms[0] != "run.services.get" {
		t.Errorf("perms = %v", perms)
	}
	if _, err := s.ServiceGetIamPolicy(ctx, "p", "l", "missing"); !isNotFound(err) {
		t.Errorf("iam on missing service err = %v, want NotFound", err)
	}
}

func TestOperations(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	op, err := s.CreateService(ctx, "p", "l", "svc", nil, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.GetOperation(ctx, "p", "l", op.ID)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	js := OperationJSON(got)
	if js["name"] != "projects/p/locations/l/operations/"+op.ID {
		t.Errorf("op name = %v", js["name"])
	}
	if js["done"] != true {
		t.Errorf("op done = %v", js["done"])
	}
	resp, _ := js["response"].(map[string]any)
	if resp["@type"] != serviceTypeURL {
		t.Errorf("response @type = %v", resp["@type"])
	}
	meta, _ := js["metadata"].(map[string]any)
	if meta["@type"] != serviceTypeURL {
		t.Errorf("metadata @type = %v", meta["@type"])
	}
	if got, err := s.WaitOperation(ctx, "p", "l", op.ID); err != nil || got.ID != op.ID {
		t.Errorf("WaitOperation = %v, %v", got.ID, err)
	}
	ops, err := s.ListOperations(ctx, "p", "l")
	if err != nil || len(ops) != 1 {
		t.Fatalf("ListOperations = %v, %v", ops, err)
	}
	if err := s.CancelOperation(ctx, "p", "l", op.ID); err != nil {
		t.Errorf("CancelOperation: %v", err)
	}
	if err := s.DeleteOperation(ctx, "p", "l", op.ID); err != nil {
		t.Errorf("DeleteOperation: %v", err)
	}
	if _, err := s.GetOperation(ctx, "p", "l", op.ID); !isNotFound(err) {
		t.Errorf("GetOperation after delete err = %v, want NotFound", err)
	}
}

// An in-flight operation carries its typed metadata snapshot from creation,
// matching real Cloud Run: the mutation's google.longrunning.operation_info
// metadata_type is Service for the service mutations and Revision for the
// revision delete. The response stays absent until the operation settles. The
// workflows and metastore renderers already behaved this way; the run renderer
// withheld metadata until done, which these assertions lock down (TD23).
func TestOperationJSONPendingMetadata(t *testing.T) {
	ctx := context.Background()
	async := NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithLROMode(lro.Mode{Enabled: true, Delay: time.Hour}))

	op, err := async.CreateService(ctx, "p", "us-central1", "svc", createRequest(), false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if op.Done {
		t.Fatalf("async create unexpectedly done")
	}
	js := OperationJSON(op)
	if js["done"] != false {
		t.Errorf("in-flight done = %v, want false", js["done"])
	}
	meta := assertTypedAny(t, js, "metadata", serviceTypeURL)
	if meta["name"] != ServiceName("p", "us-central1", "svc") {
		t.Errorf("in-flight metadata name = %v", meta["name"])
	}
	if _, ok := js["response"]; ok {
		t.Errorf("in-flight operation must not carry a response: %v", js)
	}

	// A retired revision's in-flight delete carries Revision metadata, the
	// second metadata_type the run mutations declare.
	if _, err := async.UpdateService(ctx, "p", "us-central1", "svc",
		map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "httpd:latest"}}}},
		"template", false, false); err != nil {
		t.Fatalf("update: %v", err)
	}
	del, err := async.DeleteRevision(ctx, "p", "us-central1", "svc", "svc-00001", false, "")
	if err != nil {
		t.Fatalf("delete revision: %v", err)
	}
	if del.Done {
		t.Fatalf("async revision delete unexpectedly done")
	}
	djs := OperationJSON(del)
	assertTypedAny(t, djs, "metadata", revisionTypeURL)
	if _, ok := djs["response"]; ok {
		t.Errorf("in-flight revision delete must not carry a response: %v", djs)
	}

	// A settled operation keeps its metadata and gains the response. An async
	// operation with no delay settles on the first read, so the poll path that
	// promotes the stored in-flight op is covered too.
	settling := NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithLROMode(lro.Mode{Enabled: true}))
	created, err := settling.CreateService(ctx, "p", "l", "svc", createRequest(), false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	done, err := settling.GetOperation(ctx, "p", "l", created.ID)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if !done.Done {
		t.Fatalf("zero-delay async operation did not settle on read")
	}
	sjs := OperationJSON(done)
	assertTypedAny(t, sjs, "metadata", serviceTypeURL)
	assertTypedAny(t, sjs, "response", serviceTypeURL)
}

// assertTypedAny asserts op[key] is a typed Any with the given @type and returns
// its body.
func assertTypedAny(t *testing.T, op map[string]any, key, typeURL string) map[string]any {
	t.Helper()
	body, ok := op[key].(map[string]any)
	if !ok {
		t.Fatalf("operation %q is not a typed Any: %v", key, op)
	}
	if body["@type"] != typeURL {
		t.Fatalf("%s @type = %v, want %v", key, body["@type"], typeURL)
	}
	return body
}

func TestListAllServicesAcrossLocations(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "beta", createRequest(), false); err != nil {
		t.Fatalf("CreateService beta: %v", err)
	}
	if _, err := s.CreateService(ctx, "proj", "europe-west1", "alpha", createRequest(), false); err != nil {
		t.Fatalf("CreateService alpha: %v", err)
	}
	if _, err := s.CreateService(ctx, "other", "us-central1", "gamma", createRequest(), false); err != nil {
		t.Fatalf("CreateService gamma: %v", err)
	}

	svcs, err := s.ListAllServices(ctx, "proj")
	if err != nil {
		t.Fatalf("ListAllServices: %v", err)
	}
	if len(svcs) != 2 {
		t.Fatalf("got %d services, want 2", len(svcs))
	}
	if svcs[0].ID != "alpha" || svcs[0].Location != "europe-west1" || svcs[1].ID != "beta" || svcs[1].Location != "us-central1" {
		t.Fatalf("unexpected order/scope: %+v", svcs)
	}
}

func TestDeleteRevisionRetired(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	newBody := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "httpd:latest"}}}}
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", newBody, "template", false, false); err != nil {
		t.Fatalf("update: %v", err)
	}
	op, err := s.DeleteRevision(ctx, "proj", "us-central1", "svc", "svc-00001", false, "")
	if err != nil {
		t.Fatalf("DeleteRevision: %v", err)
	}
	if op.Revision == nil || op.Revision.ID != "svc-00001" {
		t.Fatalf("op revision = %+v", op.Revision)
	}
	if op.Revision.DeleteTime.IsZero() {
		t.Errorf("delete operation revision has no deleteTime")
	}
	resp, _ := OperationJSON(op)["response"].(map[string]any)
	if resp["@type"] != revisionTypeURL {
		t.Errorf("response @type = %v, want %v", resp["@type"], revisionTypeURL)
	}
	if resp["deleteTime"] == "" || resp["deleteTime"] == nil {
		t.Errorf("response deleteTime missing: %v", resp["deleteTime"])
	}
	if _, err := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00001"); !isNotFound(err) {
		t.Errorf("retired revision still present: %v", err)
	}
	if _, err := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00002"); err != nil {
		t.Errorf("serving revision removed: %v", err)
	}
}

func TestDeleteRevisionServingRejected(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err := s.DeleteRevision(ctx, "proj", "us-central1", "svc", "svc-00001", false, "")
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.Code != "FailedPrecondition" || perr.HTTPStatus != 400 {
		t.Fatalf("serving delete err = %v, want FailedPrecondition/400", err)
	}
}

func TestDeleteRevisionValidateOnly(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	newBody := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "httpd:latest"}}}}
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", newBody, "template", false, false); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := s.DeleteRevision(ctx, "proj", "us-central1", "svc", "svc-00001", true, ""); err != nil {
		t.Fatalf("validate-only DeleteRevision: %v", err)
	}
	if _, err := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00001"); err != nil {
		t.Errorf("validate-only removed the revision: %v", err)
	}
	ops, _ := s.ListOperations(ctx, "proj", "us-central1")
	for _, op := range ops {
		if op.Revision != nil {
			t.Errorf("validate-only recorded an operation: %+v", op)
		}
	}
	if _, err := s.DeleteRevision(ctx, "proj", "us-central1", "svc", "svc-99999", false, ""); !isNotFound(err) {
		t.Errorf("delete missing revision err = %v, want NotFound", err)
	}
}

// isAborted reports whether err is the etag-precondition ABORTED/409 error.
func isAborted(err error) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.HTTPStatus == 409 && perr.Status == "ABORTED"
}

func TestCreateServiceValidateOnly(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	op, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), true)
	if err != nil {
		t.Fatalf("validate-only create: %v", err)
	}
	if op.Service == nil || op.Service.ID != "svc" {
		t.Fatalf("validate-only create preview = %+v", op.Service)
	}
	if _, err := s.GetService(ctx, "proj", "us-central1", "svc"); !isNotFound(err) {
		t.Errorf("validate-only create persisted the service: %v", err)
	}
	if ops, _ := s.ListOperations(ctx, "proj", "us-central1"); len(ops) != 0 {
		t.Errorf("validate-only create recorded %d operations", len(ops))
	}
	// A duplicate is still rejected: validate-only runs the real validation.
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("real create: %v", err)
	}
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), true); err == nil {
		t.Error("validate-only duplicate create accepted")
	}
}

func TestUpdateServiceValidateOnly(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	before, _ := s.GetService(ctx, "proj", "us-central1", "svc")
	opsBefore, _ := s.ListOperations(ctx, "proj", "us-central1")
	newBody := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "httpd:latest"}}}}
	op, err := s.UpdateService(ctx, "proj", "us-central1", "svc", newBody, "template", true, false)
	if err != nil {
		t.Fatalf("validate-only update: %v", err)
	}
	// The preview reflects the would-be new revision, but nothing persisted.
	if op.Service == nil || op.Service.LatestReadyRevision == before.LatestReadyRevision {
		t.Fatalf("validate-only update did not preview a new revision: %+v", op.Service)
	}
	after, _ := s.GetService(ctx, "proj", "us-central1", "svc")
	if after.Generation != before.Generation || after.LatestReadyRevision != before.LatestReadyRevision {
		t.Errorf("validate-only update mutated the service: gen %d->%d rev %q->%q",
			before.Generation, after.Generation, before.LatestReadyRevision, after.LatestReadyRevision)
	}
	if img := templateImage(after.Data); img != "nginx:latest" {
		t.Errorf("validate-only update mutated the persisted template (image = %q)", img)
	}
	revs, _ := s.ListRevisions(ctx, "proj", "us-central1", "svc")
	if len(revs) != 1 {
		t.Errorf("validate-only update minted a revision: %d", len(revs))
	}
	if ops, _ := s.ListOperations(ctx, "proj", "us-central1"); len(ops) != len(opsBefore) {
		t.Errorf("validate-only update recorded %d operations", len(ops)-len(opsBefore))
	}
}

func TestUpdateServiceAllowMissing(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	op, err := s.UpdateService(ctx, "proj", "us-central1", "svc", createRequest(), "template", false, true)
	if err != nil {
		t.Fatalf("allowMissing update: %v", err)
	}
	if op.Service == nil || op.Service.ID != "svc" {
		t.Fatalf("allowMissing update op = %+v", op.Service)
	}
	svc, err := s.GetService(ctx, "proj", "us-central1", "svc")
	if err != nil {
		t.Fatalf("allowMissing update did not create the service: %v", err)
	}
	if svc.Generation != 1 || svc.LatestReadyRevision != RevisionName("proj", "us-central1", "svc", "svc-00001") {
		t.Errorf("allowMissing created service = %+v", svc)
	}
	// Without allowMissing the same request is NotFound.
	if _, err := s.UpdateService(ctx, "p", "l", "missing", createRequest(), "template", false, false); !isNotFound(err) {
		t.Errorf("missing update without allowMissing err = %v, want NotFound", err)
	}
}

func TestUpdateServiceEtagMismatch(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	body := map[string]any{"etag": "stale-token", "description": "x"}
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", body, "description", false, false); !isAborted(err) {
		t.Fatalf("stale-etag update err = %v, want ABORTED", err)
	}
	cur, _ := s.GetService(ctx, "proj", "us-central1", "svc")
	body["etag"] = cur.Etag
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", body, "description", false, false); err != nil {
		t.Fatalf("matching-etag update: %v", err)
	}
}

func TestDeleteServiceValidateOnly(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	opsBefore, _ := s.ListOperations(ctx, "proj", "us-central1")
	op, err := s.DeleteService(ctx, "proj", "us-central1", "svc", true, "")
	if err != nil {
		t.Fatalf("validate-only delete: %v", err)
	}
	if op.Service == nil || op.Service.ID != "svc" {
		t.Fatalf("validate-only delete preview = %+v", op.Service)
	}
	if _, err := s.GetService(ctx, "proj", "us-central1", "svc"); err != nil {
		t.Errorf("validate-only delete removed the service: %v", err)
	}
	if ops, _ := s.ListOperations(ctx, "proj", "us-central1"); len(ops) != len(opsBefore) {
		t.Errorf("validate-only delete recorded %d operations", len(ops)-len(opsBefore))
	}
}

func TestDeleteServiceEtagMismatch(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.DeleteService(ctx, "proj", "us-central1", "svc", false, "stale-token"); !isAborted(err) {
		t.Fatalf("stale-etag delete err = %v, want ABORTED", err)
	}
	if _, err := s.GetService(ctx, "proj", "us-central1", "svc"); err != nil {
		t.Errorf("stale-etag delete removed the service: %v", err)
	}
	cur, _ := s.GetService(ctx, "proj", "us-central1", "svc")
	if _, err := s.DeleteService(ctx, "proj", "us-central1", "svc", false, cur.Etag); err != nil {
		t.Fatalf("matching-etag delete: %v", err)
	}
	if _, err := s.GetService(ctx, "proj", "us-central1", "svc"); !isNotFound(err) {
		t.Errorf("matching-etag delete left the service: %v", err)
	}
}

func TestDeleteRevisionEtagMismatch(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	newBody := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "httpd:latest"}}}}
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", newBody, "template", false, false); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := s.DeleteRevision(ctx, "proj", "us-central1", "svc", "svc-00001", false, "stale-token"); !isAborted(err) {
		t.Fatalf("stale-etag revision delete err = %v, want ABORTED", err)
	}
	if _, err := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00001"); err != nil {
		t.Errorf("stale-etag delete removed the revision: %v", err)
	}
	rev, _ := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00001")
	if _, err := s.DeleteRevision(ctx, "proj", "us-central1", "svc", "svc-00001", false, rev.Etag); err != nil {
		t.Fatalf("matching-etag revision delete: %v", err)
	}
	if _, err := s.GetRevision(ctx, "proj", "us-central1", "svc", "svc-00001"); !isNotFound(err) {
		t.Errorf("matching-etag revision delete left the revision: %v", err)
	}
}

// A validate-only operation is never persisted, so it must be terminal even in
// async LRO mode (otherwise a polling client could never complete it).
func TestValidateOnlyOperationAlwaysDone(t *testing.T) {
	ctx := context.Background()
	s := NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithLROMode(lro.Mode{Enabled: true, Delay: time.Hour}))
	op, err := s.CreateService(ctx, "p", "l", "dry", createRequest(), true)
	if err != nil {
		t.Fatalf("validate-only create: %v", err)
	}
	if !op.Done {
		t.Errorf("validate-only operation is in flight in async mode")
	}
	// A real create in the same mode is in flight, proving async is active.
	real, err := s.CreateService(ctx, "p", "l", "live", createRequest(), false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if real.Done {
		t.Errorf("async real operation unexpectedly done")
	}
}

// TestUpdateDoesNotAliasStoredData locks in the map-copy fix: merging an
// update must not mutate the stored service's Data in place (which would both
// corrupt a concurrent read and rewrite the snapshot an earlier operation
// holds).
func TestUpdateDoesNotAliasStoredData(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	createOp, err := s.CreateService(ctx, "p", "l", "svc", createRequest(), false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.UpdateService(ctx, "p", "l", "svc", map[string]any{"description": "changed"}, "description", false, false); err != nil {
		t.Fatalf("update: %v", err)
	}
	svc, _ := s.GetService(ctx, "p", "l", "svc")
	if svc.Data["description"] != "changed" {
		t.Errorf("real update did not persist the description: %v", svc.Data["description"])
	}
	snap, err := s.GetOperation(ctx, "p", "l", createOp.ID)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if snap.Service == nil {
		t.Fatalf("create operation has no snapshot")
	}
	if _, ok := snap.Service.Data["description"]; ok {
		t.Errorf("create operation snapshot aliased the live service data: %v", snap.Service.Data)
	}
}

// TestTrafficStatusesTagQualifiedURI pins the verified real-GCP `trafficStatuses`
// shape (a live `services.get`): a tagged target's status `uri` is the
// tag-qualified URL (https://{tag}---{service-host}), and an untagged target has
// **no** `uri` key at all. The Discovery's TrafficTargetStatus.tag is "the string
// used in the URI to exclusively reference this target".
func TestTrafficStatusesTagQualifiedURI(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	body := map[string]any{"traffic": []any{
		map[string]any{"type": "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", "percent": float64(90), "tag": "stable"},
		map[string]any{"type": "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", "percent": float64(10)},
	}}
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", body, "traffic", false, false); err != nil {
		t.Fatalf("update traffic: %v", err)
	}
	svc, err := s.GetService(ctx, "proj", "us-central1", "svc")
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	js := ServiceJSON(svc)
	uri, _ := js["uri"].(string)
	statuses, _ := js["trafficStatuses"].([]any)
	if len(statuses) != 2 {
		t.Fatalf("trafficStatuses = %v", statuses)
	}
	tagged, _ := statuses[0].(map[string]any)
	untagged, _ := statuses[1].(map[string]any)
	wantTagged := "https://stable---" + strings.TrimPrefix(uri, "https://")
	if tagged["uri"] != wantTagged {
		t.Errorf("tagged status uri = %v, want %v", tagged["uri"], wantTagged)
	}
	if v, ok := untagged["uri"]; ok {
		t.Errorf("untagged status must not carry a uri, got %v", v)
	}
}

// TestDefaultTrafficStatusHasNoURI pins that the synthesized default status (no
// traffic supplied) omits `uri` too: real Cloud Run emits a target's `uri` only
// when the target carries a tag, so the untagged default has none.
func TestDefaultTrafficStatusHasNoURI(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	svc, err := s.GetService(ctx, "proj", "us-central1", "svc")
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	statuses, _ := ServiceJSON(svc)["trafficStatuses"].([]any)
	if len(statuses) != 1 {
		t.Fatalf("trafficStatuses = %v", statuses)
	}
	status := statuses[0].(map[string]any)
	if v, ok := status["uri"]; ok {
		t.Errorf("default status must not carry a uri, got %v", v)
	}
	if status["percent"] != 100 || status["type"] != "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST" {
		t.Errorf("default status = %v", status)
	}
}

// TestTaggedURIShapes locks the tag-prefix transformation across the uri shapes
// the emulator can synthesize: the https mock form, the http+port k8s-execution
// form, and the empty-tag / scheme-less edges.
func TestTaggedURIShapes(t *testing.T) {
	cases := []struct {
		uri, tag, want string
	}{
		{"https://svc-abc.us-central1.run.app", "stable", "https://stable---svc-abc.us-central1.run.app"},
		{"http://svc-abc.us-central1.run.app:8080", "canary", "http://canary---svc-abc.us-central1.run.app:8080"},
		{"https://svc-abc.us-central1.run.app", "", "https://svc-abc.us-central1.run.app"},
		{"svc-abc.us-central1.run.app", "stable", "svc-abc.us-central1.run.app"},
	}
	for _, c := range cases {
		if got := taggedURI(c.uri, c.tag); got != c.want {
			t.Errorf("taggedURI(%q, %q) = %q, want %q", c.uri, c.tag, got, c.want)
		}
	}
}

// tagTrafficRequest is a one-target traffic split carrying the given tag (an
// empty tag means an untagged target).
func tagTrafficRequest(tag string) map[string]any {
	target := map[string]any{"type": "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", "percent": float64(100)}
	if tag != "" {
		target["tag"] = tag
	}
	return map[string]any{"traffic": []any{target}}
}

// requireTrafficTagRejected asserts err is the InvalidArgument/400 the real
// Cloud Run API returns for a traffic target whose tag is not a valid DNS label.
func requireTrafficTagRejected(t *testing.T, err error) {
	t.Helper()
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.HTTPStatus != 400 || perr.Code != "InvalidArgument" {
		t.Fatalf("err = %v, want InvalidArgument/400", err)
	}
}

// TestTrafficTagValidation pins the traffic-tag rules verified against real
// Cloud Run (each violation is 400 INVALID_ARGUMENT, never an echoed tag): a tag
// is an RFC 1035 label (lowercase letters/digits/hyphens, beginning with a
// letter and not ending with a hyphen) of 3 to 47 characters, the tag plus the
// service id may not exceed 46 characters, and tags must be unique. The accepted
// set includes the grammar and combined-length boundaries.
func TestTrafficTagValidation(t *testing.T) {
	ctx := context.Background()
	// Service id "s0".."s5" (2 chars) leaves 44 for the tag under the 46 combined cap.
	for i, tag := range []string{"stable", "canary-1", "abc", "a1b", "a-b", strings.Repeat("a", 43)} {
		s := newTestService()
		id := fmt.Sprintf("s%d", i)
		if _, err := s.CreateService(ctx, "p", "l", id, tagTrafficRequest(tag), false); err != nil {
			t.Fatalf("create with valid tag %q: %v", tag, err)
		}
	}
	invalid := []struct {
		name, tag string
	}{
		{"uppercase", "Stable"},
		{"leading digit", "1stable"},
		{"leading hyphen", "-stable"},
		{"trailing hyphen", "stable-"},
		{"underscore", "has_underscore"},
		{"dot", "has.dot"},
		{"space", "bad tag"},
		{"too short", "aa"},
		{"too long", strings.Repeat("a", 48)},
		{"combined over 46", strings.Repeat("a", 44)},
	}
	for _, c := range invalid {
		s := newTestService()
		_, err := s.CreateService(ctx, "p", "l", "svc", tagTrafficRequest(c.tag), false)
		requireTrafficTagRejected(t, err)
		if _, err := s.GetService(ctx, "p", "l", "svc"); !isNotFound(err) {
			t.Fatalf("%s: service persisted despite rejection: %v", c.name, err)
		}
	}
	// Duplicate tags are rejected (real GCP: "spec.traffic: may not contain a
	// duplicate tag").
	s := newTestService()
	dup := map[string]any{"traffic": []any{
		map[string]any{"type": "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", "percent": float64(50), "tag": "stable"},
		map[string]any{"type": "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", "percent": float64(50), "tag": "stable"},
	}}
	_, err := s.CreateService(ctx, "p", "l", "svc", dup, false)
	requireTrafficTagRejected(t, err)
}

// TestUpdateTrafficTagValidation pins the update path: an invalid tag applied
// through the traffic mask is rejected (400) without persisting, while the same
// invalid tag excluded by the mask is ignored exactly as real Cloud Run ignores
// unmasked fields.
func TestUpdateTrafficTagValidation(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Masked-in invalid tag: rejected and not persisted.
	_, err := s.UpdateService(ctx, "proj", "us-central1", "svc", tagTrafficRequest("Bad_Tag"), "traffic", false, false)
	requireTrafficTagRejected(t, err)
	svc, err := s.GetService(ctx, "proj", "us-central1", "svc")
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if _, ok := svc.Data["traffic"]; ok {
		t.Errorf("rejected update persisted traffic: %v", svc.Data["traffic"])
	}
	// Masked-out invalid tag: the field is not applied, so the request succeeds.
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", tagTrafficRequest("Bad_Tag"), "labels", false, false); err != nil {
		t.Fatalf("update with unmasked invalid tag: %v", err)
	}
	// A valid tag through the same mask is accepted.
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", tagTrafficRequest("stable"), "traffic", false, false); err != nil {
		t.Fatalf("update with valid tag: %v", err)
	}
}

// TestServiceURLsAreNotTagExtended pins the verified real-GCP `urls` behavior: a
// traffic tag does NOT add a tag-qualified entry to the service-level `urls`
// ("All URLs serving traffic for this Service") — the tag-qualified URL appears
// only on the tagged target's trafficStatuses[].uri (TD22). A live `services.get`
// with a tagged target returns the same two default host-form URLs as an
// untagged one; the emulator synthesizes a single form, so `urls` stays [uri].
func TestServiceURLsAreNotTagExtended(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", tagTrafficRequest("stable"), "traffic", false, false); err != nil {
		t.Fatalf("update traffic: %v", err)
	}
	svc, err := s.GetService(ctx, "proj", "us-central1", "svc")
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	js := ServiceJSON(svc)
	uri, _ := js["uri"].(string)
	urls, _ := js["urls"].([]any)
	if len(urls) != 1 || urls[0] != uri {
		t.Errorf("urls = %v, want [%s] (a tag must not extend the service-level urls)", urls, uri)
	}
	// The tag-qualified URL lives on the target status instead.
	statuses, _ := js["trafficStatuses"].([]any)
	if len(statuses) != 1 {
		t.Fatalf("trafficStatuses = %v", statuses)
	}
	if got := statuses[0].(map[string]any)["uri"]; got != taggedURI(uri, "stable") {
		t.Errorf("tagged status uri = %v, want %v", got, taggedURI(uri, "stable"))
	}
}

// templateImage extracts containers[0].image from a stored service's writable
// Data, or "" when absent.
func templateImage(data map[string]any) string {
	tpl, _ := data["template"].(map[string]any)
	containers, _ := tpl["containers"].([]any)
	if len(containers) == 0 {
		return ""
	}
	c, _ := containers[0].(map[string]any)
	img, _ := c["image"].(string)
	return img
}

func isNotFound(err error) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.HTTPStatus == 404
}
