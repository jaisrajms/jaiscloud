package tasks

import (
	"context"
	"strings"
	"testing"

	core "jaiscloud/internal/gcp/service/tasks"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newProvider(t *testing.T) *Provider {
	t.Helper()
	c := core.NewService(tasksstore.NewMemoryStore(), store.NewMemoryResourceStore())
	return NewProvider(c, "p")
}

func req(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{Service: ServiceName, Params: params}
}

func queueBody() map[string]any {
	return map[string]any{"name": "projects/p/locations/l/queues/q1"}
}

func taskBody() map[string]any {
	return map[string]any{"httpRequest": map[string]any{"url": "http://example.test/hook", "httpMethod": "POST"}}
}

func TestProviderQueueLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)

	if _, err := p.CreateQueue(ctx, req(map[string]any{"project": "p", "location": "l", "body": queueBody()})); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := p.GetQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err != nil {
		t.Fatalf("GetQueue: %v", err)
	}
	resp, err := p.ListQueues(ctx, req(map[string]any{"project": "p", "location": "l"}))
	if err != nil {
		t.Fatalf("ListQueues: %v", err)
	}
	if got := resp.Data["queues"].([]any); len(got) != 1 {
		t.Fatalf("queues = %v", resp.Data["queues"])
	}
	if _, err := p.PauseQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}
	if _, err := p.ResumeQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err != nil {
		t.Fatalf("ResumeQueue: %v", err)
	}
	if _, err := p.PurgeQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err != nil {
		t.Fatalf("PurgeQueue: %v", err)
	}
	if _, err := p.UpdateQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "body": map[string]any{"rateLimits": map[string]any{"maxDispatchesPerSecond": float64(9)}}, "updateMask": "rateLimits"})); err != nil {
		t.Fatalf("UpdateQueue: %v", err)
	}
	if _, err := p.DeleteQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	if _, err := p.GetQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err == nil {
		t.Fatalf("GetQueue after delete: expected NotFound")
	}
}

func TestProviderTaskLifecycleAndBatch(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	if _, err := p.CreateQueue(ctx, req(map[string]any{"project": "p", "location": "l", "body": queueBody()})); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	created, err := p.CreateTask(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "body": taskBody()}))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	name, _ := created.Data["name"].(string)
	if name == "" {
		t.Fatalf("task name missing: %v", created.Data)
	}

	resp, err := p.ListTasks(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"}))
	if err != nil || len(resp.Data["tasks"].([]any)) != 1 {
		t.Fatalf("ListTasks = %v, %v", resp, err)
	}

	// Batch create: cloudtasks/v2 declares an LRO; the emulator creates
	// synchronously and returns a done operation whose response carries the
	// created tasks.
	batch, err := p.BatchCreateTasks(ctx, req(map[string]any{
		"project": "p", "location": "l", "queue": "q1",
		"body": map[string]any{"requests": []any{
			map[string]any{"parent": "projects/p/locations/l/queues/q1", "task": map[string]any{"httpRequest": map[string]any{"url": "http://example.test/b", "httpMethod": "POST"}}},
		}},
	}))
	if err != nil {
		t.Fatalf("BatchCreateTasks: %v", err)
	}
	assertDoneOperation(t, batch.Data, "type.googleapis.com/google.cloud.tasks.v2.BatchCreateTasksMetadata", batchCreateResponseType)
	respEnv, _ := batch.Data["response"].(map[string]any)
	tasksOut, _ := respEnv["tasks"].([]any)
	if len(tasksOut) != 1 {
		t.Fatalf("batch create response tasks = %v", respEnv)
	}

	// Batch delete the just-created task by full name.
	batchName, _ := tasksOut[0].(map[string]any)["name"].(string)
	del, err := p.BatchDeleteTasks(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "body": map[string]any{"names": []any{batchName}}}))
	if err != nil {
		t.Fatalf("BatchDeleteTasks: %v", err)
	}
	assertDoneOperation(t, del.Data, "type.googleapis.com/google.cloud.tasks.v2.BatchDeleteTasksMetadata", emptyTypeURL)

	if _, err := p.GetTask(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "task": taskNameOf(t, name)})); err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if ran, err := p.RunTask(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "task": taskNameOf(t, name)})); err != nil {
		t.Fatalf("RunTask: %v", err)
	} else if ran.Data["name"] != name {
		t.Fatalf("RunTask name = %v, want %v", ran.Data["name"], name)
	}
	if _, err := p.DeleteTask(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "task": taskNameOf(t, name)})); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}
}

func TestProviderQueueIAM(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	if _, err := p.CreateQueue(ctx, req(map[string]any{"project": "p", "location": "l", "body": queueBody()})); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := p.GetQueueIamPolicy(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err != nil {
		t.Fatalf("GetQueueIamPolicy: %v", err)
	}
	body := map[string]any{"policy": map[string]any{"bindings": []any{map[string]any{"role": "roles/owner", "members": []any{"user:a@b"}}}}}
	if _, err := p.SetQueueIamPolicy(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "body": body})); err != nil {
		t.Fatalf("SetQueueIamPolicy: %v", err)
	}
	resp, err := p.TestQueueIamPermissions(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "body": map[string]any{"permissions": []any{"cloudtasks.queues.get"}}}))
	if err != nil || len(resp.Data["permissions"].([]string)) != 1 {
		t.Fatalf("TestQueueIamPermissions = %v, %v", resp, err)
	}
}

// TestProviderCreateTaskWrappedBody covers the v2 CreateTaskRequest body shape:
// the official REST client wraps the task in a "task" field.
func TestProviderCreateTaskWrappedBody(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	if _, err := p.CreateQueue(ctx, req(map[string]any{"project": "p", "location": "l", "body": queueBody()})); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	body := map[string]any{"task": map[string]any{
		"name":        "projects/p/locations/l/queues/q1/tasks/wrapped",
		"httpRequest": map[string]any{"url": "http://example.test/hook", "httpMethod": "POST"},
	}}
	created, err := p.CreateTask(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "body": body}))
	if err != nil {
		t.Fatalf("CreateTask (wrapped): %v", err)
	}
	if name, _ := created.Data["name"].(string); name != "projects/p/locations/l/queues/q1/tasks/wrapped" {
		t.Fatalf("name = %v", created.Data["name"])
	}
}

// assertDoneOperation asserts the wire shape of a synchronous
// google.longrunning.Operation: done=true, a location-scoped name, the expected
// metadata @type with a SUCCEEDED state, and the expected response @type.
func assertDoneOperation(t *testing.T, op map[string]any, wantMetaType, wantRespType string) {
	t.Helper()
	if op["done"] != true {
		t.Fatalf("operation done = %v, want true: %v", op["done"], op)
	}
	if name, _ := op["name"].(string); !strings.HasPrefix(name, "projects/p/locations/l/operations/") {
		t.Fatalf("operation name = %q, want projects/p/locations/l/operations/…", name)
	}
	meta, _ := op["metadata"].(map[string]any)
	if meta["@type"] != wantMetaType || meta["state"] != "SUCCEEDED" {
		t.Fatalf("operation metadata = %v, want @type %q state SUCCEEDED", meta, wantMetaType)
	}
	respEnv, _ := op["response"].(map[string]any)
	if respEnv["@type"] != wantRespType {
		t.Fatalf("operation response @type = %v, want %q", respEnv["@type"], wantRespType)
	}
}

func TestProviderListNegativePageSize(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	if _, err := p.CreateQueue(ctx, req(map[string]any{"project": "p", "location": "l", "body": queueBody()})); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	// AIP-158: a negative pageSize is INVALID_ARGUMENT on both list methods,
	// rather than silently falling back to the documented maximum.
	for name, call := range map[string]func() error{
		"queues.list": func() error {
			_, err := p.ListQueues(ctx, req(map[string]any{"project": "p", "location": "l", "pageSize": float64(-1)}))
			return err
		},
		"tasks.list": func() error {
			_, err := p.ListTasks(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "pageSize": float64(-1)}))
			return err
		},
	} {
		err := call()
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Errorf("%s(pageSize=-1) err = %v, want InvalidArgument/400", name, err)
		}
	}
	// queues.list documents a 9800 ceiling (not tasks.list's 1000), so a 9800
	// request is legal and returns the whole small collection.
	resp, err := p.ListQueues(ctx, req(map[string]any{"project": "p", "location": "l", "pageSize": float64(9800)}))
	if err != nil {
		t.Fatalf("ListQueues(pageSize=9800): %v", err)
	}
	if got := resp.Data["queues"].([]any); len(got) != 1 {
		t.Fatalf("queues = %v, want the seeded queue", got)
	}
	// A non-integer pageSize fails the int32 query binding on both list methods
	// instead of being folded into zero (an absent parameter).
	for _, v := range []any{"abc", "-0.5", "1.5"} {
		for name, err := range map[string]error{
			"queues.list": func() error {
				_, err := p.ListQueues(ctx, req(map[string]any{"project": "p", "location": "l", "pageSize": v}))
				return err
			}(),
			"tasks.list": func() error {
				_, err := p.ListTasks(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "pageSize": v}))
				return err
			}(),
		} {
			perr, ok := err.(*model.ProviderError)
			if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
				t.Errorf("%s(pageSize=%#v) err = %v, want InvalidArgument/400", name, v, err)
			}
		}
	}
}

// TestProviderBufferTask covers both tasks:buffer path forms at the REST layer:
// the collection form (.../tasks:buffer) generates an id, the item form
// (.../tasks/{taskId}:buffer) uses the path id, and a body-only request is
// rejected because the emulator models no queue-level httpTarget.
func TestProviderBufferTask(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	if _, err := p.CreateQueue(ctx, req(map[string]any{"project": "p", "location": "l", "body": queueBody()})); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	fullTask := map[string]any{"httpRequest": map[string]any{"url": "http://example.test/buffered", "httpMethod": "POST"}}

	// Item form: the path task id wins.
	created, err := p.BufferTask(ctx, req(map[string]any{
		"project": "p", "location": "l", "queue": "q1", "task": "buffered-item",
		"body": map[string]any{"task": fullTask},
	}))
	if err != nil {
		t.Fatalf("BufferTask(item): %v", err)
	}
	buffered, _ := created.Data["task"].(map[string]any)
	want := "projects/p/locations/l/queues/q1/tasks/buffered-item"
	if buffered["name"] != want {
		t.Fatalf("BufferTask(item) name = %v, want %v", buffered["name"], want)
	}

	// Collection form: an id is generated.
	created, err = p.BufferTask(ctx, req(map[string]any{
		"project": "p", "location": "l", "queue": "q1",
		"body": map[string]any{"task": fullTask},
	}))
	if err != nil {
		t.Fatalf("BufferTask(collection): %v", err)
	}
	buffered, _ = created.Data["task"].(map[string]any)
	if name, _ := buffered["name"].(string); !strings.HasPrefix(name, "projects/p/locations/l/queues/q1/tasks/") {
		t.Fatalf("BufferTask(collection) name = %q", name)
	}

	// Body-only request: rejected like real GCP rejects a queue with no HTTP
	// target (the emulator models no queue-level httpTarget).
	_, err = p.BufferTask(ctx, req(map[string]any{
		"project": "p", "location": "l", "queue": "q1",
		"body": map[string]any{"body": map[string]any{"contentType": "text/plain", "data": "aGk="}},
	}))
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("BufferTask(body only) err = %v, want InvalidArgument/400", err)
	}
}

// TestProviderCmekConfig covers locations.getCmekConfig/updateCmekConfig over
// REST: the default is an empty config, an update sets kmsKey, an empty kmsKey
// clears it and a malformed key name is INVALID_ARGUMENT.
func TestProviderCmekConfig(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	loc := func() map[string]any { return map[string]any{"project": "p", "location": "l"} }

	got, err := p.GetCmekConfig(ctx, req(loc()))
	if err != nil {
		t.Fatalf("GetCmekConfig: %v", err)
	}
	if got.Data["name"] != "projects/p/locations/l/cmekConfig" {
		t.Fatalf("GetCmekConfig name = %v", got.Data["name"])
	}
	if _, ok := got.Data["kmsKey"]; ok {
		t.Fatalf("GetCmekConfig(default) emitted kmsKey: %v", got.Data)
	}

	key := "projects/p/locations/global/keyRings/ring/cryptoKeys/key"
	params := loc()
	params["body"] = map[string]any{"kmsKey": key}
	upd, err := p.UpdateCmekConfig(ctx, req(params))
	if err != nil {
		t.Fatalf("UpdateCmekConfig: %v", err)
	}
	if upd.Data["kmsKey"] != key {
		t.Fatalf("UpdateCmekConfig = %v", upd.Data)
	}
	if got, err := p.GetCmekConfig(ctx, req(loc())); err != nil || got.Data["kmsKey"] != key {
		t.Fatalf("GetCmekConfig after update = %v, %v", got.Data, err)
	}

	// Bad key name.
	bad := loc()
	bad["body"] = map[string]any{"kmsKey": "not-a-key"}
	if _, err := p.UpdateCmekConfig(ctx, req(bad)); err == nil {
		t.Fatalf("UpdateCmekConfig(bad key) succeeded")
	}

	// Empty kmsKey clears it.
	clear := loc()
	clear["body"] = map[string]any{"kmsKey": ""}
	if _, err := p.UpdateCmekConfig(ctx, req(clear)); err != nil {
		t.Fatalf("UpdateCmekConfig(clear): %v", err)
	}
	if got, err := p.GetCmekConfig(ctx, req(loc())); err != nil {
		t.Fatalf("GetCmekConfig: %v", err)
	} else if _, ok := got.Data["kmsKey"]; ok {
		t.Fatalf("UpdateCmekConfig(clear) left kmsKey: %v", got.Data)
	}
}

func taskNameOf(t *testing.T, full string) string {
	t.Helper()
	_, _, _, id, ok := core.ParseTaskName(full)
	if !ok {
		t.Fatalf("bad task name %q", full)
	}
	return id
}
