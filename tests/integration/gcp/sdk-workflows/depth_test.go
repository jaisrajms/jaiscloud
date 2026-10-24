// depth_test.go closes TD9: the Cloud Workflows / Workflow Executions p2–p5
// behaviour classes (concurrency, error semantics, listing+paging, long-running
// lifecycle), driven through the official REST apiary clients. p1 (baseline
// lifecycle) lives in sdk_workflows_test.go; this file adds the classes the
// service previously had no behavioural assertion for.
package sdk_workflows_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/workflowexecutions/v1"
	"google.golang.org/api/workflows/v1"

	"github.com/stretchr/testify/require"
)

const depthParent = "projects/proj/locations/us-central1"

// failedSource drives an execution to FAILED: the engine rejects the unknown
// stdlib call at run time (a valid definition, a failing step).
const failedSource = "main:\n  steps:\n    - x:\n        call: http.delete\n        args:\n          url: \"http://x\"\n"

func clients(t *testing.T) (*workflows.Service, *workflowexecutions.Service) {
	t.Helper()
	ctx := context.Background()
	wfs, err := workflows.NewService(ctx, opts()...)
	require.NoError(t, err)
	execs, err := workflowexecutions.NewService(ctx, opts()...)
	require.NoError(t, err)
	return wfs, execs
}

// createWorkflow creates a workflow with the given id and source, returning its
// full resource name.
func createWorkflow(t *testing.T, wfs *workflows.Service, id, source string, labels map[string]string) string {
	t.Helper()
	name := depthParent + "/workflows/" + id
	_, err := wfs.Projects.Locations.Workflows.Create(depthParent, &workflows.Workflow{
		Name:           name,
		SourceContents: source,
		Labels:         labels,
	}).WorkflowId(id).Do()
	require.NoError(t, err)
	return name
}

// wantAPIStatus asserts err is a googleapi error with the given HTTP status.
func wantAPIStatus(t *testing.T, err error, code int) {
	t.Helper()
	require.Error(t, err)
	var gerr *googleapi.Error
	require.ErrorAs(t, err, &gerr)
	require.Equal(t, code, gerr.Code, "error body: %v", err)
}

// ─── p2: concurrency & contention ────────────────────────────────────────────

func TestWorkflowsDepthConcurrency(t *testing.T) {
	wfs, execs := clients(t)

	// Eight parallel executions of one workflow all land once, with distinct ids.
	name := createWorkflow(t, wfs, unique("wf"), source, nil)
	const n = 8
	var wg sync.WaitGroup
	ids := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e, err := execs.Projects.Locations.Workflows.Executions.Create(name, &workflowexecutions.Execution{
				Argument: fmt.Sprintf(`{"a": %d, "b": 0}`, i),
			}).Do()
			if err != nil {
				errs[i] = err
				return
			}
			ids[i] = e.Name
		}(i)
	}
	wg.Wait()
	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i], "execution %d", i)
		require.False(t, seen[ids[i]], "duplicate execution id %q", ids[i])
		seen[ids[i]] = true
	}
	list, err := execs.Projects.Locations.Workflows.Executions.List(name).PageSize(100).Do()
	require.NoError(t, err)
	require.Len(t, list.Executions, n, "every concurrent execution must be listed exactly once")

	// Eight goroutines race one workflowId: exactly one create wins, the rest 409.
	raceID := unique("race")
	successes := 0
	loserCodes := make([]int, n)
	var mu sync.Mutex
	var wg2 sync.WaitGroup
	for i := 0; i < n; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			_, err := wfs.Projects.Locations.Workflows.Create(depthParent, &workflows.Workflow{
				SourceContents: source,
			}).WorkflowId(raceID).Do()
			if err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
				return
			}
			var gerr *googleapi.Error
			if errors.As(err, &gerr) {
				loserCodes[i] = gerr.Code
			} else {
				loserCodes[i] = -1
			}
		}(i)
	}
	wg2.Wait()
	require.Equal(t, 1, successes, "exactly one create of a workflowId must win")
	for i, code := range loserCodes {
		if code != 0 {
			require.Equal(t, 409, code, "loser %d must be AlreadyExists", i)
		}
	}

	// Eight concurrent source updates each land as a revision; none is lost.
	revName := createWorkflow(t, wfs, unique("wf"), source, nil)
	var wg3 sync.WaitGroup
	upErrs := make([]error, n)
	for i := 0; i < n; i++ {
		wg3.Add(1)
		go func(i int) {
			defer wg3.Done()
			src := fmt.Sprintf("main:\n  steps:\n    - s%d:\n        return: %d\n", i, i)
			_, err := wfs.Projects.Locations.Workflows.Patch(revName, &workflows.Workflow{SourceContents: src}).
				UpdateMask("sourceContents").Do()
			upErrs[i] = err
		}(i)
	}
	wg3.Wait()
	for i := 0; i < n; i++ {
		require.NoError(t, upErrs[i], "concurrent update %d", i)
	}
	revs, err := wfs.Projects.Locations.Workflows.ListRevisions(revName).PageSize(100).Do()
	require.NoError(t, err)
	require.Len(t, revs.Workflows, n+1, "every concurrent source change must mint a revision")
	revIDs := map[string]bool{}
	for _, r := range revs.Workflows {
		require.False(t, revIDs[r.RevisionId], "duplicate revision id %q", r.RevisionId)
		revIDs[r.RevisionId] = true
	}
}

// ─── p3: errors, negatives & idempotency ─────────────────────────────────────

func TestWorkflowsDepthErrors(t *testing.T) {
	wfs, execs := clients(t)

	// A malformed definition is rejected at create (InvalidArgument), not stored
	// to fail later at execution time.
	_, err := wfs.Projects.Locations.Workflows.Create(depthParent, &workflows.Workflow{
		SourceContents: "not: a workflow definition\n",
	}).WorkflowId(unique("bad")).Do()
	wantAPIStatus(t, err, 400)

	// An invalid workflowId is rejected (must start with a letter, must not end
	// with a hyphen).
	_, err = wfs.Projects.Locations.Workflows.Create(depthParent, &workflows.Workflow{
		SourceContents: source,
	}).WorkflowId("1starts-with-a-digit").Do()
	wantAPIStatus(t, err, 400)
	_, err = wfs.Projects.Locations.Workflows.Create(depthParent, &workflows.Workflow{
		SourceContents: source,
	}).WorkflowId("ends-with-hyphen-").Do()
	wantAPIStatus(t, err, 400)

	// An oversized / missing definition is rejected at create.
	_, err = wfs.Projects.Locations.Workflows.Create(depthParent, &workflows.Workflow{}).WorkflowId(unique("wf")).Do()
	wantAPIStatus(t, err, 400)

	// A duplicate workflowId is AlreadyExists.
	dupID := unique("dup")
	createWorkflow(t, wfs, dupID, source, nil)
	_, err = wfs.Projects.Locations.Workflows.Create(depthParent, &workflows.Workflow{
		SourceContents: source,
	}).WorkflowId(dupID).Do()
	wantAPIStatus(t, err, 409)

	name := createWorkflow(t, wfs, unique("wf"), source, nil)

	// Get of a missing workflow / revision / execution is NotFound.
	_, err = wfs.Projects.Locations.Workflows.Get(name + "-missing").Do()
	wantAPIStatus(t, err, 404)
	_, err = wfs.Projects.Locations.Workflows.Get(name).RevisionId("999999-abc").Do()
	wantAPIStatus(t, err, 404)
	_, err = execs.Projects.Locations.Workflows.Executions.Get(name + "/executions/does-not-exist").Do()
	wantAPIStatus(t, err, 404)

	// Listing executions under an unknown workflow is NotFound.
	_, err = execs.Projects.Locations.Workflows.Executions.List(name + "-missing").Do()
	wantAPIStatus(t, err, 404)

	// An unsupported filter fails loud rather than returning an unfiltered list.
	_, err = execs.Projects.Locations.Workflows.Executions.List(name).Filter(`uid="x"`).Do()
	wantAPIStatus(t, err, 400)
	_, err = execs.Projects.Locations.Workflows.Executions.List(name).Filter(`state="SUCCEEDED" OR state="FAILED"`).Do()
	wantAPIStatus(t, err, 400)

	// A failing step produces a SUCCEEDED-after-failure error envelope that
	// persists on get and is parseable.
	failName := createWorkflow(t, wfs, unique("wf"), failedSource, nil)
	exec, err := execs.Projects.Locations.Workflows.Executions.Create(failName, &workflowexecutions.Execution{}).Do()
	require.NoError(t, err)
	require.Equal(t, "FAILED", exec.State)
	require.NotNil(t, exec.Error)
	require.NotEmpty(t, exec.Error.Payload)
	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(exec.Error.Payload), &payload))
	require.NotEmpty(t, payload["message"], "error payload must carry a message: %s", exec.Error.Payload)
	require.NotEmpty(t, exec.Error.Context)

	got, err := execs.Projects.Locations.Workflows.Executions.Get(exec.Name).Do()
	require.NoError(t, err)
	require.Equal(t, "FAILED", got.State)
	require.NotNil(t, got.Error)
	require.Equal(t, exec.Error.Payload, got.Error.Payload)
}

// ─── p4: listing & paging ────────────────────────────────────────────────────

func TestWorkflowsDepthListing(t *testing.T) {
	wfs, execs := clients(t)
	name := createWorkflow(t, wfs, unique("wf"), source, map[string]string{"env": "prod"})

	// A second workflow in the same location, so workflow filtering is exercised.
	otherID := unique("wf")
	createWorkflow(t, wfs, otherID, source, map[string]string{"env": "dev"})

	// Workflow list: filter by label + name (the emulator's state is shared
	// across tests, so scope the assertion to this run's workflow), order, and
	// page through one at a time.
	filtered, err := wfs.Projects.Locations.Workflows.List(depthParent).
		Filter(fmt.Sprintf("labels.env=\"prod\" AND name=%q", name)).Do()
	require.NoError(t, err)
	require.Len(t, filtered.Workflows, 1)
	require.Equal(t, name, filtered.Workflows[0].Name)

	// A label no workflow carries returns nothing.
	absent, err := wfs.Projects.Locations.Workflows.List(depthParent).
		Filter(fmt.Sprintf("labels.env=%q", "absent-"+unique("x"))).Do()
	require.NoError(t, err)
	require.Empty(t, absent.Workflows)

	desc, err := wfs.Projects.Locations.Workflows.List(depthParent).OrderBy("name desc").Do()
	require.NoError(t, err)
	require.NotEmpty(t, desc.Workflows)
	for i := 1; i < len(desc.Workflows); i++ {
		require.Less(t, desc.Workflows[i].Name, desc.Workflows[i-1].Name, "orderBy name desc")
	}

	const n = 5
	lastExec := ""
	for i := 0; i < n; i++ {
		e, err := execs.Projects.Locations.Workflows.Executions.Create(name, &workflowexecutions.Execution{
			Argument: fmt.Sprintf(`{"a": %d, "b": 1}`, i),
			Labels:   map[string]string{"env": "prod"},
		}).Do()
		require.NoError(t, err)
		lastExec = e.Name
	}

	// The documented default order is descending createTime, so the most recent
	// execution is first.
	def, err := execs.Projects.Locations.Workflows.Executions.List(name).PageSize(100).Do()
	require.NoError(t, err)
	require.Len(t, def.Executions, n)
	require.Equal(t, lastExec, def.Executions[0].Name, "default order must be newest-first")

	// Page through every execution once, two at a time.
	var walked []string
	token := ""
	for {
		page, err := execs.Projects.Locations.Workflows.Executions.List(name).
			PageSize(2).PageToken(token).OrderBy("executionId desc").Do()
		require.NoError(t, err)
		for _, e := range page.Executions {
			walked = append(walked, e.Name)
		}
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}
	require.Len(t, walked, n)
	seen := map[string]bool{}
	for _, w := range walked {
		require.False(t, seen[w], "execution %q repeated across pages", w)
		seen[w] = true
	}
	for i := 1; i < len(walked); i++ {
		require.Greater(t, walked[i-1], walked[i], "orderBy executionId desc across pages")
	}

	// Filters compose with the list: state and label.
	ok, err := execs.Projects.Locations.Workflows.Executions.List(name).Filter(`state="SUCCEEDED"`).Do()
	require.NoError(t, err)
	require.Len(t, ok.Executions, n)
	byLabel, err := execs.Projects.Locations.Workflows.Executions.List(name).Filter(`labels.env="prod"`).Do()
	require.NoError(t, err)
	require.Len(t, byLabel.Executions, n)
	// The quoted-key label spelling is accepted too.
	quoted, err := execs.Projects.Locations.Workflows.Executions.List(name).Filter(`labels."env":"prod"`).Do()
	require.NoError(t, err)
	require.Len(t, quoted.Executions, n)
	none, err := execs.Projects.Locations.Workflows.Executions.List(name).Filter(`labels.env="staging"`).Do()
	require.NoError(t, err)
	require.Empty(t, none.Executions)

	// list default view is BASIC (no argument/result), FULL carries them.
	basic, err := execs.Projects.Locations.Workflows.Executions.List(name).PageSize(1).Do()
	require.NoError(t, err)
	require.NotEmpty(t, basic.Executions)
	require.Empty(t, basic.Executions[0].Argument)
	require.Empty(t, basic.Executions[0].Result)
	full, err := execs.Projects.Locations.Workflows.Executions.List(name).PageSize(1).View("FULL").Do()
	require.NoError(t, err)
	require.NotEmpty(t, full.Executions)
	require.NotEmpty(t, full.Executions[0].Result)
}

// TestWorkflowsDepthRevisions covers the listRevisions lineage: a source change
// mints a revision, a workflow-wide change does not, and a named revision
// returns its immutable snapshot.
func TestWorkflowsDepthRevisions(t *testing.T) {
	wfs, _ := clients(t)
	original := "main:\n  steps:\n    - r:\n        return: 1\n"
	name := createWorkflow(t, wfs, unique("wf"), original, nil)

	first, err := wfs.Projects.Locations.Workflows.Get(name).Do()
	require.NoError(t, err)

	// A source change bumps the revision.
	updated := "main:\n  steps:\n    - r:\n        return: 2\n"
	_, err = wfs.Projects.Locations.Workflows.Patch(name, &workflows.Workflow{SourceContents: updated}).
		UpdateMask("sourceContents").Do()
	require.NoError(t, err)

	// A description-only change does not.
	_, err = wfs.Projects.Locations.Workflows.Patch(name, &workflows.Workflow{Description: "only desc"}).
		UpdateMask("description").Do()
	require.NoError(t, err)

	// Page through the revision history one at a time.
	var revIDs []string
	token := ""
	for {
		page, err := wfs.Projects.Locations.Workflows.ListRevisions(name).PageSize(1).PageToken(token).Do()
		require.NoError(t, err)
		for _, r := range page.Workflows {
			revIDs = append(revIDs, r.RevisionId)
		}
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}
	require.Len(t, revIDs, 2, "one create revision + one source revision")
	require.Equal(t, first.RevisionId, revIDs[1], "history is newest-first")

	// The named first revision returns its immutable snapshot.
	old, err := wfs.Projects.Locations.Workflows.Get(name).RevisionId(first.RevisionId).Do()
	require.NoError(t, err)
	require.Equal(t, original, old.SourceContents)
	current, err := wfs.Projects.Locations.Workflows.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, updated, current.SourceContents)
	require.Equal(t, "only desc", current.Description)
}

// ─── p5: long-running lifecycle & scale ──────────────────────────────────────

func TestWorkflowsDepthLifecycle(t *testing.T) {
	wfs, execs := clients(t)
	name := createWorkflow(t, wfs, unique("wf"), source, nil)

	// An execution in the emulator runs to completion, so cancel on a terminal
	// execution is a no-op that returns it unchanged.
	exec, err := execs.Projects.Locations.Workflows.Executions.Create(name, &workflowexecutions.Execution{
		Argument: `{"a": 2, "b": 3}`,
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "SUCCEEDED", exec.State)
	cancelled, err := execs.Projects.Locations.Workflows.Executions.Cancel(exec.Name, &workflowexecutions.CancelExecutionRequest{}).Do()
	require.NoError(t, err)
	require.Equal(t, exec.State, cancelled.State)
	require.Equal(t, exec.Result, cancelled.Result)

	// Cancel of a missing execution is NotFound.
	_, err = execs.Projects.Locations.Workflows.Executions.Cancel(name+"/executions/missing", &workflowexecutions.CancelExecutionRequest{}).Do()
	wantAPIStatus(t, err, 404)

	// Scale: a 12-execution listing walks every page.
	const n = 12
	for i := 0; i < n; i++ {
		_, err := execs.Projects.Locations.Workflows.Executions.Create(name, &workflowexecutions.Execution{
			Argument: fmt.Sprintf(`{"a": %d, "b": 0}`, i),
		}).Do()
		require.NoError(t, err)
	}
	total := 0
	token := ""
	for {
		page, err := execs.Projects.Locations.Workflows.Executions.List(name).PageSize(5).PageToken(token).Do()
		require.NoError(t, err)
		total += len(page.Executions)
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}
	require.Equal(t, n+1, total, "the 12 new executions plus the cancelled one")
}

// TestWorkflowsDepthAsyncLRO covers the opt-in async operations.get poll path
// through the official client. It is skipped unless JAISCLOUD_LRO_ASYNC=1 and
// the emulator was started with JAISCLOUD_LRO_MODE=async.
func TestWorkflowsDepthAsyncLRO(t *testing.T) {
	if os.Getenv("JAISCLOUD_LRO_ASYNC") != "1" {
		t.Skip("set JAISCLOUD_LRO_ASYNC=1 and start jaiscloud-gcp with JAISCLOUD_LRO_MODE=async")
	}
	ctx := context.Background()
	wfs, err := workflows.NewService(ctx, opts()...)
	require.NoError(t, err)

	id := unique("wf")
	name := depthParent + "/workflows/" + id
	op, err := wfs.Projects.Locations.Workflows.Create(depthParent, &workflows.Workflow{
		SourceContents: source,
	}).WorkflowId(id).Do()
	require.NoError(t, err)
	require.False(t, op.Done, "async create must return an in-flight operation")

	deadline := time.Now().Add(20 * time.Second)
	for {
		cur, err := wfs.Projects.Locations.Operations.Get(op.Name).Do()
		require.NoError(t, err)
		if cur.Done {
			var w workflows.Workflow
			require.NoError(t, json.Unmarshal(cur.Response, &w))
			require.Equal(t, name, w.Name)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation %s did not settle", op.Name)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
