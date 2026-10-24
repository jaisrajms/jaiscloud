// Project-tenant isolation suite (GTC3 / W1.2).
//
// GCP maps a *project* to a store account: every provider scopes its state by
// store.GlobalRegion + nr.AccountID, where nr.AccountID is the project ID
// resolved from the request path (see internal/gcp/adapter.EnrichRequest and
// internal/gcp/identity). Two different project IDs are therefore two
// independent tenants, and a resource created in one project must never be
// visible, readable or deletable from another.
//
// This suite mirrors the AWS reference shape (tests/integration/multiaccount,
// Test<Service>_AccountIsolation) over the emulator's REST surface:
//
//	create the same-named resource in two projects;
//	assert each project's list shows only its own copy (no leak);
//	assert the other project cannot read or delete it;
//	assert deleting one project's copy leaves the other's intact.
//
// Deliberately not covered (documented, not asserted vacuously):
//
//   - resourcemanager: a project *is* the tenant, so its own record is a global
//     resource rather than state scoped by another project's AccountID; there
//     is no "same-named resource in two projects" to assert.
//   - pubsub snapshots: not implemented on the emulator surface
//     (PUT /v1/projects/{p}/snapshots/{id} → UnknownService 404).
//   - KMS key rings/crypto keys, BigQuery jobs, Workflow executions: the
//     emulator exposes no delete for these (the delete leg is skipped per case
//     below; create/list/get scoping is still asserted).
//
// It is an invariant suite, not a per-service DoD suite — it drives raw HTTP
// through the same helpers as gcs_test.go. Run it against a live emulator:
//
//	./jaiscloud-gcp start --ephemeral &
//	JAISCLOUD_GCP_ENDPOINT=http://localhost:8080 \
//	    go test -race -count=1 -run 'Isolation' ./tests/integration/gcp/
package gcp_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/clock"

	"github.com/stretchr/testify/require"
)

// The three projects the isolation suite uses. A and B are independent
// tenants; C is never written to and proves nothing leaks into a pristine
// tenant.
const (
	isoProjA = "iso-proj-a"
	isoProjB = "iso-proj-b"
	isoProjC = "iso-proj-c"

	// defaultIsoLeaf is a resource name legal for most services (lowercase,
	// digits and hyphens). Cases whose resource name has a stricter charset
	// (e.g. BigQuery dataset IDs cannot contain '-') override it.
	defaultIsoLeaf = "iso-resource"
)

// isoCase describes one mutating service's project-scoped lifecycle. All paths
// are built from the project and the leaf resource name, so the runner can
// prove that the *same* name in two projects yields two independent resources.
type isoCase struct {
	name string
	// leaf overrides defaultIsoLeaf when the service restricts the name charset.
	leaf string
	// create creates the leaf-named resource under project. It must fail the
	// test if creation does not succeed.
	create func(t *testing.T, project, leaf string)
	// list returns the leaf names visible in project.
	list func(t *testing.T, project, leaf string) []string
	// get returns the HTTP status of reading leaf under project. A return of
	// http.StatusNotFound means "this project has no such resource".
	get func(t *testing.T, project, leaf string) int
	// del deletes leaf under project and returns the status. nil when the
	// service exposes no delete operation (documented in the case comment).
	del func(t *testing.T, project, leaf string) int
	// delOK is the success status returned by del.
	delOK int
	// custom, when set, replaces the generic runner (used by storage, whose
	// bucket namespace is global and whose objects live inside a bucket).
	custom func(t *testing.T)
}

// TestProjectIsolation runs one subtest per covered service. Each service is
// independent (reset + t.Run), so a failure names the service and the leaking
// project without aborting the rest.
func TestProjectIsolation(t *testing.T) {
	for _, c := range isoCases() {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if c.custom != nil {
				c.custom(t)
				return
			}
			runIsolation(t, c)
		})
	}
	for _, c := range isoIDCases() {
		c := c
		t.Run(c.name, func(t *testing.T) { runIDIsolation(t, c) })
	}
}

// runIsolation is the shared invariant body. On any failure the message names
// the service, the leaking project and the owning project.
func runIsolation(t *testing.T, c isoCase) {
	t.Helper()
	resetState(t)
	leaf := c.leaf
	if leaf == "" {
		leaf = defaultIsoLeaf
	}

	// 1. Seed the resource in project A only.
	c.create(t, isoProjA, leaf)
	isoEventually(t, func() int { return c.get(t, isoProjA, leaf) }, http.StatusOK,
		fmt.Sprintf("%s: GET of its own resource in %s", c.name, isoProjA))
	require.Contains(t, c.list(t, isoProjA, leaf), leaf,
		"%s: project %s does not list its own resource %q", c.name, isoProjA, leaf)

	// 2. The seeded negative: a project that never created the resource must
	// not see it. If scoping collapsed to a single account the list would
	// contain the leaf and the GET would return 200, failing here.
	require.NotContains(t, c.list(t, isoProjB, leaf), leaf,
		"%s: list LEAK — project %s sees a resource owned by %s", c.name, isoProjB, isoProjA)
	require.NotContains(t, c.list(t, isoProjC, leaf), leaf,
		"%s: list LEAK — project %s sees a resource owned by %s", c.name, isoProjC, isoProjA)
	require.Equal(t, http.StatusNotFound, c.get(t, isoProjB, leaf),
		"%s: GET LEAK — project %s can read a resource owned by %s", c.name, isoProjB, isoProjA)

	// 3. The identical name created in project B is an independent resource.
	c.create(t, isoProjB, leaf)
	isoEventually(t, func() int { return c.get(t, isoProjB, leaf) }, http.StatusOK,
		fmt.Sprintf("%s: GET of the same-named resource in %s", c.name, isoProjB))
	require.Contains(t, c.list(t, isoProjB, leaf), leaf,
		"%s: project %s does not list its own copy of %q", c.name, isoProjB, leaf)
	// Each project now sees exactly one copy: the same leaf name, no
	// duplication across tenants and no shadowing.
	require.Equal(t, []string{leaf}, c.list(t, isoProjA, leaf),
		"%s: project %s list is not its own single copy of %q", c.name, isoProjA, leaf)
	require.Equal(t, []string{leaf}, c.list(t, isoProjB, leaf),
		"%s: project %s list is not its own single copy of %q", c.name, isoProjB, leaf)

	// 4. Deleting A's copy must not touch B's.
	if c.del == nil {
		return
	}
	require.Equal(t, c.delOK, c.del(t, isoProjA, leaf),
		"%s: delete of %q in %s returned non-success", c.name, leaf, isoProjA)
	isoEventually(t, func() int { return c.get(t, isoProjA, leaf) }, http.StatusNotFound,
		fmt.Sprintf("%s: resource still readable in %s after its own delete", c.name, isoProjA))
	isoEventually(t, func() int { return c.get(t, isoProjB, leaf) }, http.StatusOK,
		fmt.Sprintf("%s: delete in %s affected %s's copy", c.name, isoProjA, isoProjB))
	require.Contains(t, c.list(t, isoProjB, leaf), leaf,
		"%s: delete in %s removed %s's copy from its list", c.name, isoProjA, isoProjB)
}

// idCase is like isoCase but for services whose resources carry a
// server-assigned id (Cloud Monitoring policies/channels, Workflow executions):
// create returns the id and list/get/delete operate on it. The same resource
// kind created in two projects still yields independent state, but the ids
// differ, so the runner compares list membership by id rather than by a shared
// leaf name.
type idCase struct {
	name   string
	create func(t *testing.T, project string) string
	list   func(t *testing.T, project string) []string
	get    func(t *testing.T, project, id string) int
	del    func(t *testing.T, project, id string) int
	delOK  int
}

// runIDIsolation is the id-addressed invariant body: seed only in A, prove the
// pristine B/C tenants cannot see or read A's id, create an independent
// resource in B, then delete A's and confirm B's survives.
func runIDIsolation(t *testing.T, c idCase) {
	t.Helper()
	resetState(t)

	// 1. Seed only in A and prove A lists its own resource.
	idA := c.create(t, isoProjA)
	require.NotEmpty(t, idA, "%s: create in %s returned no id", c.name, isoProjA)
	isoEventually(t, func() int { return c.get(t, isoProjA, idA) }, http.StatusOK,
		fmt.Sprintf("%s: GET of its own resource in %s", c.name, isoProjA))
	require.Contains(t, c.list(t, isoProjA), idA,
		"%s: project %s does not list its own resource %q", c.name, isoProjA, idA)

	// 2. Seeded negative: B and the pristine C must not see or read A's id.
	require.NotContains(t, c.list(t, isoProjB), idA,
		"%s: list LEAK — project %s sees a resource owned by %s", c.name, isoProjB, isoProjA)
	require.NotContains(t, c.list(t, isoProjC), idA,
		"%s: list LEAK — project %s sees a resource owned by %s", c.name, isoProjC, isoProjA)
	require.Equal(t, http.StatusNotFound, c.get(t, isoProjB, idA),
		"%s: GET LEAK — project %s can read a resource owned by %s", c.name, isoProjB, isoProjA)
	require.Equal(t, http.StatusNotFound, c.get(t, isoProjC, idA),
		"%s: GET LEAK — project %s can read a resource owned by %s", c.name, isoProjC, isoProjA)

	// 3. A resource created in B is independent of A's.
	idB := c.create(t, isoProjB)
	require.NotEmpty(t, idB, "%s: create in %s returned no id", c.name, isoProjB)
	require.NotEqual(t, idA, idB, "%s: the same id was handed to two projects", c.name)
	isoEventually(t, func() int { return c.get(t, isoProjB, idB) }, http.StatusOK,
		fmt.Sprintf("%s: GET of its own resource in %s", c.name, isoProjB))
	listA, listB := c.list(t, isoProjA), c.list(t, isoProjB)
	require.Contains(t, listA, idA)
	require.NotContains(t, listA, idB, "%s: list LEAK — %s sees %s's resource", c.name, isoProjA, isoProjB)
	require.Contains(t, listB, idB)
	require.NotContains(t, listB, idA, "%s: list LEAK — %s sees %s's resource", c.name, isoProjB, isoProjA)
	require.Equal(t, http.StatusNotFound, c.get(t, isoProjA, idB),
		"%s: GET LEAK — %s can read %s's resource", c.name, isoProjA, isoProjB)

	// 4. Deleting A's resource must not touch B's.
	if c.del == nil {
		return
	}
	require.Equal(t, c.delOK, c.del(t, isoProjA, idA),
		"%s: delete in %s returned non-success", c.name, isoProjA)
	isoEventually(t, func() int { return c.get(t, isoProjA, idA) }, http.StatusNotFound,
		fmt.Sprintf("%s: resource still readable in %s after its own delete", c.name, isoProjA))
	isoEventually(t, func() int { return c.get(t, isoProjB, idB) }, http.StatusOK,
		fmt.Sprintf("%s: delete in %s affected %s's copy", c.name, isoProjA, isoProjB))
	require.Contains(t, c.list(t, isoProjB), idB,
		"%s: delete in %s removed %s's resource from its list", c.name, isoProjA, isoProjB)
}

// isoWorkflowExecParent is the parent workflow id every execution case seeds
// (the same id in both projects); the execution id itself is server-assigned.
const isoWorkflowExecParent = "iso-wf"

// isoWorkflowSource is a minimal valid Workflows YAML mapping (the emulator
// rejects a non-mapping source), passed as a JSON sourceContents string.
const isoWorkflowSource = `{"sourceContents":"main:\n  steps:\n    - done:\n        return: 1\n"}`

// seedIsoWorkflowExecParent creates the parent workflow executions hang off,
// tolerating AlreadyExists: both the create leg and the negative-side list leg
// seed it, and executions.list under an unknown workflow is 404.
func seedIsoWorkflowExecParent(t *testing.T, p string) {
	t.Helper()
	wfPath := fmt.Sprintf("/v1/projects/%s/locations/us-central1/workflows?workflowId=%s", p, isoWorkflowExecParent)
	st, b := isoCall(t, "POST", wfPath, isoWorkflowSource)
	require.Contains(t, []int{http.StatusOK, http.StatusConflict}, st, "seed parent workflow in %s: %s", p, string(b))
}

const isoMonitorPolicyBody = `{"displayName":"iso","combiner":"OR","enabled":true,"conditions":[{"displayName":"c","conditionThreshold":{"filter":"metric.type = \"custom.googleapis.com/iso\"","comparison":"COMPARISON_GT","thresholdValue":1}}]}`

// isoMonitorChannelBody is a printf template taking the owning project.
const isoMonitorChannelBody = `{"type":"pubsub","displayName":"iso","enabled":true,"labels":{"topic":"projects/%s/topics/iso-topic"}}`

// isoIDCases covers the mutating services whose resources are addressed by a
// server-assigned id.
func isoIDCases() []idCase {
	return []idCase{
		{
			name: "monitoring-alertpolicy",
			create: func(t *testing.T, p string) string {
				st, b := isoCall(t, "POST", fmt.Sprintf("/v3/projects/%s/alertPolicies", p), isoMonitorPolicyBody)
				require.Equal(t, http.StatusOK, st, "create alert policy in %s: %s", p, string(b))
				return isoIDFromBody(t, b, "alert policy in "+p)
			},
			list: func(t *testing.T, p string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v3/projects/%s/alertPolicies", p)), "alertPolicies")
			},
			get: func(t *testing.T, p, id string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v3/projects/%s/alertPolicies/%s", p, id), "")
			},
			del: func(t *testing.T, p, id string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v3/projects/%s/alertPolicies/%s", p, id), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "monitoring-notificationchannel",
			create: func(t *testing.T, p string) string {
				st, b := isoCall(t, "POST", fmt.Sprintf("/v3/projects/%s/notificationChannels", p), fmt.Sprintf(isoMonitorChannelBody, p))
				require.Equal(t, http.StatusOK, st, "create notification channel in %s: %s", p, string(b))
				return isoIDFromBody(t, b, "notification channel in "+p)
			},
			list: func(t *testing.T, p string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v3/projects/%s/notificationChannels", p)), "notificationChannels")
			},
			get: func(t *testing.T, p, id string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v3/projects/%s/notificationChannels/%s", p, id), "")
			},
			del: func(t *testing.T, p, id string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v3/projects/%s/notificationChannels/%s", p, id), "")
			},
			delOK: http.StatusOK,
		},
		{
			// Workflow executions live under a parent workflow: seed the
			// same-named workflow per project, then create/read/list the
			// server-id-addressed execution. Execution delete is unimplemented
			// on the emulator surface, so the delete leg is skipped.
			name: "workflowexecutions",
			create: func(t *testing.T, p string) string {
				seedIsoWorkflowExecParent(t, p)
				execPath := fmt.Sprintf("/v1/projects/%s/locations/us-central1/workflows/%s/executions", p, isoWorkflowExecParent)
				st, b := isoCall(t, "POST", execPath, "{}")
				require.Equal(t, http.StatusOK, st, "create execution in %s: %s", p, string(b))
				return isoIDFromBody(t, b, "execution in "+p)
			},
			list: func(t *testing.T, p string) []string {
				// The negative-side list runs in a project that never created
				// an execution; its parent workflow must still exist for
				// executions.list to succeed (an unknown parent is 404).
				seedIsoWorkflowExecParent(t, p)
				path := fmt.Sprintf("/v1/projects/%s/locations/us-central1/workflows/%s/executions", p, isoWorkflowExecParent)
				return isoNameLeaves(t, isoGET(t, path), "executions")
			},
			get: func(t *testing.T, p, id string) int {
				path := fmt.Sprintf("/v1/projects/%s/locations/us-central1/workflows/%s/executions/%s", p, isoWorkflowExecParent, id)
				return isoStatus(t, "GET", path, "")
			},
		},
	}
}

// TestProjectIsolation_SeededNegative is the explicit non-vacuous scoping
// check: the same topic name exists in two projects, and a third project that
// never created it must see neither, cannot read it, and deleting one copy
// leaves the other intact. If project scoping were broken (one shared store
// account) the "third project sees nothing" and "delete one, keep the other"
// assertions would fail.
func TestProjectIsolation_SeededNegative(t *testing.T) {
	resetState(t)
	const leaf = "seeded-topic"
	topicPath := func(p string) string { return fmt.Sprintf("/v1/projects/%s/topics/%s", p, leaf) }

	// Seed the identical name in two projects.
	for _, p := range []string{isoProjA, isoProjB} {
		st, b := isoCall(t, "PUT", topicPath(p), "{}")
		require.Equal(t, http.StatusOK, st, "seed topic in %s: %s", p, string(b))
	}

	// Each project's in-project GET resolves to its own copy...
	_, bodyA := isoCall(t, "GET", topicPath(isoProjA), "")
	require.Contains(t, string(bodyA), "projects/"+isoProjA+"/topics/"+leaf,
		"project %s must GET its own copy", isoProjA)
	_, bodyB := isoCall(t, "GET", topicPath(isoProjB), "")
	require.Contains(t, string(bodyB), "projects/"+isoProjB+"/topics/"+leaf,
		"project %s must GET its own copy", isoProjB)

	// ...and an uninvolved third project sees nothing of either.
	_, listC := isoCall(t, "GET", "/v1/projects/"+isoProjC+"/topics", "")
	require.Empty(t, isoNameLeaves(t, listC, "topics"),
		"list LEAK: %s sees a topic it never created", isoProjC)
	require.Equal(t, http.StatusNotFound, isoStatus(t, "GET", topicPath(isoProjC), ""),
		"GET LEAK: %s can read a topic it never created", isoProjC)

	// Deleting one project's copy leaves the other's intact.
	require.Equal(t, http.StatusOK, isoStatus(t, "DELETE", topicPath(isoProjA), ""))
	require.Equal(t, http.StatusNotFound, isoStatus(t, "GET", topicPath(isoProjA), ""),
		"deleted topic still readable in %s", isoProjA)
	require.Equal(t, http.StatusOK, isoStatus(t, "GET", topicPath(isoProjB), ""),
		"delete in %s affected %s's copy", isoProjA, isoProjB)
}

// isoCases is the covered-service table. Every listed service is a mutating
// service with project-scoped state reachable over the emulator's REST surface.
func isoCases() []isoCase {
	return []isoCase{
		{
			// GCS buckets are a global namespace (real-GCP semantics): the same
			// name cannot exist in two projects. Bucket *listing* is still
			// project-scoped, and object bytes/listing never cross a bucket.
			name:   "storage",
			custom: testStorageIsolation,
		},
		{
			name: "pubsub-topic",
			create: func(t *testing.T, p, leaf string) {
				st, b := isoCall(t, "PUT", fmt.Sprintf("/v1/projects/%s/topics/%s", p, leaf), "{}")
				require.Equal(t, http.StatusOK, st, "create pubsub topic in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/topics", p)), "topics")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v1/projects/%s/topics/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v1/projects/%s/topics/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "pubsub-subscription",
			leaf: "iso-sub",
			create: func(t *testing.T, p, leaf string) {
				topic := fmt.Sprintf("/v1/projects/%s/topics/%s-topic", p, leaf)
				st, b := isoCall(t, "PUT", topic, "{}")
				require.Equal(t, http.StatusOK, st, "create backing topic in %s: %s", p, string(b))
				body := fmt.Sprintf(`{"topic":"projects/%s/topics/%s-topic"}`, p, leaf)
				st, b = isoCall(t, "PUT", fmt.Sprintf("/v1/projects/%s/subscriptions/%s", p, leaf), body)
				require.Equal(t, http.StatusOK, st, "create pubsub subscription in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/subscriptions", p)), "subscriptions")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v1/projects/%s/subscriptions/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v1/projects/%s/subscriptions/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "secretmanager",
			create: func(t *testing.T, p, leaf string) {
				st, b := isoCall(t, "POST", fmt.Sprintf("/v1/projects/%s/secrets?secretId=%s", p, leaf), `{"replication":{"automatic":{}}}`)
				require.Equal(t, http.StatusOK, st, "create secret in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/secrets", p)), "secrets")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v1/projects/%s/secrets/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v1/projects/%s/secrets/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			// KMS key rings/crypto keys have no delete operation in real GCP
			// (only cryptoKeyVersions:destroy), so the delete leg is skipped;
			// list + get scoping is still asserted.
			name: "kms-cryptokey",
			leaf: "iso-key",
			create: func(t *testing.T, p, leaf string) {
				parent := fmt.Sprintf("/v1/projects/%s/locations/us", p)
				st, b := isoCall(t, "POST", parent+"/keyRings?keyRingId="+leaf+"-ring", "{}")
				require.Equal(t, http.StatusOK, st, "create key ring in %s: %s", p, string(b))
				st, b = isoCall(t, "POST", parent+"/keyRings/"+leaf+"-ring/cryptoKeys?cryptoKeyId="+leaf, `{"purpose":"ENCRYPT_DECRYPT"}`)
				require.Equal(t, http.StatusOK, st, "create crypto key in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/locations/us/keyRings/%s-ring/cryptoKeys", p, leaf)), "cryptoKeys")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v1/projects/%s/locations/us/keyRings/%s-ring/cryptoKeys/%s", p, leaf, leaf), "")
			},
		},
		{
			name: "iam-service-account",
			create: func(t *testing.T, p, leaf string) {
				body := `{"accountId":"` + leaf + `","serviceAccount":{"displayName":"iso"}}`
				st, b := isoCall(t, "POST", fmt.Sprintf("/v1/projects/%s/serviceAccounts", p), body)
				require.Equal(t, http.StatusOK, st, "create service account in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoAccountLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/serviceAccounts", p)))
			},
			get:   func(t *testing.T, p, leaf string) int { return isoStatus(t, "GET", isoSAEmail(p, leaf), "") },
			del:   func(t *testing.T, p, leaf string) int { return isoStatus(t, "DELETE", isoSAEmail(p, leaf), "") },
			delOK: http.StatusOK,
		},
		{
			name: "firestore",
			create: func(t *testing.T, p, leaf string) {
				path := fmt.Sprintf("/v1/projects/%s/databases/(default)/documents/iso-collection/%s", p, leaf)
				st, b := isoCall(t, "PATCH", path, `{"fields":{"owner":{"stringValue":"`+p+`"}}}`)
				require.Equal(t, http.StatusOK, st, "create firestore doc in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/databases/(default)/documents/iso-collection", p)), "documents")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v1/projects/%s/databases/(default)/documents/iso-collection/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v1/projects/%s/databases/(default)/documents/iso-collection/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "datastore",
			create: func(t *testing.T, p, leaf string) {
				st, b := isoCall(t, "POST", "/v1/projects/"+p+":commit", datastoreUpsert(p, leaf))
				require.Equal(t, http.StatusOK, st, "commit datastore entity in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoDatastoreLeaves(t, isoPOST(t, "/v1/projects/"+p+":runQuery", datastoreQuery()))
			},
			// Datastore Lookup returns 200 + a missing[] list for absent keys,
			// so translate "not found" into a synthetic 404 for the runner.
			get: func(t *testing.T, p, leaf string) int {
				st, b := isoCall(t, "POST", "/v1/projects/"+p+":lookup", datastoreLookup(leaf))
				if st != http.StatusOK {
					return st
				}
				if found, _ := isoJSON(t, b)["found"].([]any); len(found) > 0 {
					return http.StatusOK
				}
				return http.StatusNotFound
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "POST", "/v1/projects/"+p+":commit", datastoreDelete(leaf))
			},
			delOK: http.StatusOK,
		},
		{
			name: "functions",
			create: func(t *testing.T, p, leaf string) {
				path := fmt.Sprintf("/v2/projects/%s/locations/us-central1/functions?functionId=%s", p, leaf)
				st, b := isoCall(t, "POST", path, `{"buildConfig":{"runtime":"go121"},"serviceConfig":{}}`)
				require.Equal(t, http.StatusOK, st, "create function in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v2/projects/%s/locations/us-central1/functions", p)), "functions")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v2/projects/%s/locations/us-central1/functions/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v2/projects/%s/locations/us-central1/functions/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "workflows",
			create: func(t *testing.T, p, leaf string) {
				path := fmt.Sprintf("/v1/projects/%s/locations/us-central1/workflows?workflowId=%s", p, leaf)
				st, b := isoCall(t, "POST", path, isoWorkflowSource)
				require.Equal(t, http.StatusOK, st, "create workflow in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/locations/us-central1/workflows", p)), "workflows")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v1/projects/%s/locations/us-central1/workflows/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v1/projects/%s/locations/us-central1/workflows/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "dataproc",
			create: func(t *testing.T, p, leaf string) {
				body := fmt.Sprintf(`{"clusterName":"%s","projectId":"%s","config":{"workerConfig":{"numInstances":2}}}`, leaf, p)
				st, b := isoCall(t, "POST", "/v1/projects/"+p+"/regions/us-central1/clusters", body)
				require.Equal(t, http.StatusOK, st, "create dataproc cluster in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoClusterLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/regions/us-central1/clusters", p)), "clusters")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v1/projects/%s/regions/us-central1/clusters/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v1/projects/%s/regions/us-central1/clusters/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			// BigQuery dataset IDs accept only letters, digits and underscores.
			name: "bigquery-dataset",
			leaf: "iso_dataset",
			create: func(t *testing.T, p, leaf string) {
				body := fmt.Sprintf(`{"datasetReference":{"projectId":"%s","datasetId":"%s"}}`, p, leaf)
				st, b := isoCall(t, "POST", fmt.Sprintf("/bigquery/v2/projects/%s/datasets", p), body)
				require.Equal(t, http.StatusOK, st, "create bigquery dataset in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoDatasetLeaves(t, isoGET(t, fmt.Sprintf("/bigquery/v2/projects/%s/datasets", p)))
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/bigquery/v2/projects/%s/datasets/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/bigquery/v2/projects/%s/datasets/%s", p, leaf), "")
			},
			delOK: http.StatusNoContent,
		},
		{
			name: "scheduler",
			create: func(t *testing.T, p, leaf string) {
				// Real GCP derives the job name from the jobId query parameter;
				// we also supply the fully-qualified name so the job is
				// addressable by the same leaf across emulator versions.
				body := fmt.Sprintf(`{"name":"projects/%s/locations/us-central1/jobs/%s","schedule":"* * * * *","timeZone":"UTC","httpTarget":{"uri":"http://example.com"}}`, p, leaf)
				path := fmt.Sprintf("/v1/projects/%s/locations/us-central1/jobs?jobId=%s", p, leaf)
				st, b := isoCall(t, "POST", path, body)
				require.Equal(t, http.StatusOK, st, "create scheduler job in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/locations/us-central1/jobs", p)), "jobs")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v1/projects/%s/locations/us-central1/jobs/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v1/projects/%s/locations/us-central1/jobs/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "tasks-queue",
			leaf: "iso-queue",
			create: func(t *testing.T, p, leaf string) {
				body := fmt.Sprintf(`{"name":"projects/%s/locations/us-central1/queues/%s"}`, p, leaf)
				path := fmt.Sprintf("/v2/projects/%s/locations/us-central1/queues?queueId=%s", p, leaf)
				st, b := isoCall(t, "POST", path, body)
				require.Equal(t, http.StatusOK, st, "create tasks queue in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v2/projects/%s/locations/us-central1/queues", p)), "queues")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v2/projects/%s/locations/us-central1/queues/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v2/projects/%s/locations/us-central1/queues/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "clouddns-managedzone",
			create: func(t *testing.T, p, leaf string) {
				body := fmt.Sprintf(`{"name":"%s","dnsName":"%s.example.com.","visibility":"public"}`, leaf, leaf)
				st, b := isoCall(t, "POST", fmt.Sprintf("/dns/v1/projects/%s/managedZones", p), body)
				require.Equal(t, http.StatusOK, st, "create DNS zone in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/dns/v1/projects/%s/managedZones", p)), "managedZones")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/dns/v1/projects/%s/managedZones/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/dns/v1/projects/%s/managedZones/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "cloudsql-instance",
			create: func(t *testing.T, p, leaf string) {
				body := fmt.Sprintf(`{"name":"%s","settings":{"tier":"db-f1-micro"},"databaseVersion":"POSTGRES_14","region":"us-central1"}`, leaf)
				st, b := isoCall(t, "POST", fmt.Sprintf("/sql/v1beta4/projects/%s/instances", p), body)
				require.Equal(t, http.StatusOK, st, "create Cloud SQL instance in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/sql/v1beta4/projects/%s/instances", p)), "items")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/sql/v1beta4/projects/%s/instances/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/sql/v1beta4/projects/%s/instances/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "compute-network",
			create: func(t *testing.T, p, leaf string) {
				body := fmt.Sprintf(`{"name":"%s","autoCreateSubnetworks":false}`, leaf)
				st, b := isoCall(t, "POST", fmt.Sprintf("/compute/v1/projects/%s/global/networks", p), body)
				require.Equal(t, http.StatusOK, st, "create compute network in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/compute/v1/projects/%s/global/networks", p)), "items")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/compute/v1/projects/%s/global/networks/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/compute/v1/projects/%s/global/networks/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "memorystore-instance",
			create: func(t *testing.T, p, leaf string) {
				body := `{"tier":"BASIC","memorySizeGb":1}`
				st, b := isoCall(t, "POST", fmt.Sprintf("/v1/projects/%s/locations/us-central1/instances?instanceId=%s", p, leaf), body)
				require.Equal(t, http.StatusOK, st, "create Memorystore instance in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/locations/us-central1/instances", p)), "instances")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v1/projects/%s/locations/us-central1/instances/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v1/projects/%s/locations/us-central1/instances/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "eventarc-trigger",
			create: func(t *testing.T, p, leaf string) {
				body := `{"destination":{"cloudRun":{"service":"iso-svc","region":"us-central1"}},"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]}`
				st, b := isoCall(t, "POST", fmt.Sprintf("/v1/projects/%s/locations/us-central1/triggers?triggerId=%s", p, leaf), body)
				require.Equal(t, http.StatusOK, st, "create Eventarc trigger in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/locations/us-central1/triggers", p)), "triggers")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v1/projects/%s/locations/us-central1/triggers/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v1/projects/%s/locations/us-central1/triggers/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "managedkafka-cluster",
			create: func(t *testing.T, p, leaf string) {
				body := `{"capacityConfig":{"vcpuCount":3,"memoryBytes":6442450944}}`
				st, b := isoCall(t, "POST", fmt.Sprintf("/v1/projects/%s/locations/us-central1/clusters?clusterId=%s", p, leaf), body)
				require.Equal(t, http.StatusOK, st, "create Managed Kafka cluster in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/locations/us-central1/clusters", p)), "clusters")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v1/projects/%s/locations/us-central1/clusters/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v1/projects/%s/locations/us-central1/clusters/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "metastore-service",
			create: func(t *testing.T, p, leaf string) {
				body := fmt.Sprintf(`{"network":"projects/%s/global/networks/default"}`, p)
				st, b := isoCall(t, "POST", fmt.Sprintf("/v1/projects/%s/locations/us-central1/services?serviceId=%s", p, leaf), body)
				require.Equal(t, http.StatusOK, st, "create Dataproc Metastore service in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/locations/us-central1/services", p)), "services")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v1/projects/%s/locations/us-central1/services/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v1/projects/%s/locations/us-central1/services/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "logging-sink",
			create: func(t *testing.T, p, leaf string) {
				body := fmt.Sprintf(`{"name":"%s","destination":"storage.googleapis.com/iso-bucket"}`, leaf)
				st, b := isoCall(t, "POST", fmt.Sprintf("/v2/projects/%s/sinks", p), body)
				require.Equal(t, http.StatusOK, st, "create Logging sink in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v2/projects/%s/sinks", p)), "sinks")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v2/projects/%s/sinks/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v2/projects/%s/sinks/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			// Logging entries are a second, independent Logging store (the
			// entries log vs. the sink/router config), scoped by the logName
			// project and the entries:list resourceNames parent.
			name:   "logging-entries",
			custom: testLoggingEntriesIsolation,
		},
		{
			name: "container-gke-cluster",
			leaf: "iso-gke",
			create: func(t *testing.T, p, leaf string) {
				body := fmt.Sprintf(`{"cluster":{"name":"%s","initialNodeCount":1}}`, leaf)
				st, b := isoCall(t, "POST", fmt.Sprintf("/container/v1/projects/%s/locations/us-central1/clusters", p), body)
				require.Equal(t, http.StatusOK, st, "create GKE cluster in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/container/v1/projects/%s/locations/us-central1/clusters", p)), "clusters")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/container/v1/projects/%s/locations/us-central1/clusters/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/container/v1/projects/%s/locations/us-central1/clusters/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			name: "run-service",
			leaf: "iso-run",
			create: func(t *testing.T, p, leaf string) {
				body := `{"template":{"containers":[{"image":"gcr.io/iso/app"}]}}`
				st, b := isoCall(t, "POST", fmt.Sprintf("/v2/projects/%s/locations/us-central1/services?serviceId=%s", p, leaf), body)
				require.Equal(t, http.StatusOK, st, "create Cloud Run service in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v2/projects/%s/locations/us-central1/services", p)), "services")
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/v2/projects/%s/locations/us-central1/services/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/v2/projects/%s/locations/us-central1/services/%s", p, leaf), "")
			},
			delOK: http.StatusOK,
		},
		{
			// Service Usage enablement is genuinely per-project: enabling an API
			// in one project leaves it DISABLED in another, and list returns
			// only the caller project's enabled services. GET returns 200 with
			// state DISABLED for a never-enabled API, so translate "not
			// enabled" into the runner's synthetic 404.
			name: "serviceusage",
			leaf: "iso-api.googleapis.com",
			create: func(t *testing.T, p, leaf string) {
				st, b := isoCall(t, "POST", fmt.Sprintf("/v1/projects/%s/services/%s:enable", p, leaf), "{}")
				require.Equal(t, http.StatusOK, st, "enable service in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoNameLeaves(t, isoGET(t, fmt.Sprintf("/v1/projects/%s/services", p)), "services")
			},
			get: func(t *testing.T, p, leaf string) int {
				st, b := isoCall(t, "GET", fmt.Sprintf("/v1/projects/%s/services/%s", p, leaf), "")
				if st != http.StatusOK {
					return st
				}
				if state, _ := isoJSON(t, b)["state"].(string); state == "ENABLED" {
					return http.StatusOK
				}
				return http.StatusNotFound
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "POST", fmt.Sprintf("/v1/projects/%s/services/%s:disable", p, leaf), "{}")
			},
			delOK: http.StatusOK,
		},
		{
			// BigQuery table IDs accept only letters, digits and underscores.
			name: "bigquery-table",
			leaf: "iso_table",
			create: func(t *testing.T, p, leaf string) {
				dsBody := fmt.Sprintf(`{"datasetReference":{"projectId":"%s","datasetId":"iso_tds"}}`, p)
				st, b := isoCall(t, "POST", fmt.Sprintf("/bigquery/v2/projects/%s/datasets", p), dsBody)
				require.Equal(t, http.StatusOK, st, "create bigquery dataset in %s: %s", p, string(b))
				tblBody := fmt.Sprintf(`{"tableReference":{"projectId":"%s","datasetId":"iso_tds","tableId":"%s"},"schema":{"fields":[{"name":"x","type":"STRING"}]}}`, p, leaf)
				st, b = isoCall(t, "POST", fmt.Sprintf("/bigquery/v2/projects/%s/datasets/iso_tds/tables", p), tblBody)
				require.Equal(t, http.StatusOK, st, "create bigquery table in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoTableLeaves(t, isoGET(t, fmt.Sprintf("/bigquery/v2/projects/%s/datasets/iso_tds/tables", p)))
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/bigquery/v2/projects/%s/datasets/iso_tds/tables/%s", p, leaf), "")
			},
			del: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "DELETE", fmt.Sprintf("/bigquery/v2/projects/%s/datasets/iso_tds/tables/%s", p, leaf), "")
			},
			delOK: http.StatusNoContent,
		},
		{
			// BigQuery jobs.delete is unimplemented on the emulator surface, so
			// the delete leg is skipped; create/list/get scoping is asserted.
			name: "bigquery-job",
			leaf: "iso_job",
			create: func(t *testing.T, p, leaf string) {
				body := fmt.Sprintf(`{"jobReference":{"projectId":"%s","jobId":"%s"},"configuration":{"query":{"query":"SELECT 1","useLegacySql":false}}}`, p, leaf)
				st, b := isoCall(t, "POST", fmt.Sprintf("/bigquery/v2/projects/%s/jobs", p), body)
				require.Equal(t, http.StatusOK, st, "create bigquery job in %s: %s", p, string(b))
			},
			list: func(t *testing.T, p, leaf string) []string {
				return isoJobLeaves(t, isoGET(t, fmt.Sprintf("/bigquery/v2/projects/%s/jobs", p)))
			},
			get: func(t *testing.T, p, leaf string) int {
				return isoStatus(t, "GET", fmt.Sprintf("/bigquery/v2/projects/%s/jobs/%s", p, leaf), "")
			},
		},
	}
}

// testStorageIsolation covers GCS buckets (a global namespace) and objects
// (bucket-scoped). Bucket listing is project-scoped; the same bucket name in a
// second project conflicts rather than creating a shadow copy; and object
// bytes/listing never cross a bucket, hence never cross a project.
func testStorageIsolation(t *testing.T) {
	resetState(t)
	const (
		bucketA = "iso-bucket-a"
		bucketB = "iso-bucket-b"
		object  = "iso-object.txt"
	)

	// Bucket created in project A only.
	st, b := isoCall(t, "POST", "/storage/v1/b?project="+isoProjA, `{"name":"`+bucketA+`"}`)
	require.Equal(t, http.StatusOK, st, "create bucket %s in %s: %s", bucketA, isoProjA, string(b))

	// Project-scoped listing: A sees it, B must not.
	require.Equal(t, []string{bucketA}, isoBucketList(t, isoProjA))
	require.Empty(t, isoBucketList(t, isoProjB),
		"list LEAK: project %s sees bucket %q owned by %s", isoProjB, bucketA, isoProjA)
	require.Empty(t, isoBucketList(t, isoProjC),
		"list LEAK: project %s sees bucket %q owned by %s", isoProjC, bucketA, isoProjA)

	// Global namespace (real-GCP semantics): re-creating the same name in
	// another project conflicts — no per-project shadow bucket.
	st, _ = isoCall(t, "POST", "/storage/v1/b?project="+isoProjB, `{"name":"`+bucketA+`"}`)
	require.Equal(t, http.StatusConflict, st,
		"global bucket name %q must conflict in project %s, not create a shadow copy", bucketA, isoProjB)

	// Two buckets owned by different projects hold the same-named object.
	st, b = isoCall(t, "POST", "/storage/v1/b?project="+isoProjB, `{"name":"`+bucketB+`"}`)
	require.Equal(t, http.StatusOK, st, "create bucket %s in %s: %s", bucketB, isoProjB, string(b))
	isoUpload(t, bucketA, object, "from-project-a")
	isoUpload(t, bucketB, object, "from-project-b")

	require.Equal(t, []string{object}, isoObjectList(t, bucketA))
	require.Equal(t, []string{object}, isoObjectList(t, bucketB))
	_, body := isoCall(t, "GET", "/storage/v1/b/"+bucketA+"/o/"+object+"?alt=media", "")
	require.Equal(t, "from-project-a", string(body), "object in %s leaked another project's bytes", bucketA)
	_, body = isoCall(t, "GET", "/storage/v1/b/"+bucketB+"/o/"+object+"?alt=media", "")
	require.Equal(t, "from-project-b", string(body), "object in %s leaked another project's bytes", bucketB)

	// Delete the object in A; B's copy is untouched.
	st, _ = isoCall(t, "DELETE", "/storage/v1/b/"+bucketA+"/o/"+object, "")
	require.Equal(t, http.StatusNoContent, st)
	require.Equal(t, http.StatusNotFound, isoStatus(t, "GET", "/storage/v1/b/"+bucketA+"/o/"+object, ""))
	_, body = isoCall(t, "GET", "/storage/v1/b/"+bucketB+"/o/"+object+"?alt=media", "")
	require.Equal(t, "from-project-b", string(body), "delete in %s/%s affected %s/%s", isoProjA, bucketA, isoProjB, bucketB)

	// Delete bucket A; project B's bucket survives.
	st, _ = isoCall(t, "DELETE", "/storage/v1/b/"+bucketA+"?project="+isoProjA, "")
	require.Equal(t, http.StatusNoContent, st)
	require.Empty(t, isoBucketList(t, isoProjA))
	require.Equal(t, []string{bucketB}, isoBucketList(t, isoProjB))
}

// testLoggingEntriesIsolation covers the Logging entries store: entries written
// under one project's log name must not surface in another project's log-name
// listing or entries:list result.
func testLoggingEntriesIsolation(t *testing.T) {
	resetState(t)
	const logName = "iso-log"

	writeEntry := func(project string) {
		t.Helper()
		body := fmt.Sprintf(`{"entries":[{"logName":"projects/%s/logs/%s","textPayload":"from-%s"}]}`, project, logName, project)
		st, b := isoCall(t, "POST", "/v2/entries:write", body)
		require.Equal(t, http.StatusOK, st, "write entry in %s: %s", project, string(b))
	}

	writeEntry(isoProjA)
	require.Contains(t, isoLogNameLeaves(t, isoGET(t, "/v2/projects/"+isoProjA+"/logs")), logName)
	require.NotContains(t, isoLogNameLeaves(t, isoGET(t, "/v2/projects/"+isoProjB+"/logs")), logName,
		"log-name LEAK: %s lists a log owned by %s", isoProjB, isoProjA)
	require.Contains(t, isoEntryTexts(t, isoProjA), "from-"+isoProjA)
	require.NotContains(t, isoEntryTexts(t, isoProjB), "from-"+isoProjA,
		"entries LEAK: %s lists an entry owned by %s", isoProjB, isoProjA)

	writeEntry(isoProjB)
	require.Contains(t, isoEntryTexts(t, isoProjB), "from-"+isoProjB)
	require.NotContains(t, isoEntryTexts(t, isoProjB), "from-"+isoProjA,
		"entries LEAK: %s sees %s's entry after both wrote", isoProjB, isoProjA)
	require.Contains(t, isoEntryTexts(t, isoProjA), "from-"+isoProjA)
	require.NotContains(t, isoEntryTexts(t, isoProjA), "from-"+isoProjB,
		"entries LEAK: %s sees %s's entry after both wrote", isoProjA, isoProjB)
}

// ── request helpers ──────────────────────────────────────────────────────────

// isoCall performs a request with an optional JSON body and returns the status
// plus response bytes.
func isoCall(t *testing.T, method, path, body string) (int, []byte) {
	t.Helper()
	var payload []byte
	headers := map[string]string{}
	if body != "" {
		payload = []byte(body)
		headers["Content-Type"] = "application/json"
	}
	resp, b := do(t, method, path, payload, headers)
	return resp.StatusCode, b
}

// isoStatus performs a request and returns only the status code.
func isoStatus(t *testing.T, method, path, body string) int {
	t.Helper()
	st, _ := isoCall(t, method, path, body)
	return st
}

// isoGET performs a GET and returns the body, requiring a 200.
func isoGET(t *testing.T, path string) []byte {
	t.Helper()
	st, b := isoCall(t, "GET", path, "")
	require.Equal(t, http.StatusOK, st, "GET %s: %s", path, string(b))
	return b
}

// isoPOST performs a POST with a body and returns the body, requiring a 200.
func isoPOST(t *testing.T, path, body string) []byte {
	t.Helper()
	st, b := isoCall(t, "POST", path, body)
	require.Equal(t, http.StatusOK, st, "POST %s: %s", path, string(b))
	return b
}

// isoEventually polls get until it returns want. The deadline uses the repo
// clock helper (clock.RealNow — infrastructure wall time, per CLAUDE.md) rather
// than time.Now, and is deliberately generous because LRO-backed services can
// settle slowly under CI load.
func isoEventually(t *testing.T, get func() int, want int, what string) {
	t.Helper()
	const settle = 30 * time.Second
	deadline := clock.RealNow().Add(settle)
	for {
		got := get()
		if got == want {
			return
		}
		if clock.RealNow().After(deadline) {
			require.Equal(t, want, got, "%s (timed out after %s)", what, settle)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ── response parsers ─────────────────────────────────────────────────────────

func isoJSON(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m), "response is not JSON: %s", string(b))
	return m
}

func isoObjects(t *testing.T, b []byte, arrayKey string) []map[string]any {
	t.Helper()
	raw, _ := isoJSON(t, b)[arrayKey].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, e := range raw {
		if obj, ok := e.(map[string]any); ok {
			out = append(out, obj)
		}
	}
	return out
}

func isoLastSeg(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// isoNameLeaves returns the trailing path segment of each object's "name".
func isoNameLeaves(t *testing.T, b []byte, arrayKey string) []string {
	t.Helper()
	objs := isoObjects(t, b, arrayKey)
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		if n, ok := o["name"].(string); ok {
			out = append(out, isoLastSeg(n))
		}
	}
	return out
}

// isoClusterLeaves reads Dataproc's clusterName field.
func isoClusterLeaves(t *testing.T, b []byte, arrayKey string) []string {
	t.Helper()
	objs := isoObjects(t, b, arrayKey)
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		if n, ok := o["clusterName"].(string); ok {
			out = append(out, n)
		}
	}
	return out
}

// isoAccountLeaves reads the accountId prefix of each IAM service-account email.
func isoAccountLeaves(t *testing.T, b []byte) []string {
	t.Helper()
	objs := isoObjects(t, b, "accounts")
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		e, ok := o["email"].(string)
		if !ok {
			continue
		}
		if i := strings.IndexByte(e, '@'); i > 0 {
			e = e[:i]
		}
		out = append(out, e)
	}
	return out
}

// isoDatasetLeaves reads BigQuery's datasetReference.datasetId.
func isoDatasetLeaves(t *testing.T, b []byte) []string {
	t.Helper()
	objs := isoObjects(t, b, "datasets")
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		ref, _ := o["datasetReference"].(map[string]any)
		if id, ok := ref["datasetId"].(string); ok {
			out = append(out, id)
		}
	}
	return out
}

// isoTableLeaves reads BigQuery's tableReference.tableId.
func isoTableLeaves(t *testing.T, b []byte) []string {
	t.Helper()
	objs := isoObjects(t, b, "tables")
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		ref, _ := o["tableReference"].(map[string]any)
		if id, ok := ref["tableId"].(string); ok {
			out = append(out, id)
		}
	}
	return out
}

// isoJobLeaves reads BigQuery's jobReference.jobId.
func isoJobLeaves(t *testing.T, b []byte) []string {
	t.Helper()
	objs := isoObjects(t, b, "jobs")
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		ref, _ := o["jobReference"].(map[string]any)
		if id, ok := ref["jobId"].(string); ok {
			out = append(out, id)
		}
	}
	return out
}

// isoIDFromBody extracts the trailing id from a created resource's "name".
func isoIDFromBody(t *testing.T, b []byte, what string) string {
	t.Helper()
	name, _ := isoJSON(t, b)["name"].(string)
	require.NotEmpty(t, name, "%s: response has no name: %s", what, string(b))
	return isoLastSeg(name)
}

// isoDatastoreLeaves reads the last key-path name of each runQuery entity.
func isoDatastoreLeaves(t *testing.T, b []byte) []string {
	t.Helper()
	batch, _ := isoJSON(t, b)["batch"].(map[string]any)
	raw, _ := batch["entityResults"].([]any)
	out := make([]string, 0, len(raw))
	for _, e := range raw {
		em, _ := e.(map[string]any)
		ent, _ := em["entity"].(map[string]any)
		key, _ := ent["key"].(map[string]any)
		path, _ := key["path"].([]any)
		if len(path) == 0 {
			continue
		}
		last, _ := path[len(path)-1].(map[string]any)
		if n, ok := last["name"].(string); ok {
			out = append(out, n)
		}
	}
	return out
}

// isoLogNameLeaves reads Logging's logNames[] list.
func isoLogNameLeaves(t *testing.T, b []byte) []string {
	t.Helper()
	raw, _ := isoJSON(t, b)["logNames"].([]any)
	out := make([]string, 0, len(raw))
	for _, e := range raw {
		if s, ok := e.(string); ok {
			out = append(out, isoLastSeg(s))
		}
	}
	return out
}

// isoEntryTexts returns the textPayload of every entry project lists.
func isoEntryTexts(t *testing.T, project string) []string {
	t.Helper()
	b := isoPOST(t, "/v2/entries:list", fmt.Sprintf(`{"resourceNames":["projects/%s"]}`, project))
	objs := isoObjects(t, b, "entries")
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		if s, ok := o["textPayload"].(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// isoBucketList returns the bucket names visible to a project.
func isoBucketList(t *testing.T, project string) []string {
	t.Helper()
	objs := isoObjects(t, isoGET(t, "/storage/v1/b?project="+project), "items")
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		if n, ok := o["name"].(string); ok {
			out = append(out, n)
		} else if id, ok := o["id"].(string); ok {
			out = append(out, id)
		}
	}
	return out
}

// isoObjectList returns the object names in a bucket.
func isoObjectList(t *testing.T, bucket string) []string {
	t.Helper()
	return isoNameLeaves(t, isoGET(t, "/storage/v1/b/"+bucket+"/o"), "items")
}

// isoUpload writes a small media object.
func isoUpload(t *testing.T, bucket, object, content string) {
	t.Helper()
	st, b := isoCall(t, "POST", "/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name="+object, content)
	require.Equal(t, http.StatusOK, st, "upload %s/%s: %s", bucket, object, string(b))
}

// isoSAEmail builds the service-account resource path for a project+accountId.
func isoSAEmail(project, accountID string) string {
	return fmt.Sprintf("/v1/projects/%s/serviceAccounts/%s@%s.iam.gserviceaccount.com", project, accountID, project)
}

// ── datastore payload helpers ────────────────────────────────────────────────

const isoDatastoreKind = "IsoEntity"

func datastoreUpsert(project, name string) string {
	return fmt.Sprintf(`{"mode":"NON_TRANSACTIONAL","mutations":[{"upsert":{"key":{"path":[{"kind":"%s","name":"%s"}]},"properties":{"owner":{"stringValue":"%s"}}}}]}`, isoDatastoreKind, name, project)
}

func datastoreLookup(name string) string {
	return fmt.Sprintf(`{"keys":[{"path":[{"kind":"%s","name":"%s"}]}]}`, isoDatastoreKind, name)
}

func datastoreDelete(name string) string {
	return fmt.Sprintf(`{"mode":"NON_TRANSACTIONAL","mutations":[{"delete":{"path":[{"kind":"%s","name":"%s"}]}}]}`, isoDatastoreKind, name)
}

func datastoreQuery() string {
	return fmt.Sprintf(`{"query":{"kind":[{"name":"%s"}]}}`, isoDatastoreKind)
}
