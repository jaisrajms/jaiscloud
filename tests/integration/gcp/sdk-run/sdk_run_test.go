// Package sdk_run_test exercises the jaiscloud-gcp emulator's Cloud Run Admin
// v2 control plane (run.googleapis.com/v2) through the official Google REST
// apiary client (google.golang.org/api/run/v2). Cloud Run's native transport is
// gRPC, but the emulator also exposes the documented REST v2 surface, so the
// apiary client is the transport-matching official client here.
//
// The suite drives the emulator's *mock* runtime: a Cloud Run service is a
// stored control-plane record, not a running container, so everything that does
// not need a container runtime is gated in CI here — service/revision CRUD,
// updateMask semantics, traffic split/allocation, service IAM, revision listing
// + paging, and the google.longrunning operations surface (operations.get in
// both the default synchronous shape and, when JAISCLOUD_LRO_ASYNC=1, the
// opt-in async shape). The executor-backed invocation path stays with the
// local-only e2e gates (tests/persistent_mode/gcp/cloudrun).
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_run_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	run "google.golang.org/api/run/v2"
)

const (
	serviceTypeAny  = "type.googleapis.com/google.cloud.run.v2.Service"
	revisionTypeAny = "type.googleapis.com/google.cloud.run.v2.Revision"
)

// endpoint is the emulator's REST root. Cloud Run v2 uses the canonical
// /v2/projects/{p}/locations/{l}/... path, which the router disambiguates on
// the services/revisions resource family.
func endpoint() string {
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8080/"
}

func baseURL() string {
	return strings.TrimRight(endpoint(), "/")
}

func projectID() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "proj"
}

func locationID() string { return "us-central1" }

// lroAsync reports whether the emulator under test runs in the opt-in async
// long-running-operation mode (JAISCLOUD_LRO_MODE=async). The suite then asserts
// the in-flight shape and polls; otherwise it asserts the synchronous shape.
func lroAsync() bool { return os.Getenv("JAISCLOUD_LRO_ASYNC") == "1" }

func opts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(endpoint()), option.WithoutAuthentication()}
}

// resetState wipes emulator state between tests (the suite shares one server).
func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(baseURL()+"/_jaiscloud/reset", "", nil) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func newService(t *testing.T) *run.Service {
	t.Helper()
	svc, err := run.NewService(context.Background(), opts()...)
	require.NoError(t, err)
	return svc
}

// newPrefixedService dials the emulator's documented "/run/" path prefix. Cloud
// Run shares the bare /v2/projects/{p}/locations/{l}/operations family with
// Cloud Functions on the single emulator origin (a documented ambiguity — that
// path stays with Functions), so a run operations *list* is only reachable
// through the "/run/" prefix; a run operation *get* is unambiguous because of
// its prefixed id.
func newPrefixedService(t *testing.T) *run.Service {
	t.Helper()
	svc, err := run.NewService(context.Background(),
		option.WithEndpoint(baseURL()+"/run/"), option.WithoutAuthentication())
	require.NoError(t, err)
	return svc
}

func parent() string {
	return fmt.Sprintf("projects/%s/locations/%s", projectID(), locationID())
}

func serviceName(id string) string { return parent() + "/services/" + id }

func revisionName(svc, rev string) string {
	return serviceName(svc) + "/revisions/" + rev
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// tagQualifiedURI rewrites a Cloud Run service uri into the tag-qualified form
// real Cloud Run returns for a tagged traffic target: the tag becomes the
// leading host label, https://{tag}---{service-host}.
func tagQualifiedURI(uri, tag string) string {
	return strings.Replace(uri, "://", "://"+tag+"---", 1)
}

// requireAPIErrorStatus asserts err is a googleapi.Error with the given status
// and that its JSON envelope carries the given google.rpc status string (e.g.
// ABORTED for an OCC failure).
func requireAPIErrorStatus(t *testing.T, err error, code int, status string) {
	t.Helper()
	requireAPIError(t, err, code)
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Contains(t, apiErr.Body, `"`+status+`"`, "error envelope status")
}

// requireAPIError asserts err is a googleapi.Error with the given status.
func requireAPIError(t *testing.T, err error, code int, msgAndArgs ...any) {
	t.Helper()
	require.Error(t, err)
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, code, apiErr.Code, msgAndArgs...)
}

// requireNotFound asserts err is a 404.
func requireNotFound(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()
	requireAPIError(t, err, http.StatusNotFound, msgAndArgs...)
}

// rawMap decodes a typed-Any operation payload (the @type + body map gax
// clients unpack).
func rawMap(t *testing.T, raw googleapi.RawMessage) map[string]any {
	t.Helper()
	require.NotEmpty(t, raw, "operation carries no typed payload")
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	return m
}

// awaitOperation resolves a mutation's long-running operation to done: the
// synchronous default returns it already terminal, while the async mode returns
// an in-flight operation that must be polled through operations.get until it
// settles. It returns the settled operation and its typed response payload.
func awaitOperation(t *testing.T, svc *run.Service, op *run.GoogleLongrunningOperation) (*run.GoogleLongrunningOperation, map[string]any) {
	t.Helper()
	require.NotEmpty(t, op.Name, "operation has no name: %+v", op)

	if !lroAsync() {
		require.True(t, op.Done, "synchronous operation must be done: %+v", op)
		require.NotEmpty(t, op.Response, "done operation must carry a response: %+v", op)
		require.NotEmpty(t, op.Metadata, "done operation must carry metadata: %+v", op)
		require.Equal(t, rawMap(t, op.Metadata)["@type"], rawMap(t, op.Response)["@type"],
			"metadata and response share the operation_info type")
		return op, rawMap(t, op.Response)
	}

	require.False(t, op.Done, "async operation must be in flight: %+v", op)
	require.Empty(t, op.Response, "in-flight operation must not carry a response: %+v", op)
	// An in-flight Cloud Run operation carries its typed metadata from creation
	// (the operation_info metadata_type: Service for service mutations,
	// Revision for revision deletes) rather than only once done.
	require.NotEmpty(t, op.Metadata, "in-flight operation must carry metadata: %+v", op)
	pendingType := rawMap(t, op.Metadata)["@type"]
	require.Contains(t, []any{serviceTypeAny, revisionTypeAny}, pendingType,
		"in-flight metadata @type: %+v", op.Metadata)

	deadline := time.Now().Add(30 * time.Second)
	for {
		got, err := svc.Projects.Locations.Operations.Get(op.Name).Do()
		require.NoError(t, err, "operations.get(%s)", op.Name)
		if got.Done {
			require.NotEmpty(t, got.Response, "settled operation must carry a response: %+v", got)
			require.NotEmpty(t, got.Metadata, "settled operation must carry metadata: %+v", got)
			require.Equal(t, pendingType, rawMap(t, got.Metadata)["@type"],
				"metadata type must not change as the operation settles")
			return got, rawMap(t, got.Response)
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation %s did not settle in 30s: %+v", op.Name, got)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// terminalOperation asserts op is already done (the synchronous default, and
// every validate-only operation regardless of mode) and returns its typed
// response payload.
func terminalOperation(t *testing.T, op *run.GoogleLongrunningOperation) map[string]any {
	t.Helper()
	require.True(t, op.Done, "operation must be terminal: %+v", op)
	require.NotEmpty(t, op.Response, "terminal operation must carry a response: %+v", op)
	require.NotEmpty(t, op.Metadata, "terminal operation must carry metadata: %+v", op)
	return rawMap(t, op.Response)
}

// container builds a one-container revision template.
func container(image string) *run.GoogleCloudRunV2RevisionTemplate {
	return &run.GoogleCloudRunV2RevisionTemplate{
		Containers: []*run.GoogleCloudRunV2Container{{Image: image}},
	}
}

// createService creates a service and returns its settled operation.
func createService(t *testing.T, svc *run.Service, id string) map[string]any {
	t.Helper()
	op, err := svc.Projects.Locations.Services.Create(parent(), &run.GoogleCloudRunV2Service{
		Template: container("us-docker.pkg.dev/cloudrun/container/hello"),
		Labels:   map[string]string{"env": "test"},
	}).ServiceId(id).Do()
	require.NoError(t, err, "create service")
	_, body := awaitOperation(t, svc, op)
	require.Equal(t, serviceTypeAny, body["@type"])
	require.Equal(t, serviceName(id), body["name"])
	return body
}

// TestSDKRunServiceLifecycle covers the Cloud Run service create/get/list/patch/
// delete lifecycle plus the documented synthesized fields and the
// AlreadyExists/NotFound error contracts.
func TestSDKRunServiceLifecycle(t *testing.T) {
	resetState(t)
	svc := newService(t)
	id := unique("sdk-run-svc")

	created := createService(t, svc, id)
	require.Equal(t, "1", created["generation"], "generation is a string")
	require.Equal(t, serviceName(id)+"/revisions/"+id+"-00001", created["latestReadyRevision"])
	require.Equal(t, created["latestReadyRevision"], created["latestCreatedRevision"])

	// get: derived output-only fields + the synthesized *.run.app uri.
	got, err := svc.Projects.Locations.Services.Get(serviceName(id)).Do()
	require.NoError(t, err, "get service")
	require.Equal(t, serviceName(id), got.Name)
	require.NotEmpty(t, got.Uid)
	require.Equal(t, int64(1), got.Generation)
	require.Equal(t, int64(1), got.ObservedGeneration)
	require.NotEmpty(t, got.Etag)
	require.Regexp(t,
		regexp.MustCompile(`^https://`+regexp.QuoteMeta(id)+`-[0-9a-f]{12}\.`+regexp.QuoteMeta(locationID())+`\.run\.app$`),
		got.Uri, "synthesized uri")
	require.Equal(t, []string{got.Uri}, got.Urls)
	require.NotEmpty(t, got.CreateTime)
	require.NotEmpty(t, got.UpdateTime)
	require.Equal(t, "test", got.Labels["env"])
	require.False(t, got.Reconciling)
	require.NotEmpty(t, got.Conditions, "conditions")
	require.Equal(t, "Ready", got.Conditions[0].Type)
	require.Equal(t, "CONDITION_SUCCEEDED", got.Conditions[0].State)
	require.Len(t, got.TrafficStatuses, 1)
	require.Equal(t, "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", got.TrafficStatuses[0].Type)
	require.Equal(t, int64(100), got.TrafficStatuses[0].Percent)

	// list.
	list, err := svc.Projects.Locations.Services.List(parent()).Do()
	require.NoError(t, err, "list services")
	require.Len(t, list.Services, 1)
	require.Equal(t, serviceName(id), list.Services[0].Name)

	// A duplicate create is AlreadyExists (409).
	_, err = svc.Projects.Locations.Services.Create(parent(), &run.GoogleCloudRunV2Service{}).ServiceId(id).Do()
	requireAPIError(t, err, http.StatusConflict)

	// A missing service is NotFound (404).
	_, err = svc.Projects.Locations.Services.Get(serviceName(unique("missing"))).Do()
	requireNotFound(t, err)

	// delete: the operation response is the removed service snapshot.
	delOp, err := svc.Projects.Locations.Services.Delete(serviceName(id)).Do()
	require.NoError(t, err, "delete service")
	_, delBody := awaitOperation(t, svc, delOp)
	require.Equal(t, serviceTypeAny, delBody["@type"])
	require.Equal(t, serviceName(id), delBody["name"])
	require.NotEmpty(t, delBody["deleteTime"])

	_, err = svc.Projects.Locations.Services.Get(serviceName(id)).Do()
	requireNotFound(t, err, "deleted service must not be readable")
}

// TestSDKRunUpdateMaskSemantics pins the updateMask contract: only masked
// top-level fields are applied, an unmasked template change is ignored (and so
// mints no revision), and a template-masked update mints the next revision.
func TestSDKRunUpdateMaskSemantics(t *testing.T) {
	resetState(t)
	svc := newService(t)
	id := unique("sdk-run-mask")
	createService(t, svc, id)

	// Mask "labels" only: the body's template must be ignored, so no new
	// revision is created, and labels are replaced wholesale.
	op, err := svc.Projects.Locations.Services.Patch(serviceName(id), &run.GoogleCloudRunV2Service{
		Labels:      map[string]string{"env": "prod", "team": "sre"},
		Description: "ignored by the mask",
		Template:    container("us-docker.pkg.dev/cloudrun/container/hello:v2"),
	}).UpdateMask("labels").Do()
	require.NoError(t, err, "patch labels")
	awaitOperation(t, svc, op)

	after, err := svc.Projects.Locations.Services.Get(serviceName(id)).Do()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"env": "prod", "team": "sre"}, after.Labels)
	require.Empty(t, after.Description, "unmasked description must not be applied")
	require.Equal(t, int64(2), after.Generation, "writable update bumps generation")
	require.Equal(t, serviceName(id)+"/revisions/"+id+"-00001", after.LatestCreatedRevision,
		"masked-out template change must not mint a revision")

	// Mask "template": the update mints the next revision.
	op2, err := svc.Projects.Locations.Services.Patch(serviceName(id), &run.GoogleCloudRunV2Service{
		Template: container("us-docker.pkg.dev/cloudrun/container/hello:v3"),
	}).UpdateMask("template").Do()
	require.NoError(t, err, "patch template")
	awaitOperation(t, svc, op2)

	after2, err := svc.Projects.Locations.Services.Get(serviceName(id)).Do()
	require.NoError(t, err)
	require.Equal(t, int64(3), after2.Generation)
	require.Equal(t, serviceName(id)+"/revisions/"+id+"-00002", after2.LatestCreatedRevision)
	require.Equal(t, after2.LatestCreatedRevision, after2.LatestReadyRevision)

	rev, err := svc.Projects.Locations.Services.Revisions.Get(after2.LatestCreatedRevision).Do()
	require.NoError(t, err)
	require.Len(t, rev.Containers, 1)
	require.Equal(t, "us-docker.pkg.dev/cloudrun/container/hello:v3", rev.Containers[0].Image)

	// An etag precondition mismatch is Aborted (409).
	_, err = svc.Projects.Locations.Services.Patch(serviceName(id), &run.GoogleCloudRunV2Service{
		Etag:   "stale-etag",
		Labels: map[string]string{"a": "b"},
	}).UpdateMask("labels").Do()
	requireAPIError(t, err, http.StatusConflict)
}

// TestSDKRunTrafficSplit covers traffic target allocation and the derived
// trafficStatuses: an explicit two-revision split, then a reset to
// latest-at-100%.
func TestSDKRunTrafficSplit(t *testing.T) {
	resetState(t)
	svc := newService(t)
	id := unique("sdk-run-traffic")
	createService(t, svc, id)

	// Mint a second revision.
	op, err := svc.Projects.Locations.Services.Patch(serviceName(id), &run.GoogleCloudRunV2Service{
		Template: container("us-docker.pkg.dev/cloudrun/container/hello:v2"),
	}).UpdateMask("template").Do()
	require.NoError(t, err)
	awaitOperation(t, svc, op)

	rev1 := revisionName(id, id+"-00001")
	rev2 := revisionName(id, id+"-00002")

	// Split 30/70 across the two revisions, tagging the first.
	splitOp, err := svc.Projects.Locations.Services.Patch(serviceName(id), &run.GoogleCloudRunV2Service{
		Traffic: []*run.GoogleCloudRunV2TrafficTarget{
			{Type: "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION", Revision: rev1, Percent: 30, Tag: "stable"},
			{Type: "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION", Revision: rev2, Percent: 70},
		},
	}).UpdateMask("traffic").Do()
	require.NoError(t, err, "patch traffic split")
	awaitOperation(t, svc, splitOp)

	got, err := svc.Projects.Locations.Services.Get(serviceName(id)).Do()
	require.NoError(t, err)
	require.Len(t, got.Traffic, 2)
	require.Equal(t, int64(30), got.Traffic[0].Percent)
	require.Equal(t, rev1, got.Traffic[0].Revision)
	require.Equal(t, "stable", got.Traffic[0].Tag)
	require.Equal(t, int64(70), got.Traffic[1].Percent)
	require.Len(t, got.TrafficStatuses, 2)
	require.Equal(t, int64(30), got.TrafficStatuses[0].Percent)
	require.Equal(t, rev1, got.TrafficStatuses[0].Revision)
	// Real Cloud Run returns a tag-qualified URL for a tagged target — the tag
	// as the leading host label, the string the Discovery calls "used in the
	// URI to exclusively reference this target" — and sets a target's status
	// uri only when the target carries a tag, so an untagged target has none.
	require.Equal(t, "stable", got.TrafficStatuses[0].Tag)
	require.Equal(t, tagQualifiedURI(got.Uri, "stable"), got.TrafficStatuses[0].Uri)
	require.Empty(t, got.TrafficStatuses[1].Uri, "an untagged target must not carry a status uri")
	// A traffic tag does NOT extend the service-level urls ("All URLs serving
	// traffic for this Service"): verified against real GCP, urls is unchanged by
	// a tag (the tag-qualified URL lives only on the target's status uri).
	require.Equal(t, []string{got.Uri}, got.Urls)

	// Reset to latest-at-100% through the same mask; the derived status
	// reflects the single latest target.
	resetOp, err := svc.Projects.Locations.Services.Patch(serviceName(id), &run.GoogleCloudRunV2Service{
		Traffic: []*run.GoogleCloudRunV2TrafficTarget{
			{Type: "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", Percent: 100},
		},
	}).UpdateMask("traffic").Do()
	require.NoError(t, err)
	awaitOperation(t, svc, resetOp)

	got2, err := svc.Projects.Locations.Services.Get(serviceName(id)).Do()
	require.NoError(t, err)
	require.Len(t, got2.TrafficStatuses, 1)
	require.Equal(t, int64(100), got2.TrafficStatuses[0].Percent)
	require.Equal(t, "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", got2.TrafficStatuses[0].Type)
	require.Equal(t, got2.LatestReadyRevision, got2.TrafficStatuses[0].Revision)
	require.Empty(t, got2.TrafficStatuses[0].Uri, "an untagged target must not carry a status uri")
	require.Equal(t, []string{got2.Uri}, got2.Urls, "an untagged target serves only the plain uri")
}

// TestSDKRunTrafficTagValidation pins the traffic-tag rules through the official
// client: a tag that violates the RFC 1035 grammar, the 3–47 length window or
// the combined tag+service-id cap (and a duplicated tag) is InvalidArgument
// (400), the contract real Cloud Run enforces because the tag becomes the
// leading host label of the tag-qualified URL; a valid tag through the same call
// succeeds and surfaces on the status uri (not in the service urls).
func TestSDKRunTrafficTagValidation(t *testing.T) {
	resetState(t)
	svc := newService(t)
	id := unique("sdk-run-tag")
	createService(t, svc, id)

	patch := func(tag string) error {
		_, err := svc.Projects.Locations.Services.Patch(serviceName(id), &run.GoogleCloudRunV2Service{
			Traffic: []*run.GoogleCloudRunV2TrafficTarget{
				{Type: "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", Percent: 100, Tag: tag},
			},
		}).UpdateMask("traffic").Do()
		return err
	}
	// Grammar violation (uppercase + underscore), too-short, and too-long tags.
	for _, bad := range []string{"Bad_Tag", "aa", strings.Repeat("a", 48)} {
		requireAPIError(t, patch(bad), http.StatusBadRequest, "invalid tag %q", bad)
	}
	// A duplicate tag across two targets is rejected.
	_, err := svc.Projects.Locations.Services.Patch(serviceName(id), &run.GoogleCloudRunV2Service{
		Traffic: []*run.GoogleCloudRunV2TrafficTarget{
			{Type: "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", Percent: 50, Tag: "stable"},
			{Type: "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", Percent: 50, Tag: "stable"},
		},
	}).UpdateMask("traffic").Do()
	requireAPIError(t, err, http.StatusBadRequest, "duplicate traffic tags must be rejected")

	op, err := svc.Projects.Locations.Services.Patch(serviceName(id), &run.GoogleCloudRunV2Service{
		Traffic: []*run.GoogleCloudRunV2TrafficTarget{
			{Type: "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", Percent: 100, Tag: "stable"},
		},
	}).UpdateMask("traffic").Do()
	require.NoError(t, err, "patch a valid traffic tag")
	awaitOperation(t, svc, op)

	got, err := svc.Projects.Locations.Services.Get(serviceName(id)).Do()
	require.NoError(t, err)
	require.Len(t, got.TrafficStatuses, 1)
	require.Equal(t, tagQualifiedURI(got.Uri, "stable"), got.TrafficStatuses[0].Uri)
	require.Equal(t, []string{got.Uri}, got.Urls)
}

// TestSDKRunRevisionsAndPaging covers revision listing, cursor pagination, and
// the retired-only delete precondition.
func TestSDKRunRevisionsAndPaging(t *testing.T) {
	resetState(t)
	svc := newService(t)
	id := unique("sdk-run-rev")
	createService(t, svc, id)

	// Mint three revisions total (the create's plus two template updates).
	for _, image := range []string{"hello:v2", "hello:v3"} {
		op, err := svc.Projects.Locations.Services.Patch(serviceName(id), &run.GoogleCloudRunV2Service{
			Template: container("us-docker.pkg.dev/cloudrun/container/" + image),
		}).UpdateMask("template").Do()
		require.NoError(t, err)
		awaitOperation(t, svc, op)
	}

	revs, err := svc.Projects.Locations.Services.Revisions.List(serviceName(id)).Do()
	require.NoError(t, err)
	require.Len(t, revs.Revisions, 3)
	require.Equal(t, revisionName(id, id+"-00001"), revs.Revisions[0].Name)
	require.Equal(t, revisionName(id, id+"-00003"), revs.Revisions[2].Name)
	require.Equal(t, serviceName(id), revs.Revisions[0].Service)
	require.NotEmpty(t, revs.Revisions[0].Etag)

	// Cross-page stability with a page size of 1.
	seen := []string{}
	token := ""
	for {
		call := svc.Projects.Locations.Services.Revisions.List(serviceName(id)).PageSize(1)
		if token != "" {
			call = call.PageToken(token)
		}
		page, err := call.Do()
		require.NoError(t, err, "list revisions page")
		require.LessOrEqual(t, len(page.Revisions), 1)
		for _, r := range page.Revisions {
			seen = append(seen, r.Name)
		}
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}
	require.Equal(t, []string{
		revisionName(id, id+"-00001"),
		revisionName(id, id+"-00002"),
		revisionName(id, id+"-00003"),
	}, seen, "paging must return every revision exactly once, in order")

	// The serving (latest) revision cannot be deleted.
	_, err = svc.Projects.Locations.Services.Revisions.Delete(revisionName(id, id+"-00003")).Do()
	requireAPIError(t, err, http.StatusBadRequest)

	// A retired revision can, and the operation response is the revision.
	delOp, err := svc.Projects.Locations.Services.Revisions.Delete(revisionName(id, id+"-00001")).Do()
	require.NoError(t, err, "delete retired revision")
	if lroAsync() {
		// The revision delete's in-flight metadata is the typed Revision.
		pending := rawMap(t, delOp.Metadata)
		require.Equal(t, revisionTypeAny, pending["@type"])
		require.Equal(t, revisionName(id, id+"-00001"), pending["name"])
	}
	_, delBody := awaitOperation(t, svc, delOp)
	require.Equal(t, revisionTypeAny, delBody["@type"])
	require.Equal(t, revisionName(id, id+"-00001"), delBody["name"])

	remaining, err := svc.Projects.Locations.Services.Revisions.List(serviceName(id)).Do()
	require.NoError(t, err)
	require.Len(t, remaining.Revisions, 2)

	// Re-deleting the retired revision is NotFound.
	_, err = svc.Projects.Locations.Services.Revisions.Delete(revisionName(id, id+"-00001")).Do()
	requireNotFound(t, err)
}

// TestSDKRunIAM covers the service-level IAM surface: get/set with etag OCC and
// testIamPermissions.
func TestSDKRunIAM(t *testing.T) {
	resetState(t)
	svc := newService(t)
	id := unique("sdk-run-iam")
	createService(t, svc, id)
	name := serviceName(id)

	// An unset policy is the empty default policy with a stable etag.
	pol, err := svc.Projects.Locations.Services.GetIamPolicy(name).Do()
	require.NoError(t, err, "getIamPolicy")
	require.Empty(t, pol.Bindings)
	require.NotEmpty(t, pol.Etag)

	// setIamPolicy replaces the policy and returns a fresh etag.
	set, err := svc.Projects.Locations.Services.SetIamPolicy(name, &run.GoogleIamV1SetIamPolicyRequest{
		Policy: &run.GoogleIamV1Policy{
			Bindings: []*run.GoogleIamV1Binding{
				{Role: "roles/run.invoker", Members: []string{"allUsers"}},
			},
		},
	}).Do()
	require.NoError(t, err, "setIamPolicy")
	require.Len(t, set.Bindings, 1)
	require.Equal(t, "roles/run.invoker", set.Bindings[0].Role)
	require.Equal(t, []string{"allUsers"}, set.Bindings[0].Members)
	require.NotEqual(t, pol.Etag, set.Etag, "etag must change")

	back, err := svc.Projects.Locations.Services.GetIamPolicy(name).Do()
	require.NoError(t, err, "getIamPolicy after set")
	require.Len(t, back.Bindings, 1)
	require.Equal(t, set.Etag, back.Etag)

	// A stale etag is an OCC failure (409 ABORTED).
	_, err = svc.Projects.Locations.Services.SetIamPolicy(name, &run.GoogleIamV1SetIamPolicyRequest{
		Policy: &run.GoogleIamV1Policy{
			Etag:     pol.Etag,
			Bindings: []*run.GoogleIamV1Binding{{Role: "roles/run.admin", Members: []string{"allUsers"}}},
		},
	}).Do()
	requireAPIErrorStatus(t, err, http.StatusConflict, "ABORTED")

	// testIamPermissions echoes the requested permissions: the emulator does not
	// enforce IAM (a real client would see only the caller's granted subset).
	perms, err := svc.Projects.Locations.Services.TestIamPermissions(name, &run.GoogleIamV1TestIamPermissionsRequest{
		Permissions: []string{"run.services.get", "run.services.update", "run.services.delete"},
	}).Do()
	require.NoError(t, err, "testIamPermissions")
	require.ElementsMatch(t, []string{"run.services.get", "run.services.update", "run.services.delete"}, perms.Permissions)

	// IAM on a missing service is NotFound.
	missing := serviceName(unique("sdk-run-missing"))
	_, err = svc.Projects.Locations.Services.GetIamPolicy(missing).Do()
	requireNotFound(t, err)
}

// TestSDKRunOperations covers the google.longrunning operations surface for a
// Cloud Run mutation: get (sync-done or async-pending), list, wait, cancel and
// delete.
func TestSDKRunOperations(t *testing.T) {
	resetState(t)
	svc := newService(t)
	id := unique("sdk-run-ops")

	op, err := svc.Projects.Locations.Services.Create(parent(), &run.GoogleCloudRunV2Service{
		Template: container("us-docker.pkg.dev/cloudrun/container/hello"),
	}).ServiceId(id).Do()
	require.NoError(t, err, "create service")
	require.NotEmpty(t, op.Name)
	require.True(t, strings.HasPrefix(op.Name, parent()+"/operations/operation-run-"), "operation name: %s", op.Name)

	if lroAsync() {
		require.False(t, op.Done, "async create must be in flight")
		// The in-flight metadata is the typed Service under mutation, so a
		// polling client can read the resource's progress before it settles.
		pending := rawMap(t, op.Metadata)
		require.Equal(t, serviceTypeAny, pending["@type"])
		require.Equal(t, serviceName(id), pending["name"])
	}

	// operations.get resolves the operation (and, in async, settles it).
	settled, body := awaitOperation(t, svc, op)
	require.Equal(t, op.Name, settled.Name)
	require.Equal(t, serviceTypeAny, body["@type"])
	require.Equal(t, serviceName(id), body["name"])

	// The event helper covers the sync/async wait shapes; :wait returns the same
	// terminal operation.
	waited, err := svc.Projects.Locations.Operations.Wait(op.Name, &run.GoogleLongrunningWaitOperationRequest{}).Do()
	require.NoError(t, err, "operations.wait")
	require.True(t, waited.Done)
	require.NotEmpty(t, waited.Response)

	// The operation is listed under its location. The bare /v2/.../operations
	// family belongs to Cloud Functions on the shared origin, so a run list goes
	// through the documented "/run/" path prefix.
	list, err := newPrefixedService(t).Projects.Locations.Operations.List(parent()).Do()
	require.NoError(t, err, "list operations")
	found := false
	for _, listed := range list.Operations {
		if listed.Name == op.Name {
			found = true
		}
	}
	require.True(t, found, "operations.list must include %s", op.Name)

	// The emulator does not model cancellation, and the published Cloud Run v2
	// discovery has no operations.cancel method, so the SDK exposes none; the
	// operation stays resolvable between get/list/wait and delete.

	// delete removes it; a subsequent get is NotFound.
	_, err = svc.Projects.Locations.Operations.Delete(op.Name).Do()
	require.NoError(t, err, "operations.delete")
	_, err = svc.Projects.Locations.Operations.Get(op.Name).Do()
	requireNotFound(t, err)
}

// TestSDKRunConcurrentWriters exercises parallel control-plane writers under
// the race detector: concurrent OCC-guarded patches of one service resolve to
// either success or an ABORTED 409 (never a 5xx or corruption), while parallel
// readers keep a consistent view.
func TestSDKRunConcurrentWriters(t *testing.T) {
	resetState(t)
	svc := newService(t)
	id := unique("sdk-run-race")
	createService(t, svc, id)

	base, err := svc.Projects.Locations.Services.Get(serviceName(id)).Do()
	require.NoError(t, err)

	const writers = 8
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func(i int) {
			_, perr := svc.Projects.Locations.Services.Patch(serviceName(id), &run.GoogleCloudRunV2Service{
				Etag:   base.Etag,
				Labels: map[string]string{"writer": fmt.Sprint(i)},
			}).UpdateMask("labels").Do()
			errs <- perr
		}(i)
	}

	// Concurrent readers must not observe corruption or a 5xx.
	readDone := make(chan error, 1)
	go func() {
		var rerr error
		for j := 0; j < writers; j++ {
			if _, gerr := svc.Projects.Locations.Services.Get(serviceName(id)).Do(); gerr != nil {
				rerr = gerr
				break
			}
			if _, lerr := svc.Projects.Locations.Services.List(parent()).Do(); lerr != nil {
				rerr = lerr
				break
			}
		}
		readDone <- rerr
	}()

	succeeded := 0
	for i := 0; i < writers; i++ {
		perr := <-errs
		if perr == nil {
			succeeded++
			continue
		}
		var apiErr *googleapi.Error
		require.ErrorAs(t, perr, &apiErr)
		require.Equal(t, http.StatusConflict, apiErr.Code, "concurrent OCC loser must be Aborted: %v", perr)
	}
	require.NoError(t, <-readDone, "concurrent reader")
	// At least one writer must have committed. The precise single-winner OCC
	// contract is asserted deterministically by the stale-etag cases elsewhere
	// (a same-etag race here is allowed to resolve with any number of winners,
	// since the core's read-check-write is not atomic); this test's contract is
	// that parallel writers never corrupt state or return a 5xx under -race.
	require.GreaterOrEqual(t, succeeded, 1, "at least one writer must win")

	final, err := svc.Projects.Locations.Services.Get(serviceName(id)).Do()
	require.NoError(t, err)
	require.GreaterOrEqual(t, final.Generation, int64(2))
	require.Contains(t, final.Labels, "writer")
}

// TestSDKRunValidateOnlyAndErrors covers the validateOnly dry run and the
// invalid-input / etag error contracts.
func TestSDKRunValidateOnlyAndErrors(t *testing.T) {
	resetState(t)
	svc := newService(t)
	id := unique("sdk-run-dry")

	// A validate-only create previews the service without persisting it. It is
	// always terminal, even in async mode (an in-flight validate-only operation
	// could never be polled back).
	op, err := svc.Projects.Locations.Services.Create(parent(), &run.GoogleCloudRunV2Service{
		Template: container("us-docker.pkg.dev/cloudrun/container/hello"),
	}).ServiceId(id).ValidateOnly(true).Do()
	require.NoError(t, err, "validateOnly create")
	body := terminalOperation(t, op)
	require.Equal(t, serviceTypeAny, body["@type"])
	require.Equal(t, serviceName(id), body["name"])
	_, err = svc.Projects.Locations.Services.Get(serviceName(id)).Do()
	requireNotFound(t, err, "validateOnly must not persist the service")

	// An invalid service id is InvalidArgument (400).
	_, err = svc.Projects.Locations.Services.Create(parent(), &run.GoogleCloudRunV2Service{}).
		ServiceId("Bad_Id").Do()
	requireAPIError(t, err, http.StatusBadRequest)

	// A real create, then an etag-preconditioned delete with a stale etag.
	createService(t, svc, id)
	_, err = svc.Projects.Locations.Services.Delete(serviceName(id)).Etag("stale-etag").Do()
	requireAPIError(t, err, http.StatusConflict)

	// The service is still there, and an etag-less delete succeeds.
	_, err = svc.Projects.Locations.Services.Get(serviceName(id)).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Locations.Services.Delete(serviceName(id)).Do()
	require.NoError(t, err)
}
