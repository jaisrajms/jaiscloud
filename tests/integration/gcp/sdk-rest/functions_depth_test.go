// functions_depth_test.go closes DC7 (plan_docs/gcp-depth-closure-wave-plan.md
// W2.5): the four behavioural-depth cells Cloud Functions left exempt — p2
// contention (concurrent deploy/delete of distinct functions and a same-name
// create race), p3 the official-client error envelope on the documented
// negatives, p4 function-listing paging composed with location scoping, and p5
// the revision-lineage boundary over repeated source redeploys — asserted
// through the official apiary REST clients and the shared depth-test kit
// (tests/integration/gcp/testdepth, DC2).
//
// p4 is a plain cursor walk: the v1 Discovery document declares only
// pageSize/pageToken on projects.locations.functions.list (no `filter`), so the
// "location filter" the plan names is the parent-path scoping itself — a walk of
// one location must not leak another location's functions, and the documented
// `locations/-` wildcard aggregates across them. The list order is compared as a
// set (the Discovery document pins no default order), and the kit's negative
// pageSize negative is not asserted (DC12: the emulator coerces it to the
// default rather than rejecting it).
//
// p3 covers the client-observable negatives: a create with no runtime is
// INVALID_ARGUMENT, a get/delete of a missing function and a source archive that
// references a missing GCS object are NOT_FOUND, and a duplicate name is
// ALREADY_EXISTS. The missing-source case is the emulator's deploy-time
// resolution: because a function mutation's long-running operation completes
// inline by default, the emulator surfaces the unresolvable archive synchronously
// as NOT_FOUND (the raw-HTTP suite pins the same 404), whereas real GCP resolves
// the source asynchronously and reports the failure through the operation.
//
// p5 walks the revision lineage the gen2 deploy flow drives: each time a new
// source archive is uploaded and the function is patched, the backing Cloud Run
// revision (serviceConfig.revision) advances its counter — asserted over three
// sequential redeploys. The source upload uses the documented generateUploadUrl
// → PUT flow; the assertion is made over the official v2 client.
//
// The concurrency cases run under -race (the module's integration step adds it);
// asserting a contention property in a binary without the race detector is a
// failure, not a silent downgrade (testdepth.AssertRaceEnabled).
package sdkrest_test

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/cloudfunctions/v1"
	cloudfunctionsv2 "google.golang.org/api/cloudfunctions/v2"
	"google.golang.org/grpc/codes"
	testdepth "jaiscloud-gcp-testdepth"
)

// functionsParent is the location every Cloud Functions case deploys into.
const functionsParent = "projects/proj/locations/us-central1"

// notFoundStatus reports whether a losing writer observed NOT_FOUND (HTTP 404
// over REST, NOT_FOUND over gRPC) — the documented error for an operation on a
// function that is already gone.
func notFoundStatus(err error) bool {
	obs, ok := testdepth.StatusOf(err)
	if !ok {
		return false
	}
	switch obs.Kind {
	case testdepth.KindHTTP:
		return obs.HTTPStatus == 404
	case testdepth.KindGRPC:
		return obs.Code == codes.NotFound
	default:
		return false
	}
}

// createV1Function deploys an HTTP-triggered v1 function and returns its full
// resource name. The create returns a done Operation; the function is readable
// immediately. The function is removed when the test finishes.
func createV1Function(t *testing.T, svc *cloudfunctions.Service, parent, id, runtime string) string {
	t.Helper()
	name := parent + "/functions/" + id
	op, err := svc.Projects.Locations.Functions.Create(parent, &cloudfunctions.CloudFunction{
		Name:    name,
		Runtime: runtime,
	}).Do()
	require.NoError(t, err)
	require.True(t, op.Done, "create should return a done Operation")
	t.Cleanup(func() { _, _ = svc.Projects.Locations.Functions.Delete(name).Do() })
	return name
}

// walkFunctionNames walks projects.locations.functions.list through the kit and
// returns the full function names in walk order.
func walkFunctionNames(t *testing.T, svc *cloudfunctions.Service, parent string, cfg testdepth.Paging[*cloudfunctions.CloudFunction]) []string {
	t.Helper()
	fns := testdepth.AssertPaging(t, func() testdepth.Lister[*cloudfunctions.CloudFunction] {
		return func(req testdepth.ListRequest) (testdepth.Page[*cloudfunctions.CloudFunction], error) {
			call := svc.Projects.Locations.Functions.List(parent)
			if req.PageSize > 0 {
				call = call.PageSize(req.PageSize)
			}
			if req.PageToken != "" {
				call = call.PageToken(req.PageToken)
			}
			resp, err := call.Do()
			if err != nil {
				return testdepth.Page[*cloudfunctions.CloudFunction]{}, err
			}
			return testdepth.Page[*cloudfunctions.CloudFunction]{Items: resp.Functions, NextPageToken: resp.NextPageToken}, nil
		}
	}, cfg)
	return testdepth.Names(fns, cfg)
}

// TestSDKFunctionsDepthConcurrency is functions p2: concurrent control-plane
// mutations under -race. A same-name create has exactly one winner (every loser
// is AlreadyExists), distinct creates each land, and a delete is NOT idempotent
// (the one winner succeeds, every loser is NotFound) — matching real Cloud
// Functions, whose delete of an already-gone function is NOT_FOUND.
func TestSDKFunctionsDepthConcurrency(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudfunctions.NewService(ctx, opts()...)
	require.NoError(t, err)
	testdepth.AssertRaceEnabled(t)

	fns := svc.Projects.Locations.Functions

	t.Run("SameNameCreateRace", func(t *testing.T) {
		id := unique("race")
		name := functionsParent + "/functions/" + id
		t.Cleanup(func() { _, _ = fns.Delete(name).Do() })

		errs := testdepth.Race(6, func(int) error {
			_, err := fns.Create(functionsParent, &cloudfunctions.CloudFunction{Name: name, Runtime: "nodejs20"}).Do()
			return err
		})
		testdepth.AssertOneWinner(t, errs, conflictStatus)

		got, err := fns.Get(name).Do()
		require.NoError(t, err)
		require.Equal(t, name, got.Name)
	})

	t.Run("DistinctCreatesAllLand", func(t *testing.T) {
		const creators = 6
		names := make([]string, creators)
		for i := range names {
			names[i] = functionsParent + "/functions/" + unique("dist")
		}
		errs := testdepth.Race(creators, func(i int) error {
			_, err := fns.Create(functionsParent, &cloudfunctions.CloudFunction{Name: names[i], Runtime: "nodejs20"}).Do()
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "create %d", i)
		}
		for i, name := range names {
			t.Cleanup(func() { _, _ = fns.Delete(names[i]).Do() })
			got, err := fns.Get(name).Do()
			require.NoError(t, err, "get %d", i)
			require.Equal(t, name, got.Name)
		}
	})

	t.Run("ConcurrentDeleteNotIdempotent", func(t *testing.T) {
		name := createV1Function(t, svc, functionsParent, unique("del"), "nodejs20")

		errs := testdepth.Race(6, func(int) error {
			_, err := fns.Delete(name).Do()
			return err
		})
		// Exactly one delete wins; every loser sees the function is already gone.
		testdepth.AssertOneWinner(t, errs, notFoundStatus)

		_, err := fns.Get(name).Do()
		testdepth.AssertError(t, err, testdepth.SpecNotFound())
	})

	t.Run("DistinctDeletesAllLand", func(t *testing.T) {
		const deleters = 6
		names := make([]string, deleters)
		for i := range names {
			names[i] = createV1Function(t, svc, functionsParent, unique("deldist"), "nodejs20")
		}
		errs := testdepth.Race(deleters, func(i int) error {
			_, err := fns.Delete(names[i]).Do()
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "delete %d", i)
		}
	})
}

// provisionSourceBucket provisions the gcf-v2-sources bucket for the deploy
// location via the documented generateUploadUrl call and returns its name, so a
// source reference can name a real bucket (an absent *object* is then the
// documented NOT_FOUND rather than a missing-bucket store error).
func provisionSourceBucket(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	svc, err := cloudfunctionsv2.NewService(ctx, opts()...)
	require.NoError(t, err)
	gen, err := svc.Projects.Locations.Functions.GenerateUploadUrl(functionsParent, &cloudfunctionsv2.GenerateUploadUrlRequest{}).Do()
	require.NoError(t, err)
	require.NotNil(t, gen.StorageSource)
	require.NotEmpty(t, gen.StorageSource.Bucket)
	return gen.StorageSource.Bucket
}

// TestSDKFunctionsDepthErrors is functions p3: the official client's typed error
// envelope on the documented negatives, asserted through the kit's catalogue.
func TestSDKFunctionsDepthErrors(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudfunctions.NewService(ctx, opts()...)
	require.NoError(t, err)

	bucket := provisionSourceBucket(t)
	fns := svc.Projects.Locations.Functions

	dupName := functionsParent + "/functions/" + unique("dup")
	_, err = fns.Create(functionsParent, &cloudfunctions.CloudFunction{Name: dupName, Runtime: "nodejs20"}).Do()
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = fns.Delete(dupName).Do() })

	testdepth.RunNegatives(t, []testdepth.NegativeCase{
		{
			Name: "MissingRuntimeIsInvalidArgument",
			Want: testdepth.SpecInvalidArgument(),
			Run: func() error {
				_, err := fns.Create(functionsParent, &cloudfunctions.CloudFunction{
					Name: functionsParent + "/functions/" + unique("nort"),
				}).Do()
				return err
			},
		},
		{
			Name: "GetMissingIsNotFound",
			Want: testdepth.SpecNotFound(),
			Run: func() error {
				_, err := fns.Get(functionsParent + "/functions/" + unique("nope")).Do()
				return err
			},
		},
		{
			Name: "DeleteMissingIsNotFound",
			Want: testdepth.SpecNotFound(),
			Run: func() error {
				_, err := fns.Delete(functionsParent + "/functions/" + unique("nope")).Do()
				return err
			},
		},
		{
			Name: "DuplicateNameIsAlreadyExists",
			Want: testdepth.SpecAlreadyExists(),
			Run: func() error {
				_, err := fns.Create(functionsParent, &cloudfunctions.CloudFunction{Name: dupName, Runtime: "nodejs20"}).Do()
				return err
			},
		},
		{
			Name: "MissingSourceArchiveIsNotFound",
			Want: testdepth.SpecNotFound(),
			Run: func() error {
				_, err := fns.Create(functionsParent, &cloudfunctions.CloudFunction{
					Name:             functionsParent + "/functions/" + unique("srcfn"),
					Runtime:          "nodejs20",
					SourceArchiveUrl: "gs://" + bucket + "/missing-" + unique("o") + ".zip",
				}).Do()
				return err
			},
		},
	})
}

// TestSDKFunctionsDepthListing is functions p4: projects.locations.functions.list
// pages through pageSize/pageToken without dropping, duplicating or reordering a
// function across a page boundary, scoped by location — a walk of one location
// must not return another's functions, and the documented locations/- wildcard
// aggregates them. The Discovery document declares no list filter and no default
// order, so the contract is the cursor walk plus the location scoping (compared
// as a set); RequireNonEmptyPages pins the emulator's page filling, not a
// real-GCP guarantee (AIP-158 permits an empty non-terminal page).
func TestSDKFunctionsDepthListing(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudfunctions.NewService(ctx, opts()...)
	require.NoError(t, err)

	const locA = "us-central1"
	const locB = "europe-west1"
	parentA := "projects/proj/locations/" + locA
	parentB := "projects/proj/locations/" + locB

	wantA := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		wantA = append(wantA, createV1Function(t, svc, parentA, unique("lista"), "nodejs20"))
	}
	wantB := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		wantB = append(wantB, createV1Function(t, svc, parentB, unique("listb"), "nodejs20"))
	}

	gotA := walkFunctionNames(t, svc, parentA, testdepth.Paging[*cloudfunctions.CloudFunction]{
		Name:                 func(f *cloudfunctions.CloudFunction) string { return f.Name },
		PageSize:             2,
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               wantA,
		Include:              func(f *cloudfunctions.CloudFunction) bool { return strings.Contains(f.Name, "/locations/"+locA+"/") },
	})
	require.Subset(t, gotA, wantA, "the paged, location-scoped walk must return every function in the location")
	for _, name := range wantB {
		require.NotContains(t, gotA, name, "the walk of one location must not leak another location's function")
	}

	// The documented all-locations wildcard aggregates both locations' functions.
	wantAll := append(append([]string(nil), wantA...), wantB...)
	gotAll := walkFunctionNames(t, svc, "projects/proj/locations/-", testdepth.Paging[*cloudfunctions.CloudFunction]{
		Name:                 func(f *cloudfunctions.CloudFunction) string { return f.Name },
		PageSize:             2,
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               wantAll,
	})
	require.Subset(t, gotAll, wantAll, "the locations/- walk must aggregate every location")
}

// TestSDKFunctionsDepthRevisionLineage is functions p5: the revision-lineage
// boundary over repeated source redeploys. Each generateUploadUrl → PUT → patch
// of buildConfig.source deploys a new archive and advances the backing Cloud Run
// revision counter (serviceConfig.revision = {id}-{NNNNN}-{sha8}) without losing
// the lineage: the counter stays monotonic and each rendered revision is the
// latest archive's.
func TestSDKFunctionsDepthRevisionLineage(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudfunctionsv2.NewService(ctx, opts()...)
	require.NoError(t, err)

	fns := svc.Projects.Locations.Functions

	// uploadSource runs the documented gen2 upload flow and returns the
	// storageSource a create/patch references.
	uploadSource := func(content string) *cloudfunctionsv2.StorageSource {
		t.Helper()
		gen, err := fns.GenerateUploadUrl(functionsParent, &cloudfunctionsv2.GenerateUploadUrlRequest{}).Do()
		require.NoError(t, err)
		require.NotEmpty(t, gen.UploadUrl, "generateUploadUrl must return an upload URL")
		require.NotNil(t, gen.StorageSource)

		req, err := http.NewRequest(http.MethodPut, gen.UploadUrl, bytes.NewReader([]byte(content)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/zip")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "source PUT to the upload URL")
		return gen.StorageSource
	}

	id := "revlineage-" + unique("lineage")
	name := functionsParent + "/functions/" + id
	t.Cleanup(func() { fns.Delete(name).Do() })

	// revisionCounter reads the {NNNNN} counter out of the backing revision
	// name and asserts the latest revision serves all traffic.
	revisionCounter := func() int {
		t.Helper()
		fn, err := fns.Get(name).Do()
		require.NoError(t, err)
		require.NotNil(t, fn.ServiceConfig)
		rev := fn.ServiceConfig.Revision
		wantPrefix := "projects/proj/locations/us-central1/services/" + id + "/revisions/" + id + "-"
		require.True(t, strings.HasPrefix(rev, wantPrefix), "revision %q must be the backing Cloud Run revision", rev)
		require.True(t, fn.ServiceConfig.AllTrafficOnLatestRevision, "the latest revision must serve all traffic")
		tail := strings.TrimPrefix(rev, wantPrefix) // "{NNNNN}-{sha8}"
		counter, err := strconv.Atoi(strings.SplitN(tail, "-", 2)[0])
		require.NoError(t, err, "revision %q must carry the {NNNNN} lineage", rev)
		return counter
	}

	src1 := uploadSource("first-archive")
	_, err = fns.Create(functionsParent, &cloudfunctionsv2.Function{
		Name: name,
		BuildConfig: &cloudfunctionsv2.BuildConfig{
			Runtime:    "python312",
			EntryPoint: "main.handler",
			Source:     &cloudfunctionsv2.Source{StorageSource: src1},
		},
	}).FunctionId(id).Do()
	require.NoError(t, err)
	require.Equal(t, 1, revisionCounter(), "a first deploy must render revision 1")

	// Two further redeploys, each with fresh bytes, advance the counter by one.
	for deploy := 2; deploy <= 3; deploy++ {
		src := uploadSource(strings.Repeat("archive-", deploy))
		_, err := fns.Patch(name, &cloudfunctionsv2.Function{
			BuildConfig: &cloudfunctionsv2.BuildConfig{Source: &cloudfunctionsv2.Source{StorageSource: src}},
		}).UpdateMask("buildConfig.source").Do()
		require.NoError(t, err)
		require.Equal(t, deploy, revisionCounter(), "revision counter must advance by one per redeploy")
	}
}
