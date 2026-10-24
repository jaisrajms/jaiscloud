// depth_test.go closes DC5 (plan_docs/gcp-depth-closure-wave-plan.md W2.3) for
// Cloud Scheduler: the two cells its record left exempt — p2 (concurrent job
// create/delete and concurrent dispatch) and p5 (a many-jobs scale walk plus the
// documented dispatch-configuration boundaries) — asserted through the official
// apiary client and the shared depth-test kit (tests/integration/gcp/testdepth).
//
// The contention arbiter is the store, not the client: CreateJob is a
// check-then-insert under the store lock, so a same-name create race has exactly
// one winner and every loser observes the documented ALREADY_EXISTS/409;
// DeleteJob returns the documented NOT_FOUND/404 for a job that is already gone,
// so a concurrent delete of one job is a one-winner race whose losers see 404
// (the emulator's delete is NOT idempotent — see the delete case below). Concurrent
// dispatch (Jobs.run) delivers every attempt synchronously and folds each result
// into the job under UpdateJobAtomic, so no attempt's LastAttemptTime is lost.
//
// p5 is the boundary the class names: a twenty-five-job collection walked with a
// small page size (the cursor chain over a large collection, not one response),
// plus the documented dispatch-configuration caps — retryCount and maxDoublings
// in 0..5 and an HTTP attemptDeadline of 15s..30m. Beyond those caps is a 400
// INVALID_ARGUMENT, asserted as the p5 dispatch boundary.
//
// The concurrency cases run under -race (the module's gate adds it); a contention
// assertion in a binary without the race detector is a failure
// (testdepth.AssertRaceEnabled), not a silent downgrade.
package sdk_scheduler_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	scheduler "google.golang.org/api/cloudscheduler/v1"
	testdepth "jaiscloud-gcp-testdepth"
)

// lostWithHTTP returns a contention predicate matching a loser that observed the
// documented HTTP status. The scheduler core answers a duplicate create with
// ALREADY_EXISTS/409 and a delete of an absent job with NOT_FOUND/404.
func lostWithHTTP(status int) func(error) bool {
	return func(err error) bool {
		obs, ok := testdepth.StatusOf(err)
		return ok && obs.Kind == testdepth.KindHTTP && obs.HTTPStatus == status
	}
}

// walkJobNames walks projects.locations.jobs.list through the kit and returns the
// full job names in walk order. The Discovery document specifies no order, so the
// walk is compared as a set (OrderUnspecified) and the server fills every
// non-terminal page (RequireNonEmptyPages).
func walkJobNames(t *testing.T, svc *scheduler.Service, parent string, cfg testdepth.Paging[*scheduler.Job]) []string {
	t.Helper()
	jobs := testdepth.AssertPaging(t, func() testdepth.Lister[*scheduler.Job] {
		return func(req testdepth.ListRequest) (testdepth.Page[*scheduler.Job], error) {
			call := svc.Projects.Locations.Jobs.List(parent)
			if req.PageSize > 0 {
				call = call.PageSize(req.PageSize)
			}
			if req.PageToken != "" {
				call = call.PageToken(req.PageToken)
			}
			resp, err := call.Do()
			if err != nil {
				return testdepth.Page[*scheduler.Job]{}, err
			}
			return testdepth.Page[*scheduler.Job]{Items: resp.Jobs, NextPageToken: resp.NextPageToken}, nil
		}
	}, cfg)
	return testdepth.Names(jobs, cfg)
}

// TestSDKSchedulerDepthConcurrency is scheduler p2: concurrent job mutations
// under -race. A same-name create has exactly one winner (every loser is
// ALREADY_EXISTS/409), distinct creates all land, a concurrent delete of one job
// has one winner (every loser is NOT_FOUND/404 — the documented error for a job
// that is already gone, so delete is not idempotent), and concurrent dispatch
// delivers every attempt without losing one.
func TestSDKSchedulerDepthConcurrency(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()
	testdepth.AssertRaceEnabled(t)

	newHTTPJob := func(id string) *scheduler.Job {
		return &scheduler.Job{
			Name:       parent + "/jobs/" + id,
			Schedule:   "* * * * *",
			TimeZone:   "UTC",
			HttpTarget: &scheduler.HttpTarget{Uri: "http://example.com/hook", HttpMethod: "POST"},
		}
	}

	t.Run("SameNameCreateRace", func(t *testing.T) {
		id := unique("race")
		errs := testdepth.Race(6, func(int) error {
			_, err := svc.Projects.Locations.Jobs.Create(parent, newHTTPJob(id)).Do()
			return err
		})
		testdepth.AssertOneWinner(t, errs, lostWithHTTP(http.StatusConflict))

		got, err := svc.Projects.Locations.Jobs.Get(parent + "/jobs/" + id).Do()
		require.NoError(t, err)
		require.Equal(t, "ENABLED", got.State)
	})

	t.Run("DistinctCreatesAllLand", func(t *testing.T) {
		const creators = 8
		ids := make([]string, creators)
		for i := range ids {
			ids[i] = unique("dist")
		}
		errs := testdepth.Race(creators, func(i int) error {
			_, err := svc.Projects.Locations.Jobs.Create(parent, newHTTPJob(ids[i])).Do()
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "create %d", i)
		}
		for i, id := range ids {
			got, err := svc.Projects.Locations.Jobs.Get(parent + "/jobs/" + id).Do()
			require.NoError(t, err, "get %d", i)
			require.Equal(t, parent+"/jobs/"+id, got.Name)
		}
	})

	t.Run("ConcurrentDeleteOneWinner", func(t *testing.T) {
		id := unique("del")
		mustCreateJob(t, svc, parent, newHTTPJob(id))

		errs := testdepth.Race(6, func(int) error {
			_, err := svc.Projects.Locations.Jobs.Delete(parent + "/jobs/" + id).Do()
			return err
		})
		testdepth.AssertOneWinner(t, errs, lostWithHTTP(http.StatusNotFound))

		_, err := svc.Projects.Locations.Jobs.Get(parent + "/jobs/" + id).Do()
		require.Equal(t, http.StatusNotFound, apiErr(t, err).Code, "the job must be gone after the winning delete")
	})

	t.Run("ConcurrentDispatchDeliversEveryAttempt", func(t *testing.T) {
		h := newHook()
		srv := httptest.NewServer(h)
		defer srv.Close()

		id := unique("dispatch")
		mustCreateJob(t, svc, parent, &scheduler.Job{
			Name:       parent + "/jobs/" + id,
			Schedule:   "* * * * *",
			TimeZone:   "UTC",
			HttpTarget: &scheduler.HttpTarget{Uri: srv.URL, HttpMethod: "POST"},
		})

		const dispatchers = 6
		errs := testdepth.Race(dispatchers, func(int) error {
			_, err := svc.Projects.Locations.Jobs.Run(parent+"/jobs/"+id, &scheduler.RunJobRequest{}).Do()
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "dispatch %d", i)
		}

		require.Eventually(t, func() bool { return h.count() >= dispatchers }, 5*time.Second, 20*time.Millisecond,
			"every concurrent dispatch must reach the target")
		got, err := svc.Projects.Locations.Jobs.Get(parent + "/jobs/" + id).Do()
		require.NoError(t, err)
		require.NotEmpty(t, got.LastAttemptTime, "a dispatch must stamp the job's last attempt time")
	})
}

// TestSDKSchedulerDepthScale is scheduler p5: a many-jobs boundary. Twenty-five
// jobs are created in one location and then walked with a small page size, so the
// boundary is the cursor chain over a large collection rather than a single
// response.
func TestSDKSchedulerDepthScale(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	const jobs = 25
	want := make([]string, 0, jobs)
	for i := 0; i < jobs; i++ {
		id := unique("scale")
		mustCreateJob(t, svc, parent, &scheduler.Job{
			Name:       parent + "/jobs/" + id,
			Schedule:   "* * * * *",
			TimeZone:   "UTC",
			HttpTarget: &scheduler.HttpTarget{Uri: "http://example.com/hook", HttpMethod: "POST"},
		})
		want = append(want, parent+"/jobs/"+id)
	}

	got := walkJobNames(t, svc, parent, testdepth.Paging[*scheduler.Job]{
		Name:                 func(j *scheduler.Job) string { return j.Name },
		PageSize:             4,
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true,
		Expect:               want,
	})
	require.Subset(t, got, want, "every one of the %d jobs must survive the page walk", jobs)
}

// TestSDKSchedulerDepthDispatchBoundary is the dispatch half of scheduler p5:
// the documented dispatch-configuration caps reject an out-of-range retry or
// attempt deadline with INVALID_ARGUMENT/400 rather than accepting a job the
// engine cannot honor.
func TestSDKSchedulerDepthDispatchBoundary(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	httpJob := func(id string) *scheduler.Job {
		return &scheduler.Job{
			Name:       parent + "/jobs/" + id,
			Schedule:   "* * * * *",
			TimeZone:   "UTC",
			HttpTarget: &scheduler.HttpTarget{Uri: "http://example.com/hook", HttpMethod: "POST"},
		}
	}

	testdepth.RunNegatives(t, []testdepth.NegativeCase{
		{
			Name: "RetryCountAboveMax",
			Run: func() error {
				j := httpJob(unique("retryhi"))
				j.RetryConfig = &scheduler.RetryConfig{RetryCount: 6}
				_, err := svc.Projects.Locations.Jobs.Create(parent, j).Do()
				return err
			},
			Want: testdepth.SpecInvalidArgument(),
		},
		{
			Name: "MaxDoublingsAboveMax",
			Run: func() error {
				j := httpJob(unique("doubhi"))
				j.RetryConfig = &scheduler.RetryConfig{RetryCount: 1, MaxDoublings: 6}
				_, err := svc.Projects.Locations.Jobs.Create(parent, j).Do()
				return err
			},
			Want: testdepth.SpecInvalidArgument(),
		},
		{
			Name: "AttemptDeadlineBelowMin",
			Run: func() error {
				j := httpJob(unique("deadline"))
				j.AttemptDeadline = "10s"
				_, err := svc.Projects.Locations.Jobs.Create(parent, j).Do()
				return err
			},
			Want: testdepth.SpecInvalidArgument(),
		},
	})
}

// TestSDKSchedulerDepthNegativePageSize pins AIP-158 on the REST surface through
// the official apiary client: a negative pageSize is INVALID_ARGUMENT/400 rather
// than a silent fall-back to the documented 500-job maximum.
func TestSDKSchedulerDepthNegativePageSize(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()
	parent := "projects/" + project + "/locations/" + locationID()

	testdepth.RunNegatives(t, []testdepth.NegativeCase{
		{
			Name: "JobsListNegativePageSize",
			Run: func() error {
				_, err := svc.Projects.Locations.Jobs.List(parent).PageSize(-1).Do()
				return err
			},
			Want: testdepth.SpecInvalidArgument(),
		},
	})

	// The documented maximum itself stays legal (coercion only applies above it).
	list, err := svc.Projects.Locations.Jobs.List(parent).PageSize(500).Do()
	require.NoError(t, err)
	require.NotNil(t, list)

	// A non-integer pageSize fails the int32 query binding with 400 — the
	// apiary client cannot emit one (PageSize is an int64), so pin it over raw
	// HTTP against the same REST surface.
	for _, v := range []string{"abc", "-0.5", "1.5"} {
		status, body := httpDo(t, http.MethodGet, "/v1/"+parent+"/jobs?pageSize="+v, nil)
		require.Equal(t, http.StatusBadRequest, status, "pageSize=%s body=%s", v, body)
		require.Contains(t, string(body), "INVALID_ARGUMENT")
	}
}
