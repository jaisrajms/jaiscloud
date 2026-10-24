// Package sdk_dataproc_test — behavioural depth (p2–p5) for the Cloud Dataproc
// v1 surface, driven by the official apiary client against the emulator. Where
// sdk_dataproc_test.go proves the baseline lifecycle (p1), this file exercises
// the behaviour classes docs/gcp/testability.md §10 names:
//
//	p2 concurrency      — parallel submits with the same jobId (exactly one
//	                      winner, losers 409), parallel distinct submits and
//	                      concurrent get/cancel churn on one cluster, asserted
//	                      from a -race test client.
//	p3 errors           — fail-loud unsupported/malformed job types landing on
//	                      ERROR, unknown cluster/job 404, duplicate job 409,
//	                      delete-active-job FAILED_PRECONDITION, invalid
//	                      filter/matcher/pageSize/scheduling 400.
//	p4 listing & paging — listJobs by pageSize/pageToken composed with
//	                      clusterName, jobStateMatcher and the filter grammar
//	                      (filter overrides the matcher), and cluster listing by
//	                      pageSize/status.state/labels filters.
//	p5 scale & boundary — a long-running streaming job that stays RUNNING until
//	                      cancelled, a submitAsOperation LRO that tracks the job
//	                      and closes on cancellation, scheduling counters, and a
//	                      many-job listing across pages.
//
// The suite targets the emulator's mock executor (the CI shape): it asserts the
// control plane and the lazy state machine, not a real Spark driver. The
// real-k8s restart leg of the scheduling contract stays the local
// `make test-dataproc-streaming-k8s` gate (STR3).
//
// Run with the GCP binary running:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -race ./...
package sdk_dataproc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/dataproc/v1"
	"google.golang.org/api/googleapi"
)

const (
	dpProject = "proj"
	dpRegion  = "us-central1"
)

// dpService returns an apiary Dataproc client pointed at the emulator.
func dpService(t *testing.T) *dataproc.Service {
	t.Helper()
	svc, err := dataproc.NewService(context.Background(), opts()...)
	require.NoError(t, err)
	return svc
}

// newCluster creates a cluster and waits for its create LRO to complete (which
// settles the cluster to RUNNING).
func newCluster(t *testing.T, svc *dataproc.Service) string {
	t.Helper()
	name := unique("c")
	op, err := svc.Projects.Regions.Clusters.Create(dpProject, dpRegion, &dataproc.Cluster{
		ProjectId:   dpProject,
		ClusterName: name,
		Config: &dataproc.ClusterConfig{
			GceClusterConfig: &dataproc.GceClusterConfig{ZoneUri: "us-central1-a"},
		},
	}).Do()
	require.NoError(t, err)
	require.True(t, pollOperation(t, svc, op.Name).Done, "cluster create operation")
	return name
}

// submitJob submits a pyspark job to cluster with the supplied mutations applied
// to the Job first (labels, scheduling, another job type, streaming properties).
func submitJob(t *testing.T, svc *dataproc.Service, cluster, jobID string, mut ...func(*dataproc.Job)) *dataproc.Job {
	t.Helper()
	job := &dataproc.Job{
		Reference:  &dataproc.JobReference{ProjectId: dpProject, JobId: jobID},
		Placement:  &dataproc.JobPlacement{ClusterName: cluster},
		PysparkJob: &dataproc.PySparkJob{MainPythonFileUri: "gs://jaiscloud-bucket/main.py"},
	}
	for _, m := range mut {
		m(job)
	}
	j, err := svc.Projects.Regions.Jobs.Submit(dpProject, dpRegion, &dataproc.SubmitJobRequest{Job: job}).Do()
	require.NoError(t, err)
	return j
}

// requireCode asserts err is a *googleapi.Error carrying the given HTTP status.
func requireCode(t *testing.T, err error, code int, what string) {
	t.Helper()
	require.Error(t, err, what)
	var gerr *googleapi.Error
	require.True(t, errors.As(err, &gerr), "%s: expected *googleapi.Error, got %v", what, err)
	require.Equal(t, code, gerr.Code, "%s: %v", what, gerr)
}

// isCode reports whether err is a *googleapi.Error with the given HTTP status.
func isCode(err error, code int) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == code
}

// listJobs drives jobs.list with the optional clusterName/filter/matcher/page
// parameters and returns the page plus the next page token.
func listJobs(t *testing.T, svc *dataproc.Service, cluster, filter, matcher string, pageSize int64, pageToken string) ([]*dataproc.Job, string) {
	t.Helper()
	call := svc.Projects.Regions.Jobs.List(dpProject, dpRegion)
	if cluster != "" {
		call = call.ClusterName(cluster)
	}
	if filter != "" {
		call = call.Filter(filter)
	}
	if matcher != "" {
		call = call.JobStateMatcher(matcher)
	}
	if pageSize != 0 {
		call = call.PageSize(pageSize)
	}
	if pageToken != "" {
		call = call.PageToken(pageToken)
	}
	resp, err := call.Do()
	require.NoError(t, err)
	return resp.Jobs, resp.NextPageToken
}

// jobIDs returns the jobs' ids in id order.
func jobIDs(jobs []*dataproc.Job) []string {
	out := make([]string, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.Reference.JobId)
	}
	sort.Strings(out)
	return out
}

// walkJobPages walks every page of a filtered list and returns the ids seen,
// asserting the walk terminated and produced no duplicate across pages.
func walkJobPages(t *testing.T, svc *dataproc.Service, cluster, filter string, pageSize int64) []string {
	t.Helper()
	var all []string
	token := ""
	for i := 0; ; i++ {
		require.Less(t, i, 50, "too many pages")
		page, next := listJobs(t, svc, cluster, filter, "", pageSize, token)
		all = append(all, jobIDs(page)...)
		if next == "" {
			break
		}
		token = next
	}
	sort.Strings(all)
	uniq := append([]string(nil), all...)
	require.Equal(t, dedupe(uniq), all, "page walk returned a duplicate")
	return all
}

// dedupe returns the sorted unique elements of a sorted slice.
func dedupe(sorted []string) []string {
	if len(sorted) == 0 {
		return nil
	}
	out := []string{sorted[0]}
	for _, s := range sorted[1:] {
		if s != out[len(out)-1] {
			out = append(out, s)
		}
	}
	return out
}

// errCollector gathers goroutine errors for assertions on the test goroutine.
type errCollector struct {
	mu   sync.Mutex
	errs []error
}

func (c *errCollector) add(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
}

func (c *errCollector) requireEmpty(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.Empty(t, c.errs)
}

// TestSDKDataprocErrors covers the p3 class: the real error envelope and codes
// for unsupported/malformed jobs, missing resources, invalid input and
// out-of-range scheduling.
func TestSDKDataprocErrors(t *testing.T) {
	svc := dpService(t)
	cluster := newCluster(t, svc)

	t.Run("UnsupportedJobTypesFailLoud", func(t *testing.T) {
		cases := []struct {
			name string
			mut  func(*dataproc.Job)
		}{
			{"pigJob", func(j *dataproc.Job) {
				j.PysparkJob = nil
				j.PigJob = &dataproc.PigJob{QueryList: &dataproc.QueryList{Queries: []string{"A = LOAD 'x'"}}}
			}},
			{"hiveJob", func(j *dataproc.Job) {
				j.PysparkJob = nil
				j.HiveJob = &dataproc.HiveJob{QueryList: &dataproc.QueryList{Queries: []string{"SELECT 1"}}}
			}},
			{"hadoopJob", func(j *dataproc.Job) {
				j.PysparkJob = nil
				j.HadoopJob = &dataproc.HadoopJob{MainJarFileUri: "gs://jaiscloud-bucket/mr.jar"}
			}},
		}
		for _, tc := range cases {
			got := submitJob(t, svc, cluster, unique("unsup"), tc.mut)
			require.Equal(t, "ERROR", got.Status.State, tc.name)
			require.True(t, got.Done, tc.name)
			require.Contains(t, got.Status.Details, "not supported", tc.name)
		}
	})

	t.Run("MalformedSparkJobIsError", func(t *testing.T) {
		// A sparkJob with neither mainJarFileUri nor mainClass cannot run and
		// must fail loud, never appear to succeed.
		got := submitJob(t, svc, cluster, unique("badspark"), func(j *dataproc.Job) {
			j.PysparkJob = nil
			j.SparkJob = &dataproc.SparkJob{}
		})
		require.Equal(t, "ERROR", got.Status.State)
		require.Contains(t, got.Status.Details, "mainJarFileUri")
	})

	t.Run("UnknownClusterIs404", func(t *testing.T) {
		_, err := svc.Projects.Regions.Jobs.Submit(dpProject, dpRegion, &dataproc.SubmitJobRequest{Job: &dataproc.Job{
			Reference:  &dataproc.JobReference{ProjectId: dpProject, JobId: unique("x")},
			Placement:  &dataproc.JobPlacement{ClusterName: unique("nope")},
			PysparkJob: &dataproc.PySparkJob{MainPythonFileUri: "gs://jaiscloud-bucket/main.py"},
		}}).Do()
		requireCode(t, err, 404, "submit to unknown cluster")
	})

	t.Run("DuplicateJobIdIs409", func(t *testing.T) {
		id := unique("dup")
		submitJob(t, svc, cluster, id)
		_, err := svc.Projects.Regions.Jobs.Submit(dpProject, dpRegion, &dataproc.SubmitJobRequest{Job: &dataproc.Job{
			Reference:  &dataproc.JobReference{ProjectId: dpProject, JobId: id},
			Placement:  &dataproc.JobPlacement{ClusterName: cluster},
			PysparkJob: &dataproc.PySparkJob{MainPythonFileUri: "gs://jaiscloud-bucket/main.py"},
		}}).Do()
		requireCode(t, err, 409, "duplicate jobId")
	})

	t.Run("MissingResourcesAre404", func(t *testing.T) {
		_, err := svc.Projects.Regions.Jobs.Get(dpProject, dpRegion, unique("nope")).Do()
		requireCode(t, err, 404, "get unknown job")

		_, err = svc.Projects.Regions.Jobs.Cancel(dpProject, dpRegion, unique("nope"), &dataproc.CancelJobRequest{}).Do()
		requireCode(t, err, 404, "cancel unknown job")

		_, err = svc.Projects.Regions.Jobs.Delete(dpProject, dpRegion, unique("nope")).Do()
		requireCode(t, err, 404, "delete unknown job")

		_, err = svc.Projects.Regions.Clusters.Get(dpProject, dpRegion, unique("nope")).Do()
		requireCode(t, err, 404, "get unknown cluster")
	})

	t.Run("DeleteActiveJobIsFailedPrecondition", func(t *testing.T) {
		// A freshly submitted job is PENDING (non-terminal), so deleting it must
		// fail with FAILED_PRECONDITION rather than silently drop a live job.
		id := unique("active")
		submitJob(t, svc, cluster, id)
		_, err := svc.Projects.Regions.Jobs.Delete(dpProject, dpRegion, id).Do()
		var gerr *googleapi.Error
		require.True(t, errors.As(err, &gerr), "delete active job: expected *googleapi.Error, got %v", err)
		require.Equal(t, 400, gerr.Code)
		require.Contains(t, gerr.Message, "active")
	})

	t.Run("InvalidListingInputIs400", func(t *testing.T) {
		_, err := svc.Projects.Regions.Jobs.List(dpProject, dpRegion).JobStateMatcher("RUNNING").Do()
		requireCode(t, err, 400, "unknown jobStateMatcher")

		_, err = svc.Projects.Regions.Jobs.List(dpProject, dpRegion).Filter("bogus = 1").Do()
		requireCode(t, err, 400, "malformed jobs filter")

		_, err = svc.Projects.Regions.Clusters.List(dpProject, dpRegion).Filter("bogus = 1").Do()
		requireCode(t, err, 400, "malformed clusters filter")

		_, err = svc.Projects.Regions.Jobs.List(dpProject, dpRegion).PageSize(-1).Do()
		requireCode(t, err, 400, "jobs.list negative pageSize")

		_, err = svc.Projects.Regions.Clusters.List(dpProject, dpRegion).PageSize(-1).Do()
		requireCode(t, err, 400, "clusters.list negative pageSize")
	})

	t.Run("SchedulingOutOfRangeIs400", func(t *testing.T) {
		_, err := svc.Projects.Regions.Jobs.Submit(dpProject, dpRegion, &dataproc.SubmitJobRequest{Job: &dataproc.Job{
			Reference:  &dataproc.JobReference{ProjectId: dpProject, JobId: unique("sched")},
			Placement:  &dataproc.JobPlacement{ClusterName: cluster},
			PysparkJob: &dataproc.PySparkJob{MainPythonFileUri: "gs://jaiscloud-bucket/main.py"},
			Scheduling: &dataproc.JobScheduling{MaxFailuresPerHour: 11},
		}}).Do()
		requireCode(t, err, 400, "maxFailuresPerHour > 10")
	})

	t.Run("ClusterShapeValidationIs400", func(t *testing.T) {
		// config and virtualClusterConfig are mutually exclusive.
		_, err := svc.Projects.Regions.Clusters.Create(dpProject, dpRegion, &dataproc.Cluster{
			ProjectId:            dpProject,
			ClusterName:          unique("both"),
			Config:               &dataproc.ClusterConfig{},
			VirtualClusterConfig: &dataproc.VirtualClusterConfig{},
		}).Do()
		requireCode(t, err, 400, "both config and virtualClusterConfig")

		// a duplicate cluster name is 409.
		name := unique("dupc")
		_, err = svc.Projects.Regions.Clusters.Create(dpProject, dpRegion, &dataproc.Cluster{
			ProjectId: dpProject, ClusterName: name,
		}).Do()
		require.NoError(t, err)
		_, err = svc.Projects.Regions.Clusters.Create(dpProject, dpRegion, &dataproc.Cluster{
			ProjectId: dpProject, ClusterName: name,
		}).Do()
		requireCode(t, err, 409, "duplicate cluster name")
	})
}

// TestSDKDataprocConcurrency covers the p2 class: same-key submit races, parallel
// distinct submits and concurrent get/cancel churn, asserted under -race.
func TestSDKDataprocConcurrency(t *testing.T) {
	svc := dpService(t)
	cluster := newCluster(t, svc)

	t.Run("SameJobIdSubmitRaceHasOneWinner", func(t *testing.T) {
		const workers = 8
		jobID := unique("race")
		var wg sync.WaitGroup
		var mu sync.Mutex
		wins, conflicts := 0, 0
		var others []error
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Build the request per goroutine: the client is shared, the
				// request object must not be.
				_, err := svc.Projects.Regions.Jobs.Submit(dpProject, dpRegion, &dataproc.SubmitJobRequest{Job: &dataproc.Job{
					Reference:  &dataproc.JobReference{ProjectId: dpProject, JobId: jobID},
					Placement:  &dataproc.JobPlacement{ClusterName: cluster},
					PysparkJob: &dataproc.PySparkJob{MainPythonFileUri: "gs://jaiscloud-bucket/main.py"},
				}}).Do()
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					wins++
				case isCode(err, 409):
					conflicts++
				default:
					others = append(others, err)
				}
			}()
		}
		wg.Wait()

		require.Empty(t, others, "unexpected submit errors")
		require.Equal(t, 1, wins, "exactly one submit may win")
		require.Equal(t, workers-1, conflicts, "every loser must be 409")
	})

	t.Run("ParallelSubmitsAndCancelsStayConsistent", func(t *testing.T) {
		const n = 8
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf("%s-%02d", unique("par"), i)
		}
		sort.Strings(ids)

		errs := &errCollector{}
		var wg sync.WaitGroup
		for _, id := range ids {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				_, err := svc.Projects.Regions.Jobs.Submit(dpProject, dpRegion, &dataproc.SubmitJobRequest{Job: &dataproc.Job{
					Reference:  &dataproc.JobReference{ProjectId: dpProject, JobId: id},
					Placement:  &dataproc.JobPlacement{ClusterName: cluster},
					PysparkJob: &dataproc.PySparkJob{MainPythonFileUri: "gs://jaiscloud-bucket/main.py"},
				}}).Do()
				errs.add(err)
			}(id)
		}
		wg.Wait()
		errs.requireEmpty(t)

		// Concurrent readers and cancellers on the same jobs.
		var wg2 sync.WaitGroup
		for _, id := range ids {
			wg2.Add(2)
			go func(id string) {
				defer wg2.Done()
				_, err := svc.Projects.Regions.Jobs.Get(dpProject, dpRegion, id).Do()
				errs.add(err)
			}(id)
			go func(id string) {
				defer wg2.Done()
				_, err := svc.Projects.Regions.Jobs.Cancel(dpProject, dpRegion, id, &dataproc.CancelJobRequest{}).Do()
				errs.add(err)
			}(id)
		}
		wg2.Wait()
		errs.requireEmpty(t)

		// Every job must be terminal (a cancel that lost the race to DONE is a
		// no-op, not an error) and every id must appear exactly once in the
		// cluster listing — a concurrent update must never duplicate a record.
		for _, id := range ids {
			got := pollJob(t, svc, dpProject, dpRegion, id)
			require.Contains(t, []string{"DONE", "CANCELLED"}, got.Status.State, "job %s", id)
		}
		require.Equal(t, ids, sortedUnionOfClusterJobs(t, svc, cluster, ids))
	})
}

// sortedUnionOfClusterJobs lists cluster's jobs and returns the subset of want
// seen exactly once, in id order (a duplicate would make the slices differ).
func sortedUnionOfClusterJobs(t *testing.T, svc *dataproc.Service, cluster string, want []string) []string {
	t.Helper()
	wantSet := map[string]bool{}
	for _, id := range want {
		wantSet[id] = true
	}
	page, _ := listJobs(t, svc, cluster, "", "", 0, "")
	seen := map[string]int{}
	for _, j := range page {
		seen[j.Reference.JobId]++
	}
	out := make([]string, 0, len(want))
	for id := range wantSet {
		require.Equal(t, 1, seen[id], "job %s appears %d times in the listing", id, seen[id])
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// TestSDKDataprocListingPaging covers the p4 class over the list surfaces:
// listJobs by pageSize/pageToken composed with clusterName, jobStateMatcher and
// the filter grammar, and listClusters by pageSize and filter.
func TestSDKDataprocListingPaging(t *testing.T) {
	svc := dpService(t)
	cluster := newCluster(t, svc)
	group := unique("grp")

	// Five jobs on one cluster, all carrying the same label; settle each to a
	// terminal state so the state filters are deterministic.
	ids := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%s-%02d", unique("pg"), i)
		submitJob(t, svc, cluster, id, func(j *dataproc.Job) {
			j.Labels = map[string]string{"group": group}
		})
		require.Equal(t, "DONE", pollJob(t, svc, dpProject, dpRegion, id).Status.State)
		ids = append(ids, id)
	}
	sort.Strings(ids)

	// A second cluster whose jobs must not leak into the first cluster's page
	// walk.
	otherCluster := newCluster(t, svc)
	submitJob(t, svc, otherCluster, unique("other"), func(j *dataproc.Job) {
		j.Labels = map[string]string{"group": group}
	})

	t.Run("ClusterScopedPagingIsStableAcrossPages", func(t *testing.T) {
		first := walkJobPages(t, svc, cluster, "", 2)
		require.Equal(t, ids, first)
		// A second walk must produce the same set (cursor stability).
		require.Equal(t, ids, walkJobPages(t, svc, cluster, "", 2))
	})

	t.Run("FilterOverridesMatcher", func(t *testing.T) {
		// The filter selects NON_ACTIVE (terminal) jobs while the matcher asks
		// for ACTIVE; per the API the filter wins.
		page, _ := listJobs(t, svc, cluster, `status.state = NON_ACTIVE`, "ACTIVE", 0, "")
		require.Equal(t, ids, jobIDs(page))
	})

	t.Run("LabelFilterIsScoped", func(t *testing.T) {
		page, _ := listJobs(t, svc, "", `labels.group = `+group, "", 0, "")
		// The group label is shared with otherCluster's job; the page holds all
		// of them (six), proving the label filter matched both clusters.
		require.Len(t, page, 6)
		for _, j := range page {
			require.Equal(t, group, j.Labels["group"])
		}
	})

	t.Run("MatcherNarrowsByState", func(t *testing.T) {
		page, _ := listJobs(t, svc, cluster, "", "NON_ACTIVE", 0, "")
		require.Equal(t, ids, jobIDs(page))
	})

	t.Run("ClusterListFiltersAndPaging", func(t *testing.T) {
		cgroup := unique("cgrp")
		names := make([]string, 0, 4)
		for i := 0; i < 4; i++ {
			name := fmt.Sprintf("%s-%02d", unique("lc"), i)
			_, err := svc.Projects.Regions.Clusters.Create(dpProject, dpRegion, &dataproc.Cluster{
				ProjectId: dpProject, ClusterName: name,
				Labels: map[string]string{"group": cgroup},
			}).Do()
			require.NoError(t, err)
			names = append(names, name)
		}
		sort.Strings(names)

		// Filter + paging: walk every page of the label-scoped set.
		var got []string
		token := ""
		for i := 0; ; i++ {
			require.Less(t, i, 20, "too many cluster pages")
			resp, err := svc.Projects.Regions.Clusters.List(dpProject, dpRegion).
				Filter(`labels.group = ` + cgroup).PageSize(2).PageToken(token).Do()
			require.NoError(t, err)
			for _, c := range resp.Clusters {
				got = append(got, c.ClusterName)
			}
			if resp.NextPageToken == "" {
				break
			}
			token = resp.NextPageToken
		}
		sort.Strings(got)
		require.Equal(t, names, got)

		// status.state filters: freshly created clusters are ACTIVE.
		resp, err := svc.Projects.Regions.Clusters.List(dpProject, dpRegion).
			Filter(`labels.group = ` + cgroup + ` AND status.state = ACTIVE`).Do()
		require.NoError(t, err)
		require.Len(t, resp.Clusters, 4)

		activeNames, err := svc.Projects.Regions.Clusters.List(dpProject, dpRegion).
			Filter(`clusterName = ` + names[0] + ` AND status.state = ACTIVE`).Do()
		require.NoError(t, err)
		require.Len(t, activeNames.Clusters, 1)
		require.Equal(t, names[0], activeNames.Clusters[0].ClusterName)

		inactive, err := svc.Projects.Regions.Clusters.List(dpProject, dpRegion).
			Filter(`labels.group = ` + cgroup + ` AND status.state = INACTIVE`).Do()
		require.NoError(t, err)
		require.Empty(t, inactive.Clusters)
	})
}

// TestSDKDataprocLongRunningAndBoundaries covers the p5 class: the streaming
// lifecycle, the submitAsOperation LRO, scheduling counters and a many-job
// listing.
func TestSDKDataprocLongRunningAndBoundaries(t *testing.T) {
	svc := dpService(t)
	cluster := newCluster(t, svc)

	// streaming marks the job long-running (the emulator does not auto-settle it
	// to DONE).
	streaming := func(j *dataproc.Job) {
		j.PysparkJob.Properties = map[string]string{
			"spark.sql.streaming.checkpointLocation": "gs://jaiscloud-bucket/ckpt",
		}
	}

	t.Run("StreamingJobStaysRunningUntilCancelled", func(t *testing.T) {
		id := unique("stream")
		submitJob(t, svc, cluster, id, streaming)

		// The mock state machine walks PENDING -> SETUP_DONE -> RUNNING on
		// reads; once RUNNING a long-running job must not settle to DONE.
		deadline := time.Now().Add(30 * time.Second)
		for {
			got, err := svc.Projects.Regions.Jobs.Get(dpProject, dpRegion, id).Do()
			require.NoError(t, err)
			if got.Status.State == "RUNNING" {
				break
			}
			require.NotContains(t, []string{"DONE", "ERROR"}, got.Status.State,
				"a streaming job must not terminate on its own")
			require.False(t, time.Now().After(deadline), "job %s never reached RUNNING", id)
			time.Sleep(20 * time.Millisecond)
		}
		for i := 0; i < 5; i++ {
			got, err := svc.Projects.Regions.Jobs.Get(dpProject, dpRegion, id).Do()
			require.NoError(t, err)
			require.Equal(t, "RUNNING", got.Status.State, "read %d", i)
		}

		_, err := svc.Projects.Regions.Jobs.Cancel(dpProject, dpRegion, id, &dataproc.CancelJobRequest{}).Do()
		require.NoError(t, err)
		require.Equal(t, "CANCELLED", pollJob(t, svc, dpProject, dpRegion, id).Status.State)
	})

	t.Run("SubmitAsOperationTracksJobThroughCancel", func(t *testing.T) {
		id := unique("lro")
		op, err := svc.Projects.Regions.Jobs.SubmitAsOperation(dpProject, dpRegion, &dataproc.SubmitJobRequest{Job: &dataproc.Job{
			Reference: &dataproc.JobReference{ProjectId: dpProject, JobId: id},
			Placement: &dataproc.JobPlacement{ClusterName: cluster},
			PysparkJob: &dataproc.PySparkJob{
				MainPythonFileUri: "gs://jaiscloud-bucket/main.py",
				Properties:        map[string]string{"spark.sql.streaming.checkpointLocation": "gs://jaiscloud-bucket/ckpt"},
			},
		}}).Do()
		require.NoError(t, err)
		require.False(t, op.Done, "a streaming job's operation is not done at submit")

		polled, err := svc.Projects.Regions.Operations.Get(op.Name).Do()
		require.NoError(t, err)
		require.False(t, polled.Done, "the operation tracks the non-terminal job")

		_, err = svc.Projects.Regions.Jobs.Cancel(dpProject, dpRegion, id, &dataproc.CancelJobRequest{}).Do()
		require.NoError(t, err)

		done := pollOperation(t, svc, op.Name)
		require.True(t, done.Done)
		var final dataproc.Job
		require.NoError(t, json.Unmarshal(done.Response, &final))
		require.Equal(t, "CANCELLED", final.Status.State)
	})

	t.Run("SchedulingCountersRoundTrip", func(t *testing.T) {
		id := unique("sched")
		got := submitJob(t, svc, cluster, id, func(j *dataproc.Job) {
			j.Scheduling = &dataproc.JobScheduling{MaxFailuresPerHour: 5, MaxFailuresTotal: 10}
		})
		require.NotNil(t, got.Scheduling)
		require.Equal(t, int64(5), got.Scheduling.MaxFailuresPerHour)
		require.Equal(t, int64(10), got.Scheduling.MaxFailuresTotal)

		read, err := svc.Projects.Regions.Jobs.Get(dpProject, dpRegion, id).Do()
		require.NoError(t, err)
		require.NotNil(t, read.Scheduling)
		require.Equal(t, int64(5), read.Scheduling.MaxFailuresPerHour)
		require.Equal(t, int64(10), read.Scheduling.MaxFailuresTotal)
	})

	t.Run("ManyJobsAcrossPages", func(t *testing.T) {
		const many = 12
		big := newCluster(t, svc)
		ids := make([]string, 0, many)
		for i := 0; i < many; i++ {
			id := fmt.Sprintf("%s-%02d", unique("many"), i)
			submitJob(t, svc, big, id)
			ids = append(ids, id)
		}
		sort.Strings(ids)
		require.Equal(t, ids, walkJobPages(t, svc, big, "", 5))
	})
}
