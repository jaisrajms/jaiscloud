package dataproc

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"jaiscloud/internal/gcp/sparkgcp"
	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/store"
)

// fakeBlobSink records the objects the Dataproc core stages into GCS, so a test
// can assert a job's advertised driverOutputResourceUri resolves to a readable
// object without wiring the real storage provider.
type fakeBlobSink struct {
	mu       sync.Mutex
	ensured  map[string]string // bucket -> location
	objects  map[string][]byte // "bucket/object" -> bytes
	contents map[string]string // "bucket/object" -> contentType
}

func newFakeBlobSink() *fakeBlobSink {
	return &fakeBlobSink{
		ensured:  map[string]string{},
		objects:  map[string][]byte{},
		contents: map[string]string{},
	}
}

func (f *fakeBlobSink) EnsureBucket(_ context.Context, _ /*project*/, bucket, location string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensured[bucket] = location
	return nil
}

func (f *fakeBlobSink) PutObjectBytes(_ context.Context, _ /*project*/, bucket, object, contentType string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := bucket + "/" + object
	f.objects[key] = append([]byte(nil), data...)
	f.contents[key] = contentType
	return nil
}

func (f *fakeBlobSink) get(bucket, object string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[bucket+"/"+object]
	return b, ok
}

func (f *fakeBlobSink) hasBucket(bucket string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.ensured[bucket]
	return ok
}

// newProviderWithSink returns a mock-mode service whose driver output is staged
// into the fake sink.
func newProviderWithSink(t *testing.T, sink BlobSink) *Service {
	t.Helper()
	return NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithBlobSink(sink))
}

// TestDefaultStagingBucket verifies the default bucket name is stable, valid and
// region-scoped.
func TestDefaultStagingBucket(t *testing.T) {
	b := defaultStagingBucket("my-proj", "us-central1")
	require.True(t, strings.HasPrefix(b, "dataproc-staging-my-proj-us-central1-"), b)
	require.LessOrEqual(t, len(b), maxStagingBucketName)
	require.Regexp(t, `^[a-z0-9._-]+$`, b)
	require.Equal(t, b, defaultStagingBucket("my-proj", "us-central1"), "must be deterministic")
	require.NotEqual(t, b, defaultStagingBucket("my-proj", "us-east1"), "must be region-scoped")

	// An over-long project name is sanitized into a <=63-char bucket name.
	long := defaultStagingBucket(strings.Repeat("P", 80), "us-central1")
	require.LessOrEqual(t, len(long), maxStagingBucketName)
	require.Regexp(t, `^[a-z0-9._-]+$`, long)

	// GCS forbids adjacent dots in a bucket name.
	dots := defaultStagingBucket("a..b", "us-central1")
	require.NotContains(t, dots, "..")
	require.Regexp(t, `^[a-z0-9._-]+$`, dots)
}

// TestStagingBucketForCluster verifies the override precedence.
func TestStagingBucketForCluster(t *testing.T) {
	cases := []struct {
		name    string
		cluster dataprocstore.Cluster
		want    string
	}{
		{
			name:    "configBucket",
			cluster: dataprocstore.Cluster{ProjectID: "p", Region: "r", Config: []byte(`{"configBucket":"explicit-config"}`)},
			want:    "explicit-config",
		},
		{
			name:    "tempBucket",
			cluster: dataprocstore.Cluster{ProjectID: "p", Region: "r", Config: []byte(`{"tempBucket":"tmp-bkt"}`)},
			want:    "tmp-bkt",
		},
		{
			name:    "configBucket wins over tempBucket",
			cluster: dataprocstore.Cluster{ProjectID: "p", Region: "r", Config: []byte(`{"configBucket":"cfg","tempBucket":"tmp"}`)},
			want:    "cfg",
		},
		{
			name:    "virtualClusterConfig stagingBucket",
			cluster: dataprocstore.Cluster{ProjectID: "p", Region: "r", VirtualClusterConfig: []byte(`{"stagingBucket":"gs://vcc-bkt"}`)},
			want:    "vcc-bkt",
		},
		{
			name:    "default",
			cluster: dataprocstore.Cluster{ProjectID: "p", Region: "r"},
			want:    defaultStagingBucket("p", "r"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, stagingBucketForCluster(tc.cluster))
		})
	}
}

// TestPrepareDriverOutputUsesClusterBucket verifies a cluster's configured
// staging bucket is provisioned and used in the job's advertised URIs.
func TestPrepareDriverOutputUsesClusterBucket(t *testing.T) {
	sink := newFakeBlobSink()
	p := newProviderWithSink(t, sink)
	ctx := context.Background()

	_, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{
		Config: []byte(`{"tempBucket":"my-stage"}`),
	})
	require.NoError(t, err)

	j, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "j1"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	}))
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(j.DriverOutputResourceURI, "gs://my-stage/"), "uri = %q", j.DriverOutputResourceURI)
	require.True(t, sink.hasBucket("my-stage"), "staging bucket was not provisioned")
}

// TestMaterializeDriverOutputOnSuccess verifies a DONE job's driver-output URI
// resolves to a readable object (and a control file is written alongside).
func TestMaterializeDriverOutputOnSuccess(t *testing.T) {
	sink := newFakeBlobSink()
	p := newProviderWithSink(t, sink)
	ctx := context.Background()

	_, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{})
	require.NoError(t, err)
	_, err = p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "j-done"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	}))
	require.NoError(t, err)

	final := advanceJobToTerminal(t, p, "proj", "us-central1", "j-done")
	require.Equal(t, "DONE", final.Status.State)
	require.NotEmpty(t, final.DriverOutputResourceURI)
	require.NotEmpty(t, final.DriverControlFilesURI)

	bucket, object, ok := splitGSURI(final.DriverOutputResourceURI)
	require.True(t, ok, "uri = %q", final.DriverOutputResourceURI)
	// Real readers fetch the attempt-indexed object, not the bare prefix.
	data, found := sink.get(bucket, object+driverOutputAttemptSuffix)
	require.True(t, found, "driver output object not written")
	require.NotEmpty(t, data)

	_, ctrlPrefix, ok := splitGSURI(final.DriverControlFilesURI)
	require.True(t, ok)
	_, found = sink.get(bucket, ctrlPrefix+"drivercontrol")
	require.True(t, found, "driver control file not written")

	require.True(t, sink.hasBucket(bucket), "staging bucket advertised but not provisioned")
}

// TestMaterializeDriverOutputOnError verifies a job that fails before it runs
// still advertises a resolvable driver-output object.
func TestMaterializeDriverOutputOnError(t *testing.T) {
	sink := newFakeBlobSink()
	p := newProviderWithSink(t, sink)
	ctx := context.Background()

	_, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{})
	require.NoError(t, err)

	// A pysparkJob with no mainPythonFileUri is malformed → stored ERROR.
	j, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "j-bad"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{},
	}))
	require.NoError(t, err)
	require.Equal(t, "ERROR", j.Status.State)

	bucket, object, ok := splitGSURI(j.DriverOutputResourceURI)
	require.True(t, ok, "uri = %q", j.DriverOutputResourceURI)
	_, found := sink.get(bucket, object+driverOutputAttemptSuffix)
	require.True(t, found, "driver output object not written for a failed job")
}

// TestCallerPropertiesCannotStripConnectorConfs verifies a caller's job
// properties cannot override the emulator's GCS connector wiring, while every
// other property passes through untouched.
func TestCallerPropertiesCannotStripConnectorConfs(t *testing.T) {
	args := propertiesToConfArgs(map[string]any{
		"properties": map[string]any{
			"spark.hadoop.fs.gs.impl":                 "com.evil.FileSystem",
			"spark.hadoop.fs.gs.project.id":           "someone-else",
			"spark.executorEnv.STORAGE_EMULATOR_HOST": "http://evil",
			"spark.hadoop.hive.metastore.uris":        "thrift://hms:9083",
			"spark.some.other":                        "keep",
		},
	})
	got := strings.Join(args, " ")
	require.NotContains(t, got, "com.evil.FileSystem")
	require.NotContains(t, got, "someone-else")
	require.NotContains(t, got, "http://evil")
	require.Contains(t, got, "spark.hadoop.hive.metastore.uris=thrift://hms:9083")
	require.Contains(t, got, "spark.some.other=keep")
}

// runJobCapturingLogs runs a k8s job to completion while serving the given
// driver container logs from the fake clientset's log subresource, so the
// capture -> stage path can be asserted end to end.
func runJobCapturingLogs(t *testing.T, p *Service, client *fake.Clientset, j dataprocstore.Job, logData string) {
	t.Helper()
	fw := prependPodWatch(t, client)
	client.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "log" {
			return true, &runtime.Unknown{Raw: []byte(logData)}, nil
		}
		return false, nil, nil
	})
	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.runJob(ctx, j.ProjectID, j.Region, j, "", "")
	}()
	require.Eventually(t, func() bool {
		jobs, err := client.BatchV1().Jobs("jaiscloud").List(ctx, metav1.ListOptions{})
		return err == nil && len(jobs.Items) > 0
	}, 5*time.Second, 10*time.Millisecond)

	// Track the pod so TailLogs can resolve it (by job-name label), then emit it
	// on the watch so WaitTerminal completes.
	pod := succeededDriverPod("driver-logs", "jc-spark-cm-"+j.JobID)
	pod.Spec.Containers = []corev1.Container{{Name: "spark-submit", Image: "spark:test"}}
	if _, err := client.CoreV1().Pods("jaiscloud").Create(ctx, pod, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create driver pod: %v", err)
	}
	fw.Add(pod)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("runJob did not complete")
	}
}

// TestMaterializeDriverOutputUsesCapturedLogs verifies the k8s execution path
// stages the driver container's actual stdout (not a synthetic placeholder)
// into the attempt-indexed driver-output object.
func TestMaterializeDriverOutputUsesCapturedLogs(t *testing.T) {
	client := fake.NewSimpleClientset()
	sink := newFakeBlobSink()
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithK8s(client, "jaiscloud", nil),
		WithSparkImage("spark:test"),
		WithGCPEmulator(&sparkgcp.GCPEmulatorConfig{ProjectID: "proj", Region: "global"}),
		WithBlobSink(sink),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	j := newTestJob()
	require.NoError(t, p.store.CreateJob(context.Background(), j.ProjectID, j.Region, j))

	const logData = "hello from the spark driver\nsecond line\n"
	runJobCapturingLogs(t, p, client, j, logData)

	got, err := p.store.GetJob(context.Background(), j.ProjectID, j.Region, j.JobID)
	require.NoError(t, err)
	require.Equal(t, "DONE", got.Status.State)

	bucket, object, ok := splitGSURI(got.DriverOutputResourceURI)
	require.True(t, ok, "uri = %q", got.DriverOutputResourceURI)
	data, found := sink.get(bucket, object+driverOutputAttemptSuffix)
	require.True(t, found, "driver output object not written")
	require.Equal(t, logData, string(data))
}

// TestDriverPodCarriesConnectorConfs verifies the real k8s driver pod argv
// carries the GCS connector confs and the executor env mirror, scoped to the
// submitted job's project/region.
func TestDriverPodCarriesConnectorConfs(t *testing.T) {
	client := fake.NewSimpleClientset()
	sink := newFakeBlobSink()
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithK8s(client, "jaiscloud", nil),
		WithSparkImage("spark:test"),
		WithGCPEmulator(&sparkgcp.GCPEmulatorConfig{ProjectID: "proj", Region: "global", GCSEndpoint: "http://emu:8080"}),
		WithBlobSink(sink),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	j := newTestJob()
	require.NoError(t, p.store.CreateJob(context.Background(), j.ProjectID, j.Region, j))
	runJobWithDriverPod(t, p, client, j, succeededDriverPod("driver-ok", "jc-spark-cm-j-lc-1"))

	jobs, err := client.BatchV1().Jobs("jaiscloud").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, jobs.Items, 1)
	require.NotEmpty(t, jobs.Items[0].Spec.Template.Spec.Containers)
	args := strings.Join(jobs.Items[0].Spec.Template.Spec.Containers[0].Args, " ")
	require.Contains(t, args, "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
	require.Contains(t, args, "spark.hadoop.fs.gs.storage.root.url=http://emu:8080")
	require.Contains(t, args, "spark.executorEnv.STORAGE_EMULATOR_HOST=http://emu:8080")
	require.Contains(t, args, "spark.executorEnv.GOOGLE_CLOUD_PROJECT=proj")
	require.Contains(t, args, "spark.executorEnv.GOOGLE_CLOUD_LOCATION=us-central1")
}
