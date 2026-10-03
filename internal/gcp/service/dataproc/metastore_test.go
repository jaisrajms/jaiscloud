package dataproc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// metastoreEP is the synthesized endpoint the fake resolver returns.
const metastoreEP = "thrift://hms.us-central1.metastore.jaiscloud.local:9083"

// gceMetastoreConfig is a GCE ClusterConfig carrying a metastore attachment.
const gceMetastoreConfig = `{"metastoreConfig":{"dataprocMetastoreService":"projects/proj/locations/us-central1/services/hms"}}`

// fakeMetastoreResolver records calls and returns a fixed endpoint/error.
type fakeMetastoreResolver struct {
	endpoint  string
	err       error
	validateN int
	endpointN int
	lastRef   string
}

func (f *fakeMetastoreResolver) ValidateMetastoreService(_ context.Context, ref, _, _ string) error {
	f.validateN++
	f.lastRef = ref
	return f.err
}

func (f *fakeMetastoreResolver) MetastoreEndpoint(ref, _, _ string) (string, error) {
	f.endpointN++
	f.lastRef = ref
	if f.err != nil {
		return "", f.err
	}
	return f.endpoint, nil
}

// isCode reports whether err is a provider error with the given code.
func isCode(err error, code string) bool {
	var pe *model.ProviderError
	return errors.As(err, &pe) && pe.Code == code
}

// sparkConfMap extracts the effective --conf key=value pairs from a
// spark-submit argv, last occurrence winning (matching Spark).
func sparkConfMap(args []string) map[string]string {
	out := map[string]string{}
	for i := 0; i+1 < len(args); i++ {
		if args[i] != "--conf" {
			continue
		}
		kv := args[i+1]
		if k, v, ok := strings.Cut(kv, "="); ok {
			out[k] = v
		}
	}
	return out
}

// TestMetastoreServiceRefExtraction covers both wire locations and the
// preference for the GKE auxiliaryServicesConfig over the GCE config.
func TestMetastoreServiceRefExtraction(t *testing.T) {
	want := "projects/proj/locations/us-central1/services/hms"

	if got := metastoreServiceRefFromInput(ClusterInput{VirtualClusterConfig: []byte(gkeVirtualClusterConfig)}); got != want {
		t.Errorf("GKE ref = %q, want %q", got, want)
	}
	if got := metastoreServiceRefFromInput(ClusterInput{Config: []byte(gceMetastoreConfig)}); got != want {
		t.Errorf("GCE ref = %q, want %q", got, want)
	}
	// GKE wins when both are present (the API forbids this, but extraction
	// must still be deterministic).
	both := ClusterInput{
		Config:               []byte(gceMetastoreConfig),
		VirtualClusterConfig: []byte(gkeVirtualClusterConfig),
	}
	if got := metastoreServiceRefFromInput(both); got != want {
		t.Errorf("both ref = %q, want %q", got, want)
	}
	if got := metastoreServiceRefFromInput(ClusterInput{Config: []byte(`{"gceClusterConfig":{}}`)}); got != "" {
		t.Errorf("no attachment = %q, want empty", got)
	}
}

// TestMetastoreSparkConfs verifies the injected spark-submit confs.
func TestMetastoreSparkConfs(t *testing.T) {
	confs := metastoreSparkConfs(metastoreEP)
	got := sparkConfMap(confs)
	if got["spark.hadoop.hive.metastore.uris"] != metastoreEP {
		t.Errorf("hive.metastore.uris = %q", got["spark.hadoop.hive.metastore.uris"])
	}
	if got["spark.sql.catalogImplementation"] != "hive" {
		t.Errorf("catalogImplementation = %q", got["spark.sql.catalogImplementation"])
	}
	if metastoreSparkConfs("") != nil {
		t.Error("empty endpoint must inject nothing")
	}
}

// TestNormalizeHMSEndpoint verifies the override accepts host:port or a URI.
func TestNormalizeHMSEndpoint(t *testing.T) {
	if got := normalizeHMSEndpoint("host.docker.internal:9083"); got != "thrift://host.docker.internal:9083" {
		t.Errorf("bare host:port = %q", got)
	}
	if got := normalizeHMSEndpoint("thrift://hms:9083"); got != "thrift://hms:9083" {
		t.Errorf("thrift URI = %q", got)
	}
	if got := normalizeHMSEndpoint("  "); got != "" {
		t.Errorf("blank = %q", got)
	}
}

// TestCreateCluster_MetastoreValidation verifies a present attachment is
// validated at create (NotFound/InvalidArgument propagate), the GKE and GCE
// locations are both honored, and a nil resolver keeps metadata-only behavior.
func TestCreateCluster_MetastoreValidation(t *testing.T) {
	ctx := context.Background()

	r := &fakeMetastoreResolver{endpoint: metastoreEP}
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithMetastoreResolver(r))
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gke-1", gkeInput(t, mustJSONMap([]byte(gkeVirtualClusterConfig)))); err != nil {
		t.Fatalf("CreateCluster (GKE): %v", err)
	}
	if r.validateN != 1 {
		t.Fatalf("resolver validate calls = %d, want 1", r.validateN)
	}
	if r.lastRef != "projects/proj/locations/us-central1/services/hms" {
		t.Fatalf("validated ref = %q", r.lastRef)
	}
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gce-1", ClusterInput{Config: []byte(gceMetastoreConfig)}); err != nil {
		t.Fatalf("CreateCluster (GCE): %v", err)
	}

	// Unknown service -> NotFound.
	nf := &fakeMetastoreResolver{err: model.NewProviderError("NotFound", "service not found", 404)}
	pNF := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithMetastoreResolver(nf))
	if _, _, err := pNF.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{Config: []byte(gceMetastoreConfig)}); !isCode(err, "NotFound") {
		t.Fatalf("unknown service = %v, want NotFound", err)
	}

	// Malformed reference -> InvalidArgument.
	bad := &fakeMetastoreResolver{err: model.NewProviderError("InvalidArgument", "malformed", 400)}
	pBad := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithMetastoreResolver(bad))
	if _, _, err := pBad.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{Config: []byte(gceMetastoreConfig)}); !isCode(err, "InvalidArgument") {
		t.Fatalf("malformed ref = %v, want InvalidArgument", err)
	}

	// Nil resolver: the attachment is stored but not validated.
	pNil := newProvider(t)
	if _, _, err := pNil.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{Config: []byte(gceMetastoreConfig)}); err != nil {
		t.Fatalf("nil resolver CreateCluster: %v", err)
	}
	c, err := pNil.GetCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if metastoreServiceRefFromCluster(c) != "projects/proj/locations/us-central1/services/hms" {
		t.Fatalf("stored ref lost: %s", c.Config)
	}
}

// TestCreateCluster_MetastoreConfigShape verifies the required
// dataprocMetastoreService field is enforced even without a resolver.
func TestCreateCluster_MetastoreConfigShape(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	for name, cfg := range map[string]string{
		"empty object":  `{"metastoreConfig":{}}`,
		"missing field": `{"metastoreConfig":{"other":1}}`,
		"non-object":    `{"metastoreConfig":"hms"}`,
	} {
		if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "bad-"+name, ClusterInput{Config: []byte(cfg)}); !isCode(err, "InvalidArgument") {
			t.Errorf("%s: %v, want InvalidArgument", name, err)
		}
	}
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "ok", ClusterInput{Config: []byte(`{"gceClusterConfig":{}}`)}); err != nil {
		t.Fatalf("no metastoreConfig should be accepted: %v", err)
	}
	gkeEmpty := `{"kubernetesClusterConfig":{"gkeClusterConfig":{"gkeClusterTarget":"projects/proj/locations/us-central1/clusters/gke-1"}},"auxiliaryServicesConfig":{"metastoreConfig":{}}}`
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gke-bad", ClusterInput{VirtualClusterConfig: []byte(gkeEmpty)}); !isCode(err, "InvalidArgument") {
		t.Fatalf("GKE empty metastoreConfig = %v, want InvalidArgument", err)
	}
}

// TestUpdateCluster_MetastoreValidation verifies a masked update that adds a
// metastore attachment is validated too.
func TestUpdateCluster_MetastoreValidation(t *testing.T) {
	ctx := context.Background()
	nf := &fakeMetastoreResolver{err: model.NewProviderError("NotFound", "service not found", 404)}
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithMetastoreResolver(nf))
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{Config: []byte(`{"gceClusterConfig":{}}`)}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, _, err := p.UpdateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{Config: []byte(gceMetastoreConfig)}, []string{"config.metastoreConfig"}); !isCode(err, "NotFound") {
		t.Fatalf("update with unknown metastore = %v, want NotFound", err)
	}
	// A mask that does not apply the metastore path leaves the stored
	// (attachment-free) cluster unchanged and must not validate it.
	if _, _, err := p.UpdateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{Config: []byte(gceMetastoreConfig)}, []string{"config.workerConfig.numInstances"}); err != nil {
		t.Fatalf("non-applying mask update = %v, want nil", err)
	}
}

// TestClusterMetastoreEndpointOverride verifies the deployment override
// replaces the synthesized endpoint and that no attachment yields "".
func TestClusterMetastoreEndpointOverride(t *testing.T) {
	r := &fakeMetastoreResolver{endpoint: metastoreEP}
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithMetastoreResolver(r), WithHMSEndpointOverride("host.docker.internal:9083"))
	c := dataprocstore.Cluster{ProjectID: "proj", Region: "us-central1", Config: []byte(gceMetastoreConfig)}
	got, err := p.clusterMetastoreEndpoint("proj", "us-central1", c)
	if err != nil {
		t.Fatalf("clusterMetastoreEndpoint: %v", err)
	}
	if got != "thrift://host.docker.internal:9083" {
		t.Fatalf("override = %q", got)
	}

	noAttach := dataprocstore.Cluster{ProjectID: "proj", Region: "us-central1", Config: []byte(`{"gceClusterConfig":{}}`)}
	got, err = p.clusterMetastoreEndpoint("proj", "us-central1", noAttach)
	if err != nil || got != "" {
		t.Fatalf("no attachment = %q, %v", got, err)
	}
}

// TestSubmitJob_InjectsMetastoreConfs drives a real submit through the fake
// clientset and asserts the driver spark-submit argv carries the attachment.
func TestSubmitJob_InjectsMetastoreConfs(t *testing.T) {
	client := fake.NewSimpleClientset()
	r := &fakeMetastoreResolver{endpoint: metastoreEP}
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithK8s(client, "jaiscloud", nil),
		WithSparkImage("spark:test"),
		WithMetastoreResolver(r),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	ctx := context.Background()

	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", gkeInput(t, mustJSONMap([]byte(gkeVirtualClusterConfig)))); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	fw := prependPodWatch(t, client)

	if _, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "j-hms"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	})); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	var jobName string
	var args []string
	require.Eventually(t, func() bool {
		jobs, err := client.BatchV1().Jobs("jaiscloud").List(ctx, metav1.ListOptions{})
		if err != nil || len(jobs.Items) == 0 {
			return false
		}
		jobName = jobs.Items[0].Name
		args = jobs.Items[0].Spec.Template.Spec.Containers[0].Args
		return len(args) > 0
	}, 5*time.Second, 10*time.Millisecond, "spark-submit job was not created")

	confs := sparkConfMap(args)
	if confs["spark.hadoop.hive.metastore.uris"] != metastoreEP {
		t.Errorf("hive.metastore.uris = %q, want %q", confs["spark.hadoop.hive.metastore.uris"], metastoreEP)
	}
	if confs["spark.sql.catalogImplementation"] != "hive" {
		t.Errorf("catalogImplementation = %q, want hive", confs["spark.sql.catalogImplementation"])
	}
	// The attachment is formatted, not re-validated, at submit.
	if r.endpointN != 1 {
		t.Errorf("resolver endpoint calls = %d, want 1", r.endpointN)
	}

	// Drive the driver to a terminal pod so runJob drains.
	fw.Add(succeededDriverPod("driver-hms", jobName))
}

// TestSubmitJob_CallerMetastoreConfWins verifies a caller-supplied conf beats
// the injected attachment via Spark's last-value-wins.
func TestSubmitJob_CallerMetastoreConfWins(t *testing.T) {
	client := fake.NewSimpleClientset()
	r := &fakeMetastoreResolver{endpoint: metastoreEP}
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithK8s(client, "jaiscloud", nil),
		WithSparkImage("spark:test"),
		WithMetastoreResolver(r),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	ctx := context.Background()

	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", gkeInput(t, mustJSONMap([]byte(gkeVirtualClusterConfig)))); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	fw := prependPodWatch(t, client)

	if _, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference": map[string]any{"jobId": "j-hms-caller"},
		"placement": map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{
			"mainPythonFileUri": "gs://b/main.py",
			"properties":        map[string]any{"spark.hadoop.hive.metastore.uris": "thrift://caller:9083"},
		},
	})); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	var jobName string
	var args []string
	require.Eventually(t, func() bool {
		jobs, err := client.BatchV1().Jobs("jaiscloud").List(ctx, metav1.ListOptions{})
		if err != nil || len(jobs.Items) == 0 {
			return false
		}
		jobName = jobs.Items[0].Name
		args = jobs.Items[0].Spec.Template.Spec.Containers[0].Args
		return len(args) > 0
	}, 5*time.Second, 10*time.Millisecond, "spark-submit job was not created")

	if got := sparkConfMap(args)["spark.hadoop.hive.metastore.uris"]; got != "thrift://caller:9083" {
		t.Errorf("effective hive.metastore.uris = %q, want the caller's value", got)
	}

	fw.Add(succeededDriverPod("driver-hms-caller", jobName))
}
