package dataproc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/store"
)

// phsVCC builds the standard GKE virtualClusterConfig with a Spark History
// Server reference. The reference may be empty to model an unattached cluster.
func phsVCC(t *testing.T, ref string) map[string]any {
	t.Helper()
	vcc := mustJSONMap([]byte(gkeVirtualClusterConfig))
	aux, _ := vcc["auxiliaryServicesConfig"].(map[string]any)
	if ref == "" {
		delete(aux, "sparkHistoryServerConfig")
	} else {
		aux["sparkHistoryServerConfig"] = map[string]any{"dataprocCluster": ref}
	}
	return vcc
}

// createPHSSource creates the Dataproc cluster that acts as the history server.
// imageVersion may be empty to model a cluster without a Dataproc image.
func createPHSSource(t *testing.T, p *Service, project, region, name, imageVersion string) {
	t.Helper()
	in := ClusterInput{}
	if imageVersion != "" {
		in.Config = []byte(fmt.Sprintf(`{"softwareConfig":{"imageVersion":%q}}`, imageVersion))
	}
	if _, _, err := p.CreateCluster(context.Background(), project, region, name, in); err != nil {
		t.Fatalf("create PHS source cluster %s: %v", name, err)
	}
}

// phsProperties reads the stored virtual cluster's software properties.
func phsProperties(t *testing.T, p *Service, project, region, name string) map[string]any {
	t.Helper()
	c, err := p.GetCluster(context.Background(), project, region, name)
	require.NoError(t, err)
	vcc := mustJSONMap(c.VirtualClusterConfig)
	kcc, _ := vcc["kubernetesClusterConfig"].(map[string]any)
	software, _ := kcc["kubernetesSoftwareConfig"].(map[string]any)
	properties, _ := software["properties"].(map[string]any)
	return properties
}

// TestPHSReferenceValidation is the documented validation matrix: name shape,
// same project, same region, existence (404) and a Dataproc image version.
func TestPHSReferenceValidation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name        string
		ref         string
		createImage bool
		code        string
		wantMsg     string
	}{
		{
			name:        "valid",
			ref:         "projects/proj/regions/us-central1/clusters/phs",
			createImage: true,
		},
		{
			name:    "malformed name",
			ref:     "projects/proj/regions/us-central1/clusters",
			code:    "InvalidArgument",
			wantMsg: "must be a fully qualified Dataproc cluster resource name",
		},
		{
			name:    "short name",
			ref:     "clusters/phs",
			code:    "InvalidArgument",
			wantMsg: "must be a fully qualified Dataproc cluster resource name",
		},
		{
			name:    "wrong project",
			ref:     "projects/other/regions/us-central1/clusters/phs",
			code:    "InvalidArgument",
			wantMsg: "must be in the cluster project proj, got other",
		},
		{
			name:    "wrong region",
			ref:     "projects/proj/regions/us-east1/clusters/phs",
			code:    "InvalidArgument",
			wantMsg: "must be in the cluster region us-central1, got us-east1",
		},
		{
			name:    "missing cluster",
			ref:     "projects/proj/regions/us-central1/clusters/missing",
			code:    "NotFound",
			wantMsg: "spark history server cluster not found",
		},
		{
			name:        "no image version",
			ref:         "projects/proj/regions/us-central1/clusters/noimage",
			code:        "InvalidArgument",
			wantMsg:     "with a softwareConfig.imageVersion",
			createImage: false,
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newProvider(t)
			if tc.createImage {
				createPHSSource(t, p, "proj", "us-central1", "phs", "2.2.0")
			} else if tc.code == "InvalidArgument" && strings.Contains(tc.wantMsg, "imageVersion") {
				createPHSSource(t, p, "proj", "us-central1", "noimage", "")
			}
			cluster := fmt.Sprintf("gke-%d", i)
			_, _, err := p.CreateCluster(ctx, "proj", "us-central1", cluster,
				gkeInput(t, phsVCC(t, tc.ref)))
			if tc.code == "" {
				require.NoError(t, err)
				props := phsProperties(t, p, "proj", "us-central1", cluster)
				require.Equal(t, "true", props[phsEventLogEnabledKey])
				require.NotEmpty(t, props[phsEventLogDirKey])
				return
			}
			require.Error(t, err)
			require.Truef(t, isCode(err, tc.code), "got %v, want %s", err, tc.code)
			require.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}

// TestCreateCluster_PHSInjectsWorkloadProperties covers the documented create
// response properties, the deterministic log-directory segment choice, and that
// the rest of the virtualClusterConfig survives the injection.
func TestCreateCluster_PHSInjectsWorkloadProperties(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	createPHSSource(t, p, "proj", "us-central1", "phs", "2.2.0")

	ref := "projects/proj/regions/us-central1/clusters/phs"
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gke-phs",
		ClusterInput{VirtualClusterConfig: mustJSON(phsVCC(t, ref))}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	props := phsProperties(t, p, "proj", "us-central1", "gke-phs")
	require.Equal(t, "true", props[phsEventLogEnabledKey])
	// The documented location is gs://BUCKET/.../spark-job-history; the
	// emulator uses the virtual cluster name for the elided segment.
	require.Equal(t,
		"gs://dataproc-staging-proj/gke-phs/spark-job-history",
		props[phsEventLogDirKey])

	c, err := p.GetCluster(ctx, "proj", "us-central1", "gke-phs")
	require.NoError(t, err)
	vcc := mustJSONMap(c.VirtualClusterConfig)
	require.Equal(t, "dataproc-staging-proj", vcc["stagingBucket"])
	aux, _ := vcc["auxiliaryServicesConfig"].(map[string]any)
	phs, _ := aux["sparkHistoryServerConfig"].(map[string]any)
	require.Equal(t, ref, phs["dataprocCluster"])
	kcc, _ := vcc["kubernetesClusterConfig"].(map[string]any)
	gke, _ := kcc["gkeClusterConfig"].(map[string]any)
	require.Equal(t, "projects/proj/locations/us-central1/clusters/gke-1", gke["gkeClusterTarget"])
}

// TestCreateCluster_PHSClientPropertiesWin verifies a caller-supplied property
// is never overwritten by the injected default.
func TestCreateCluster_PHSClientPropertiesWin(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	createPHSSource(t, p, "proj", "us-central1", "phs", "2.2.0")

	vcc := phsVCC(t, "projects/proj/regions/us-central1/clusters/phs")
	kcc, _ := vcc["kubernetesClusterConfig"].(map[string]any)
	software, _ := kcc["kubernetesSoftwareConfig"].(map[string]any)
	software["properties"] = map[string]any{
		phsEventLogEnabledKey: "false",
		phsEventLogDirKey:     "gs://custom/logs",
	}
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gke-custom",
		ClusterInput{VirtualClusterConfig: mustJSON(vcc)}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	props := phsProperties(t, p, "proj", "us-central1", "gke-custom")
	require.Equal(t, "false", props[phsEventLogEnabledKey])
	require.Equal(t, "gs://custom/logs", props[phsEventLogDirKey])
}

// TestUpdateCluster_PHSMaskedVCC covers validation and injection on a masked
// virtualClusterConfig update, plus the mask rule: an update that does not
// apply the vcc neither validates nor injects it.
func TestUpdateCluster_PHSMaskedVCC(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	createPHSSource(t, p, "proj", "us-central1", "phs", "2.2.0")

	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gke-up",
		gkeInput(t, phsVCC(t, ""))); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	// A masked vcc update attaches the PHS and injects the properties.
	if _, _, err := p.UpdateCluster(ctx, "proj", "us-central1", "gke-up",
		gkeInput(t, phsVCC(t, "projects/proj/regions/us-central1/clusters/phs")),
		[]string{"virtualClusterConfig"}); err != nil {
		t.Fatalf("UpdateCluster: %v", err)
	}
	props := phsProperties(t, p, "proj", "us-central1", "gke-up")
	require.Equal(t, "true", props[phsEventLogEnabledKey])
	require.Equal(t, "gs://dataproc-staging-proj/gke-up/spark-job-history", props[phsEventLogDirKey])

	// A cross-region reference on a masked vcc update is rejected and the
	// stored vcc is left unchanged.
	_, _, err := p.UpdateCluster(ctx, "proj", "us-central1", "gke-up",
		gkeInput(t, phsVCC(t, "projects/proj/regions/us-east1/clusters/phs")),
		[]string{"virtualClusterConfig"})
	require.Error(t, err)
	require.True(t, isCode(err, "InvalidArgument"))
	require.Contains(t, err.Error(), "must be in the cluster region us-central1, got us-east1")

	// A reference that is not applied by the mask is not validated: the update
	// only changes labels, so a bad PHS ref in the body must not fail it.
	vcc := phsVCC(t, "projects/proj/regions/us-east1/clusters/phs")
	if _, _, err := p.UpdateCluster(ctx, "proj", "us-central1", "gke-up",
		ClusterInput{Labels: map[string]string{"env": "dev"}, VirtualClusterConfig: mustJSON(vcc)},
		[]string{"labels"}); err != nil {
		t.Fatalf("label-only update must ignore the vcc: %v", err)
	}
}

// TestPHSSparkConfs verifies the submit-time conf tokens and their fallback.
func TestPHSSparkConfs(t *testing.T) {
	noPHS := dpstore.Cluster{ProjectID: "proj", Region: "us-central1", Name: "c",
		VirtualClusterConfig: mustJSON(phsVCC(t, ""))}
	require.Nil(t, phsSparkConfs(noPHS))

	withPHS := dpstore.Cluster{ProjectID: "proj", Region: "us-central1", Name: "c",
		VirtualClusterConfig: mustJSON(phsVCC(t, "projects/proj/regions/us-central1/clusters/phs"))}
	confs := sparkConfMap(phsSparkConfs(withPHS))
	require.Equal(t, "true", confs[phsEventLogEnabledConf])
	require.Equal(t, "gs://dataproc-staging-proj/c/spark-job-history", confs[phsEventLogDirConf])

	// A stored client override is what the driver sees.
	override := phsVCC(t, "projects/proj/regions/us-central1/clusters/phs")
	kcc, _ := override["kubernetesClusterConfig"].(map[string]any)
	software, _ := kcc["kubernetesSoftwareConfig"].(map[string]any)
	software["properties"] = map[string]any{phsEventLogDirKey: "gs://custom/logs"}
	withOverride := dpstore.Cluster{ProjectID: "proj", Region: "us-central1", Name: "c",
		VirtualClusterConfig: mustJSON(override)}
	require.Equal(t, "gs://custom/logs", sparkConfMap(phsSparkConfs(withOverride))[phsEventLogDirConf])
}

// TestSubmitJob_InjectsPHSConfs drives a real submit through the fake
// clientset and asserts the driver spark-submit argv carries the PHS event-log
// pair. The emulator has no GKE execution plane, so this argv assertion is the
// reachable proof that the driver receives the properties.
func TestSubmitJob_InjectsPHSConfs(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithK8s(client, "jaiscloud", nil),
		WithSparkImage("spark:test"),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	ctx := context.Background()

	createPHSSource(t, p, "proj", "us-central1", "phs", "2.2.0")
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1",
		gkeInput(t, phsVCC(t, "projects/proj/regions/us-central1/clusters/phs"))); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	fw := prependPodWatch(t, client)

	if _, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "j-phs"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	})); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	jobName := waitForDriverJob(t, client)
	args := driverJobArgs(t, client, jobName)
	confs := sparkConfMap(args)
	require.Equal(t, "true", confs[phsEventLogEnabledConf])
	require.Equal(t, "gs://dataproc-staging-proj/c1/spark-job-history", confs[phsEventLogDirConf])

	fw.Add(succeededDriverPod("driver-phs", jobName))
}

// TestSubmitJob_PHSJobPropertyWins verifies a job property overrides the
// cluster-level event-log directory (Spark last-value-wins).
func TestSubmitJob_PHSJobPropertyWins(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithK8s(client, "jaiscloud", nil),
		WithSparkImage("spark:test"),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	ctx := context.Background()

	createPHSSource(t, p, "proj", "us-central1", "phs", "2.2.0")
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1",
		gkeInput(t, phsVCC(t, "projects/proj/regions/us-central1/clusters/phs"))); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	fw := prependPodWatch(t, client)

	if _, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference": map[string]any{"jobId": "j-phs-override"},
		"placement": map[string]any{"clusterName": "c1"},
		"sparkJob": map[string]any{
			"mainJarFileUri": "gs://b/app.jar",
			"properties":     map[string]any{"spark.eventLog.dir": "gs://override/logs"},
		},
	})); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	jobName := waitForDriverJob(t, client)
	confs := sparkConfMap(driverJobArgs(t, client, jobName))
	require.Equal(t, "gs://override/logs", confs[phsEventLogDirConf])
	require.Equal(t, "true", confs[phsEventLogEnabledConf])

	fw.Add(succeededDriverPod("driver-phs-override", jobName))
}

// TestSubmitJob_NoPHSNoInjection verifies an unattached cluster gets neither
// event-log conf.
func TestSubmitJob_NoPHSNoInjection(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithK8s(client, "jaiscloud", nil),
		WithSparkImage("spark:test"),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	ctx := context.Background()

	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1",
		gkeInput(t, phsVCC(t, ""))); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	fw := prependPodWatch(t, client)

	if _, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "j-nophs"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	})); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	jobName := waitForDriverJob(t, client)
	confs := sparkConfMap(driverJobArgs(t, client, jobName))
	_, hasEnabled := confs[phsEventLogEnabledConf]
	_, hasDir := confs[phsEventLogDirConf]
	require.False(t, hasEnabled)
	require.False(t, hasDir)

	fw.Add(succeededDriverPod("driver-nophs", jobName))
}

// waitForDriverJob waits for the spark-submit k8s Job and returns its name.
func waitForDriverJob(t *testing.T, client *fake.Clientset) string {
	t.Helper()
	var jobName string
	require.Eventually(t, func() bool {
		jobs, err := client.BatchV1().Jobs("dataproc").List(context.Background(), metav1.ListOptions{})
		if err != nil || len(jobs.Items) == 0 {
			return false
		}
		jobName = jobs.Items[0].Name
		return true
	}, 5*time.Second, 10*time.Millisecond, "spark-submit job was not created")
	return jobName
}

// driverJobArgs returns the driver container's argv.
func driverJobArgs(t *testing.T, client *fake.Clientset, jobName string) []string {
	t.Helper()
	job, err := client.BatchV1().Jobs("dataproc").Get(context.Background(), jobName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers)
	return job.Spec.Template.Spec.Containers[0].Args
}

// mustJSON marshals a test map, failing the test on an encoding error.
func mustJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
