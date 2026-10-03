package sparkhelpers

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/platform"
)

// TestBuildClientModeArgs_JAR verifies argv ordering for a JAR entry point.
func TestBuildClientModeArgs_JAR(t *testing.T) {
	job := ClientModeJob{
		JobID:     "test-job-1",
		Namespace: "default",
		Image:     "spark:3.5",
		EntryPoint: JarEntryPoint{
			JarURI:    "s3://bucket/app.jar",
			MainClass: "com.example.Main",
		},
		SparkSubmitArgs: []string{"--conf", "spark.driver.memory=2g"},
		JarArgs:         []string{"--input", "s3://bucket/data"},
	}

	args := BuildClientModeArgs(job)

	// Fixed leading tokens must always be first.
	require.GreaterOrEqual(t, len(args), 10)
	assert.Equal(t, "--master", args[0])
	assert.Equal(t, defaultMaster, args[1])
	assert.Equal(t, "--deploy-mode", args[2])
	assert.Equal(t, "client", args[3])
	assert.Equal(t, "--conf", args[4])
	assert.Equal(t, "spark.kubernetes.namespace=default", args[5])
	assert.Equal(t, "--conf", args[6])
	assert.Equal(t, "spark.kubernetes.driver.pod.name=$(SPARK_DRIVER_POD_NAME)", args[7])
	assert.Equal(t, "--conf", args[8])
	assert.Equal(t, "spark.kubernetes.executor.podTemplateFile=file:///jaiscloud/spark/executor-template.yaml", args[9])

	// Remaining conf pairs and caller args verified by key-based lookup — more
	// resilient to new confs being inserted in the fixed section.
	cm := argsConfMap(args)
	assert.Equal(t, "spark:3.5", cm["spark.kubernetes.container.image"])
	assert.Equal(t, "0.0.0.0", cm["spark.driver.bindAddress"])
	assert.Equal(t, "2g", cm["spark.driver.memory"], "caller SparkSubmitArgs must appear in confs")

	// Entry-point and jar args must be last.
	n := len(args)
	assert.Equal(t, "s3://bucket/data", args[n-1])
	assert.Equal(t, "--input", args[n-2])
	assert.Equal(t, "s3://bucket/app.jar", args[n-3])
	assert.Equal(t, "com.example.Main", args[n-4])
	assert.Equal(t, "--class", args[n-5])
}

// TestBuildClientModeArgs_Python verifies argv for a Python entry point.
func TestBuildClientModeArgs_Python(t *testing.T) {
	job := ClientModeJob{
		JobID:     "py-job",
		Namespace: "spark",
		EntryPoint: PythonEntryPoint{
			MainPythonFile: "app.py",
			PyFiles:        []string{"lib1.zip", "lib2.zip"},
		},
	}

	args := BuildClientModeArgs(job)

	// Find py-files in args.
	idx := -1
	for i, a := range args {
		if a == "--py-files" {
			idx = i
			break
		}
	}
	require.NotEqual(t, -1, idx, "expected --py-files in args")
	assert.Equal(t, "lib1.zip,lib2.zip", args[idx+1])
	assert.Equal(t, "app.py", args[len(args)-1])
}

// TestBuildClientModeArgs_R verifies argv for an R entry point.
func TestBuildClientModeArgs_R(t *testing.T) {
	job := ClientModeJob{
		JobID:      "r-job",
		Namespace:  "spark",
		EntryPoint: REntryPoint{MainRFile: "analysis.R"},
	}

	args := BuildClientModeArgs(job)
	assert.Equal(t, "analysis.R", args[len(args)-1])
}

// TestSubmitClientMode verifies the Job and ConfigMap are created.
func TestSubmitClientMode(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	ctx := context.Background()

	mutatorCalled := false
	var mutator k8shelpers.IdentityMutator = func(_ context.Context, _ kubernetes.Interface, tpl *corev1.PodTemplateSpec) error {
		mutatorCalled = true
		if tpl.Annotations == nil {
			tpl.Annotations = map[string]string{}
		}
		tpl.Annotations["test-mutator"] = "called"
		return nil
	}

	job := ClientModeJob{
		JobID:     "test-job-abc",
		Namespace: "jaiscloud",
		Image:     "spark-test:latest",
		EntryPoint: JarEntryPoint{
			JarURI:    "local:///opt/spark/app.jar",
			MainClass: "com.test.Main",
		},
		Labels:          map[string]string{"app": "spark-test"},
		IdentityMutator: mutator,
	}

	handle, err := SubmitClientMode(ctx, k8s, job)
	require.NoError(t, err)
	assert.NotEmpty(t, handle.JobName)

	// Verify ConfigMap was created.
	cmName := "spark-exec-tpl-" + sanitizeJobID(job.JobID)
	cm, err := k8s.CoreV1().ConfigMaps(job.Namespace).Get(ctx, cmName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Contains(t, cm.Data, executorTemplateKey)

	// Verify Job was created.
	k8sJob, err := k8s.BatchV1().Jobs(job.Namespace).Get(ctx, handle.JobName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "spark-test", k8sJob.Labels["app"])

	// Verify IdentityMutator was called.
	assert.True(t, mutatorCalled)
}

// TestSubmitClientMode_WithPlatformOverlay verifies platform overlay is applied to the spark-submit pod.
func TestSubmitClientMode_WithPlatformOverlay(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	ctx := context.Background()

	overlay := &platform.PlatformConfig{
		TLS: platform.TLSConfig{
			Enabled: true,
			CASources: []platform.CASource{
				{
					Name:   "testca",
					Source: platform.CASourceRef{Kind: "configMap", Name: "my-ca", Key: "ca.crt"},
				},
			},
			TruststorePassword: "changeit",
		},
	}

	job := ClientModeJob{
		JobID:           "overlay-job",
		Namespace:       "jaiscloud",
		Image:           "spark-test:latest",
		EntryPoint:      JarEntryPoint{JarURI: "local:///app.jar"},
		PlatformOverlay: overlay,
	}

	handle, err := SubmitClientMode(ctx, k8s, job)
	require.NoError(t, err)
	assert.NotEmpty(t, handle.JobName)

	// Verify the spark-submit job's pod has init containers from platform overlay.
	k8sJob, err := k8s.BatchV1().Jobs(job.Namespace).Get(ctx, handle.JobName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.NotEmpty(t, k8sJob.Spec.Template.Spec.InitContainers,
		"expected platform overlay TLS init containers in spark-submit pod")
}

// TestSubmitClientMode_CustomSparkSubmitPath verifies custom spark-submit binary path.
func TestSubmitClientMode_CustomSparkSubmitPath(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	ctx := context.Background()

	job := ClientModeJob{
		JobID:           "custom-path-job",
		Namespace:       "jaiscloud",
		Image:           "spark-test:latest",
		EntryPoint:      JarEntryPoint{JarURI: "local:///app.jar"},
		SparkSubmitPath: "/opt/spark/bin/spark-submit",
	}

	handle, err := SubmitClientMode(ctx, k8s, job)
	require.NoError(t, err)

	k8sJob, err := k8s.BatchV1().Jobs(job.Namespace).Get(ctx, handle.JobName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, k8sJob.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, []string{"/opt/spark/bin/spark-submit"}, k8sJob.Spec.Template.Spec.Containers[0].Command)
}

// TestBuildClientModeArgs_SQL verifies argv for a SQL entry point: --jars and
// --hivevar precede the -e query, and no primary resource is set.
func TestBuildClientModeArgs_SQL(t *testing.T) {
	job := ClientModeJob{
		JobID:     "sql-job",
		Namespace: "spark",
		EntryPoint: SqlEntryPoint{
			Queries:     []string{"SELECT 1", "SELECT 2"},
			JarFileURIs: []string{"gs://b/dep.jar"},
			HiveVars:    map[string]string{"b": "2", "a": "1"},
		},
	}

	args := BuildClientModeArgs(job)

	// The SQL argv still carries the standard k8s client-mode flags.
	assert.Equal(t, "--master", args[0])
	assert.Equal(t, defaultMaster, args[1])
	assert.Equal(t, "--deploy-mode", args[2])
	assert.Equal(t, "client", args[3])

	n := len(args)
	assert.Equal(t, "-e", args[n-2])
	assert.Equal(t, "SELECT 1;\nSELECT 2", args[n-1])
	// --jars and sorted --hivevar pairs precede the query.
	assert.Contains(t, args, "--jars")
	assert.Contains(t, args, "gs://b/dep.jar")
	assert.Contains(t, args, "--hivevar")
	assert.Equal(t, []string{"--jars", "gs://b/dep.jar", "--hivevar", "a=1", "--hivevar", "b=2", "-e", "SELECT 1;\nSELECT 2"}, args[n-8:])
}

// TestBuildClientModeArgs_SQL_FileURI verifies -f for a queryFileUri job.
func TestBuildClientModeArgs_SQL_FileURI(t *testing.T) {
	args := BuildClientModeArgs(ClientModeJob{
		JobID:      "sql-file-job",
		Namespace:  "spark",
		EntryPoint: SqlEntryPoint{FileURI: "gs://b/q.sql"},
	})
	n := len(args)
	assert.Equal(t, "-f", args[n-2])
	assert.Equal(t, "gs://b/q.sql", args[n-1])
}

// TestSubmitClientMode_CustomSparkSqlPath verifies a custom spark-sql binary is
// used as the driver command for a SQL entry point.
func TestSubmitClientMode_CustomSparkSqlPath(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	ctx := context.Background()

	job := ClientModeJob{
		JobID:        "sql-path-job",
		Namespace:    "jaiscloud",
		Image:        "spark-test:latest",
		EntryPoint:   SqlEntryPoint{Queries: []string{"SELECT 1"}},
		SparkSqlPath: "/opt/custom/bin/spark-sql",
	}

	handle, err := SubmitClientMode(ctx, k8s, job)
	require.NoError(t, err)

	k8sJob, err := k8s.BatchV1().Jobs(job.Namespace).Get(ctx, handle.JobName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, k8sJob.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, []string{"/opt/custom/bin/spark-sql"}, k8sJob.Spec.Template.Spec.Containers[0].Command)
}

// TestSubmitClientMode_SparkSqlPathDerivedFromSubmitPath verifies spark-sql is
// derived as a sibling of the spark-submit path when no explicit path is set.
func TestSubmitClientMode_SparkSqlPathDerivedFromSubmitPath(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	ctx := context.Background()

	job := ClientModeJob{
		JobID:           "sql-derive-job",
		Namespace:       "jaiscloud",
		Image:           "spark-test:latest",
		EntryPoint:      SqlEntryPoint{Queries: []string{"SELECT 1"}},
		SparkSubmitPath: "/opt/spark/bin/spark-submit",
	}

	handle, err := SubmitClientMode(ctx, k8s, job)
	require.NoError(t, err)

	k8sJob, err := k8s.BatchV1().Jobs(job.Namespace).Get(ctx, handle.JobName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, k8sJob.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, []string{"/opt/spark/bin/spark-sql"}, k8sJob.Spec.Template.Spec.Containers[0].Command)
}

// TestSubmitClientMode_SQLDefaultPath verifies the default spark-sql binary
// when neither path is configured.
func TestSubmitClientMode_SQLDefaultPath(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	ctx := context.Background()

	job := ClientModeJob{
		JobID:      "sql-default-job",
		Namespace:  "jaiscloud",
		Image:      "spark-test:latest",
		EntryPoint: SqlEntryPoint{Queries: []string{"SELECT 1"}},
	}

	handle, err := SubmitClientMode(ctx, k8s, job)
	require.NoError(t, err)

	k8sJob, err := k8s.BatchV1().Jobs(job.Namespace).Get(ctx, handle.JobName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, k8sJob.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, []string{"spark-sql"}, k8sJob.Spec.Template.Spec.Containers[0].Command)
}

// ─── new BuildClientModeArgs tests ───────────────────────────────────────────

// TestBuildClientModeArgs_PodTemplateContainerName verifies the executor pod
// template container name conf is emitted so Spark injects image/command into
// the right container.
func TestBuildClientModeArgs_PodTemplateContainerName(t *testing.T) {
	args := BuildClientModeArgs(ClientModeJob{
		JobID: "j", Namespace: "ns", Image: "img:1",
		EntryPoint: JarEntryPoint{JarURI: "app.jar"},
	})
	cm := argsConfMap(args)
	assert.Equal(t, "spark-kubernetes-executor",
		cm["spark.kubernetes.executor.podTemplateContainerName"])
}

// TestBuildClientModeArgs_ContainerImageConf verifies spark.kubernetes.container.image
// is injected so executor pods use the correct image without an explicit --conf.
func TestBuildClientModeArgs_ContainerImageConf(t *testing.T) {
	args := BuildClientModeArgs(ClientModeJob{
		JobID: "j", Namespace: "ns", Image: "spark-custom:3.5",
		EntryPoint: JarEntryPoint{JarURI: "app.jar"},
	})
	assert.Equal(t, "spark-custom:3.5", argsConfMap(args)["spark.kubernetes.container.image"])
}

// TestBuildClientModeArgs_ServiceAccountDefault verifies fallback to "default".
func TestBuildClientModeArgs_ServiceAccountDefault(t *testing.T) {
	args := BuildClientModeArgs(ClientModeJob{
		JobID: "j", Namespace: "ns", Image: "img:1",
		EntryPoint: JarEntryPoint{JarURI: "app.jar"},
	})
	assert.Equal(t, "default",
		argsConfMap(args)["spark.kubernetes.authenticate.executor.serviceAccountName"])
}

// TestBuildClientModeArgs_ServiceAccountExplicit verifies that a non-empty
// ServiceAccountName is forwarded to the executor SA conf.
func TestBuildClientModeArgs_ServiceAccountExplicit(t *testing.T) {
	args := BuildClientModeArgs(ClientModeJob{
		JobID: "j", Namespace: "ns", Image: "img:1",
		ServiceAccountName: "spark-sa",
		EntryPoint:         JarEntryPoint{JarURI: "app.jar"},
	})
	assert.Equal(t, "spark-sa",
		argsConfMap(args)["spark.kubernetes.authenticate.executor.serviceAccountName"])
}

// TestBuildClientModeArgs_ClientConfPrecedence verifies that Spark's
// last-value-wins semantics give user-supplied SparkSubmitArgs precedence over
// JaisCloud's ExtraSparkConfs defaults. This applies to both EMR on EC2
// (spark_step.go) and EMR on EKS (jobrun.go), which both route through
// BuildClientModeArgs with ExtraSparkConfs from sparkaws.DriverSparkConfsFromEnv.
func TestBuildClientModeArgs_ClientConfPrecedence(t *testing.T) {
	const jaisCloudEndpoint = "http://jaiscloud:4566"
	const userEndpoint = "http://my-custom-minio:9000"

	job := ClientModeJob{
		JobID:      "j",
		Namespace:  "ns",
		Image:      "img:1",
		EntryPoint: JarEntryPoint{JarURI: "app.jar"},
		// JaisCloud injects its own s3a endpoint via ExtraSparkConfs.
		ExtraSparkConfs: []string{
			"--conf", "spark.hadoop.fs.s3a.endpoint=" + jaisCloudEndpoint,
		},
		// User wants a different endpoint — must win because it's appended later.
		SparkSubmitArgs: []string{
			"--conf", "spark.hadoop.fs.s3a.endpoint=" + userEndpoint,
		},
	}

	args := BuildClientModeArgs(job)

	// Find last occurrence of the key in the argv — that's what Spark honours.
	lastValue := ""
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "--conf" {
			kv := args[i+1]
			if len(kv) > len("spark.hadoop.fs.s3a.endpoint=") &&
				kv[:len("spark.hadoop.fs.s3a.endpoint=")] == "spark.hadoop.fs.s3a.endpoint=" {
				lastValue = kv[len("spark.hadoop.fs.s3a.endpoint="):]
			}
		}
	}
	assert.Equal(t, userEndpoint, lastValue,
		"SparkSubmitArgs must appear after ExtraSparkConfs so user conf wins via last-value-wins")
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// argsConfMap extracts --conf key=value pairs from a spark-submit argv slice
// into a map of key → value. When the same key appears more than once, the
// last occurrence wins (matching Spark's own last-value-wins behaviour).
func argsConfMap(args []string) map[string]string {
	out := make(map[string]string)
	for i := 0; i+1 < len(args); i++ {
		if args[i] != "--conf" {
			continue
		}
		kv := args[i+1]
		if eq := strings.IndexByte(kv, '='); eq >= 0 {
			out[kv[:eq]] = kv[eq+1:]
		}
		i++ // skip the value token
	}
	return out
}

// TestSubmitClientMode_ExecutorConfigMapHasTemplate verifies executor template is in ConfigMap.
func TestSubmitClientMode_ExecutorConfigMapHasTemplate(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	ctx := context.Background()

	callerExecTpl := []byte(`
spec:
  serviceAccountName: spark-executor-sa
`)

	job := ClientModeJob{
		JobID:                "exec-tpl-job",
		Namespace:            "jaiscloud",
		Image:                "spark-test:latest",
		EntryPoint:           JarEntryPoint{JarURI: "local:///app.jar"},
		CallerExecutorPodTpl: callerExecTpl,
	}

	_, err := SubmitClientMode(ctx, k8s, job)
	require.NoError(t, err)

	cmName := "spark-exec-tpl-" + sanitizeJobID(job.JobID)
	cm, err := k8s.CoreV1().ConfigMaps(job.Namespace).Get(ctx, cmName, metav1.GetOptions{})
	require.NoError(t, err)

	tplData := cm.Data[executorTemplateKey]
	assert.Contains(t, tplData, "spark-executor-sa")
}
