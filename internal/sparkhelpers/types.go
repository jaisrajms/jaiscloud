package sparkhelpers

import (
	"io"

	corev1 "k8s.io/api/core/v1"

	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/platform"
)

// EntryPoint is a sealed interface for Spark job entry points.
type EntryPoint interface{ isEntryPoint() }

// JarEntryPoint describes a JAR-based Spark job. JarFileURIs are additional
// jars added to the driver/executor classpath (dataproc.v1.SparkJob.jar_file_uris
// → spark-submit --jars).
type JarEntryPoint struct {
	JarURI      string
	MainClass   string
	JarFileURIs []string
}

func (JarEntryPoint) isEntryPoint() {}

// PythonEntryPoint describes a Python-based Spark job. JarFileURIs are
// additional jars added to the classpath (dataproc.v1.PySparkJob.jar_file_uris
// → spark-submit --jars).
type PythonEntryPoint struct {
	MainPythonFile string
	PyFiles        []string
	JarFileURIs    []string
}

func (PythonEntryPoint) isEntryPoint() {}

// REntryPoint describes an R-based Spark job.
type REntryPoint struct {
	MainRFile string
}

func (REntryPoint) isEntryPoint() {}

// SqlEntryPoint describes a Spark SQL CLI (spark-sql) job. Exactly one of
// Queries or FileURI is set: Queries become a single `-e` argument (joined with
// ";"), FileURI a `-f` argument (an HCFS/gs:// URI is resolved by the wired
// connector). JarFileURIs map to `--jars`; HiveVars map to `--hivevar k=v`.
type SqlEntryPoint struct {
	Queries     []string
	FileURI     string
	JarFileURIs []string
	HiveVars    map[string]string
}

func (SqlEntryPoint) isEntryPoint() {}

// ResourceProfile holds CPU/memory/count for a driver or executor.
type ResourceProfile struct {
	CPU    string
	Memory string
	Count  int
}

// ClientModeJob is the input to SubmitClientMode.
type ClientModeJob struct {
	JobID     string
	Namespace string
	// Image is the container image for the spark-submit driver pod. Required —
	// no sensible default exists (EMR and EMR-on-EKS use different images).
	Image             string
	EntryPoint        EntryPoint
	SparkSubmitArgs   []string
	JarArgs           []string
	DriverResources   ResourceProfile
	ExecutorResources ResourceProfile
	PlatformOverlay   *platform.PlatformConfig
	// CallerDriverPodTpl is a YAML PodTemplateSpec merged onto the spark-submit pod.
	CallerDriverPodTpl []byte
	// CallerExecutorPodTpl is a YAML PodTemplateSpec merged into the executor pod template ConfigMap.
	CallerExecutorPodTpl    []byte
	Labels                  map[string]string
	Annotations             map[string]string
	OwnerHint               *k8shelpers.OwnerRefHint
	LogSink                 io.Writer
	IdentityMutator         k8shelpers.IdentityMutator
	TTLSecondsAfterFinished *int32
	// SparkSubmitPath overrides the spark-submit binary path (default: "spark-submit").
	SparkSubmitPath string
	// Attempt is the 0-based restart attempt. It only affects the k8s object
	// names (the executor-template ConfigMap and the batch Job), which are
	// suffixed on retries so a restarted job does not collide with the objects
	// its previous attempt left behind.
	Attempt int
	// SparkSqlPath overrides the spark-sql binary path for a SqlEntryPoint
	// (default: "spark-sql", or a sibling of SparkSubmitPath when that is set;
	// the apache/spark image keeps it at /opt/spark/bin/spark-sql, off PATH).
	SparkSqlPath string
	// ExtraDriverEnv are env vars appended to the spark-submit driver container.
	// Providers build this from cloud-specific emulator config; sparkhelpers
	// is cloud-agnostic and forwards unchanged.
	ExtraDriverEnv []corev1.EnvVar
	// ExtraSparkConfs are spark-submit flag tokens (already paired as "--conf",
	// "key=value") prepended before the caller's SparkSubmitArgs so caller
	// confs win via Spark's last-value-wins semantics.
	ExtraSparkConfs    []string
	ServiceAccountName string
}

// Final is the terminal result of a Spark client-mode job.
// Embeds k8shelpers.Final (pod-level) and adds Spark-level classification.
type Final struct {
	k8shelpers.Final
	SparkSucceeded bool
	SparkReason    string
}
