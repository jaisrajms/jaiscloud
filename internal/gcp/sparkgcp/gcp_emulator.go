package sparkgcp

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// gcpDriverEnvNames are the environment variable names DriverEnv mirrors into a
// job's driver/executor pods. They are also the executorEnv.* Spark conf keys
// the emulator owns (see ProtectedSparkConf).
var gcpDriverEnvNames = []string{
	"GOOGLE_CLOUD_PROJECT",
	"GOOGLE_CLOUD_PROJECT_ID",
	"GOOGLE_CLOUD_LOCATION",
	"STORAGE_EMULATOR_HOST",
	"GOOGLE_APPLICATION_CREDENTIALS",
}

// ProtectedSparkConf reports whether a spark-submit conf key belongs to the
// emulator's GCS connector wiring (DriverSparkConfsFromEnv). Caller-supplied
// job `properties` with these keys are dropped at the Dataproc layer so a
// caller cannot strip fs.gs.* / executorEnv.* and break gs:// access against
// the local emulator. Keys the emulator does not own (including the later
// metastore confs) are left to the caller.
func ProtectedSparkConf(key string) bool {
	if strings.HasPrefix(key, "spark.hadoop.fs.gs.") {
		return true
	}
	if key == "spark.hadoop.fs.AbstractFileSystem.gs.impl" {
		return true
	}
	for _, name := range gcpDriverEnvNames {
		if key == "spark.executorEnv."+name {
			return true
		}
	}
	return false
}

// GCPEmulatorConfig carries the GCP emulator endpoint wiring needed by Spark
// driver pods so that gs:// paths and the metadata (Hive Metastore) service hit
// the local emulator with zero code changes. All fields are optional; a nil
// config is a no-op.
//
// BigQuery is deliberately NOT wired here: although the emulator now evaluates
// a Standard SQL subset, it ships no BigQuery Storage Read/Write API, and the
// spark-bigquery connector reads via the Storage API, so pointing a Spark job
// at BigQuery would silently return no data. See
// docs/gcp-bigquery-sql-engine.md (BQ0).
type GCPEmulatorConfig struct {
	ProjectID string
	Region    string
	// GCSEndpoint is the GCS emulator host, surfaced as STORAGE_EMULATOR_HOST.
	// The GCS Hadoop connector honours this env var natively, so gs:// reads
	// resolve against the local emulator without any code change.
	GCSEndpoint string
	// Credentials is an optional path to a service-account JSON key file,
	// surfaced as GOOGLE_APPLICATION_CREDENTIALS. Empty → ADC (the emulator
	// falls back to project-only auth, matching the AWS SimpleAWSCredentials
	// provider with "test"/"test").
	Credentials string
}

// DriverEnv returns env vars to inject into the spark-submit driver container.
// Returns nil when cfg is nil.
func DriverEnv(cfg *GCPEmulatorConfig) []corev1.EnvVar {
	if cfg == nil {
		return nil
	}
	env := []corev1.EnvVar{
		{Name: "GOOGLE_CLOUD_PROJECT", Value: cfg.ProjectID},
		{Name: "GOOGLE_CLOUD_PROJECT_ID", Value: cfg.ProjectID},
	}
	if cfg.Region != "" {
		env = append(env, corev1.EnvVar{Name: "GOOGLE_CLOUD_LOCATION", Value: cfg.Region})
	}
	if cfg.GCSEndpoint != "" {
		env = append(env, corev1.EnvVar{Name: "STORAGE_EMULATOR_HOST", Value: cfg.GCSEndpoint})
	}
	if cfg.Credentials != "" {
		env = append(env, corev1.EnvVar{Name: "GOOGLE_APPLICATION_CREDENTIALS", Value: cfg.Credentials})
	}
	return env
}

// DriverSparkConfsFromEnv returns spark-submit --conf tokens wiring the GCS
// Hadoop connector so gs:// paths hit the local emulator. Accepts pre-computed
// driverEnv (from DriverEnv) so callers that already hold the slice avoid a
// second computation. Returns nil when cfg is nil.
//
// The fs.gs.* keys are Hadoop configuration properties; they are emitted with
// the spark.hadoop. prefix so Spark forwards them into the driver's Hadoop
// Configuration (Spark only propagates spark.hadoop.* into Hadoop conf). The
// STORAGE_EMULATOR_HOST env var is what actually redirects the connector to the
// local emulator, and it is mirrored into spark.executorEnv.* below so executor
// pods share the same wiring.
func DriverSparkConfsFromEnv(cfg *GCPEmulatorConfig, driverEnv []corev1.EnvVar) []string {
	if cfg == nil {
		return nil
	}
	project := cfg.ProjectID
	if project == "" {
		project = "test-project"
	}
	confs := []string{
		"--conf", "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
		"--conf", "spark.hadoop.fs.gs.project.id=" + project,
		"--conf", "spark.hadoop.fs.gs.auth.service.account.enable=false",
		"--conf", "spark.hadoop.fs.gs.auth.null.enable=true",
		"--conf", "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
	}
	if cfg.GCSEndpoint != "" {
		// The gcs-connector's gcsio layer does not honour STORAGE_EMULATOR_HOST;
		// its explicit JSON-API root URL must point at the emulator.
		confs = append(confs, "--conf", "spark.hadoop.fs.gs.storage.root.url="+cfg.GCSEndpoint)
	}
	// Mirror every driver env var into spark.executorEnv.* so executor pods
	// inherit the same GCP wiring.
	for _, e := range driverEnv {
		confs = append(confs, "--conf", fmt.Sprintf("spark.executorEnv.%s=%s", e.Name, e.Value))
	}
	return confs
}

// DriverSparkConfs returns spark-submit --conf tokens for GCS wiring.
// Use DriverSparkConfsFromEnv when DriverEnv has already been computed to avoid
// computing it twice per job submission.
func DriverSparkConfs(cfg *GCPEmulatorConfig) []string {
	return DriverSparkConfsFromEnv(cfg, DriverEnv(cfg))
}
