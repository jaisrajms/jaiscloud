package config

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"

	"github.com/spf13/viper"
)

type Config struct {
	Port int
	// UI Portal
	UIEnabled   bool   // --ui / JAISCLOUD_UI
	UIPort      int    // --ui-port / JAISCLOUD_UI_PORT (default 4567)
	UIOpen      bool   // --ui-open / JAISCLOUD_UI_OPEN
	UIDevMode   bool   // JAISCLOUD_UI_DEV (dev mode: relaxed CORS, verbose errors)
	DevUIOrigin string // JAISCLOUD_DEV_UI_ORIGIN (default "http://localhost:5173")
	// Ephemeral disables all state persistence. State is lost on process exit.
	// Mutually exclusive with DSN. Intended for CI, unit tests, and throw-away runs.
	Ephemeral     bool
	Cloud         model.Cloud // Cloud provider to emulate: aws (default), azure, gcp
	LogLevel      string
	Region        string
	AccountID     string
	ExtraAccounts []string // additional account IDs available in the UI; JAISCLOUD_EXTRA_ACCOUNTS=111111111111,222222222222
	DSN           string   // PostgreSQL DSN; when set all state is stored in PostgreSQL
	// BlobDir is deprecated; use DataDir instead. Kept for backward compatibility.
	BlobDir string // Deprecated: use DataDir
	DataDir string // Root data directory for state.json saves and named snapshots (default: ~/.jaiscloud/<binary>)
	// FreshStart wipes existing state on startup before initializing stores.
	FreshStart bool
	// SnapshotInterval controls how often the file-backend snapshot loop saves state (default: 30s).
	SnapshotInterval time.Duration
	// ExportSoftLimit is the size threshold (bytes) above which export logs a warning (default: 2 GiB).
	ExportSoftLimit int64

	// ExecutorMode selects the container orchestrator for all executors (Spark + Lambda).
	// Values: "" (default, mock/instant) | "mock" | "docker" | "k8s".
	// Set via --executor-mode / JAISCLOUD_EXECUTOR_MODE.
	ExecutorMode string

	// KMS
	KMSMasterKey string // 32-byte hex KEK; if unset DEK is stored plaintext (dev only)

	// Lambda executor
	LambdaImage         string // override default runtime image
	LambdaNetwork       string // Docker network for Lambda containers (default: "jaiscloud-net")
	LambdaKeepaliveSecs int    // Docker warm container idle timeout in seconds (default: 300)

	// AWS emulator wiring for Spark drivers. When set, Spark driver pods route
	// S3/IMDS/creds calls at this HTTP(S) endpoint instead of real AWS. Works
	// against any K8s cluster and any S3-compatible endpoint; s3a SSL is
	// derived from the URL scheme.
	AWSEmulatorEndpoint string
	// S3VirtualHostBases are host suffixes the S3 codec treats as virtual-hosted
	// bases (in addition to amazonaws.com). Comma-separated env var.
	S3VirtualHostBases []string

	// IMDSEnabled turns on the AWS instance-metadata emulator endpoints at the
	// gateway. Requires Cloud == "aws". Independent of executor mode so the
	// same flag works for bare-metal SDK consumers pointing their metadata
	// service endpoint at JaisCloud.
	IMDSEnabled bool

	// GCP identity configuration (used by jaiscloud-gcp).
	// ProjectID is the GCP project used when a request carries no project in
	// its URL path or bearer token (the analogue of AWS AccountID).
	ProjectID string
	// GCPServiceAccount is the default service-account identity returned when
	// the bearer token carries no recognisable email/sub.
	GCPServiceAccount string
	// GCPMetadataEnabled turns on the GCP metadata-server emulator at the
	// gateway (analogue of AWS IMDS). Requires Cloud == "gcp".
	GCPMetadataEnabled bool
	// GCPTransports selects the enabled GCP wire transports globally
	// ("rest,grpc" default; also "rest", "grpc", "both", "none").
	GCPTransports string
	// GCPTransportOverrides refines GCPTransports per service, e.g.
	// "storage=grpc,pubsub=rest,memorystore=none".
	GCPTransportOverrides string

	// OIDCIssuers maps OIDC issuer URLs to their JWKS endpoint URLs.
	// Used by AssumeRoleWithWebIdentity to verify JWT signatures.
	// Env var: JAISCLOUD_OIDC_ISSUERS=issuer1=jwks_url1,issuer2=jwks_url2
	// If empty, JWT verification is skipped (back-compat for tests).
	OIDCIssuers map[string]string

	// K8s/Spark image configuration
	K8sNamespace     string
	K8sSparkImage    string
	K8sSparkSA       string
	SparkEMRImage    string
	SparkEMREKSImage string
	// K8sSparkSubmitPath overrides the spark-submit binary path inside the
	// Spark driver image (default "spark-submit"; the official apache/spark
	// image keeps it at /opt/spark/bin/spark-submit, off PATH).
	K8sSparkSubmitPath string
	// K8sSparkSqlPath overrides the spark-sql binary path used for Dataproc
	// sparkSqlJob driver pods (default: a sibling of K8sSparkSubmitPath).
	K8sSparkSqlPath string

	// Observability (opt-in)
	Metrics bool // expose /metrics endpoint
	Tracing bool // emit OTel traces to stdout

	// Deterministic mode
	Deterministic bool
	Seed          int64
	TimeStart     time.Time
	TimeMode      string // "frozen" or "offset"

	// Resolved at startup
	Clock      clock.Clock
	RandSource rand.Source
}

// splitCSV splits a comma-separated string into trimmed, non-empty tokens.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ExecutorMode returns the effective executor mode for a subsystem.
// Resolution: JAISCLOUD_{SUBSYSTEM}_EXECUTOR_MODE → JAISCLOUD_EXECUTOR_MODE → defaultMode.
func ExecutorMode(subsystem, defaultMode string) (mode, source string) {
	key := "JAISCLOUD_" + strings.ToUpper(subsystem) + "_EXECUTOR_MODE"
	if v := os.Getenv(key); v != "" {
		return v, key
	}
	if v := os.Getenv("JAISCLOUD_EXECUTOR_MODE"); v != "" {
		return v, "JAISCLOUD_EXECUTOR_MODE"
	}
	return defaultMode, "default"
}

func Load(cloud model.Cloud) (*Config, error) {
	// ── Common defaults (cloud-neutral) ─────────────────────────────────────
	viper.SetDefault("port", 4566)
	viper.SetDefault("ui", false)
	viper.SetDefault("ui_port", 4567)
	viper.SetDefault("ui_open", false)
	viper.SetDefault("ui_dev", false)
	viper.SetDefault("dev_ui_origin", "http://localhost:5173")
	viper.SetDefault("ephemeral", false)
	viper.SetDefault("log_level", "info")
	viper.SetDefault("region", "us-east-1")
	viper.SetDefault("account_id", "000000000000")
	viper.SetDefault("extra_accounts", "")
	viper.SetDefault("dsn", "")
	if home, err := os.UserHomeDir(); err == nil {
		viper.SetDefault("blob_dir", filepath.Join(home, ".jaiscloud", "blobs"))
		viper.SetDefault("data_dir", "")
	} else {
		viper.SetDefault("blob_dir", ".jaiscloud/blobs")
		viper.SetDefault("data_dir", "")
	}
	viper.SetDefault("fresh_start", false)
	viper.SetDefault("snapshot_interval", "30s")
	viper.SetDefault("export_soft_limit", int64(2*1024*1024*1024))
	viper.SetDefault("executor_mode", "")
	viper.SetDefault("metrics", false)
	viper.SetDefault("tracing", false)
	viper.SetDefault("deterministic", false)
	viper.SetDefault("seed", 0)
	viper.SetDefault("time_mode", "offset")

	// ── Shared Kubernetes/Spark executor config ──────────────────────────────
	// EMR and Dataproc both run Spark on Kubernetes; these keys are cloud-neutral
	// and read regardless of the active cloud.
	viper.SetDefault("k8s_namespace", "jaiscloud")
	viper.SetDefault("k8s_spark_image", "")
	viper.SetDefault("k8s_spark_sa", "")
	viper.SetDefault("k8s_spark_submit_path", "")
	viper.SetDefault("k8s_spark_sql_path", "")

	// ── Cloud-specific defaults ─────────────────────────────────────────────
	// Only the active cloud's keys are registered; the other cloud's
	// configuration is not supported.
	switch cloud {
	case model.CloudAWS:
		viper.SetDefault("kms_master_key", "")
		viper.SetDefault("spark_emr_image", "")
		viper.SetDefault("spark_emreks_image", "")
		viper.SetDefault("aws_emulator_endpoint", "")
		viper.SetDefault("s3_virtual_host_bases", "")
		viper.SetDefault("imds_enabled", false)
		viper.SetDefault("lambda_image", "")
		viper.SetDefault("lambda_network", "jaiscloud-net")
		viper.SetDefault("lambda_keepalive_secs", 300)
		viper.SetDefault("oidc_issuers", "")
	case model.CloudGCP:
		viper.SetDefault("gcp_project_id", "jaiscloud-project")
		viper.SetDefault("gcp_service_account", "jaiscloud@example.iam.gserviceaccount.com")
		viper.SetDefault("gcp_metadata_enabled", false)
		viper.SetDefault("transports", "rest,grpc")
		viper.SetDefault("transport_overrides", "")
	}

	viper.SetEnvPrefix("JAISCLOUD")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// Parse snapshot interval.
	snapshotInterval := 30 * time.Second
	if s := viper.GetString("snapshot_interval"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			snapshotInterval = d
		}
	}

	cfg := &Config{
		Cloud:              cloud,
		Port:               viper.GetInt("port"),
		UIEnabled:          viper.GetBool("ui"),
		UIPort:             viper.GetInt("ui_port"),
		UIOpen:             viper.GetBool("ui_open"),
		UIDevMode:          viper.GetBool("ui_dev"),
		DevUIOrigin:        viper.GetString("dev_ui_origin"),
		Ephemeral:          viper.GetBool("ephemeral"),
		LogLevel:           viper.GetString("log_level"),
		Region:             viper.GetString("region"),
		AccountID:          viper.GetString("account_id"),
		ExtraAccounts:      splitCSV(viper.GetString("extra_accounts")),
		DSN:                viper.GetString("dsn"),
		BlobDir:            viper.GetString("blob_dir"),
		DataDir:            viper.GetString("data_dir"),
		FreshStart:         viper.GetBool("fresh_start"),
		SnapshotInterval:   snapshotInterval,
		ExportSoftLimit:    viper.GetInt64("export_soft_limit"),
		ExecutorMode:       viper.GetString("executor_mode"),
		Metrics:            viper.GetBool("metrics"),
		Tracing:            viper.GetBool("tracing"),
		Deterministic:      viper.GetBool("deterministic"),
		Seed:               viper.GetInt64("seed"),
		TimeMode:           viper.GetString("time_mode"),
		K8sNamespace:       viper.GetString("k8s_namespace"),
		K8sSparkImage:      viper.GetString("k8s_spark_image"),
		K8sSparkSA:         viper.GetString("k8s_spark_sa"),
		K8sSparkSubmitPath: viper.GetString("k8s_spark_submit_path"),
		K8sSparkSqlPath:    viper.GetString("k8s_spark_sql_path"),
	}

	// Cloud-specific fields — only the active cloud's keys are read.
	switch cloud {
	case model.CloudAWS:
		cfg.KMSMasterKey = viper.GetString("kms_master_key")
		cfg.SparkEMRImage = viper.GetString("spark_emr_image")
		cfg.SparkEMREKSImage = viper.GetString("spark_emreks_image")
		cfg.AWSEmulatorEndpoint = viper.GetString("aws_emulator_endpoint")
		cfg.S3VirtualHostBases = splitCSV(viper.GetString("s3_virtual_host_bases"))
		cfg.IMDSEnabled = viper.GetBool("imds_enabled")
		cfg.LambdaImage = viper.GetString("lambda_image")
		cfg.LambdaNetwork = viper.GetString("lambda_network")
		cfg.LambdaKeepaliveSecs = viper.GetInt("lambda_keepalive_secs")
		if raw := viper.GetString("oidc_issuers"); raw != "" {
			cfg.OIDCIssuers = make(map[string]string)
			for _, pair := range strings.Split(raw, ",") {
				pair = strings.TrimSpace(pair)
				if k, v, ok := strings.Cut(pair, "="); ok {
					cfg.OIDCIssuers[strings.TrimSpace(k)] = strings.TrimSpace(v)
				}
			}
		}
	case model.CloudGCP:
		cfg.ProjectID = viper.GetString("gcp_project_id")
		cfg.GCPServiceAccount = viper.GetString("gcp_service_account")
		cfg.GCPMetadataEnabled = viper.GetBool("gcp_metadata_enabled")
		cfg.GCPTransports = viper.GetString("transports")
		cfg.GCPTransportOverrides = viper.GetString("transport_overrides")
	}

	if cfg.Ephemeral && cfg.DSN != "" {
		return nil, fmt.Errorf("--ephemeral and --dsn are mutually exclusive: ephemeral mode runs with no persistence, a DSN implies PostgreSQL storage")
	}

	// Parse base time for deterministic mode
	if ts := viper.GetString("time"); ts != "" {
		t, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			return nil, fmt.Errorf("invalid --time %q: %w", ts, err)
		}
		cfg.TimeStart = t
	} else {
		cfg.TimeStart = clock.RealNow()
	}

	// Resolve clock
	if cfg.Deterministic {
		cfg.RandSource = rand.NewSource(cfg.Seed)
		if cfg.TimeMode == "frozen" {
			cfg.Clock = clock.FixedClock{T: cfg.TimeStart}
		} else {
			cfg.Clock = clock.NewOffsetClock(cfg.TimeStart)
		}
	} else {
		cfg.Clock = clock.RealClock{}
	}

	return cfg, nil
}
