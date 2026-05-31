package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	awsadapter "jaiscloud/internal/aws/adapter"
	"jaiscloud/internal/aws/adapter/services"
	keyprovider "jaiscloud/internal/aws/key"
	paramprovider "jaiscloud/internal/aws/parameter"
	apigwprovider "jaiscloud/internal/aws/provider/apigw"
	cacheprovider "jaiscloud/internal/aws/provider/cache"
	"jaiscloud/internal/aws/provider/catalog"
	cloudwatchprovider "jaiscloud/internal/aws/provider/cloudwatch"
	cwlogs "jaiscloud/internal/aws/provider/cloudwatch/logs"
	"jaiscloud/internal/aws/provider/compute"
	containerprovider "jaiscloud/internal/aws/provider/container"
	"jaiscloud/internal/aws/provider/dns"
	ecrprovider "jaiscloud/internal/aws/provider/ecr"
	eksprovider "jaiscloud/internal/aws/provider/eks"
	emrprovider "jaiscloud/internal/aws/provider/emr"
	emrcontainersprovider "jaiscloud/internal/aws/provider/emroneks"
	eventsprovider "jaiscloud/internal/aws/provider/events"
	ebscheduler "jaiscloud/internal/aws/provider/events/scheduler"
	"jaiscloud/internal/aws/provider/events/targets"
	iamprovider "jaiscloud/internal/aws/provider/iam"
	kinesisprovider "jaiscloud/internal/aws/provider/kinesis"
	functionprovider "jaiscloud/internal/aws/provider/lambda"
	lambdaesm "jaiscloud/internal/aws/provider/lambda/esm"
	"jaiscloud/internal/aws/provider/notification"
	objectprovider "jaiscloud/internal/aws/provider/object"
	"jaiscloud/internal/aws/provider/queue"
	rdsprovider "jaiscloud/internal/aws/provider/rds"
	sparkaws "jaiscloud/internal/aws/provider/sparkaws"
	"jaiscloud/internal/aws/provider/stack/handlers"
	sfnprovider "jaiscloud/internal/aws/provider/stepfunctions"
	sfndispatcher "jaiscloud/internal/aws/provider/stepfunctions/dispatcher"
	sfnengine "jaiscloud/internal/aws/provider/stepfunctions/engine"
	awsstore "jaiscloud/internal/aws/store"
	ecrstore "jaiscloud/internal/aws/store/ecr"
	kinesisstore "jaiscloud/internal/aws/store/kinesis"
	sfnstore "jaiscloud/internal/aws/store/stepfunctions"
	stsprovider "jaiscloud/internal/aws/sts"
	"jaiscloud/internal/logstream"
	"jaiscloud/internal/workers"

	// Phase 15 providers
	acmprovider "jaiscloud/internal/aws/provider/acm"
	athenaprovider "jaiscloud/internal/aws/provider/athena"
	cloudfrontprovider "jaiscloud/internal/aws/provider/cloudfront"
	cognitoprovider "jaiscloud/internal/aws/provider/cognito"
	cognitoidentityprovider "jaiscloud/internal/aws/provider/cognitoidentity"
	firehoseprovider "jaiscloud/internal/aws/provider/firehose"
	redshiftprovider "jaiscloud/internal/aws/provider/redshift"
	sesprovider "jaiscloud/internal/aws/provider/ses"

	// G-PENDING new providers
	"jaiscloud/internal/adapter"
	ui "jaiscloud/internal/aws/ui"
	"jaiscloud/internal/admin"
	"jaiscloud/internal/clock"
	awsconfigprovider "jaiscloud/internal/aws/provider/awsconfig"
	elbv2provider "jaiscloud/internal/aws/provider/elbv2"
	resourcegroupsprovider "jaiscloud/internal/aws/provider/resourcegroups"
	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/aws/provider/table"
	taggingprovider "jaiscloud/internal/aws/provider/tagging"
	secretprovider "jaiscloud/internal/aws/secret"
	dynamostore "jaiscloud/internal/aws/store/dynamodb"
	objectstore "jaiscloud/internal/aws/store/object"
	s3store "jaiscloud/internal/aws/store/s3"
	sqsstore "jaiscloud/internal/aws/store/sqs"
	streamstore "jaiscloud/internal/aws/store/stream"
	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/certstore"
	"jaiscloud/internal/config"
	"jaiscloud/internal/events"
	ecsexec "jaiscloud/internal/executor/ecs"
	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/gateway"
	"jaiscloud/internal/model"
	"jaiscloud/internal/persistence/snapshot"
	snapversion "jaiscloud/internal/persistence/version"
	"jaiscloud/internal/platform"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/snapshottypes"
	"jaiscloud/internal/store"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	root := &cobra.Command{
		Use:   "jaiscloud-aws",
		Short: "JaisCloud AWS - local AWS emulator",
	}
	root.AddCommand(startCmd())
	root.AddCommand(versionCmd())
	root.AddCommand(envCmd())
	root.AddCommand(doctorCmd())
	root.AddCommand(resetCmd())
	root.AddCommand(exportCmd())
	root.AddCommand(importCmd())
	root.AddCommand(snapshotCmd())
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// ─── start ────────────────────────────────────────────────────────────────────

func startCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the emulator",
		RunE: func(cmd *cobra.Command, args []string) error {
			bindFlags(cmd)

			cfg, err := config.Load()
			if err != nil {
				return err
			}
			cfg.Cloud = model.CloudAWS
			clock.SetGlobalClock(cfg.Clock)

			ctx := context.Background()

			// Resolve instance ID before initStores so it can be used for the
			// session blob store directory name in memory mode.
			stateDir, _ := config.ResolveStateDir(os.Getenv("JAISCLOUD_STATE_DIR"))
			instanceID, idSource := config.LoadOrCreateInstanceID(stateDir)
			slog.Info("instance id", "id", instanceID, "source", idSource, "state_dir", stateDir)

			s, err := initStores(ctx, cfg, instanceID)
			if err != nil {
				return err
			}

			dek, err := bootstrapDEK(ctx, cfg, s)
			if err != nil {
				return err
			}

			platformCfg, err := platform.LoadFromEnv()
			if err != nil {
				return fmt.Errorf("platform config: %w", err)
			}

			ecrP := buildECRProvider(ctx, cfg, s)
			app := buildRegistry(ctx, cfg, s, dek, platformCfg, instanceID, ecrP)
			cleanup := app.Cleanup
			defer func() { cleanup() }()

			// Wire Step Functions execution engine — provides real ASL execution.
			sfnDisp := sfndispatcher.New(app.Registry, cfg)
			sfnEng := sfnengine.New(s.sfn, sfnDisp)
			app.SfnP.SetEngine(sfnEng)
			prevCleanup := cleanup
			cleanup = func() {
				shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = sfnEng.Shutdown(shutCtx)
				prevCleanup()
			}

			cloudAdapter := buildAWSAdapter(cfg.S3VirtualHostBases)
			adminHandler := buildAdminHandler(s, app.StreamStore, app.KeyStore, app.SecretStore, app.ParamStore, app.LambdaResetter, app.ESMProvider, app.EventsProvider, app.QueueP, app.LogsP, app.CWP, app.ComputeP)
			adminHandler.RegisterSnapshotter("cw_logs", app.LogsP)
			adminHandler.SetLambdaCodeFetcher(app.FuncP)
			adminHandler.SetCWAlarmEvaluator(app.CWP.Evaluator())
			adminHandler.SetFirehoseFlusher(app.FirehoseP)
			if app.TableP.TTLWorker() != nil {
				adminHandler.RegisterTTLSweeper(app.TableP.TTLWorker())
			}
			adminHandler.RegisterEBScheduler(app.Sched)
			adminHandler.SetMeta(admin.HandlerMeta{
				InstanceID: instanceID,
				Cloud:      "aws",
				Region:     cfg.Region,
				AccountID:  cfg.AccountID,
				StateDir:   stateDir,
			})
			// Wire blob store and KEK fingerprint for tarball export/import.
			// SetDataDir is called below after dataDir is resolved.
			if s.localBlobs != nil {
				adminHandler.RegisterBlobStore(s.localBlobs)
			} else if sb, ok := s.blobs.(admin.SnapshotBlobStore); ok {
				adminHandler.RegisterBlobStore(sb)
			}
			if cfg.KMSMasterKey != "" {
				// FingerprintKEK expects raw bytes; KMSMasterKey is a 32-byte hex string.
				kekBytes := []byte(cfg.KMSMasterKey)
				adminHandler.SetKEKFingerprint(snapversion.FingerprintKEK(kekBytes))
			}

			var certs certstore.CertStore
			if fsCS, err := certstore.NewFilesystemCertStore(stateDir); err == nil {
				certs = fsCS
			} else {
				slog.Warn("certstore: using in-memory store; TLS cert will regenerate on restart")
				certs = certstore.NewMemoryCertStore()
			}

			// Create persistence barrier — gates cloud requests during import/reset.
			barrier := snapshot.NewBarrier()
			adminHandler.SetBarrier(barrier)

			var gatewayOpts []func(*gateway.Server)
			gatewayOpts = append(gatewayOpts, gateway.WithBarrier(barrier))
			if cfg.IMDSEnabled {
				imdsCfg := awsadapter.IMDSConfig{
					Region:    cfg.Region,
					AccountID: cfg.AccountID,
					RoleName:  "jaiscloud-emulator-role",
				}
				gatewayOpts = append(gatewayOpts, gateway.WithExtraRoutes(func(r chi.Router) {
					awsadapter.RegisterIMDSRoutes(r, imdsCfg)
				}))
			}
			gatewayOpts = append(gatewayOpts, gateway.WithCORSLookup(app.ObjectP.GetBucketCORSRules))

			// ECR persistent mode: register OCI Distribution v2 routes before the wildcard.
			if ociHandler := ecrP.OCIHandler(); ociHandler != nil {
				gatewayOpts = append(gatewayOpts, gateway.WithExtraRoutes(func(r chi.Router) {
					r.HandleFunc("/v2/*", ociHandler)
					r.HandleFunc("/v2/", ociHandler)
				}))
			}

			// Resolve DataDir for admin endpoints (snapshot commands, export/import).
			dataDir := cfg.DataDir
			if dataDir == "" {
				if home, err := os.UserHomeDir(); err == nil {
					dataDir = filepath.Join(home, ".jaiscloud", "jaiscloud-aws")
				} else {
					dataDir = filepath.Join(".jaiscloud", "jaiscloud-aws")
				}
			}
			adminHandler.SetDataDir(dataDir)

			// Wire snapshot loop unless running in ephemeral mode or memory mode.
			// Attempts to restore state.json from a previous run, then starts periodic saves.
			if !cfg.Ephemeral && cfg.DSN == "" {
				adminSnaps := adminHandler.Snapshotters()
				loopStores := make(map[string]snapshottypes.Snapshotter, len(adminSnaps))
				for k, v := range adminSnaps {
					loopStores[k] = v
				}

				stateFile := filepath.Join(dataDir, "state.json")
				if stateData, readErr := os.ReadFile(stateFile); readErr == nil {
					var env snapversion.Envelope
					if parseErr := json.Unmarshal(stateData, &env); parseErr != nil {
						slog.Warn("startup: state.json parse failed; starting fresh", "err", parseErr)
					} else if versionErr := snapversion.CheckSnapshotVersion(env.SchemaVersion); versionErr != nil {
						return fmt.Errorf("startup: state.json version check failed: %w\nRun with --fresh-start to wipe state", versionErr)
					} else if env.Cloud != "" && env.Cloud != string(cfg.Cloud) {
						return fmt.Errorf("startup: state.json cloud mismatch: stored=%q running=%q", env.Cloud, cfg.Cloud)
					} else {
						for name, s := range loopStores {
							data, ok := env.Stores[name]
							if !ok {
								continue
							}
							if restoreErr := s.Restore(ctx, bytes.NewReader(data)); restoreErr != nil {
								slog.Warn("startup: restore store failed; skipping", "store", name, "err", restoreErr)
							}
						}
						slog.Info("startup: state restored from snapshot", "path", stateFile, "stores", len(env.Stores))
					}
				}

				loopCfg := snapshot.SnapshotLoopConfig{
					Barrier:     barrier,
					Stores:      loopStores,
					BlobStore:   s.localBlobs,
					DataDir:     dataDir,
					Interval:    cfg.SnapshotInterval,
					Clock:       cfg.Clock,
					SaveTimeout: 10 * time.Second,
				}
				loop := snapshot.NewSnapshotLoop(loopCfg)
				loopCtx, loopCancel := context.WithCancel(ctx)
				loop.Start(loopCtx)
				prevCleanup4 := cleanup
				cleanup = func() { loopCancel(); loop.Stop(); prevCleanup4() }
			}

			var uiServer *ui.UIServer
			if cfg.UIEnabled {
				uiProviders := &ui.AWSProviders{
					Queue:    app.QueueP,
					Function: app.FuncP,
					Logs:     app.LogsP,
				}
				var uiErr error
				uiServer, uiErr = ui.New(uiProviders, adminHandler, cfg, app.Bus, version)
				if uiErr != nil {
					slog.Warn("ui server init failed", "err", uiErr)
				} else if uiServer != nil {
					go func() {
						addr := fmt.Sprintf(":%d", cfg.UIPort)
						if err := uiServer.ListenAndServe(addr); err != nil && err != http.ErrServerClosed {
							slog.Warn("ui server stopped", "err", err)
						}
					}()
					if cfg.UIOpen {
						ui.OpenBrowser(fmt.Sprintf("http://localhost:%d/ui/", cfg.UIPort))
					}
					prevCleanup5 := cleanup
					cleanup = func() {
						shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						_ = uiServer.Shutdown(shutCtx)
						prevCleanup5()
					}
				}
			}

			srv := gateway.NewServer(cfg, adminHandler, app.Registry, cloudAdapter, certs, gatewayOpts...)
			_ = app.Bus
			return srv.ListenAndServe()
		},
	}

	cmd.Flags().Int("port", 4566, "Listen port")
	cmd.Flags().Bool("ephemeral", false, `Run with purely in-memory state — no periodic saves to disk.
	Use for CI, unit tests, and throw-away runs where a clean slate on every start is desired.
	Mutually exclusive with --dsn.
	Env var: JAISCLOUD_EPHEMERAL`)
	cmd.Flags().String("dsn", "", `PostgreSQL connection string. When set, all state is stored in PostgreSQL.
	Mutually exclusive with --ephemeral.
	Format:  postgres://USER:PASSWORD@HOST:PORT/DBNAME
	Example: postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud
	Env var: JAISCLOUD_DSN`)
	cmd.Flags().String("data-dir", "", `Root data directory for persistent-file backend.
	Defaults to ~/.jaiscloud/jaiscloud-aws.
	Env var: JAISCLOUD_DATA_DIR`)
	cmd.Flags().Bool("fresh-start", false, `Wipe existing state on startup before initializing stores.
	Env var: JAISCLOUD_FRESH_START`)
	cmd.Flags().String("log-level", "info", "Log level: debug/info/warn/error")
	cmd.Flags().Bool("metrics", false, "Expose Prometheus metrics at /metrics")
	cmd.Flags().Bool("tracing", false, "Emit OTel trace IDs in response headers")
	cmd.Flags().Bool("deterministic", false, "Enable deterministic mode")
	cmd.Flags().Int64("seed", 0, "Random seed (requires --deterministic)")
	cmd.Flags().String("time", "", "Base time RFC3339 (requires --deterministic)")
	cmd.Flags().String("time-mode", "offset", "Time mode: frozen or offset")
	cmd.Flags().String("blob-dir", "", `Directory for S3 blob bytes (persistent mode only).
	Defaults to ~/.jaiscloud/blobs.
	Env var: JAISCLOUD_BLOB_DIR`)
	cmd.Flags().String("kms-master-key", "", `32-byte hex KEK for KMS envelope encryption.
	If unset, DEK is stored plaintext (dev only).
	Env var: JAISCLOUD_KMS_MASTER_KEY`)
	cmd.Flags().String("executor-mode", "", `Container orchestrator for all executors (Spark + Lambda): mock, docker, or k8s.
	"" / unset: instant mock completion (no containers).  "mock": explicit mock.
	"docker": Docker daemon.  "k8s": Kubernetes (docker-desktop or in-cluster).
	Env var: JAISCLOUD_EXECUTOR_MODE`)
	cmd.Flags().String("lambda-image", "", `Override default Lambda runtime image.
	Env var: JAISCLOUD_LAMBDA_IMAGE`)
	cmd.Flags().String("lambda-network", "jaiscloud-net", `Docker network for Lambda containers.
	Env var: JAISCLOUD_LAMBDA_NETWORK`)
	cmd.Flags().Int("lambda-keepalive-secs", 300, `Docker warm container idle timeout in seconds.
	Env var: JAISCLOUD_LAMBDA_KEEPALIVE_SECS`)
	cmd.Flags().String("aws-emulator-endpoint", "", `AWS emulator endpoint for Spark driver pods (e.g. http://host:4566).
	Env var: JAISCLOUD_AWS_EMULATOR_ENDPOINT`)
	cmd.Flags().Bool("imds-enabled", false, `Enable the AWS instance-metadata emulator endpoints.
	Env var: JAISCLOUD_IMDS_ENABLED`)
	cmd.Flags().String("k8s-namespace", "jaiscloud", `Kubernetes namespace for Spark and Lambda workloads.
	Env var: JAISCLOUD_K8S_NAMESPACE`)
	cmd.Flags().String("k8s-spark-image", "", `Default container image for spark-submit driver pods.
	Env var: JAISCLOUD_K8S_SPARK_IMAGE`)
	cmd.Flags().String("k8s-spark-sa", "", `Kubernetes service account for Spark driver pods.
	Env var: JAISCLOUD_K8S_SPARK_SA`)
	cmd.Flags().String("spark-emr-image", "", `Container image for EMR on EC2 spark-submit pods (overrides k8s-spark-image).
	Env var: JAISCLOUD_SPARK_EMR_IMAGE`)
	cmd.Flags().String("spark-emreks-image", "", `Container image for EMR on EKS spark-submit pods (overrides k8s-spark-image).
	Env var: JAISCLOUD_SPARK_EMREKS_IMAGE`)
	cmd.Flags().String("s3-virtual-host-bases", "", `Comma-separated host suffixes treated as S3 virtual-hosted bases.
	Env var: JAISCLOUD_S3_VIRTUAL_HOST_BASES`)
	cmd.Flags().Bool("ui", false, "Enable the UI Portal listener on --ui-port (default 4567)")
	cmd.Flags().Int("ui-port", 4567, "Port for the UI Portal HTTP listener")
	cmd.Flags().Bool("ui-open", false, "Auto-open browser when UI starts (no-op when not a TTY)")

	return cmd
}

// bindFlags binds all cobra flags to their viper keys so config.Load() picks
// them up. Must be called at the start of RunE, after flag parsing is complete.
func bindFlags(cmd *cobra.Command) {
	viper.BindPFlag("port", cmd.Flags().Lookup("port"))
	viper.BindPFlag("ephemeral", cmd.Flags().Lookup("ephemeral"))
	viper.BindPFlag("dsn", cmd.Flags().Lookup("dsn"))
	viper.BindPFlag("data_dir", cmd.Flags().Lookup("data-dir"))
	viper.BindPFlag("fresh_start", cmd.Flags().Lookup("fresh-start"))
	viper.BindPFlag("log_level", cmd.Flags().Lookup("log-level"))
	viper.BindPFlag("metrics", cmd.Flags().Lookup("metrics"))
	viper.BindPFlag("tracing", cmd.Flags().Lookup("tracing"))
	viper.BindPFlag("deterministic", cmd.Flags().Lookup("deterministic"))
	viper.BindPFlag("seed", cmd.Flags().Lookup("seed"))
	viper.BindPFlag("time", cmd.Flags().Lookup("time"))
	viper.BindPFlag("time_mode", cmd.Flags().Lookup("time-mode"))
	viper.BindPFlag("blob_dir", cmd.Flags().Lookup("blob-dir"))
	viper.BindPFlag("kms_master_key", cmd.Flags().Lookup("kms-master-key"))
	viper.BindPFlag("executor_mode", cmd.Flags().Lookup("executor-mode"))
	viper.BindPFlag("lambda_image", cmd.Flags().Lookup("lambda-image"))
	viper.BindPFlag("lambda_network", cmd.Flags().Lookup("lambda-network"))
	viper.BindPFlag("lambda_keepalive_secs", cmd.Flags().Lookup("lambda-keepalive-secs"))
	viper.BindPFlag("aws_emulator_endpoint", cmd.Flags().Lookup("aws-emulator-endpoint"))
	viper.BindPFlag("imds_enabled", cmd.Flags().Lookup("imds-enabled"))
	viper.BindPFlag("k8s_namespace", cmd.Flags().Lookup("k8s-namespace"))
	viper.BindPFlag("k8s_spark_image", cmd.Flags().Lookup("k8s-spark-image"))
	viper.BindPFlag("k8s_spark_sa", cmd.Flags().Lookup("k8s-spark-sa"))
	viper.BindPFlag("spark_emr_image", cmd.Flags().Lookup("spark-emr-image"))
	viper.BindPFlag("spark_emreks_image", cmd.Flags().Lookup("spark-emreks-image"))
	viper.BindPFlag("s3_virtual_host_bases", cmd.Flags().Lookup("s3-virtual-host-bases"))
	viper.BindPFlag("ui", cmd.Flags().Lookup("ui"))
	viper.BindPFlag("ui_port", cmd.Flags().Lookup("ui-port"))
	viper.BindPFlag("ui_open", cmd.Flags().Lookup("ui-open"))
}

// AppContext holds every provider and service created by buildRegistry.
// Replacing 16 individual return values with a single struct keeps the wiring
// function signature manageable as new services are added.
type AppContext struct {
	Registry       *provider.Registry
	StreamStore    *streamstore.MemoryStreamStore
	Bus            *events.EventBus
	KeyStore       keyprovider.KeyStore
	SecretStore    secretprovider.SecretStore
	ParamStore     paramprovider.ParameterStore
	LambdaResetter admin.Resetter
	ESMProvider    admin.Resetter
	EventsProvider admin.Resetter
	Cleanup        func()
	ObjectP        *objectprovider.ObjectProvider
	QueueP         *queue.QueueProvider
	LogsP          *cwlogs.Provider
	SfnP           *sfnprovider.Provider
	CWP            *cloudwatchprovider.Provider
	FuncP          *functionprovider.FunctionProvider
	FirehoseP      *firehoseprovider.Provider
	ComputeP       *compute.ComputeProvider
	TableP         *table.TableProvider
	Sched          *ebscheduler.Scheduler
}

// appStores holds all store instances that the server depends on.
type appStores struct {
	resources store.ResourceStore
	messages  sqsstore.SQSMessageStore
	dynamo    dynamostore.DynamoDBItemStore
	s3Meta    objectstore.ObjectMetaStore
	blobs     blobfs.BlobStore
	// localBlobs holds the concrete *LocalFSBlobStore when in session or persistent
	// file mode, so it can be registered with the admin handler for tarball export/import.
	// nil when using MemoryBlobStore (no disk backing).
	localBlobs *blobfs.LocalFSBlobStore
	secrets    secretprovider.SecretStore
	parameters paramprovider.ParameterStore
	stsSession *stsprovider.MemorySessionStore
	kinesis    *kinesisstore.MemoryKinesisStore
	ecr        *ecrstore.MemoryECRStore
	sfn        *sfnstore.MemoryStepFunctionsStore
}

// initStores constructs the store layer based on configuration:
//   - DSN set → PostgreSQL-backed stores
//   - Ephemeral → pure in-memory stores, no disk writes
//   - Default → in-memory stores with session blob dir; snapshot loop (wired in startCmd)
//     saves state to data-dir/state.json periodically and restores it on startup
func initStores(ctx context.Context, cfg *config.Config, instanceID string) (appStores, error) {
	if cfg.DSN != "" {
		slog.Info("starting with postgres backend", "dsn", cfg.DSN)
		pgStore, err := store.NewPostgresResourceStore(ctx, cfg.DSN, string(cfg.Cloud))
		if err != nil {
			return appStores{}, fmt.Errorf("postgres: %w", err)
		}
		if err := store.RunMigrations(ctx, pgStore.Pool(), string(cfg.Cloud), awsstore.MigrationFS, "aws"); err != nil {
			pgStore.Close()
			return appStores{}, fmt.Errorf("aws migrations: %w", err)
		}
		pool := pgStore.Pool()
		blobs, err := blobfs.NewLocalFSBlobStore(cfg.BlobDir)
		if err != nil {
			pgStore.Close()
			return appStores{}, fmt.Errorf("blobfs: %w", err)
		}
		slog.Info("blob storage", "dir", cfg.BlobDir)
		return appStores{
			resources:  pgStore,
			messages:   sqsstore.NewPostgresSQSMessageStore(pool),
			dynamo:     dynamostore.NewPostgresDynamoDBItemStore(pool),
			s3Meta:     s3store.NewPostgresS3ObjectMetaStore(pool),
			blobs:      blobs,
			localBlobs: blobs,
			secrets:    secretprovider.NewPostgresSecretStore(pool),
			parameters: paramprovider.NewPostgresParameterStore(pool),
			stsSession: stsprovider.NewMemorySessionStore(),
			kinesis:    kinesisstore.NewMemoryKinesisStore(),
			ecr:        ecrstore.NewMemoryECRStore(),
			sfn:        sfnstore.NewMemoryStepFunctionsStore(),
		}, nil
	}

	if cfg.Ephemeral {
		slog.Info("starting in ephemeral mode: state will not be persisted")
		return appStores{
			resources:  store.NewMemoryResourceStore(),
			messages:   sqsstore.NewBundledSQSStore(),
			dynamo:     dynamostore.NewBundledDynamoDBItemStore(),
			s3Meta:     s3store.NewMemoryS3ObjectMetaStore(),
			blobs:      blobfs.NewMemoryBlobStore(),
			secrets:    secretprovider.NewMemorySecretStore(),
			parameters: paramprovider.NewMemoryParameterStore(),
			stsSession: stsprovider.NewMemorySessionStore(),
			kinesis:    kinesisstore.NewMemoryKinesisStore(),
			ecr:        ecrstore.NewMemoryECRStore(),
			sfn:        sfnstore.NewMemoryStepFunctionsStore(),
		}, nil
	}

	// Default: memory stores + session blob dir; snapshot loop saves state.json periodically.
	slog.Info("starting with memory stores: state will be saved periodically to state.json")
	sessionBlobs, err := blobfs.NewSessionBlobStore(instanceID)
	if err != nil {
		slog.Warn("session blob store unavailable, falling back to MemoryBlobStore", "err", err)
		return appStores{
			resources:  store.NewMemoryResourceStore(),
			messages:   sqsstore.NewBundledSQSStore(),
			dynamo:     dynamostore.NewBundledDynamoDBItemStore(),
			s3Meta:     s3store.NewMemoryS3ObjectMetaStore(),
			blobs:      blobfs.NewMemoryBlobStore(),
			secrets:    secretprovider.NewMemorySecretStore(),
			parameters: paramprovider.NewMemoryParameterStore(),
			stsSession: stsprovider.NewMemorySessionStore(),
			kinesis:    kinesisstore.NewMemoryKinesisStore(),
			ecr:        ecrstore.NewMemoryECRStore(),
			sfn:        sfnstore.NewMemoryStepFunctionsStore(),
		}, nil
	}
	slog.Info("session blob storage", "dir", sessionBlobs.BaseDir())
	return appStores{
		resources:  store.NewMemoryResourceStore(),
		messages:   sqsstore.NewBundledSQSStore(),
		dynamo:     dynamostore.NewBundledDynamoDBItemStore(),
		s3Meta:     s3store.NewMemoryS3ObjectMetaStore(),
		blobs:      sessionBlobs,
		localBlobs: sessionBlobs,
		secrets:    secretprovider.NewMemorySecretStore(),
		parameters: paramprovider.NewMemoryParameterStore(),
		stsSession: stsprovider.NewMemorySessionStore(),
		kinesis:    kinesisstore.NewMemoryKinesisStore(),
		ecr:        ecrstore.NewMemoryECRStore(),
		sfn:        sfnstore.NewMemoryStepFunctionsStore(),
	}, nil
}

// bootstrapDEK loads or creates the server data-encryption key.
// With a DSN (Postgres backend) the DEK is persisted in PostgreSQL, wrapped by KMSMasterKey if set.
// Otherwise a fresh ephemeral key is generated each startup.
func bootstrapDEK(ctx context.Context, cfg *config.Config, s appStores) ([]byte, error) {
	if cfg.DSN != "" {
		pgStore := s.resources.(*store.PostgresResourceStore)
		keyStore := keyprovider.NewPostgresKeyStore(pgStore.Pool(), nil)
		dek, err := keyprovider.LoadOrCreateDEK(ctx, keyStore, cfg.KMSMasterKey)
		if err != nil {
			return nil, fmt.Errorf("kms bootstrap: %w", err)
		}
		return dek, nil
	}
	dek, err := keyprovider.Generate32()
	if err != nil {
		return nil, fmt.Errorf("kms bootstrap: generate ephemeral DEK: %w", err)
	}
	return dek, nil
}

// buildRegistry wires all providers and returns an AppContext holding the
// populated registry, all provider references, and a cleanup func.
func buildRegistry(ctx context.Context, cfg *config.Config, s appStores, dek []byte, platformCfg *platform.PlatformConfig, instanceID string, ecrP *ecrprovider.Provider) *AppContext {
	bus := events.NewEventBus()
	streams := streamstore.NewMemoryStreamStore()

	sparkMode, _ := config.ExecutorMode("spark", "mock")

	s3Fetcher := blobfs.NewS3BlobFetcher(s.blobs)
	bootstrapPrefixes := []string{"/etc/pki", "/home/hadoop"}
	if v := os.Getenv("JAISCLOUD_BOOTSTRAP_RELOCATE_PREFIXES"); v != "" {
		bootstrapPrefixes = nil
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				bootstrapPrefixes = append(bootstrapPrefixes, p)
			}
		}
	}
	bootstrapImage := "amazon/aws-cli:2.18"
	if v := os.Getenv("JAISCLOUD_BOOTSTRAP_IMAGE"); v != "" {
		bootstrapImage = v
	}
	bootstrapMaxBytes := int64(1024 * 1024)
	if v := os.Getenv("JAISCLOUD_BOOTSTRAP_SCRIPT_MAX_BYTES"); v != "" {
		if n, parseErr := strconv.ParseInt(v, 10, 64); parseErr == nil {
			bootstrapMaxBytes = n
		} else {
			slog.Warn("invalid JAISCLOUD_BOOTSTRAP_SCRIPT_MAX_BYTES; using default 1 MiB", "value", v, "err", parseErr)
		}
	}
	bootstrapCfg := emrprovider.BootstrapConfig{
		Image:    bootstrapImage,
		MaxBytes: bootstrapMaxBytes,
		Prefixes: bootstrapPrefixes,
	}

	emrImage := cfg.SparkEMRImage
	if emrImage == "" {
		emrImage = cfg.K8sSparkImage
	}
	emrcImage := cfg.SparkEMREKSImage
	if emrcImage == "" {
		emrcImage = cfg.K8sSparkImage
	}

	if sparkMode == "k8s" {
		if emrImage == "" {
			slog.Error("emr: JAISCLOUD_K8S_SPARK_IMAGE (or JAISCLOUD_SPARK_EMR_IMAGE) is required when executor mode is k8s")
			os.Exit(1)
		}
		if emrcImage == "" {
			slog.Error("emroneks: JAISCLOUD_K8S_SPARK_IMAGE (or JAISCLOUD_SPARK_EMREKS_IMAGE) is required when executor mode is k8s")
			os.Exit(1)
		}
	}

	var emrOpts []emrprovider.Option
	var emrcOpts []emrcontainersprovider.Option
	emrOpts = append(emrOpts, emrprovider.WithBootstrap(s3Fetcher, bootstrapCfg))
	if emrImage != "" {
		emrOpts = append(emrOpts, emrprovider.WithSparkImage(emrImage))
	}
	if emrcImage != "" {
		emrcOpts = append(emrcOpts, emrcontainersprovider.WithSparkImage(emrcImage))
	}

	emrOpts = append(emrOpts, emrprovider.WithInstanceID(instanceID))
	emrcOpts = append(emrcOpts, emrcontainersprovider.WithInstanceID(instanceID))

	if cfg.K8sSparkSA != "" {
		emrOpts = append(emrOpts, emrprovider.WithServiceAccountName(cfg.K8sSparkSA))
		emrcOpts = append(emrcOpts, emrcontainersprovider.WithServiceAccountName(cfg.K8sSparkSA))
	}

	if cfg.AWSEmulatorEndpoint != "" {
		imdsEP := ""
		if cfg.IMDSEnabled {
			imdsEP = cfg.AWSEmulatorEndpoint
		}
		emulatorCfg := &sparkaws.AWSEmulatorConfig{
			Region:       cfg.Region,
			AccountID:    cfg.AccountID,
			S3Endpoint:   cfg.AWSEmulatorEndpoint,
			IMDSEndpoint: imdsEP,
		}
		emrOpts = append(emrOpts, emrprovider.WithAWSEmulator(emulatorCfg))
		emrcOpts = append(emrcOpts, emrcontainersprovider.WithAWSEmulator(emulatorCfg))
	}

	if sparkMode == "k8s" {
		k8sNS := cfg.K8sNamespace
		if k8sNS == "" {
			k8sNS = "jaiscloud"
		}
		if k8sClient, err := buildK8sClient(); err != nil {
			slog.Warn("emr: failed to build k8s client; falling back to mock", "err", err)
		} else {
			emrOpts = append(emrOpts, emrprovider.WithK8s(k8sClient, k8sNS, platformCfg))
			emrcOpts = append(emrcOpts, emrcontainersprovider.WithK8s(k8sClient, k8sNS, platformCfg))
		}
	}

	emrP := emrprovider.New(s.resources, bus, emrOpts...)
	emrcP := emrcontainersprovider.New(s.resources, bus, emrcOpts...)

	cleanup := func() {}

	tableProvider := table.NewWithTTL(s.resources, s.dynamo, streams, string(cfg.Cloud), cfg.Region, cfg.AccountID, time.Hour)

	var keyStore keyprovider.KeyStore
	if cfg.DSN != "" {
		pgStore := s.resources.(*store.PostgresResourceStore)
		keyStore = keyprovider.NewPostgresKeyStore(pgStore.Pool(), dek)
	} else {
		keyStore = keyprovider.NewMemoryKeyStore()
	}
	keyProv := keyprovider.New(keyStore, nil, dek)
	kmsEncryptor := keyProv.AsKeyEncryptor()
	secretProv := secretprovider.New(s.secrets, kmsEncryptor)
	paramProv := paramprovider.New(s.parameters, kmsEncryptor)

	lambdaMode, lambdaModeSrc := config.ExecutorMode("lambda", "mock")
	lambdaCfg := lambdaexec.DefaultLambdaConfig()
	lambdaCfg.Mode = lambdaMode
	lambdaCfg.Region = cfg.Region
	lambdaCfg.InstanceID = instanceID
	if cfg.LambdaImage != "" {
		lambdaCfg.DefaultImage = cfg.LambdaImage
	}
	if cfg.LambdaNetwork != "" {
		lambdaCfg.Network = cfg.LambdaNetwork
	}
	if cfg.LambdaKeepaliveSecs > 0 {
		lambdaCfg.KeepaliveSecs = cfg.LambdaKeepaliveSecs
	}
	if v := os.Getenv("JAISCLOUD_ENDPOINT"); v != "" {
		lambdaCfg.JaisCloudEndpoint = v
	}
	lambdaCfg = lambdaexec.LambdaConfigFrom(lambdaCfg)
	var lambdaExec lambdaexec.LambdaExecutor
	switch lambdaMode {
	case "docker":
		lambdaExec = lambdaexec.NewDockerExecutor(lambdaCfg, platformCfg)
	case "k8s":
		lambdaExec = lambdaexec.NewK8sExecutor(lambdaCfg, platformCfg)
	default:
		lambdaExec = lambdaexec.NewExecutor(lambdaCfg)
	}
	slog.Info("lambda executor", "mode", lambdaMode, "source", lambdaModeSrc)
	prevCleanup := cleanup
	cleanup = func() { lambdaExec.Close(); prevCleanup() }

	funcP := functionprovider.NewWithLimits(s.resources, lambdaExec, lambdaCfg)
	queueP := queue.New(s.resources, s.messages, bus)
	iamP := iamprovider.New(s.resources)
	stsP := stsprovider.NewWithOIDC(s.stsSession, cfg.OIDCIssuers)
	kinesisP := buildKinesisProvider(ctx, cfg, s)
	notifP := notification.New(s.resources, s.messages, bus)
	notifP.SetLambdaInvoker(funcP)
	notifP.SetSQSSender(sqsSenderAdapter{q: queueP})
	objectP := objectprovider.NewWithBus(s.s3Meta, s.blobs, bus).WithResourceStore(s.resources)
	stackP := stackprovider.New(s.resources)

	esmProvider := lambdaesm.New(ctx, s.resources, funcP, queueP, streams, slog.Default())
	esmProvider.SetSQSSender(esmSQSSenderAdapter{q: queueP})
	esmProvider.RehydratePollers(ctx)
	prevCleanup2 := cleanup
	cleanup = func() { esmProvider.Shutdown(ctx); funcP.Shutdown(ctx); prevCleanup2() }

	// ── Construct providers that need cross-service wiring before registration ──
	sfnP := sfnprovider.New(s.sfn)
	glueP := catalog.New(s.resources)
	glueP.SetObjectProvider(objectP)
	// TODO: seedDefaultVPC runs at construction time, before any request arrives,
	// so there is no NormalizedRequest to supply account/region. Refactor to lazy
	// seeding (seed on first DescribeVpcs per account+region) to remove this coupling
	// and support multi-account EC2 correctly.
	computeP := compute.New(s.resources, cfg.AccountID, cfg.Region)
	ecsP := containerprovider.New(s.resources)
	emrP.SetObjectProvider(objectP)
	emrcP.SetObjectProvider(objectP)
	eventsP := eventsprovider.New(s.resources, s.messages, bus).WithPort(cfg.Port)
	apigwP := apigwprovider.New(s.resources)
	cwP := cloudwatchprovider.New(s.resources, bus)
	logsProvider := cwlogs.New()
	sesP := sesprovider.New(s.resources)
	firehoseP := firehoseprovider.New(s.resources).WithS3Meta(s.s3Meta).WithS3Writer(objectP)

	// Wire code loader and CW Logs ingestor into real Lambda executors.
	if dockerExec, ok := lambdaExec.(*lambdaexec.DockerExecutor); ok {
		dockerExec.SetCodeLoader(funcP)
		dockerExec.SetLogsAPI(logsProvider)
	}
	if k8sExec, ok := lambdaExec.(*lambdaexec.K8sExecutor); ok {
		k8sExec.SetCodeLoader(funcP)
		k8sExec.SetLogsAPI(logsProvider)
	}
	// Wire CW Logs ingestor into FunctionProvider so all invocations (including mock)
	// write START/END/REPORT platform logs to /aws/lambda/{name}.
	funcP.SetLogsAPI(logsProvider)

	// Wire ECS executor.
	ecsMode, _ := config.ExecutorMode("ecs", "mock")
	var ecsExec ecsexec.Executor
	switch ecsMode {
	case "docker":
		ecsExec = ecsexec.New(ecsexec.ModeDocker, logsProvider)
	case "k8s":
		ecsExec = ecsexec.New(ecsexec.ModeK8s, logsProvider)
	default:
		ecsExec = ecsexec.New(ecsexec.ModeMock, nil)
	}
	ecsP.SetExecutor(ecsExec)
	if cfg.AWSEmulatorEndpoint != "" {
		ecsP.SetJaisCloudEndpoint(cfg.AWSEmulatorEndpoint)
	}

	firehoseP.Start()

	// ── Register all providers via the builder chain ──────────────────────────
	// All providers are fully constructed and wired above; registration is a
	// single declarative block. Adding a new service requires one Register call.
	registry := provider.NewRegistry().
		Register(keyProv).
		Register(secretProv).
		Register(paramProv).
		Register(funcP).
		Register(esmProvider).
		Register(queueP).
		Register(iamP).
		Register(stsP).
		Register(kinesisP).
		Register(ecrP).
		Register(sfnP).
		Register(notifP).
		Register(tableProvider).
		RegisterAll(tableProvider.StreamRoutes()). // StreamRoutes is a second route set on the same provider
		Register(objectP).
		Register(glueP).
		Register(computeP).
		Register(ecsP).
		Register(stackP).
		Register(emrP).
		Register(emrcP).
		Register(eventsP).
		Register(apigwP).
		Register(cwP).
		Register(logsProvider).
		Register(sesP).
		Register(firehoseP)
	registerStatelessProviders(registry, s.resources)

	// Second-pass cross-service wiring.
	objectP.SetFanout(objectprovider.S3FanoutConfig{
		SQS:          queueP,
		SNSPublisher: notifP,
		Lambda:       funcP,
	})
	cwP.SetSNSPublisher(notifP)
	cwP.SetLambdaInvoker(funcP)
	logsProvider.SetSubscriptionDispatcher(funcP)
	logsProvider.SetMetricDataPutter(&cwMetricAdapter{cwP})
	emrcP.SetLogsIngestor(logsProvider)
	sfnP.SetLogsIngestor(logsProvider)
	secretProv.SetInvoker(funcP)
	paramProv.SetEventPublisher(eventsP)
	paramProv.SetSecretGetter(secretProv)

	// Wire EventBridge target dispatcher and scheduler.
	tgtDisp := targets.New(queueP, funcP, notifP, &logsWriterAdapter{logsProvider}, eventsP)
	eventsP.SetTargetDispatcher(tgtDisp)
	sched := ebscheduler.New(tgtDisp)
	eventsP.SetScheduler(sched)

	// Wire SQS DLQ sender into the async queue.
	funcP.SetAsyncSQSSend(func(ctx context.Context, arn string, body string) error {
		return queueP.InternalSend(ctx, arn, body, nil, queue.SourceContext{
			SourceArn:        "lambda.amazonaws.com",
			ServicePrincipal: "lambda.amazonaws.com",
		})
	})

	// Wire workers registry — start all background workers.
	workerReg := workers.New()
	workerReg.Add("eventbridge-scheduler", sched)
	workerReg.Add("cw-alarm-evaluator", cwP.Evaluator())
	workerReg.Add("lambda-async-queue", funcP.AsyncQueue())
	workerReg.Start(ctx)
	prevCleanup3 := cleanup
	cleanup = func() { workerReg.Stop(); prevCleanup3() }

	registerCFNHandlers(stackP, queueP, notifP, objectP, tableProvider, iamP, funcP, keyProv, secretProv, paramProv,
		logsProvider, cwP, eventsP, ecsP, sfnP, esmProvider, apigwP, computeP)

	return &AppContext{
		Registry:       registry,
		StreamStore:    streams,
		Bus:            bus,
		KeyStore:       keyStore,
		SecretStore:    s.secrets,
		ParamStore:     s.parameters,
		LambdaResetter: lambdaExec,
		ESMProvider:    esmProvider,
		EventsProvider: eventsP,
		Cleanup:        cleanup,
		ObjectP:        objectP,
		QueueP:         queueP,
		LogsP:          logsProvider,
		SfnP:           sfnP,
		CWP:            cwP,
		FuncP:          funcP,
		FirehoseP:      firehoseP,
		ComputeP:       computeP,
		TableP:         tableProvider,
		Sched:          sched,
	}
}

// registerStatelessProviders registers providers whose only constructor dependency
// is a ResourceStore and that require no post-construction Set* wiring calls.
// Adding a new AWS service with no cross-service dependencies requires one line
// here rather than touching the buildRegistry body.
//
// Invariant: every provider listed here must have a New(store.ResourceStore)
// constructor and must NOT require any post-construction Set* calls. If a provider
// grows cross-service dependencies, move it into buildRegistry with explicit wiring.
func registerStatelessProviders(reg *provider.Registry, res store.ResourceStore) {
	reg.Register(dns.New(res))
	reg.Register(rdsprovider.New(res))
	reg.Register(cacheprovider.New(res))
	reg.Register(eksprovider.New(res))
	reg.Register(cognitoprovider.New(res))
	reg.Register(cognitoidentityprovider.New(res))
	reg.Register(acmprovider.New(res))
	reg.Register(cloudfrontprovider.New(res))
	reg.Register(athenaprovider.New(res))
	reg.Register(redshiftprovider.New(res))
	reg.Register(elbv2provider.New(res))
	reg.Register(awsconfigprovider.New(res))
	reg.Register(resourcegroupsprovider.New(res))
	reg.Register(taggingprovider.New(res))
}

// cwMetricAdapter bridges cloudwatch.Provider.InternalPutMetricData (uses cloudwatch.MetricDatum)
// to the cwlogs.MetricDataPutter interface (uses logs.MetricDatum). Both types are structurally
// identical; the adapter copies fields to satisfy the distinct type system.
type cwMetricAdapter struct{ p *cloudwatchprovider.Provider }

func (a *cwMetricAdapter) InternalPutMetricData(ctx context.Context, namespace string, data []cwlogs.MetricDatum) error {
	cw := make([]cloudwatchprovider.MetricDatum, len(data))
	for i, d := range data {
		cw[i] = cloudwatchprovider.MetricDatum{Name: d.Name, Value: d.Value, Unit: d.Unit}
	}
	return a.p.InternalPutMetricData(ctx, namespace, cw)
}

// buildK8sClient constructs a kubernetes.Interface using in-cluster config if
// available, falling back to JAISCLOUD_K8S_APISERVER + JAISCLOUD_K8S_TOKEN env vars.
func buildK8sClient() (kubernetes.Interface, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return kubernetes.NewForConfig(cfg)
	}
	apiServer := os.Getenv("JAISCLOUD_K8S_APISERVER")
	if apiServer == "" {
		apiServer = "https://kubernetes.default.svc"
	}
	token := os.Getenv("JAISCLOUD_K8S_TOKEN")
	cfg := &rest.Config{
		Host:        apiServer,
		BearerToken: token,
		TLSClientConfig: rest.TLSClientConfig{
			Insecure: true,
		},
	}
	return kubernetes.NewForConfig(cfg)
}

// registerCFNHandlers wires real resource provisioning for CloudFormation resource types.
func registerCFNHandlers(
	stackP *stackprovider.StackProvider,
	queueP *queue.QueueProvider,
	notifP *notification.SNSProvider,
	objectP *objectprovider.ObjectProvider,
	tableP *table.TableProvider,
	iamP *iamprovider.IAMProvider,
	funcP *functionprovider.FunctionProvider,
	keyP *keyprovider.KeyProvider,
	secretP *secretprovider.SecretProvider,
	paramP *paramprovider.ParameterProvider,
	logsP *cwlogs.Provider,
	cwP *cloudwatchprovider.Provider,
	eventsP *eventsprovider.EventBridgeProvider,
	ecsP *containerprovider.ContainerProvider,
	sfnP *sfnprovider.Provider,
	esmP *lambdaesm.Provider,
	apigwP *apigwprovider.GatewayProvider,
	computeP *compute.ComputeProvider,
) {
	// Existing 27 resource types — delegated to per-file constructors.
	stackP.RegisterHandler("AWS::SQS::Queue", handlers.NewSQSQueueHandler(queueP))
	stackP.RegisterHandler("AWS::SNS::Topic", handlers.NewSNSTopicHandler(notifP))
	stackP.RegisterHandler("AWS::S3::Bucket", handlers.NewS3BucketHandler(objectP))
	stackP.RegisterHandler("AWS::DynamoDB::Table", handlers.NewDynamoDBTableHandler(tableP))
	stackP.RegisterHandler("AWS::IAM::Role", handlers.NewIAMRoleHandler(iamP))
	stackP.RegisterHandler("AWS::Lambda::Function", handlers.NewLambdaFunctionHandler(funcP))
	stackP.RegisterHandler("AWS::SSM::Parameter", handlers.NewSSMParameterHandler(paramP))
	stackP.RegisterHandler("AWS::SecretsManager::Secret", handlers.NewSecretsManagerSecretHandler(secretP))
	stackP.RegisterHandler("AWS::KMS::Key", handlers.NewKMSKeyHandler(keyP))
	stackP.RegisterHandler("AWS::KMS::Alias", handlers.NewKMSAliasHandler(keyP))
	stackP.RegisterHandler("AWS::IAM::Policy", handlers.NewIAMPolicyHandler(iamP))
	stackP.RegisterHandler("AWS::IAM::ManagedPolicy", handlers.NewIAMManagedPolicyHandler(iamP))
	stackP.RegisterHandler("AWS::IAM::InstanceProfile", handlers.NewIAMInstanceProfileHandler(iamP))
	stackP.RegisterHandler("AWS::Lambda::Permission", handlers.NewLambdaPermissionHandler(funcP))
	stackP.RegisterHandler("AWS::Lambda::Alias", handlers.NewLambdaAliasHandler(funcP))
	stackP.RegisterHandler("AWS::Lambda::EventSourceMapping", handlers.NewLambdaEventSourceMappingHandler(esmP))
	stackP.RegisterHandler("AWS::SNS::Subscription", handlers.NewSNSSubscriptionHandler(notifP))
	stackP.RegisterHandler("AWS::Logs::LogGroup", handlers.NewLogsLogGroupHandler(logsP))
	stackP.RegisterHandler("AWS::Logs::LogStream", handlers.NewLogsLogStreamHandler(logsP))
	stackP.RegisterHandler("AWS::CloudWatch::Alarm", handlers.NewCloudWatchAlarmHandler(cwP))
	stackP.RegisterHandler("AWS::Events::Rule", handlers.NewEventsRuleHandler(eventsP))
	stackP.RegisterHandler("AWS::Events::EventBus", handlers.NewEventsEventBusHandler(eventsP))
	stackP.RegisterHandler("AWS::ECS::Cluster", handlers.NewECSClusterHandler(ecsP))
	stackP.RegisterHandler("AWS::ECS::TaskDefinition", handlers.NewECSTaskDefinitionHandler(ecsP))
	stackP.RegisterHandler("AWS::ECS::Service", handlers.NewECSServiceHandler(ecsP))
	stackP.RegisterHandler("AWS::StepFunctions::StateMachine", handlers.NewStepFunctionsStateMachineHandler(sfnP))
	stackP.RegisterHandler("AWS::ApiGateway::RestApi", handlers.NewAPIGatewayRestApiHandler(apigwP))
	stackP.RegisterHandler("AWS::CloudFormation::Stack", handlers.NewCFNStackHandler(stackP))

	// 17 new resource types.
	stackP.RegisterHandler("AWS::EC2::VPC", handlers.NewEC2VPCHandler(computeP))
	stackP.RegisterHandler("AWS::EC2::Subnet", handlers.NewEC2SubnetHandler(computeP))
	stackP.RegisterHandler("AWS::EC2::SecurityGroup", handlers.NewEC2SecurityGroupHandler(computeP))
	stackP.RegisterHandler("AWS::EC2::InternetGateway", handlers.NewEC2InternetGatewayHandler(computeP))
	stackP.RegisterHandler("AWS::EC2::RouteTable", handlers.NewEC2RouteTableHandler(computeP))
	stackP.RegisterHandler("AWS::EC2::Route", handlers.NewEC2RouteHandler(computeP))
	stackP.RegisterHandler("AWS::EC2::SubnetRouteTableAssociation", handlers.NewEC2SubnetRouteTableAssociationHandler(computeP))
	stackP.RegisterHandler("AWS::S3::BucketPolicy", handlers.NewS3BucketPolicyHandler(objectP))
	stackP.RegisterHandler("AWS::Lambda::Version", handlers.NewLambdaVersionHandler(funcP))
	stackP.RegisterHandler("AWS::Lambda::Url", handlers.NewLambdaUrlHandler(funcP))
	stackP.RegisterHandler("AWS::ApiGateway::Resource", handlers.NewAPIGatewayResourceHandler(apigwP))
	stackP.RegisterHandler("AWS::ApiGateway::Method", handlers.NewAPIGatewayMethodHandler(apigwP))
	stackP.RegisterHandler("AWS::ApiGateway::Integration", handlers.NewAPIGatewayIntegrationHandler(apigwP))
	stackP.RegisterHandler("AWS::ApiGateway::Deployment", handlers.NewAPIGatewayDeploymentHandler(apigwP))
	stackP.RegisterHandler("AWS::ApiGateway::Stage", handlers.NewAPIGatewayStageHandler(apigwP))
	stackP.RegisterHandler("AWS::Logs::SubscriptionFilter", handlers.NewLogsSubscriptionFilterHandler(logsP))
	stackP.RegisterHandler("AWS::DynamoDB::GlobalTable", handlers.NewDynamoDBGlobalTableHandler(tableP))
}

// buildAWSAdapter constructs all AWS service codecs and returns the wired adapter.
func buildAWSAdapter(s3VirtualHostBases []string) *awsadapter.AWSAdapter {
	iamCodec := &services.IAMCodec{}
	return awsadapter.NewAdapter(map[string]adapter.Codec{
		"kms":             &services.KMSCodec{},
		"secretsmanager":  &services.SecretsManagerCodec{},
		"ssm":             &services.SSMCodec{},
		"sqs":             &services.SQSCodec{},
		"iam":             iamCodec,
		"sts":             iamCodec,
		"sns":             &services.SNSCodec{},
		"dynamodb":        &services.DynamoDBCodec{},
		"s3":              &services.S3Codec{VirtualHostBases: s3VirtualHostBases},
		"lambda":          &services.LambdaCodec{},
		"glue":            &services.GlueCodec{},
		"ec2":             services.NewEC2Codec("ec2"),
		"route53":         &services.Route53Codec{},
		"rds":             &services.RDSCodec{},
		"elasticache":     &services.ElastiCacheCodec{},
		"ecs":             &services.ECSCodec{},
		"dynamodbstreams": &services.DynamoDBStreamsCodec{},
		"cloudformation":  &services.CloudFormationCodec{},
		"emr":             &services.EMRCodec{},
		"emr-containers":  &services.EMRContainersCodec{},
		"events":          &services.EventBridgeCodec{},
		"eks":             &services.EKSCodec{},
		"apigateway":      &services.APIGatewayCodec{},
		"execute-api":     &services.ExecuteAPICodec{},
		"monitoring":      &services.CloudWatchCodec{},
		"logs":            &services.LogsCodec{},
		"ecr":             &services.ECRCodec{},
		"states":          &services.StepFunctionsCodec{},
		"kinesis":         &services.KinesisCodec{},
		// G-PENDING new codecs
		"elasticloadbalancing": &services.ELBv2Codec{},
		"config":               &services.GenericJSONTargetCodec{Service: "config", TargetPrefix: "StarlingDoveService."},
		"resource-groups":      &services.ResourceGroupsCodec{},
		"tagging":              &services.GenericJSONTargetCodec{Service: "tagging", TargetPrefix: "ResourceGroupsTaggingAPI_20170126."},
		"email":                &services.SESCodec{},
		"acm":                  &services.GenericJSONTargetCodec{Service: "acm", TargetPrefix: "CertificateManager."},
		"cognito-idp":          &services.GenericJSONTargetCodec{Service: "cognito-idp", TargetPrefix: "AWSCognitoIdentityProviderService."},
		"cognito-identity":     &services.GenericJSONTargetCodec{Service: "cognito-identity", TargetPrefix: "AWSCognitoIdentityService."},
		"firehose":             &services.GenericJSONTargetCodec{Service: "firehose", TargetPrefix: "Firehose_20150804."},
	})
}

// buildAdminHandler registers all resetters and snapshotters, then returns the handler.
func buildAdminHandler(s appStores, streams *streamstore.MemoryStreamStore, keyStore keyprovider.KeyStore, secretStore secretprovider.SecretStore, paramStore paramprovider.ParameterStore, extra ...admin.Resetter) *admin.Handler {
	h := admin.NewHandler()
	h.RegisterResetter(s.resources)
	h.RegisterResetter(s.messages)
	h.RegisterResetter(s.dynamo)
	h.RegisterResetter(s.s3Meta)
	h.RegisterResetter(s.blobs)
	h.RegisterResetter(streams)
	h.RegisterResetter(keyStore)
	h.RegisterResetter(secretStore)
	h.RegisterResetter(paramStore)
	h.RegisterResetter(s.stsSession)
	h.RegisterResetter(s.kinesis)
	h.RegisterResetter(s.ecr)
	h.RegisterResetter(s.sfn)
	for _, r := range extra {
		if r == nil {
			continue
		}
		h.RegisterResetter(r)
		if hook, ok := r.(admin.PostRestoreHook); ok {
			h.RegisterPostRestoreHook(hook)
		}
	}
	if snap, ok := s.resources.(admin.Snapshotter); ok {
		h.RegisterSnapshotter("resources", snap)
	}
	if snap, ok := s.messages.(admin.Snapshotter); ok {
		h.RegisterSnapshotter("sqs_messages", snap)
	}
	if snap, ok := s.dynamo.(admin.Snapshotter); ok {
		h.RegisterSnapshotter("dynamodb_items", snap)
	}
	if snap, ok := s.s3Meta.(admin.Snapshotter); ok {
		h.RegisterSnapshotter("s3_objects", snap)
	}
	h.RegisterSnapshotter("dynamodb_streams", streams)
	if snap, ok := keyStore.(admin.Snapshotter); ok {
		h.RegisterSnapshotter("keys", snap)
	}
	if snap, ok := secretStore.(admin.Snapshotter); ok {
		h.RegisterSnapshotter("secrets", snap)
	}
	if snap, ok := paramStore.(admin.Snapshotter); ok {
		h.RegisterSnapshotter("parameters", snap)
	}
	h.RegisterSnapshotter("sts-sessions", s.stsSession)
	h.RegisterSnapshotter("kinesis", s.kinesis)
	h.RegisterSnapshotter("ecr", s.ecr)
	h.RegisterSnapshotter("stepfunctions", s.sfn)
	return h
}

// ─── ecr provider factory ─────────────────────────────────────────────────────

func buildECRProvider(ctx context.Context, cfg *config.Config, s appStores) *ecrprovider.Provider {
	if cfg.DSN != "" && cfg.ExecutorMode == "k8s" {
		k8sNS := cfg.K8sNamespace
		if k8sNS == "" {
			k8sNS = "jaiscloud"
		}
		k8sClient, err := buildK8sClient()
		if err != nil {
			slog.Warn("ecr: cannot reach k8s, falling back to memory mode", "err", err)
			return ecrprovider.New(s.ecr)
		}
		proxy := ecrprovider.NewRegistryProxy(k8sClient, k8sNS, s.ecr)
		if err := proxy.Start(ctx); err != nil {
			slog.Warn("ecr: registry:2 failed to start, falling back to memory mode", "err", err)
			return ecrprovider.New(s.ecr)
		}
		slog.Info("ecr persistent mode: registry:2 proxy ready")
		return ecrprovider.NewFull(s.ecr, proxy)
	}
	return ecrprovider.New(s.ecr)
}

// ─── kinesis provider factory ─────────────────────────────────────────────────

func buildKinesisProvider(ctx context.Context, cfg *config.Config, s appStores) *kinesisprovider.Provider {
	if cfg.DSN != "" {
		dataDir := filepath.Join(cfg.BlobDir, "kinesis")
		mock, err := kinesisprovider.NewMockServer(cfg.AccountID, dataDir)
		if err != nil {
			slog.Warn("kinesis-mock unavailable, falling back to memory mode", "err", err)
			return kinesisprovider.New(s.kinesis)
		}
		if err := mock.Start(ctx); err != nil {
			slog.Warn("kinesis-mock failed to start, falling back to memory mode", "err", err)
			return kinesisprovider.New(s.kinesis)
		}
		slog.Info("kinesis persistent mode: using kinesis-mock subprocess", "port", mock.Port())
		return kinesisprovider.NewFull(s.kinesis, mock)
	}
	return kinesisprovider.New(s.kinesis)
}

// ─── version ──────────────────────────────────────────────────────────────────

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("jaiscloud-aws %s (commit: %s, built: %s)\n", version, commit, date)
		},
	}
}

// ─── env ──────────────────────────────────────────────────────────────────────

func envCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "env",
		Short: "Print effective configuration as environment variables",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			fmt.Printf("JAISCLOUD_PORT=%d\n", cfg.Port)
			if cfg.Ephemeral {
				fmt.Printf("JAISCLOUD_EPHEMERAL=true\n")
			}
			if cfg.DSN != "" {
				fmt.Printf("JAISCLOUD_DSN=%s\n", cfg.DSN)
			}
			fmt.Printf("JAISCLOUD_CLOUD=aws\n")
			fmt.Printf("JAISCLOUD_REGION=%s\n", cfg.Region)
			fmt.Printf("JAISCLOUD_ACCOUNT_ID=%s\n", cfg.AccountID)
			fmt.Printf("JAISCLOUD_LOG_LEVEL=%s\n", cfg.LogLevel)
			return nil
		},
	}
}

// ─── doctor ───────────────────────────────────────────────────────────────────

func doctorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check that the emulator is reachable",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			resp, err := http.Get(host + "/_jaiscloud/health")
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: cannot reach %s: %v\n", host, err)
				os.Exit(1)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				fmt.Fprintf(os.Stderr, "ERROR: health check returned HTTP %d\n", resp.StatusCode)
				os.Exit(1)
			}
			fmt.Printf("OK: jaiscloud-aws is running at %s\n", host)
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	return cmd
}

// ─── reset ────────────────────────────────────────────────────────────────────

func resetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset",
		Short: "Wipe all emulator state",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			resp, err := http.Post(host+"/_jaiscloud/reset", "application/json", nil)
			if err != nil {
				return fmt.Errorf("reset: %w", err)
			}
			defer resp.Body.Close()
			fmt.Println("State reset.")
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	return cmd
}

// ─── export ───────────────────────────────────────────────────────────────────

func exportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export emulator state to a snapshot tarball (or stdout)",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			output, _ := cmd.Flags().GetString("output")

			resp, err := http.Get(host + "/_jaiscloud/export")
			if err != nil {
				return fmt.Errorf("export: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("export failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
			}
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("read export: %w", err)
			}

			if output == "" || output == "-" {
				_, err = os.Stdout.Write(data)
				return err
			}
			if err := os.WriteFile(output, data, 0o644); err != nil {
				return fmt.Errorf("write %s: %w", output, err)
			}
			sizeMB := float64(len(data)) / (1024 * 1024)
			fmt.Fprintf(os.Stderr, "Export complete -> %s (%.2f MB)\n", output, sizeMB)
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	cmd.Flags().StringP("output", "o", "-", "Output file (default: stdout)")
	return cmd
}

// ─── import ───────────────────────────────────────────────────────────────────

func importCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import emulator state from a snapshot tarball (or stdin)",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			input, _ := cmd.Flags().GetString("input")
			newInstance, _ := cmd.Flags().GetBool("new-instance")
			dryRun, _ := cmd.Flags().GetBool("dry-run")

			var data []byte
			var err error
			if input == "" || input == "-" {
				data, err = io.ReadAll(os.Stdin)
			} else {
				data, err = os.ReadFile(input)
			}
			if err != nil {
				return fmt.Errorf("read input: %w", err)
			}

			importURL := host + "/_jaiscloud/import"
			sep := "?"
			if newInstance {
				importURL += sep + "new_instance=true"
				sep = "&"
			}
			if dryRun {
				importURL += sep + "dry_run=true"
			}

			// Detect content type: gzip magic bytes = tarball.
			contentType := "application/json"
			if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
				contentType = "application/x-tar"
			}

			resp, err := http.Post(importURL, contentType, bytes.NewReader(data))
			if err != nil {
				return fmt.Errorf("import: %w", err)
			}
			defer resp.Body.Close()
			respBody, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("import failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
			}

			// Parse response for progress output.
			var result map[string]any
			if json.Unmarshal(respBody, &result) == nil {
				if dryRun {
					if valid, _ := result["dry_run"].(bool); valid {
						storeCount, _ := result["stores_parseable"].(float64)
						fmt.Fprintf(os.Stderr, "Dry-run complete: %d stores parseable, no state modified.\n", int(storeCount))
						return nil
					}
				}
				storeCount, _ := result["stores_restored"].(float64)
				fmt.Fprintf(os.Stderr, "Import complete (%d stores restored).\n", int(storeCount))
			} else {
				fmt.Fprintln(os.Stderr, "Import complete.")
			}
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	cmd.Flags().StringP("input", "i", "-", "Input file (default: stdin)")
	cmd.Flags().Bool("new-instance", false, "Assign a fresh instance ID on import (blocks snapshots with KMS key material)")
	cmd.Flags().Bool("dry-run", false, "Validate the snapshot without modifying state")
	return cmd
}

// ─── snapshot ─────────────────────────────────────────────────────────────────

func snapshotCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Manage named snapshots",
	}
	cmd.AddCommand(snapshotCreateCmd())
	cmd.AddCommand(snapshotListCmd())
	cmd.AddCommand(snapshotRevertCmd())
	cmd.AddCommand(snapshotDeleteCmd())
	cmd.AddCommand(snapshotInspectCmd())
	return cmd
}

func snapshotCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a named snapshot of current state",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			name, _ := cmd.Flags().GetString("name")
			desc, _ := cmd.Flags().GetString("description")
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			body, _ := json.Marshal(map[string]string{"name": name, "description": desc})
			resp, err := http.Post(host+"/_jaiscloud/snapshot", "application/json", bytes.NewReader(body))
			if err != nil {
				return fmt.Errorf("snapshot create: %w", err)
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusCreated {
				return fmt.Errorf("snapshot create failed (HTTP %d): %s", resp.StatusCode, data)
			}
			fmt.Fprintf(os.Stderr, "Snapshot %q created.\n", name)
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	cmd.Flags().String("name", "", "Snapshot name (required)")
	cmd.Flags().String("description", "", "Optional description")
	return cmd
}

func snapshotListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List named snapshots",
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			resp, err := http.Get(host + "/_jaiscloud/snapshots")
			if err != nil {
				return fmt.Errorf("snapshot list: %w", err)
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("snapshot list failed (HTTP %d): %s", resp.StatusCode, data)
			}
			var metas []map[string]any
			if err := json.Unmarshal(data, &metas); err != nil {
				fmt.Println(string(data))
				return nil
			}
			if len(metas) == 0 {
				fmt.Println("No snapshots found.")
				return nil
			}
			fmt.Printf("%-30s %-25s %s\n", "NAME", "CREATED", "DESCRIPTION")
			for _, m := range metas {
				fmt.Printf("%-30s %-25s %s\n",
					m["name"], m["created_at"], m["description"])
			}
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	return cmd
}

func snapshotRevertCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revert <name>",
		Short: "Revert to a named snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			resetFirst, _ := cmd.Flags().GetBool("reset-first")
			name := args[0]
			url := host + "/_jaiscloud/snapshot/" + name + "/revert"
			if resetFirst {
				url += "?reset_first=true"
			}
			resp, err := http.Post(url, "application/json", nil)
			if err != nil {
				return fmt.Errorf("snapshot revert: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("snapshot revert failed (HTTP %d): %s", resp.StatusCode, body)
			}
			fmt.Fprintf(os.Stderr, "Reverted to snapshot %q.\n", name)
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	cmd.Flags().Bool("reset-first", false, "Reset state before reverting")
	return cmd
}

func snapshotDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a named snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			yes, _ := cmd.Flags().GetBool("yes")
			name := args[0]
			if !yes {
				return fmt.Errorf("pass --yes to confirm deletion of snapshot %q", name)
			}
			req, _ := http.NewRequest(http.MethodDelete,
				host+"/_jaiscloud/snapshot/"+name+"?yes=true", nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("snapshot delete: %w", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("snapshot delete failed (HTTP %d): %s", resp.StatusCode, body)
			}
			fmt.Fprintf(os.Stderr, "Snapshot %q deleted.\n", name)
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	cmd.Flags().Bool("yes", false, "Confirm deletion")
	return cmd
}

func snapshotInspectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect <name>",
		Short: "Inspect a named snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			host, _ := cmd.Flags().GetString("host")
			name := args[0]
			resp, err := http.Get(host + "/_jaiscloud/snapshot/" + name)
			if err != nil {
				return fmt.Errorf("snapshot inspect: %w", err)
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("snapshot inspect failed (HTTP %d): %s", resp.StatusCode, data)
			}
			var pretty json.RawMessage
			if json.Unmarshal(data, &pretty) == nil {
				if indented, err := json.MarshalIndent(pretty, "", "  "); err == nil {
					fmt.Println(string(indented))
					return nil
				}
			}
			fmt.Println(string(data))
			return nil
		},
	}
	cmd.Flags().String("host", "http://localhost:4566", "Emulator host URL")
	return cmd
}

// logsWriterAdapter adapts cwlogs.Provider to targets.LogsWriter ([]string events).
type logsWriterAdapter struct{ p *cwlogs.Provider }

func (a *logsWriterAdapter) InternalPutLogEvents(ctx context.Context, logGroupName, logStreamName string, events []string) error {
	_ = a.p.InternalCreateLogGroup(ctx, logGroupName)
	lsEvents := make([]logstream.Event, len(events))
	now := clock.Now().UnixMilli()
	for i, e := range events {
		lsEvents[i] = logstream.Event{Timestamp: now, Message: e}
	}
	return a.p.InternalPutEvents(ctx, logGroupName, logStreamName, lsEvents)
}

// sqsSenderAdapter adapts *queue.QueueProvider to notification.SQSSender.
type sqsSenderAdapter struct{ q *queue.QueueProvider }

func (a sqsSenderAdapter) InternalSend(ctx context.Context, queueARNorURL string, body string, attrs map[string]notification.SQSMessageAttribute, src notification.SQSSourceContext) error {
	queueAttrs := make(map[string]queue.MessageAttribute, len(attrs))
	for k, v := range attrs {
		queueAttrs[k] = queue.MessageAttribute{DataType: v.DataType, StringValue: v.StringValue}
	}
	return a.q.InternalSend(ctx, queueARNorURL, body, queueAttrs, queue.SourceContext{
		SourceArn:        src.SourceArn,
		ServicePrincipal: src.ServicePrincipal,
	})
}

// esmSQSSenderAdapter adapts *queue.QueueProvider to lambdaesm.SQSSenderAPI.
type esmSQSSenderAdapter struct{ q *queue.QueueProvider }

func (a esmSQSSenderAdapter) InternalSend(ctx context.Context, queueARNorURL string, body string, attrs map[string]lambdaesm.SQSMessageAttribute, src lambdaesm.SQSSourceContext) error {
	queueAttrs := make(map[string]queue.MessageAttribute, len(attrs))
	for k, v := range attrs {
		queueAttrs[k] = queue.MessageAttribute{DataType: v.DataType, StringValue: v.StringValue}
	}
	return a.q.InternalSend(ctx, queueARNorURL, body, queueAttrs, queue.SourceContext{
		SourceArn:        src.SourceArn,
		ServicePrincipal: src.ServicePrincipal,
	})
}
