package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/certstore"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/gateway"
	gcpadapter "jaiscloud/internal/gcp/adapter"
	gcauth "jaiscloud/internal/gcp/auth"
	"jaiscloud/internal/gcp/crypto"
	grpcserver "jaiscloud/internal/gcp/grpc"
	grpcfirestore "jaiscloud/internal/gcp/grpc/firestore"
	grpcfirestoreadmin "jaiscloud/internal/gcp/grpc/firestoreadmin"
	grpckms "jaiscloud/internal/gcp/grpc/kms"
	grpcoperations "jaiscloud/internal/gcp/grpc/operations"
	grpcpubsub "jaiscloud/internal/gcp/grpc/pubsub"
	grpcsecretmanager "jaiscloud/internal/gcp/grpc/secretmanager"
	grpcstorage "jaiscloud/internal/gcp/grpc/storage"
	grpcstoragepb "jaiscloud/internal/gcp/grpc/storage/storagepb"
	hms "jaiscloud/internal/gcp/hms"
	"jaiscloud/internal/gcp/lro"
	bigqueryprovider "jaiscloud/internal/gcp/provider/bigquery"
	clouddnsprovider "jaiscloud/internal/gcp/provider/clouddns"
	cloudsqlprovider "jaiscloud/internal/gcp/provider/cloudsql"
	computeprovider "jaiscloud/internal/gcp/provider/compute"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	iamprovider "jaiscloud/internal/gcp/provider/iam"
	icebergprovider "jaiscloud/internal/gcp/provider/iceberg"
	kmsprovider "jaiscloud/internal/gcp/provider/kms"
	memorystoreprovider "jaiscloud/internal/gcp/provider/memorystore"
	pubsubprovider "jaiscloud/internal/gcp/provider/pubsub"
	secretmanagerprovider "jaiscloud/internal/gcp/provider/secretmanager"
	storageprovider "jaiscloud/internal/gcp/provider/storage"
	dataproccore "jaiscloud/internal/gcp/service/dataproc"
	datastorecore "jaiscloud/internal/gcp/service/datastore"
	eventarccore "jaiscloud/internal/gcp/service/eventarc"
	functionscore "jaiscloud/internal/gcp/service/functions"
	iamcredentialscore "jaiscloud/internal/gcp/service/iamcredentials"
	loggingcore "jaiscloud/internal/gcp/service/logging"
	managedkafkacore "jaiscloud/internal/gcp/service/managedkafka"
	metastorecore "jaiscloud/internal/gcp/service/metastore"
	monitoringcore "jaiscloud/internal/gcp/service/monitoring"
	resourcemanagercore "jaiscloud/internal/gcp/service/resourcemanager"
	schedulercore "jaiscloud/internal/gcp/service/scheduler"
	serviceusagecore "jaiscloud/internal/gcp/service/serviceusage"
	taskscore "jaiscloud/internal/gcp/service/tasks"
	workflowexecutionscore "jaiscloud/internal/gcp/service/workflowexecutions"
	workflowscore "jaiscloud/internal/gcp/service/workflows"
	"jaiscloud/internal/gcp/sparkgcp"
	gcpstore "jaiscloud/internal/gcp/store"
	bigquerystore "jaiscloud/internal/gcp/store/bigquery"
	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	datastorestore "jaiscloud/internal/gcp/store/datastore"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/gcp/store/gcs"
	hmsstore "jaiscloud/internal/gcp/store/hms"
	icebergstore "jaiscloud/internal/gcp/store/iceberg"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	loggingstore "jaiscloud/internal/gcp/store/logging"
	managedkafkastore "jaiscloud/internal/gcp/store/managedkafka"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	schedulerstore "jaiscloud/internal/gcp/store/scheduler"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/gcp/throttle"
	grpcdataproc "jaiscloud/internal/gcp/transport/grpc/dataproc"
	grpcdatastore "jaiscloud/internal/gcp/transport/grpc/datastore"
	grpceventarc "jaiscloud/internal/gcp/transport/grpc/eventarc"
	grpcfunctions "jaiscloud/internal/gcp/transport/grpc/functions"
	grpciamcredentials "jaiscloud/internal/gcp/transport/grpc/iamcredentials"
	grpclogging "jaiscloud/internal/gcp/transport/grpc/logging"
	grpcmanagedkafka "jaiscloud/internal/gcp/transport/grpc/managedkafka"
	grpcmetastore "jaiscloud/internal/gcp/transport/grpc/metastore"
	grpcmonitoring "jaiscloud/internal/gcp/transport/grpc/monitoring"
	grpcresourcemanager "jaiscloud/internal/gcp/transport/grpc/resourcemanager"
	grpcscheduler "jaiscloud/internal/gcp/transport/grpc/scheduler"
	grpcserviceusage "jaiscloud/internal/gcp/transport/grpc/serviceusage"
	grpctasks "jaiscloud/internal/gcp/transport/grpc/tasks"
	grpcworkflowexecutions "jaiscloud/internal/gcp/transport/grpc/workflowexecutions"
	grpcworkflows "jaiscloud/internal/gcp/transport/grpc/workflows"
	restdataproc "jaiscloud/internal/gcp/transport/rest/dataproc"
	restdatastore "jaiscloud/internal/gcp/transport/rest/datastore"
	resteventarc "jaiscloud/internal/gcp/transport/rest/eventarc"
	restfunctions "jaiscloud/internal/gcp/transport/rest/functions"
	restiamcredentials "jaiscloud/internal/gcp/transport/rest/iamcredentials"
	restlogging "jaiscloud/internal/gcp/transport/rest/logging"
	restmanagedkafka "jaiscloud/internal/gcp/transport/rest/managedkafka"
	restmetastore "jaiscloud/internal/gcp/transport/rest/metastore"
	restmonitoring "jaiscloud/internal/gcp/transport/rest/monitoring"
	restresourcemanager "jaiscloud/internal/gcp/transport/rest/resourcemanager"
	restscheduler "jaiscloud/internal/gcp/transport/rest/scheduler"
	restserviceusage "jaiscloud/internal/gcp/transport/rest/serviceusage"
	resttasks "jaiscloud/internal/gcp/transport/rest/tasks"
	restworkflowexecutions "jaiscloud/internal/gcp/transport/rest/workflowexecutions"
	restworkflows "jaiscloud/internal/gcp/transport/rest/workflows"
	"jaiscloud/internal/gcp/transportcfg"
	workflowengine "jaiscloud/internal/gcp/workflows/engine"
	"jaiscloud/internal/model"
	"jaiscloud/internal/persistence/snapshot"
	snapversion "jaiscloud/internal/persistence/version"
	"jaiscloud/internal/platform"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/snapshottypes"
	"jaiscloud/internal/store"

	cloudtaskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	eventarcpb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	firestoreadminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	credentialspb "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	serviceusagepb "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	workflowspb "cloud.google.com/go/workflows/apiv1/workflowspb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const version = "1.1.0"

const defaultHost = "http://localhost:8080"

func main() {
	root := &cobra.Command{
		Use:   "jaiscloud-gcp",
		Short: "JaisCloud GCP - local GCP emulator",
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

func startCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the emulator",
		RunE: func(cmd *cobra.Command, args []string) error {
			bindFlags(cmd)

			cfg, err := config.Load(model.CloudGCP)
			if err != nil {
				return err
			}
			// GCP has no account ID; map the project onto the account scope used
			// by the shared store. Region defaults to global (GCP services omit
			// region from most REST paths).
			cfg.AccountID = cfg.ProjectID
			cfg.Region = "global"
			// config.Load defaults the port to 4566 (the AWS default); GCP listens
			// on 8080 unless the user overrides via --port or JAISCLOUD_PORT.
			if !cmd.Flags().Changed("port") && os.Getenv("JAISCLOUD_PORT") == "" {
				cfg.Port = 8080
			}
			// Cloud Functions v2 source-upload URLs must point back at this
			// emulator: `gcloud functions deploy --gen2` PUTs the archive directly
			// to the returned uploadUrl and the Storage endpoint override does not
			// apply to that raw URL. The REST transport derives it per request; the
			// gRPC transport uses this origin.
			functionsUploadBase := functionsUploadOrigin(cfg.Port)
			clock.SetGlobalClock(cfg.Clock)

			// Resolve which wire transports to expose. Only the selected
			// listeners/routes are started, so a REST-only or gRPC-only user
			// never runs the other transport's listener.
			transports, err := transportcfg.Parse(cfg.GCPTransports, cfg.GCPTransportOverrides, gcpadapter.KnownServiceNames())
			if err != nil {
				return err
			}
			slog.Info("gcp transports selected", "rest", transports.REST(), "grpc", transports.GRPC(), "selection", transports.String())

			// Long-running-operation timing is an opt-in, cross-service mode.
			// The default (env unset) keeps every operation synchronously done,
			// matching the v1.1.0 contract and the conformance transcripts;
			// JAISCLOUD_LRO_MODE=async stores operations in flight and settles
			// them lazily when a client polls Operations.GetOperation.
			lroMode := lro.FromEnv()
			slog.Info("gcp lro mode", "async", lroMode.Async(), "delay", lroMode.Delay)

			// Throttle/quota fault injection is an opt-in, cross-service,
			// emulator-only testing affordance (default OFF). The same injector
			// backs both the REST gateway RequestFilter and the gRPC
			// interceptors, so the two transports cannot drift.
			throttleCfg := throttle.FromEnv()
			throttleInj := throttle.New(throttleCfg)
			slog.Info("gcp throttle injection",
				"enabled", throttleCfg.Enabled(),
				"rate", throttleCfg.Rate,
				"fault", throttleCfg.Fault,
				"status", throttleCfg.Status,
				"services", throttleCfg.Services,
			)

			// serviceEnabled reports whether a wire service is exposed on at
			// least one transport. Disabled services are neither constructed
			// (for the heavyweight ones) nor registered.
			serviceEnabled := func(name string) bool {
				return transports.RESTFor(name) || transports.GRPCFor(name)
			}

			ctx := context.Background()

			stateDir, _ := config.ResolveStateDir(os.Getenv("JAISCLOUD_STATE_DIR"))
			instanceID, _ := config.LoadOrCreateInstanceID(stateDir)

			stores, err := initStores(ctx, cfg, instanceID)
			if err != nil {
				return err
			}
			defer stores.close()

			// KMS envelope encryption: when a master key (KEK) is configured,
			// wrap the server DEK at rest (mirrors AWS key bootstrap.LoadOrCreateDEK).
			if cfg.KMSMasterKey != "" {
				kek, err := kmsstore.ParseHexKey(cfg.KMSMasterKey)
				if err != nil {
					return fmt.Errorf("kms master key: %w", err)
				}
				if ps, ok := stores.keys.(*kmsstore.PostgresStore); ok {
					ps.SetKEK(kek)
				}
			}

			storageP := storageprovider.New(stores.objects, stores.resources, stores.blobs, crypto.NewEnvelopeEncryptor(stores.keys))
			secretP := secretmanagerprovider.New(stores.secrets, stores.resources, crypto.NewEnvelopeEncryptor(stores.keys))
			kmsP := kmsprovider.New(stores.keys, stores.resources)
			iamP := iamprovider.New(stores.resources)
			// IAM Service Account Credentials (iamcredentials) shares its key
			// material with iam (internal/gcp/serviceaccount). Its
			// transport-neutral core is shared by the REST provider and the gRPC
			// adapter below, so both transports mint identical credentials.
			iamCredentialsCore := iamcredentialscore.New(stores.resources)
			iamCredentialsP := restiamcredentials.NewProvider(iamCredentialsCore, cfg.ProjectID)
			pubsubP := pubsubprovider.New(stores.resources, stores.messages, crypto.NewEnvelopeEncryptor(stores.keys))
			// GCS object notifications fan out through Pub/Sub; the storage
			// provider only sees the interface, so it never imports the Pub/Sub
			// provider (no provider→provider dependency).
			storageP.SetEventPublisher(pubsubP)
			firestoreP := firestoreprovider.New(stores.documents, stores.resources)

			// Cloud Datastore's transport-neutral core is shared by the REST
			// provider and the gRPC adapter below, so both transports use one
			// transaction read-set registry and cannot drift.
			datastoreCore := datastorecore.NewService(stores.entities, cfg.ProjectID)
			datastoreRestP := restdatastore.NewProvider(datastoreCore, cfg.ProjectID)

			// Cloud Logging's transport-neutral core is shared by the REST
			// provider and the gRPC adapter below, so both transports use one
			// log store and cannot drift.
			loggingCore := loggingcore.NewService(stores.logEntries, cfg.ProjectID)
			loggingRestP := restlogging.NewProvider(loggingCore, cfg.ProjectID)

			// Cloud Monitoring's transport-neutral core is shared by the REST
			// provider and the gRPC adapter (and the background evaluator)
			// below, so every surface reads and writes one store.
			monitoringCore := monitoringcore.NewService(stores.monitoring, cfg.ProjectID)
			monitoringRestP := restmonitoring.NewProvider(monitoringCore, cfg.ProjectID)

			// Cloud Functions reuses the Lambda executor: mock echo by default,
			// Docker/K8s under JAISCLOUD_EXECUTOR_MODE. The transport-neutral
			// core is shared by the REST provider and the gRPC adapter below, so
			// both transports own one function store and one executor. The
			// executor (warm container pool / K8s client) is only built when
			// functions is enabled.
			var functionsCore *functionscore.Service
			if serviceEnabled("functions") {
				lambdaMode, lambdaModeSrc := config.ExecutorMode("lambda", "mock")
				lambdaCfg := lambdaexec.DefaultLambdaConfig()
				lambdaCfg.Mode = lambdaMode
				lambdaCfg.Region = cfg.Region
				lambdaCfg.InstanceID = instanceID
				lambdaCfg = lambdaexec.LambdaConfigFrom(lambdaCfg)
				// K8s mode mounts source archives via a code-fetch init container,
				// which downloads them from the admin API. Prefer an explicit
				// JAISCLOUD_LAMBDA_CODE_URL (already read into lambdaCfg), else
				// derive it from a cluster-reachable emulator origin.
				if lambdaCfg.CodeURL == "" {
					lambdaCfg.CodeURL = lambdaCodeURL()
				}
				lambdaExec := lambdaexec.NewExecutor(lambdaCfg)
				defer lambdaExec.Close()
				slog.Info("lambda executor", "mode", lambdaMode, "source", lambdaModeSrc)
				// The core is its own lambdaexec.CodeLoader: it resolves a
				// function's persisted source archive (FD1) from the blob store
				// so Docker/K8s mode mounts and runs real code (mock stays the
				// default). It reads GCS source references (v1 sourceArchiveUrl /
				// v2 storageSource) through the storage provider, which owns
				// object decryption.
				functionsCore = functionscore.NewService(stores.functions, stores.resources,
					functionscore.WithExecutor(lambdaExec),
					functionscore.WithBlobs(stores.blobs),
					functionscore.WithSourceFetcher(storageP),
					functionscore.WithSourceBuckets(storageP),
					// Long-running-operation timing (LRO2): opt-in async mode,
					// default sync, reusing the cross-service mode parsed above.
					functionscore.WithLROMode(lroMode),
					// The executor is the shared concurrency resource, so its
					// account-level cap is also the project-wide admission cap
					// (FP1). JAISCLOUD_LAMBDA_CONCURRENCY_LIMIT, default 1000.
					functionscore.WithAccountConcurrencyLimit(lambdaCfg.ConcurrencyLimit),
				)
				if dockerExec, ok := lambdaExec.(*lambdaexec.DockerExecutor); ok {
					dockerExec.SetCodeLoader(functionsCore)
				}
				if k8sExec, ok := lambdaExec.(*lambdaexec.K8sExecutor); ok {
					k8sExec.SetCodeLoader(functionsCore)
				}
			}
			functionsP := restfunctions.NewProvider(functionsCore, cfg.ProjectID)
			// Producers hand events to the functions core's delivery engine. The
			// core is nil when functions is disabled, so only wire it then.
			if functionsCore != nil {
				pubsubP.SetFunctionDispatcher(functionsCore)
				storageP.SetFunctionDispatcher(functionsCore)
				// Dead-letter resolution/forwarding (FD9) goes through the Pub/Sub
				// provider; the Eventarc trigger provisioner is wired below once
				// the Eventarc core exists.
				functionsCore.SetSubscriptions(pubsubP)
			}

			workflowsEngine := workflowengine.New()
			// Cloud Workflows' transport-neutral core is shared by the REST
			// provider and the gRPC adapter below, so both transports run
			// against one store and one piece of state and cannot drift. The
			// core is only built when workflows is enabled on some transport.
			var workflowsCore *workflowscore.Service
			if serviceEnabled("workflows") {
				workflowsCore = workflowscore.NewService(stores.workflows, workflowscore.WithLROMode(lroMode))
			}
			workflowsP := restworkflows.NewProvider(workflowsCore, cfg.ProjectID)
			// Cloud Workflow Executions' transport-neutral core is shared by the
			// REST provider and the gRPC adapter below, so both transports run
			// against one store and one engine and cannot drift.
			workflowExecutionsCore := workflowexecutionscore.NewService(stores.workflows, workflowsEngine)
			workflowExecutionsP := restworkflowexecutions.NewProvider(workflowExecutionsCore, cfg.ProjectID)

			// Dataproc Metastore's transport-neutral core is shared by the REST
			// provider and the gRPC adapter below, so both transports run against
			// one store and cannot drift. It is built before Dataproc because the
			// Dataproc core resolves a cluster's metastore attachment through it.
			metastoreCore := metastorecore.NewService(stores.metastore, metastorecore.WithLROMode(lroMode))
			metastoreP := restmetastore.NewProvider(metastoreCore, cfg.ProjectID)

			// Cloud Dataproc reuses the Spark client-mode executor: mock by
			// default, K8s under JAISCLOUD_SPARK_EXECUTOR_MODE. Docker executor
			// for Spark is out of scope (Phase A) — mock only. The core (and its
			// K8s client) is only built when dataproc is enabled, and it is
			// shared by the REST provider and the gRPC adapter below so both
			// transports own one cluster/job state and one executor.
			var dataprocCore *dataproccore.Service
			if serviceEnabled("dataproc") {
				sparkMode, sparkModeSrc := config.ExecutorMode("spark", "mock")
				if sparkMode == "docker" {
					slog.Warn("dataproc: docker Spark executor not supported, falling back to mock")
					sparkMode = "mock"
				}
				dataprocOpts := []dataproccore.Option{
					dataproccore.WithInstanceID(instanceID),
					dataproccore.WithProjectID(cfg.ProjectID),
					// Lifecycle events publish to the Pub/Sub provider (the
					// core only sees the interface, so it never imports
					// provider/pubsub). Empty topic (the default) disables
					// publishing; a cluster may override per-cluster with the
					// jaiscloud-events-topic label.
					dataproccore.WithEventPublisher(pubsubP),
					dataproccore.WithEventsTopic(os.Getenv("JAISCLOUD_DATAPROC_EVENTS_TOPIC")),
					// Stage each job's driver output/control files into the
					// emulated GCS. The core only sees the BlobSink interface,
					// so it never imports provider/storage.
					dataproccore.WithBlobSink(storageP),
					// Resolve a cluster's metastoreConfig attachment through the
					// Metastore core (validated at cluster create, formatted at
					// job submit). The core only sees the interface, so it never
					// imports service/metastore.
					dataproccore.WithMetastoreResolver(metastoreCore),
					// The synthesized per-service endpoint is not resolvable from
					// Spark pods; a deployment points this at the reachable HMS.
					dataproccore.WithHMSEndpointOverride(os.Getenv("JAISCLOUD_DATAPROC_HMS_ENDPOINT")),
				}
				if cfg.K8sSparkSA != "" {
					dataprocOpts = append(dataprocOpts, dataproccore.WithServiceAccountName(cfg.K8sSparkSA))
				}
				if cfg.K8sSparkSubmitPath != "" {
					dataprocOpts = append(dataprocOpts, dataproccore.WithSparkSubmitPath(cfg.K8sSparkSubmitPath))
				}
				if cfg.K8sSparkSqlPath != "" {
					dataprocOpts = append(dataprocOpts, dataproccore.WithSparkSqlPath(cfg.K8sSparkSqlPath))
				}
				gcpEmulatorCfg := &sparkgcp.GCPEmulatorConfig{ProjectID: cfg.ProjectID, Region: "global"}
				if v := os.Getenv("STORAGE_EMULATOR_HOST"); v != "" {
					gcpEmulatorCfg.GCSEndpoint = v
				} else if v := os.Getenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT"); v != "" {
					gcpEmulatorCfg.GCSEndpoint = v
				}
				dataprocOpts = append(dataprocOpts, dataproccore.WithGCPEmulator(gcpEmulatorCfg))
				// Cluster mutations are asynchronous and settle lazily on a read.
				// The default (zero) settles on the first read; a positive value
				// keeps clusters in CREATING/DELETING long enough to observe.
				if v := os.Getenv("JAISCLOUD_DATAPROC_CLUSTER_READY_DELAY"); v != "" {
					if d, err := time.ParseDuration(v); err != nil {
						slog.Warn("dataproc: invalid JAISCLOUD_DATAPROC_CLUSTER_READY_DELAY", "value", v, "err", err)
					} else {
						dataprocOpts = append(dataprocOpts, dataproccore.WithClusterReadyDelay(d))
					}
				}
				// Jobs walk PENDING -> SETUP_DONE -> RUNNING -> terminal lazily
				// on a read in mock mode; a positive delay keeps each hop
				// observable, the default (zero) settles one hop per read.
				if v := os.Getenv("JAISCLOUD_DATAPROC_JOB_STATE_DELAY"); v != "" {
					if d, err := time.ParseDuration(v); err != nil {
						slog.Warn("dataproc: invalid JAISCLOUD_DATAPROC_JOB_STATE_DELAY", "value", v, "err", err)
					} else {
						dataprocOpts = append(dataprocOpts, dataproccore.WithJobStateDelay(d))
					}
				}
				if sparkMode == "k8s" {
					sparkImage := cfg.K8sSparkImage
					if sparkImage == "" {
						slog.Error("dataproc: JAISCLOUD_K8S_SPARK_IMAGE is required when executor mode is k8s")
						os.Exit(1)
					}
					dataprocOpts = append(dataprocOpts, dataproccore.WithSparkImage(sparkImage))
					k8sNS := cfg.K8sNamespace
					if k8sNS == "" {
						k8sNS = "jaiscloud"
					}
					platformCfg, err := platform.LoadFromEnv()
					if err != nil {
						return fmt.Errorf("platform config: %w", err)
					}
					if k8sClient, err := buildK8sClient(); err != nil {
						slog.Warn("dataproc: failed to build k8s client; falling back to mock", "err", err)
					} else {
						dataprocOpts = append(dataprocOpts, dataproccore.WithK8s(k8sClient, k8sNS, platformCfg))
					}
				} else if sparkImage := cfg.K8sSparkImage; sparkImage != "" {
					dataprocOpts = append(dataprocOpts, dataproccore.WithSparkImage(sparkImage))
				}
				slog.Info("dataproc executor", "mode", sparkMode, "source", sparkModeSrc)
				dataprocCore = dataproccore.NewService(stores.dataproc, stores.resources, dataprocOpts...)
				// Deliver lifecycle events directly to functions whose
				// eventTrigger names a Dataproc state-change type (independent
				// of the Pub/Sub topic). Only wire when functions is enabled.
				if functionsCore != nil {
					dataprocCore.SetEventDispatcher(functionsCore)
				}
				defer dataprocCore.Shutdown(context.Background())
			}
			dataprocP := restdataproc.NewProvider(dataprocCore, cfg.ProjectID)

			// Managed Kafka's transport-neutral core is shared by the REST
			// provider and the gRPC adapter below, so both transports run
			// against one store and cannot drift.
			managedKafkaCore := managedkafkacore.NewService(stores.managedkafka, managedkafkacore.WithLROMode(lroMode))
			managedkafkaP := restmanagedkafka.NewProvider(managedKafkaCore, cfg.ProjectID)

			icebergP := icebergprovider.New(stores.iceberg)

			bigqueryP := bigqueryprovider.New(stores.bigquery)

			// Eventarc's transport-neutral core is shared by the REST provider
			// and the gRPC adapter below, so both transports run against one
			// store and cannot drift. The core is only built when eventarc is
			// enabled.
			var eventarcCore *eventarccore.Service
			if serviceEnabled("eventarc") {
				eventarcCore = eventarccore.NewService(stores.eventarc, stores.resources, stores.workflows)
				// The platform-provisioned backing Pub/Sub subscription of a
				// trigger (its dead-letter surface) is created by the Pub/Sub
				// provider (FD9).
				eventarcCore.SetSubscriptionProvisioner(pubsubP)
			}
			// Eventarc validates destination.cloudFunction against the functions
			// core, and the functions delivery engine consults Eventarc triggers
			// whose destination is a cloudFunction. Both are optional cross-links,
			// so they are wired only when each side is enabled.
			if eventarcCore != nil && functionsCore != nil {
				eventarcCore.SetFunctionExister(functionsCore)
				functionsCore.SetEventTargets(eventarcCore)
				// A Pub/Sub event trigger materializes a backing Eventarc trigger
				// (FD9); the functions core holds it as an interface so it never
				// imports the Eventarc core.
				functionsCore.SetTriggerProvisioner(eventarcCore)
			}
			eventarcP := resteventarc.NewProvider(eventarcCore, cfg.ProjectID)

			// Cloud DNS is metadata-only over the shared ResourceStore.
			clouddnsP := clouddnsprovider.New(stores.resources)

			// Memorystore for Redis is metadata-only over the shared ResourceStore.
			memorystoreP := memorystoreprovider.New(stores.resources)

			// Cloud SQL Admin is metadata-only over the shared ResourceStore.
			cloudsqlP := cloudsqlprovider.New(stores.resources)

			// Compute Engine is metadata-only over the shared ResourceStore.
			computeP := computeprovider.New(stores.resources)

			// Service Usage v1's transport-neutral core is shared by the REST
			// provider and the gRPC adapter below, so both transports run
			// against one store and cannot drift.
			serviceUsageCore := serviceusagecore.NewService(stores.resources, serviceusagecore.WithLROMode(lroMode))
			serviceusageP := restserviceusage.NewProvider(serviceUsageCore, cfg.ProjectID)

			// Cloud Scheduler v1's transport-neutral core is shared by the REST
			// provider, the gRPC adapter, and the cron engine, so all three run
			// against one store and cannot drift. The engine fires
			// httpTarget/pubsubTarget jobs on the emulator clock; pubsubTarget
			// delivery reuses the Pub/Sub provider's fan-out path.
			schedulerCore := schedulercore.NewService(stores.scheduler)
			schedulerEngine := schedulercore.NewEngine(stores.scheduler, schedulercore.NewRunner(schedulerPubSubPublisher{provider: pubsubP}))
			schedulerCore.SetEngine(schedulerEngine)
			schedulerP := restscheduler.NewProvider(schedulerCore)

			// Cloud Tasks v2's transport-neutral core is shared by the REST
			// provider, the gRPC adapter, and the dispatch engine, so all three
			// run against one store and cannot drift. The engine delivers due
			// httpRequest tasks on the emulator clock (advance /_jaiscloud/clock
			// or POST /_jaiscloud/tasks-tick to fire deterministically) with
			// per-queue rate limits and exponential-backoff retries; RunTask
			// forces a synchronous attempt.
			tasksCore := taskscore.NewService(stores.tasks, stores.resources)
			tasksEngine := taskscore.NewEngine(stores.tasks, taskscore.NewRunner())
			tasksCore.SetEngine(tasksEngine)
			tasksP := resttasks.NewProvider(tasksCore, cfg.ProjectID)

			// Cloud Resource Manager's transport-neutral core is shared by the
			// v1 REST provider and the v3 gRPC adapter below, so project IAM
			// policy lives in one store and the two transports cannot drift.
			// The core is only built when resourcemanager is enabled.
			var resourceManagerCore *resourcemanagercore.Service
			if serviceEnabled("resourcemanager") {
				resourceManagerCore = resourcemanagercore.NewService(stores.resources)
			}
			resourcemanagerP := restresourcemanager.NewProvider(resourceManagerCore, cfg.ProjectID)

			// The REST locations/{location}/operations/{id} path is decoded as
			// a Workflows operation, so in the opt-in async LRO mode wire the
			// other regional services as fallback resolvers; without this a
			// Metastore/Managed Kafka poll would 404. The default sync mode
			// returns every operation done inline, so nothing is wired and the
			// REST contract is unchanged.
			if lroMode.Async() {
				workflowsP.SetOperationResolvers(metastoreP, managedkafkaP)
				// The top-level /v1/operations/{id} route is decoded as a
				// Functions v1 operation, so wire Service Usage (which shares
				// the operations/{id} namespace) as a fallback resolver: a
				// Service Usage poll 404s in Functions first, then resolves
				// here. Sync mode returns every operation done inline, so
				// nothing is wired and the REST contract is unchanged.
				functionsP.SetOperationResolvers(serviceusageP)
			}

			// Register only services enabled on at least one transport, so a
			// per-service `none` override removes both its REST and gRPC surface
			// (an unregistered provider falls through to a 404/unknown-action).
			reg := provider.NewRegistry()
			for _, sp := range []struct {
				name string
				p    provider.Provider
			}{
				{"storage", storageP},
				{"pubsub", pubsubP},
				{"secretmanager", secretP},
				{"kms", kmsP},
				{"iam", iamP},
				{"iamcredentials", iamCredentialsP},
				{"firestore", firestoreP},
				{"functions", functionsP},
				{"workflows", workflowsP},
				{"workflowexecutions", workflowExecutionsP},
				{"dataproc", dataprocP},
				{"managedkafka", managedkafkaP},
				{"bigquery", bigqueryP},
				{"metastore", metastoreP},
				{"iceberg", icebergP},
				{"eventarc", eventarcP},
				{"dns", clouddnsP},
				{"redis", memorystoreP},
				{"sqladmin", cloudsqlP},
				{"compute", computeP},
				{"serviceusage", serviceusageP},
				{"scheduler", schedulerP},
				{"tasks", tasksP},
				{"resourcemanager", resourcemanagerP},
				{"datastore", datastoreRestP},
				{"logging", loggingRestP},
				{"monitoring", monitoringRestP},
			} {
				if serviceEnabled(sp.name) {
					reg.Register(sp.p)
				}
			}

			// gRPC transport shares the SAME Firestore provider Service as the
			// REST adapter, so both transports use one transaction read-set
			// registry.
			grpcPort, _ := cmd.Flags().GetInt("grpc-port")
			firestoreGRPC := grpcfirestore.NewService(firestoreP.Service, cfg.ProjectID)
			firestoreAdminGRPC := grpcfirestoreadmin.NewService(firestoreP.Service, cfg.ProjectID)
			pubsubGRPC := grpcpubsub.NewService(stores.resources, stores.messages, crypto.NewEnvelopeEncryptor(stores.keys), cfg.ProjectID)
			secretGRPC := grpcsecretmanager.NewService(stores.secrets, stores.resources, crypto.NewEnvelopeEncryptor(stores.keys), cfg.ProjectID)
			kmsGRPC := grpckms.NewService(stores.keys, stores.resources, crypto.NewEnvelopeEncryptor(stores.keys), cfg.ProjectID)
			loggingGRPC := grpclogging.NewService(loggingCore, cfg.ProjectID)
			loggingConfigGRPC := grpclogging.NewConfigService(loggingCore, cfg.ProjectID)
			loggingMetricsGRPC := grpclogging.NewMetricsService(loggingCore, cfg.ProjectID)
			monitoringGRPC := grpcmonitoring.NewService(monitoringCore, cfg.ProjectID)
			// The background evaluator evaluates alert-policy condition_threshold
			// conditions, opens/closes incidents, and publishes notifications to
			// pubsub notification channels via the emulator's Pub/Sub store.
			monitoringEval := grpcmonitoring.NewEvaluator(stores.monitoring, pubsubNotificationPublisher{
				messages:  stores.messages,
				encryptor: crypto.NewEnvelopeEncryptor(stores.keys),
			})
			storageGRPC := grpcstorage.NewService(stores.objects, stores.resources, storageP, cfg.ProjectID)
			datastoreGRPC := grpcdatastore.NewService(datastoreCore, cfg.ProjectID)
			workflowExecutionsGRPC := grpcworkflowexecutions.NewService(workflowExecutionsCore, cfg.ProjectID)
			managedKafkaGRPC := grpcmanagedkafka.NewService(managedKafkaCore, cfg.ProjectID)
			metastoreGRPC := grpcmetastore.NewService(metastoreCore, cfg.ProjectID)
			eventarcGRPC := grpceventarc.NewService(eventarcCore, cfg.ProjectID)
			serviceUsageGRPC := grpcserviceusage.NewService(serviceUsageCore, cfg.ProjectID)
			schedulerGRPC := grpcscheduler.NewService(schedulerCore, cfg.ProjectID)
			tasksGRPC := grpctasks.NewService(tasksCore, cfg.ProjectID)
			resourceManagerGRPC := grpcresourcemanager.NewService(resourceManagerCore, cfg.ProjectID)
			iamCredentialsGRPC := grpciamcredentials.NewService(iamCredentialsCore, cfg.ProjectID)
			dataprocGRPC := grpcdataproc.NewService(dataprocCore, cfg.ProjectID)
			workflowsGRPC := grpcworkflows.NewService(workflowsCore, cfg.ProjectID)
			functionsGRPC := grpcfunctions.NewService(functionsCore, cfg.ProjectID, functionsUploadBase)
			functionsV2GRPC := grpcfunctions.NewServiceV2(functionsCore, cfg.ProjectID, functionsUploadBase)
			// The gRPC listener is built and bound only when the gRPC transport
			// is selected for at least one service; otherwise no :grpc-port
			// socket is opened.
			var gserv *grpcserver.Server
			if transports.GRPC() {
				gserv = grpcserver.NewServer(fmt.Sprintf(":%d", grpcPort), grpcserver.ThrottleServerOptions(throttleInj)...)
				if transports.GRPCFor("firestore") {
					firestorepb.RegisterFirestoreServer(gserv.GRPC(), firestoreGRPC)
				}
				if transports.GRPCFor("firestoreadmin") {
					firestoreadminpb.RegisterFirestoreAdminServer(gserv.GRPC(), firestoreAdminGRPC)
				}
				if transports.GRPCFor("datastore") {
					datastorepb.RegisterDatastoreServer(gserv.GRPC(), datastoreGRPC)
				}
				if transports.GRPCFor("workflowexecutions") {
					executionspb.RegisterExecutionsServer(gserv.GRPC(), workflowExecutionsGRPC)
				}
				if transports.GRPCFor("workflows") {
					workflowspb.RegisterWorkflowsServer(gserv.GRPC(), workflowsGRPC)
				}
				if transports.GRPCFor("managedkafka") {
					managedkafkapb.RegisterManagedKafkaServer(gserv.GRPC(), managedKafkaGRPC)
				}
				if transports.GRPCFor("metastore") {
					metastorepb.RegisterDataprocMetastoreServer(gserv.GRPC(), metastoreGRPC)
				}
				if transports.GRPCFor("eventarc") {
					eventarcpb.RegisterEventarcServer(gserv.GRPC(), eventarcGRPC)
				}
				if transports.GRPCFor("serviceusage") {
					serviceusagepb.RegisterServiceUsageServer(gserv.GRPC(), serviceUsageGRPC)
				}
				if transports.GRPCFor("scheduler") {
					schedulerpb.RegisterCloudSchedulerServer(gserv.GRPC(), schedulerGRPC)
				}
				if transports.GRPCFor("tasks") {
					cloudtaskspb.RegisterCloudTasksServer(gserv.GRPC(), tasksGRPC)
				}
				if transports.GRPCFor("resourcemanager") {
					resourcemanagerpb.RegisterProjectsServer(gserv.GRPC(), resourceManagerGRPC)
				}
				if transports.GRPCFor("iamcredentials") {
					credentialspb.RegisterIAMCredentialsServer(gserv.GRPC(), iamCredentialsGRPC)
				}
				if transports.GRPCFor("dataproc") {
					dataprocpb.RegisterClusterControllerServer(gserv.GRPC(), dataprocGRPC)
					dataprocpb.RegisterJobControllerServer(gserv.GRPC(), dataprocGRPC)
					dataprocpb.RegisterWorkflowTemplateServiceServer(gserv.GRPC(), dataprocGRPC)
				}
				if transports.GRPCFor("functions") {
					functionspb.RegisterCloudFunctionsServiceServer(gserv.GRPC(), functionsGRPC)
					apiv2functionspb.RegisterFunctionServiceServer(gserv.GRPC(), functionsV2GRPC)
				}
				if transports.GRPCFor("pubsub") {
					pubsubpb.RegisterPublisherServer(gserv.GRPC(), pubsubGRPC)
					pubsubpb.RegisterSubscriberServer(gserv.GRPC(), pubsubGRPC)
				}
				if transports.GRPCFor("kms") {
					kmspb.RegisterKeyManagementServiceServer(gserv.GRPC(), kmsGRPC)
				}
				if transports.GRPCFor("logging") {
					loggingpb.RegisterLoggingServiceV2Server(gserv.GRPC(), loggingGRPC)
					loggingpb.RegisterConfigServiceV2Server(gserv.GRPC(), loggingConfigGRPC)
					loggingpb.RegisterMetricsServiceV2Server(gserv.GRPC(), loggingMetricsGRPC)
				}
				if transports.GRPCFor("monitoring") {
					monitoringpb.RegisterMetricServiceServer(gserv.GRPC(), monitoringGRPC)
					monitoringpb.RegisterAlertPolicyServiceServer(gserv.GRPC(), monitoringGRPC)
					monitoringpb.RegisterNotificationChannelServiceServer(gserv.GRPC(), monitoringGRPC)
					monitoringpb.RegisterServiceMonitoringServiceServer(gserv.GRPC(), monitoringGRPC)
				}
				if transports.GRPCFor("storage") {
					grpcstoragepb.RegisterStorageServer(gserv.GRPC(), storageGRPC)
				}
				// Secret Manager's IAM surface (GetIamPolicy/SetIamPolicy/
				// TestIamPermissions) is served by the SecretManagerService itself
				// (its proto embeds the methods), so it does not re-register the
				// standalone google.iam.v1.IAMPolicy service that Pub/Sub and KMS own.
				if transports.GRPCFor("secretmanager") {
					secretmanagerpb.RegisterSecretManagerServiceServer(gserv.GRPC(), secretGRPC)
				}
				// Pub/Sub, KMS and Eventarc share the single IAMPolicy service, so
				// their IAM surfaces are dispatched through one router. Eventarc
				// only joins the router when its gRPC transport is selected, so a
				// trigger/channel IAM name cannot reach a nil core.
				if transports.GRPCFor("kms") || transports.GRPCFor("pubsub") || transports.GRPCFor("eventarc") {
					iamHandlers := []grpcserver.IAMResourceServer{pubsubGRPC, kmsGRPC}
					if transports.GRPCFor("eventarc") {
						iamHandlers = append(iamHandlers, eventarcGRPC)
					}
					iampb.RegisterIAMPolicyServer(gserv.GRPC(), grpcserver.NewIAMRouter(iamHandlers...))
				}
				// google.longrunning.Operations serves every service: it reports
				// synchronous operations as terminal and delegates genuinely
				// asynchronous ones (Dataproc cluster mutations) to a resolver.
				opsResolvers := []grpcoperations.Resolver{}
				if transports.GRPCFor("dataproc") && dataprocCore != nil {
					opsResolvers = append(opsResolvers, dataprocGRPC)
				}
				// Service Usage publishes top-level operations (operations/{id})
				// and MUST be consulted BEFORE Cloud Functions v1, which claims
				// the same namespace and answers an unknown top-level id with
				// NotFound. The serviceusage resolver returns handled=false for
				// ids it does not own, so functions keeps its own 404. It is
				// registered only in the opt-in async mode: in sync mode every
				// operation is returned done inline so nothing polls, and
				// omitting it leaves Functions' existing top-level NotFound (the
				// v1.1.0 contract) unchanged. Mirrors the REST fallback wiring.
				if lroMode.Async() && transports.GRPCFor("serviceusage") && serviceUsageCore != nil {
					opsResolvers = append(opsResolvers, serviceUsageGRPC)
				}
				// Cloud Functions publishes typed operations (J58); v1 names are
				// top-level (operations/{id}) and v2 names are location-scoped.
				if transports.GRPCFor("functions") && functionsCore != nil {
					opsResolvers = append(opsResolvers, functionsGRPC, functionsV2GRPC)
				}
				// Cloud Workflows publishes location-scoped operations. In the
				// default sync mode a done operation never reaches the resolver
				// (the create response is already done) and unknown names fall
				// through to the terminal stub, so registering it is safe.
				if transports.GRPCFor("workflows") && workflowsCore != nil {
					opsResolvers = append(opsResolvers, workflowsGRPC)
				}
				// Dataproc Metastore and Managed Kafka publish location-scoped
				// operations. In the default sync mode a done operation never
				// reaches the resolver (the create response is already done) and
				// unknown names fall through to the terminal stub, so registering
				// them is safe; in async mode the resolver settles a poll.
				if transports.GRPCFor("metastore") && metastoreCore != nil {
					opsResolvers = append(opsResolvers, metastoreGRPC)
				}
				if transports.GRPCFor("managedkafka") && managedKafkaCore != nil {
					opsResolvers = append(opsResolvers, managedKafkaGRPC)
				}
				opsService := grpcoperations.New(opsResolvers...)
				// The opt-in async mode reports a name no resolver or registry
				// owns as NotFound (real google.longrunning semantics); the
				// default keeps the lenient terminal stub the synchronous SDK
				// init paths rely on.
				opsService.SetStrict(lroMode.Async())
				longrunningpb.RegisterOperationsServer(gserv.GRPC(), opsService)
			}

			adminHandler := admin.NewHandler()
			// Cloud Functions source archives are served to the K8s code-mount
			// init container through the shared /lambda/code admin route (the
			// executor's codeKey is path-safe: "location.id").
			if functionsCore != nil {
				adminHandler.SetLambdaCodeFetcher(functionsCore)
			}
			adminHandler.RegisterResetter(stores.objects)
			adminHandler.RegisterResetter(stores.messages)
			adminHandler.RegisterResetter(stores.secrets)
			adminHandler.RegisterResetter(stores.keys)
			adminHandler.RegisterResetter(stores.documents)
			adminHandler.RegisterResetter(stores.entities)
			adminHandler.RegisterResetter(stores.functions)
			// The functions core owns the event-delivery engine; register it so
			// /_jaiscloud/reset invalidates in-flight deliveries (and clears the
			// persisted source archives) in addition to the store reset above.
			if functionsCore != nil {
				adminHandler.RegisterResetter(functionsCore)
			}
			adminHandler.RegisterResetter(stores.workflows)
			adminHandler.RegisterResetter(stores.dataproc)
			adminHandler.RegisterResetter(stores.managedkafka)
			adminHandler.RegisterResetter(stores.metastore)
			adminHandler.RegisterResetter(stores.iceberg)
			adminHandler.RegisterResetter(stores.hms)
			adminHandler.RegisterResetter(stores.bigquery)
			adminHandler.RegisterResetter(stores.logEntries)
			adminHandler.RegisterResetter(stores.monitoring)
			adminHandler.RegisterResetter(stores.eventarc)
			adminHandler.RegisterResetter(schedulerCore)
			adminHandler.RegisterSchedulerTicker(schedulerEngine)
			adminHandler.RegisterResetter(tasksCore)
			adminHandler.RegisterTasksTicker(tasksEngine)
			adminHandler.RegisterResetter(stores.resources)
			adminHandler.RegisterResetter(stores.blobs)
			adminHandler.RegisterResetter(storageP)
			adminHandler.RegisterResetter(storageGRPC)
			adminHandler.RegisterResetter(firestoreP)
			adminHandler.RegisterResetter(firestoreGRPC)
			adminHandler.RegisterResetter(firestoreAdminGRPC)
			// The Datastore core owns the in-memory transaction read-set
			// registry; register it so /_jaiscloud/reset clears open
			// transactions. Its entity store (stores.entities) is registered
			// separately above.
			adminHandler.RegisterResetter(datastoreCore)
			adminHandler.RegisterPostRestoreHook(storageP)
			if snap, ok := stores.resources.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("resources", snap)
			}
			if snap, ok := stores.objects.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("gcs_objects", snap)
			}
			if snap, ok := stores.messages.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("pubsub_messages", snap)
			}
			if snap, ok := stores.secrets.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("secrets", snap)
			}
			if snap, ok := stores.keys.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("keys", snap)
			}
			if snap, ok := stores.documents.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("firestore_documents", snap)
			}
			if snap, ok := stores.entities.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("datastore_entities", snap)
			}
			if snap, ok := stores.functions.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("functions", snap)
			}
			if snap, ok := stores.workflows.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("workflows", snap)
			}
			if snap, ok := stores.dataproc.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("dataproc", snap)
			}
			if snap, ok := stores.managedkafka.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("managedkafka", snap)
			}
			if snap, ok := stores.metastore.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("metastore", snap)
			}
			if snap, ok := stores.iceberg.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("iceberg", snap)
			}
			if snap, ok := stores.hms.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("hms", snap)
			}
			if snap, ok := stores.bigquery.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("bigquery", snap)
			}
			if snap, ok := stores.logEntries.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("log_entries", snap)
			}
			if snap, ok := stores.monitoring.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("monitoring", snap)
			}
			if snap, ok := stores.eventarc.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("eventarc", snap)
			}
			if snap, ok := stores.scheduler.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("scheduler", snap)
			}
			if snap, ok := stores.tasks.(admin.Snapshotter); ok {
				adminHandler.RegisterSnapshotter("tasks", snap)
			}
			if sb, ok := stores.blobs.(admin.SnapshotBlobStore); ok {
				adminHandler.RegisterBlobStore(sb)
			}
			adminHandler.SetMeta(admin.HandlerMeta{
				Cloud:     "gcp",
				Region:    cfg.Region,
				AccountID: cfg.ProjectID,
				StateDir:  stateDir,
			})
			if cfg.KMSMasterKey != "" {
				adminHandler.SetKEKFingerprint(snapversion.FingerprintKEK([]byte(cfg.KMSMasterKey)))
			}

			cloudAdapter := gcpadapter.NewAdapter(cfg.GCPServiceAccount)
			if throttleCfg.Enabled() {
				// The adapter exposes the injector to the gateway as the
				// optional RequestFilter capability; registering it as a
				// Resetter clears counters/buckets on /_jaiscloud/reset.
				cloudAdapter.SetThrottle(throttleInj)
				adminHandler.RegisterResetter(throttleInj)
			}

			var certs certstore.CertStore
			if fsCS, err := certstore.NewFilesystemCertStore(stateDir); err == nil {
				certs = fsCS
			} else {
				certs = certstore.NewMemoryCertStore()
			}

			barrier := snapshot.NewBarrier()
			adminHandler.SetBarrier(barrier)

			var gatewayOpts []func(*gateway.Server)
			gatewayOpts = append(gatewayOpts, gateway.WithBarrier(barrier))
			gatewayOpts = append(gatewayOpts, gateway.WithGCSCORSLookup(storageP.GetBucketCORSRules))
			if !transports.REST() {
				// gRPC-only: keep the always-on admin/health/metrics listener but
				// do not expose the GCP REST API on it.
				gatewayOpts = append(gatewayOpts, gateway.WithCloudRoutesDisabled())
			}
			if cfg.GCPMetadataEnabled {
				metaCfg := gcpadapter.MetadataConfig{
					ProjectID:      cfg.ProjectID,
					ServiceAccount: cfg.GCPServiceAccount,
				}
				gatewayOpts = append(gatewayOpts, gateway.WithExtraRoutes(func(r chi.Router) {
					gcpadapter.RegisterMetadataRoutes(r, metaCfg)
				}))
			}
			if transports.REST() {
				// OAuth2 token endpoint at the emulator root (/token, /v1/token).
				// google-auth-library service-account credentials exchange a
				// signed JWT assertion here for an access token; without it the
				// whole SA-credentials flow 404s. REST-only, since gRPC clients
				// authenticate via metadata/ADC.
				tokenSvc := gcauth.NewService(gcauth.Config{
					ProjectID:      cfg.ProjectID,
					ServiceAccount: cfg.GCPServiceAccount,
				})
				gatewayOpts = append(gatewayOpts, gateway.WithExtraRoutes(func(r chi.Router) {
					tokenSvc.RegisterRoutes(r)
				}))
			}

			dataDir := cfg.DataDir
			if dataDir == "" {
				if home, err := os.UserHomeDir(); err == nil {
					dataDir = filepath.Join(home, ".jaiscloud", "jaiscloud-gcp")
				} else {
					dataDir = filepath.Join(".jaiscloud", "jaiscloud-gcp")
				}
			}
			adminHandler.SetDataDir(dataDir)

			var cleanup func() = func() {}
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
				// Re-seed the GCS generation counter from the restored store so
				// generations stay monotonic across restart (memory mode restores
				// after the provider is constructed).
				storageP.SeedGeneration(ctx)

				var localBlobs *blobfs.LocalFSBlobStore
				if lb, ok := stores.blobs.(*blobfs.LocalFSBlobStore); ok {
					localBlobs = lb
				}
				loopCfg := snapshot.SnapshotLoopConfig{
					Barrier:     barrier,
					Stores:      loopStores,
					BlobStore:   localBlobs,
					DataDir:     dataDir,
					Interval:    cfg.SnapshotInterval,
					Clock:       cfg.Clock,
					SaveTimeout: 10 * time.Second,
				}
				loop := snapshot.NewSnapshotLoop(loopCfg)
				loopCtx, loopCancel := context.WithCancel(ctx)
				loop.Start(loopCtx)
				prevCleanup := cleanup
				cleanup = func() { loopCancel(); loop.Stop(); prevCleanup() }
			}
			defer cleanup()

			srv := gateway.NewServer(cfg, adminHandler, reg, cloudAdapter, certs, gatewayOpts...)

			// Background Cloud Monitoring alert-policy evaluator (30s ticker,
			// matching the AWS CloudWatch alarm evaluator). Stopped cleanly when
			// the server shuts down. Runs whenever the Monitoring service is
			// exposed on either transport (REST or gRPC), since alert policies
			// can be managed over either.
			evalCtx, evalCancel := context.WithCancel(ctx)
			if serviceEnabled("monitoring") {
				go monitoringEval.Run(evalCtx)
			}
			defer evalCancel()

			// Cloud Scheduler cron engine: fires due httpTarget/pubsubTarget
			// jobs on the virtual clock. Stopped cleanly on shutdown.
			if serviceEnabled("scheduler") {
				schedulerCtx, schedulerCancel := context.WithCancel(ctx)
				go schedulerEngine.Run(schedulerCtx)
				defer schedulerCancel()
			}

			// Cloud Tasks dispatch engine: delivers due httpRequest tasks on the
			// virtual clock. Stopped cleanly on shutdown.
			if serviceEnabled("tasks") {
				tasksCtx, tasksCancel := context.WithCancel(ctx)
				go tasksEngine.Run(tasksCtx)
				defer tasksCancel()
			}

			// Cloud Functions event-trigger delivery workers. Stopped cleanly on
			// shutdown; before this point dispatch runs inline.
			if functionsCore != nil {
				deliverCtx, deliverCancel := context.WithCancel(ctx)
				functionsCore.Start(deliverCtx)
				defer deliverCancel()
			}

			// Serve gRPC on its own listener (plaintext h2c) alongside the HTTP
			// gateway. Emulator-mode SDKs point FIRESTORE_EMULATOR_HOST here.
			// Skipped entirely when the gRPC transport is not selected.
			if gserv != nil {
				go func() {
					slog.Info("grpc server starting", "grpc_port", grpcPort)
					if err := gserv.Serve(); err != nil {
						slog.Error("grpc server error", "err", err)
					}
				}()
				defer gserv.Stop()
			}

			// Serve the Hive Metastore (Thrift) serving plane on its own TCP
			// listener. Thrift is a binary protocol over raw TCP — it does not
			// flow through the HTTP gateway or the gRPC server. The catalog is
			// single-global (accepted divergence, MP4): all control-plane
			// Services share it, matching the AWS Glue Data Catalog's
			// one-catalog-per-account+region model; the per-Service endpoint_uri
			// emitted by the control plane is cosmetic. Only started when
			// metastore is enabled.
			if serviceEnabled("metastore") {
				hmsPort, _ := cmd.Flags().GetInt("hms-port")
				hmsServer := hms.NewServer(fmt.Sprintf(":%d", hmsPort), stores.hms)
				go func() {
					slog.Info("hive metastore (thrift) server starting", "hms_port", hmsPort)
					if err := hmsServer.Serve(); err != nil {
						slog.Error("hive metastore server error", "err", err)
					}
				}()
				defer hmsServer.Stop()
			}

			return srv.ListenAndServe()
		},
	}
	cmd.Flags().Int("port", 8080, "Listen port")
	cmd.Flags().Bool("ephemeral", false, "Run with purely in-memory state (no persistence)")
	cmd.Flags().String("dsn", "", "PostgreSQL connection string (postgres://...)")
	cmd.Flags().String("data-dir", "", "Root data directory for blobs and snapshots")
	cmd.Flags().Bool("fresh-start", false, "Wipe existing state on startup before initializing stores")
	cmd.Flags().String("log-level", "info", "Log level: debug/info/warn/error")
	cmd.Flags().Bool("metrics", false, "Expose Prometheus metrics at /metrics")
	cmd.Flags().Bool("deterministic", false, "Enable deterministic mode")
	cmd.Flags().Int64("seed", 0, "Random seed (requires --deterministic)")
	cmd.Flags().String("time", "", "Base time RFC3339 (requires --deterministic)")
	cmd.Flags().String("time-mode", "offset", "Time mode: frozen or offset")
	cmd.Flags().String("blob-dir", "", "Directory for GCS blob bytes (persistent mode only)")
	cmd.Flags().Bool("gcp-metadata", false, "Enable the GCP metadata-server emulator")
	cmd.Flags().String("kms-master-key", "", "32-byte hex KEK for KMS envelope encryption")
	cmd.Flags().Int("grpc-port", 8081, "gRPC (h2c) listen port")
	cmd.Flags().Int("hms-port", 9083, "Hive Metastore (Thrift) listen port")
	cmd.Flags().String("transports", "rest,grpc", "GCP wire transports to expose: rest,grpc,both,none")
	cmd.Flags().String("transport-overrides", "", "Per-service transport overrides, e.g. storage=grpc,pubsub=rest,memorystore=none")
	return cmd
}

func bindFlags(cmd *cobra.Command) {
	viper.BindPFlag("port", cmd.Flags().Lookup("port"))
	viper.BindPFlag("ephemeral", cmd.Flags().Lookup("ephemeral"))
	viper.BindPFlag("dsn", cmd.Flags().Lookup("dsn"))
	viper.BindPFlag("data_dir", cmd.Flags().Lookup("data-dir"))
	viper.BindPFlag("fresh_start", cmd.Flags().Lookup("fresh-start"))
	viper.BindPFlag("log_level", cmd.Flags().Lookup("log-level"))
	viper.BindPFlag("metrics", cmd.Flags().Lookup("metrics"))
	viper.BindPFlag("deterministic", cmd.Flags().Lookup("deterministic"))
	viper.BindPFlag("seed", cmd.Flags().Lookup("seed"))
	viper.BindPFlag("time", cmd.Flags().Lookup("time"))
	viper.BindPFlag("time_mode", cmd.Flags().Lookup("time-mode"))
	viper.BindPFlag("blob_dir", cmd.Flags().Lookup("blob-dir"))
	viper.BindPFlag("gcp_metadata_enabled", cmd.Flags().Lookup("gcp-metadata"))
	viper.BindPFlag("kms_master_key", cmd.Flags().Lookup("kms-master-key"))
	viper.BindPFlag("transports", cmd.Flags().Lookup("transports"))
	viper.BindPFlag("transport_overrides", cmd.Flags().Lookup("transport-overrides"))
}

// pubsubNotificationPublisher adapts the emulator's Pub/Sub message store to
// the monitoring evaluator's Publisher interface. Alert-policy notifications
// are written as ordinary Pub/Sub messages (envelope-encrypted with the server
// DEK, matching the Publish service) on the channel's labels.topic, which is
// the observable effect (mirrors AWS CloudWatch alarm -> SNS delivery).
type pubsubNotificationPublisher struct {
	messages  pubsubstore.Messages
	encryptor crypto.EnvelopeEncryptor
}

// schedulerPubSubPublisher adapts the Pub/Sub provider's fan-out publish path to
// the Cloud Scheduler engine's Publisher interface. The topic is the full
// resource name carried by a pubsubTarget.
type schedulerPubSubPublisher struct {
	provider *pubsubprovider.Provider
}

// Publish writes a message to topic (projects/{p}/topics/{t}) with the same
// envelope encryption and per-subscription fan-out as a topics.publish call.
func (p schedulerPubSubPublisher) Publish(ctx context.Context, topic string, data []byte, attributes map[string]string) error {
	_, err := p.provider.PublishEvent(ctx, "", topic, data, attributes)
	return err
}

// Publish writes a message to the topic named by topic (a full
// "projects/{p}/topics/{t}" resource name).
func (p pubsubNotificationPublisher) Publish(ctx context.Context, topic string, data []byte) error {
	short := topic
	if i := strings.LastIndex(topic, "/topics/"); i >= 0 {
		short = topic[i+len("/topics/"):]
	}
	rawDEK, wrappedDEK, err := p.encryptor.Wrap(ctx, "", "")
	if err != nil {
		return err
	}
	ciphertext, err := kmsstore.EncryptData(rawDEK, data, nil)
	if err != nil {
		return err
	}
	id, err := p.messages.NextID(ctx)
	if err != nil {
		return err
	}
	return p.messages.Put(ctx, pubsubstore.Message{
		Topic:       short,
		MessageID:   id,
		Data:        base64.StdEncoding.EncodeToString(ciphertext),
		PublishTime: clock.Now(),
		WrappedDEK:  wrappedDEK,
	})
}

// stores bundles the per-mode store backends constructed by initStores.
type stores struct {
	objects      gcs.ObjectStore
	messages     pubsubstore.Messages
	secrets      secretmanagerstore.Store
	keys         kmsstore.Store
	documents    firestorestore.FirestoreStore
	entities     datastorestore.Store
	functions    functionsstore.Store
	workflows    workflowsstore.Store
	dataproc     dataprocstore.Store
	managedkafka managedkafkastore.Store
	metastore    metastorestore.Store
	iceberg      icebergstore.Store
	hms          hmsstore.Store
	bigquery     bigquerystore.Store
	logEntries   loggingstore.Store
	monitoring   monitoringstore.Store
	eventarc     eventarcstore.Store
	scheduler    schedulerstore.Store
	tasks        tasksstore.Store
	resources    store.ResourceStore
	blobs        blobfs.BlobStore
	close        func()
}

func initStores(ctx context.Context, cfg *config.Config, instanceID string) (*stores, error) {
	if cfg.DSN != "" {
		pg, err := store.NewPostgresResourceStore(ctx, cfg.DSN, string(cfg.Cloud))
		if err != nil {
			return nil, fmt.Errorf("postgres: %w", err)
		}
		if err := store.RunMigrations(ctx, pg.Pool(), string(cfg.Cloud), gcpstore.MigrationFS, "gcp"); err != nil {
			pg.Close()
			return nil, fmt.Errorf("gcp migrations: %w", err)
		}
		blobs, err := blobfs.NewLocalFSBlobStore(cfg.BlobDir)
		if err != nil {
			pg.Close()
			return nil, fmt.Errorf("blobfs: %w", err)
		}
		return &stores{
			objects:      gcs.NewPostgresObjectStore(pg.Pool()),
			messages:     pubsubstore.NewPostgresMessages(pg.Pool()),
			secrets:      secretmanagerstore.NewPostgresStore(pg.Pool()),
			keys:         kmsstore.NewPostgresStore(pg.Pool()),
			documents:    firestorestore.NewPostgresStore(pg.Pool()),
			entities:     datastorestore.NewPostgresStore(pg.Pool()),
			functions:    functionsstore.NewPostgresStore(pg.Pool()),
			workflows:    workflowsstore.NewPostgresStore(pg.Pool()),
			dataproc:     dataprocstore.NewPostgresStore(pg.Pool()),
			managedkafka: managedkafkastore.NewPostgresStore(pg.Pool()),
			metastore:    metastorestore.NewPostgresStore(pg.Pool()),
			iceberg:      icebergstore.NewPostgresStore(pg.Pool()),
			hms:          hmsstore.NewPostgresStore(pg.Pool()),
			bigquery:     bigquerystore.NewPostgresStore(pg.Pool()),
			logEntries:   loggingstore.NewPostgresStore(pg.Pool()),
			monitoring:   monitoringstore.NewPostgresStore(pg.Pool()),
			eventarc:     eventarcstore.NewPostgresStore(pg.Pool()),
			scheduler:    schedulerstore.NewPostgresStore(pg.Pool()),
			tasks:        tasksstore.NewPostgresStore(pg.Pool()),
			resources:    pg,
			blobs:        blobs,
			close:        func() { pg.Close() },
		}, nil
	}
	if cfg.Ephemeral {
		return &stores{
			objects:      gcs.NewMemoryObjectStore(),
			messages:     pubsubstore.NewMemoryMessages(),
			secrets:      secretmanagerstore.NewMemoryStore(),
			keys:         kmsstore.NewMemoryStore(),
			documents:    firestorestore.NewMemoryStore(),
			entities:     datastorestore.NewMemoryStore(),
			functions:    functionsstore.NewMemoryStore(),
			workflows:    workflowsstore.NewMemoryStore(),
			dataproc:     dataprocstore.NewMemoryStore(),
			managedkafka: managedkafkastore.NewMemoryStore(),
			metastore:    metastorestore.NewMemoryStore(),
			iceberg:      icebergstore.NewMemoryStore(),
			hms:          hmsstore.NewMemoryStore(),
			bigquery:     bigquerystore.NewMemoryStore(),
			logEntries:   loggingstore.NewMemoryStore(),
			monitoring:   monitoringstore.NewMemoryStore(),
			eventarc:     eventarcstore.NewMemoryStore(),
			scheduler:    schedulerstore.NewMemoryStore(),
			tasks:        tasksstore.NewMemoryStore(),
			resources:    store.NewMemoryResourceStore(),
			blobs:        blobfs.NewMemoryBlobStore(),
			close:        func() {},
		}, nil
	}
	blobs, err := blobfs.NewSessionBlobStore(instanceID)
	if err != nil {
		return nil, fmt.Errorf("blobfs: %w", err)
	}
	return &stores{
		objects:      gcs.NewMemoryObjectStore(),
		messages:     pubsubstore.NewMemoryMessages(),
		secrets:      secretmanagerstore.NewMemoryStore(),
		keys:         kmsstore.NewMemoryStore(),
		documents:    firestorestore.NewMemoryStore(),
		entities:     datastorestore.NewMemoryStore(),
		functions:    functionsstore.NewMemoryStore(),
		workflows:    workflowsstore.NewMemoryStore(),
		dataproc:     dataprocstore.NewMemoryStore(),
		managedkafka: managedkafkastore.NewMemoryStore(),
		metastore:    metastorestore.NewMemoryStore(),
		iceberg:      icebergstore.NewMemoryStore(),
		hms:          hmsstore.NewMemoryStore(),
		bigquery:     bigquerystore.NewMemoryStore(),
		logEntries:   loggingstore.NewMemoryStore(),
		monitoring:   monitoringstore.NewMemoryStore(),
		eventarc:     eventarcstore.NewMemoryStore(),
		scheduler:    schedulerstore.NewMemoryStore(),
		tasks:        tasksstore.NewMemoryStore(),
		resources:    store.NewMemoryResourceStore(),
		blobs:        blobs,
		close:        func() {},
	}, nil
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("jaiscloud-gcp %s\n", version)
		},
	}
}

// buildK8sClient constructs a kubernetes.Interface using in-cluster config if
// available, falling back to JAISCLOUD_K8S_APISERVER + JAISCLOUD_K8S_TOKEN env
// vars (mirrors cmd/jaiscloud-aws/main.go).
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

func envCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "env",
		Short: "Print effective configuration as environment variables",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load(model.CloudGCP)
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
			fmt.Printf("JAISCLOUD_CLOUD=gcp\n")
			fmt.Printf("JAISCLOUD_PROJECT_ID=%s\n", cfg.ProjectID)
			fmt.Printf("JAISCLOUD_LOG_LEVEL=%s\n", cfg.LogLevel)
			return nil
		},
	}
}

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
			fmt.Printf("OK: jaiscloud-gcp is running at %s\n", host)
			return nil
		},
	}
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	return cmd
}

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
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	return cmd
}

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
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	cmd.Flags().StringP("output", "o", "-", "Output file (default: stdout)")
	return cmd
}

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
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	cmd.Flags().StringP("input", "i", "-", "Input file (default: stdin)")
	cmd.Flags().Bool("new-instance", false, "Assign a fresh instance ID on import")
	cmd.Flags().Bool("dry-run", false, "Validate the snapshot without modifying state")
	return cmd
}

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
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
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
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
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
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
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
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
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
	cmd.Flags().String("host", defaultHost, "Emulator host URL")
	return cmd
}

// functionsUploadOrigin returns the origin used to build Cloud Functions v2
// source-upload URLs for the gRPC transport (the REST transport derives it from
// the request host). It honours STORAGE_EMULATOR_HOST /
// JAISCLOUD_GCS_EMULATOR_ENDPOINT (the same precedence as the Dataproc Spark
// wiring) so a deploy through a port-forward or k3d address reaches the
// emulator, and otherwise falls back to the local REST listener.
func functionsUploadOrigin(port int) string {
	for _, env := range []string{"STORAGE_EMULATOR_HOST", "JAISCLOUD_GCS_EMULATOR_ENDPOINT"} {
		if v := os.Getenv(env); v != "" {
			if !strings.Contains(v, "://") {
				v = "http://" + v
			}
			return strings.TrimRight(v, "/")
		}
	}
	return fmt.Sprintf("http://localhost:%d", port)
}

// lambdaCodeURL returns the admin base a K8s code-fetch init container uses to
// download a Cloud Function's source archive, including the /_jaiscloud prefix
// (the executor appends /lambda/code/{account}/{key}/$LATEST). It derives the
// base from the cluster-reachable JAISCLOUD_GCS_EMULATOR_ENDPOINT (the in-cluster
// Service DNS name) and returns "" when it is unset, so the code mount stays
// disabled rather than guessing an unreachable localhost address.
// JAISCLOUD_LAMBDA_CODE_URL takes precedence and is applied earlier (the executor
// reads it in DefaultLambdaConfig).
func lambdaCodeURL() string {
	v := os.Getenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT")
	if v == "" {
		return ""
	}
	if !strings.Contains(v, "://") {
		v = "http://" + v
	}
	return strings.TrimRight(v, "/") + "/_jaiscloud"
}
