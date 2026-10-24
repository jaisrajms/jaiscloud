// Package ui contributes the GCP service catalog and API routes to the shared
// UI core. It exposes the GCP cloud identity and mounts per-service UI APIs
// (Cloud Storage, Pub/Sub, Firestore, Compute, Cloud Run, Cloud Functions, Cloud
// Scheduler, Cloud Tasks, Workflows, Eventarc, BigQuery, Dataproc, Managed Kafka,
// IAM, Cloud KMS, Secret Manager, Logging, Monitoring, Resource Manager; the
// rest follow) over the providers wired in cmd/jaiscloud-gcp.
package ui

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
	resourcemanagercore "jaiscloud/internal/gcp/service/resourcemanager"
	bigqueryui "jaiscloud/internal/gcp/ui/bigquery"
	computeui "jaiscloud/internal/gcp/ui/compute"
	dataprocui "jaiscloud/internal/gcp/ui/dataproc"
	datastoreui "jaiscloud/internal/gcp/ui/datastore"
	eventarcui "jaiscloud/internal/gcp/ui/eventarc"
	firestoreui "jaiscloud/internal/gcp/ui/firestore"
	functionsui "jaiscloud/internal/gcp/ui/functions"
	iamui "jaiscloud/internal/gcp/ui/iam"
	kmsui "jaiscloud/internal/gcp/ui/kms"
	loggingui "jaiscloud/internal/gcp/ui/logging"
	managedkafkaui "jaiscloud/internal/gcp/ui/managedkafka"
	monitoringui "jaiscloud/internal/gcp/ui/monitoring"
	pubsubui "jaiscloud/internal/gcp/ui/pubsub"
	resourcemanagerui "jaiscloud/internal/gcp/ui/resourcemanager"
	runui "jaiscloud/internal/gcp/ui/run"
	schedulerui "jaiscloud/internal/gcp/ui/scheduler"
	secretmanagerui "jaiscloud/internal/gcp/ui/secretmanager"
	storageui "jaiscloud/internal/gcp/ui/storage"
	tasksui "jaiscloud/internal/gcp/ui/tasks"
	workflowsui "jaiscloud/internal/gcp/ui/workflows"
	"jaiscloud/internal/model"
	coreui "jaiscloud/internal/ui"
)

// Registrar is the GCP implementation of coreui.Registrar.
type Registrar struct {
	storage       storageui.ProviderInterface
	pubsub        pubsubui.ProviderInterface
	firestore     firestoreui.ProviderInterface
	datastore     datastoreui.ProviderInterface
	compute       computeui.ProviderInterface
	dataproc      dataprocui.ProviderInterface
	bigquery      bigqueryui.ProviderInterface
	run           runui.ProviderInterface
	scheduler     schedulerui.ProviderInterface
	iam           iamui.ProviderInterface
	kms           kmsui.ProviderInterface
	secretmanager secretmanagerui.ProviderInterface
	logging       loggingui.ProviderInterface
	monitoring    monitoringui.ProviderInterface
	tasks         tasksui.ProviderInterface
	workflows     workflowsui.ProviderInterface
	eventarc      eventarcui.ProviderInterface
	functions     functionsui.ProviderInterface
	managedkafka  managedkafkaui.ProviderInterface
	cfg           *config.Config
	// resourcemanager is the project registry the console's accounts endpoint
	// enumerates (created + configured projects). It is nil when the service is
	// disabled, in which case only the configured accounts are reported.
	resourcemanager *resourcemanagercore.Service
	// modes reports the configured engine/executor modes so the console status
	// reflects what is actually running (see ServiceModes).
	modes ServiceModes
}

// ServiceModes reports the configured engine/executor modes that upgrade an
// otherwise shape-only service to an engine-backed one. An empty or "mock"
// value means no engine. main.go resolves these once from the environment and
// the executor config.
type ServiceModes struct {
	KafkaBroker string // managedkafka: mock (default) | docker | k8s | native
	Spark       string // dataproc: mock (default) | docker | k8s
	Lambda      string // functions: mock (default) | docker | k8s
	CloudRun    string // run: mock (default) | docker | k8s

	// Sources name where each mode came from (env var or "default"), shown in
	// the admin Runtime view.
	KafkaBrokerSource string
	SparkSource       string
	LambdaSource      string
	CloudRunSource    string
}

// WithServiceModes sets the configured engine modes the catalog reports. It is
// optional; the zero value reports every engine-capable service as if no engine
// were configured.
func (r *Registrar) WithServiceModes(modes ServiceModes) *Registrar {
	r.modes = modes
	return r
}

// engineBacked reports whether mode selects a real engine. allowed is the set
// of modes the service actually honours; every other value (including a mode
// that silently falls back to mock, e.g. Dataproc's unsupported docker mode)
// reports no engine.
func engineBacked(mode string, allowed ...string) bool {
	m := strings.ToLower(strings.TrimSpace(mode))
	for _, a := range allowed {
		if m == a {
			return true
		}
	}
	return false
}

// statusTier returns the full tier when an engine is configured, else fallback.
func statusTier(engineOn bool, fallback string) string {
	if engineOn {
		return coreui.TierFull
	}
	return fallback
}

// engineNote describes an engine-backed service, or returns the shape-only
// fallback when no engine is configured.
func engineNote(engineOn bool, engine, mode, fallback string) string {
	if engineOn {
		return fmt.Sprintf("Engine-backed — %s (%s)", engine, mode)
	}
	return fallback
}

// engineInfo builds the structured backend info for an engine-capable service.
// realModes is the set of modes that actually run an engine (deciding Active);
// modes lists every backend with its support state and caveat. Mode is only
// reported when the configured backend is real, so a mode that falls back to
// mock (e.g. Dataproc docker) reads as "no engine".
func engineInfo(activeMode, source string, realModes []string, modes []coreui.EngineMode) *coreui.Engine {
	info := &coreui.Engine{Source: source, Modes: modes}
	if engineBacked(activeMode, realModes...) {
		info.Active = true
		info.Mode = strings.ToLower(strings.TrimSpace(activeMode))
	}
	return info
}

// NewRegistrar returns the GCP UI registrar. A nil provider leaves that
// service's pages out of the catalog.
func NewRegistrar(storageProvider storageui.ProviderInterface, pubsubProvider pubsubui.ProviderInterface, firestoreProvider firestoreui.ProviderInterface, datastoreProvider datastoreui.ProviderInterface, computeProvider computeui.ProviderInterface, dataprocProvider dataprocui.ProviderInterface, bigqueryProvider bigqueryui.ProviderInterface, runProvider runui.ProviderInterface, schedulerProvider schedulerui.ProviderInterface, iamProvider iamui.ProviderInterface, kmsProvider kmsui.ProviderInterface, secretProvider secretmanagerui.ProviderInterface, loggingProvider loggingui.ProviderInterface, monitoringProvider monitoringui.ProviderInterface, tasksProvider tasksui.ProviderInterface, workflowsProvider workflowsui.ProviderInterface, eventarcProvider eventarcui.ProviderInterface, functionsProvider functionsui.ProviderInterface, managedkafkaProvider managedkafkaui.ProviderInterface, resourceManager *resourcemanagercore.Service, cfg *config.Config) *Registrar {
	return &Registrar{
		storage:         storageProvider,
		pubsub:          pubsubProvider,
		firestore:       firestoreProvider,
		datastore:       datastoreProvider,
		compute:         computeProvider,
		dataproc:        dataprocProvider,
		bigquery:        bigqueryProvider,
		run:             runProvider,
		scheduler:       schedulerProvider,
		iam:             iamProvider,
		kms:             kmsProvider,
		secretmanager:   secretProvider,
		logging:         loggingProvider,
		monitoring:      monitoringProvider,
		tasks:           tasksProvider,
		workflows:       workflowsProvider,
		eventarc:        eventarcProvider,
		functions:       functionsProvider,
		managedkafka:    managedkafkaProvider,
		resourcemanager: resourceManager,
		cfg:             cfg,
	}
}

// Accounts implements coreui.AccountsProvider: it contributes every project the
// Resource Manager registry knows (created projects unioned with the configured
// default + extra accounts) so the console project picker reflects the real
// registry instead of only the startup config. A nil resourcemanager core (the
// service disabled) contributes nothing and the configured accounts stand
// alone. A store error is swallowed to an empty contribution: the accounts
// endpoint must never fail the console over a listing problem.
func (r *Registrar) Accounts(ctx context.Context) []string {
	if r.resourcemanager == nil {
		return nil
	}
	var out []string
	token := ""
	for {
		page, next, err := r.resourcemanager.ListProjects(ctx, 0, token, false, "")
		if err != nil {
			return out
		}
		for _, p := range page {
			out = append(out, p.ProjectID)
		}
		if next == "" {
			return out
		}
		token = next
	}
}

// Cloud implements coreui.Registrar.
func (r *Registrar) Cloud() model.Cloud { return model.CloudGCP }

// Services implements coreui.Registrar: only services whose provider is wired
// are advertised.
func (r *Registrar) Services() []coreui.ServiceDescriptor {
	services := make([]coreui.ServiceDescriptor, 0, 16)

	// Engine-capable services report full fidelity when a real engine is
	// configured, and their documented shape-only/metadata status otherwise.
	// Each service honours a specific set of engine modes: Kafka
	// docker/k8s/native, Spark docker/k8s, Lambda docker/k8s, Cloud Run
	// docker/k8s.
	kafkaOn := engineBacked(r.modes.KafkaBroker, "docker", "k8s", "native")
	sparkOn := engineBacked(r.modes.Spark, "k8s", "docker")
	lambdaOn := engineBacked(r.modes.Lambda, "docker", "k8s")
	cloudRunOn := engineBacked(r.modes.CloudRun, "k8s", "docker")

	// Structured backend availability, shown as the console's mode tag + matrix.
	runEngine := engineInfo(r.modes.CloudRun, r.modes.CloudRunSource, []string{"k8s", "docker"}, []coreui.EngineMode{
		{Name: "mock", Supported: true, Note: "stored record; no runtime"},
		{Name: "docker", Supported: true, Note: "container + published port, reverse-proxied"},
		{Name: "k8s", Supported: true, Note: "Pod + ClusterIP Service, reverse-proxied"},
	})
	functionsEngine := engineInfo(r.modes.Lambda, r.modes.LambdaSource, []string{"docker", "k8s"}, []coreui.EngineMode{
		{Name: "mock", Supported: true, Note: "echo handler; no real execution"},
		{Name: "docker", Supported: true, Note: "warm container pool"},
		{Name: "k8s", Supported: true, Note: "warm Pod + Service; survives restarts"},
	})
	dataprocEngine := engineInfo(r.modes.Spark, r.modes.SparkSource, []string{"k8s", "docker"}, []coreui.EngineMode{
		{Name: "mock", Supported: true, Note: "jobs simulated"},
		{Name: "docker", Supported: true, Note: "containerized spark-submit (local[*])"},
		{Name: "k8s", Supported: true, Note: "real Spark driver pods"},
	})
	kafkaEngine := engineInfo(r.modes.KafkaBroker, r.modes.KafkaBrokerSource, []string{"docker", "k8s", "native"}, []coreui.EngineMode{
		{Name: "mock", Supported: true, Note: "no broker; metadata only"},
		{Name: "docker", Supported: true, Note: "Redpanda container on the local Docker daemon"},
		{Name: "k8s", Supported: true, Note: "Redpanda Pod + Service"},
		{Name: "native", Supported: true, Note: "local rpk subprocess"},
	})
	if r.storage != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "storage",
			Label:    "Cloud Storage",
			Category: "Storage",
			RootPath: "/gcp/storage/buckets",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{{Label: "Buckets", Path: "/gcp/storage/buckets"}},
		})
	}
	if r.pubsub != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "pubsub",
			Label:    "Pub/Sub",
			Category: "Integration",
			RootPath: "/gcp/pubsub/topics",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{
				{Label: "Topics", Path: "/gcp/pubsub/topics"},
				{Label: "Subscriptions", Path: "/gcp/pubsub/subscriptions"},
			},
		})
	}
	if r.firestore != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "firestore",
			Label:    "Firestore",
			Category: "Databases",
			RootPath: "/gcp/firestore/collections",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{
				{Label: "Collections", Path: "/gcp/firestore/collections"},
				{Label: "Query", Path: "/gcp/firestore/query"},
				{Label: "Indexes", Path: "/gcp/firestore/indexes"},
			},
		})
	}
	if r.datastore != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "datastore",
			Label:    "Datastore",
			Category: "Databases",
			RootPath: "/gcp/datastore/kinds",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{
				{Label: "Kinds", Path: "/gcp/datastore/kinds"},
				{Label: "Query", Path: "/gcp/datastore/query"},
			},
		})
	}
	if r.compute != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "compute",
			Label:    "Compute Engine",
			Category: "Compute",
			RootPath: "/gcp/compute/instances",
			Tier:     coreui.TierMetadata,
			Note:     "Metadata only — no VM/disk/network data plane",
			Children: []coreui.ServiceChild{{Label: "Instances", Path: "/gcp/compute/instances"}},
		})
	}
	if r.run != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "run",
			Label:    "Cloud Run",
			Category: "Compute",
			RootPath: "/gcp/run/services",
			Tier:     statusTier(cloudRunOn, coreui.TierShape),
			Note:     engineNote(cloudRunOn, "container runtime executor", r.modes.CloudRun, "Shape only — control plane; docker/k8s executor optional"),
			Engine:   runEngine,
			Children: []coreui.ServiceChild{{Label: "Services", Path: "/gcp/run/services"}},
		})
	}
	if r.functions != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "functions",
			Label:    "Cloud Functions",
			Category: "Compute",
			RootPath: "/gcp/functions",
			Tier:     statusTier(lambdaOn, coreui.TierShape),
			Note:     engineNote(lambdaOn, "Functions Framework executor", r.modes.Lambda, "Shape only — GCS-source execution runs the GCP Functions Framework (Docker/K8s); no container build"),
			Engine:   functionsEngine,
			Children: []coreui.ServiceChild{{Label: "Functions", Path: "/gcp/functions"}},
		})
	}
	if r.scheduler != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "scheduler",
			Label:    "Cloud Scheduler",
			Category: "Integration",
			RootPath: "/gcp/scheduler/jobs",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{{Label: "Jobs", Path: "/gcp/scheduler/jobs"}},
		})
	}
	if r.tasks != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "tasks",
			Label:    "Cloud Tasks",
			Category: "Integration",
			RootPath: "/gcp/tasks/queues",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{{Label: "Queues", Path: "/gcp/tasks/queues"}},
		})
	}
	if r.workflows != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "workflows",
			Label:    "Workflows",
			Category: "Integration",
			RootPath: "/gcp/workflows",
			Tier:     coreui.TierShape,
			Note:     "Shape only — executions complete synchronously",
			Children: []coreui.ServiceChild{{Label: "Workflows", Path: "/gcp/workflows"}},
		})
	}
	if r.eventarc != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "eventarc",
			Label:    "Eventarc",
			Category: "Integration",
			RootPath: "/gcp/eventarc/triggers",
			Tier:     coreui.TierShape,
			Note:     "Shape only — trigger/channel CRUD; limited delivery",
			Children: []coreui.ServiceChild{
				{Label: "Triggers", Path: "/gcp/eventarc/triggers"},
				{Label: "Channels", Path: "/gcp/eventarc/channels"},
			},
		})
	}
	if r.managedkafka != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "managedkafka",
			Label:    "Managed Kafka",
			Category: "Integration",
			RootPath: "/gcp/managedkafka/clusters",
			Tier:     statusTier(kafkaOn, coreui.TierMetadata),
			Note:     engineNote(kafkaOn, "live Kafka broker", r.modes.KafkaBroker, "Metadata only — no broker; opt-in JAISCLOUD_KAFKA_BROKER_MODE"),
			Engine:   kafkaEngine,
			Children: []coreui.ServiceChild{
				{Label: "Clusters", Path: "/gcp/managedkafka/clusters"},
				{Label: "Topics", Path: "/gcp/managedkafka/topics"},
			},
		})
	}
	if r.bigquery != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "bigquery",
			Label:    "BigQuery",
			Category: "Analytics",
			RootPath: "/gcp/bigquery/datasets",
			Tier:     coreui.TierShape,
			Note:     "Shape only — documented SQL subset on an in-process engine",
			Children: []coreui.ServiceChild{
				{Label: "Datasets", Path: "/gcp/bigquery/datasets"},
				{Label: "Jobs", Path: "/gcp/bigquery/jobs"},
				{Label: "Query", Path: "/gcp/bigquery/query"},
			},
		})
	}
	if r.dataproc != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "dataproc",
			Label:    "Dataproc",
			Category: "Analytics",
			RootPath: "/gcp/dataproc/clusters",
			Tier:     statusTier(sparkOn, coreui.TierShape),
			Note:     engineNote(sparkOn, "real Spark executor", r.modes.Spark, "Shape only — Spark family; no real cluster without an executor"),
			Engine:   dataprocEngine,
			Children: []coreui.ServiceChild{
				{Label: "Clusters", Path: "/gcp/dataproc/clusters"},
				{Label: "Jobs", Path: "/gcp/dataproc/jobs"},
				{Label: "Workflow templates", Path: "/gcp/dataproc/workflow-templates"},
			},
		})
	}
	if r.iam != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "iam",
			Label:    "IAM",
			Category: "Security",
			RootPath: "/gcp/iam/service-accounts",
			Tier:     coreui.TierShape,
			Note:     "Shape only — authorization not enforced",
			Children: []coreui.ServiceChild{{Label: "Service accounts", Path: "/gcp/iam/service-accounts"}},
		})
	}
	if r.kms != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "kms",
			Label:    "Cloud KMS",
			Category: "Security",
			RootPath: "/gcp/kms/keyrings",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{{Label: "Key rings", Path: "/gcp/kms/keyrings"}},
		})
	}
	if r.secretmanager != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "secretmanager",
			Label:    "Secret Manager",
			Category: "Security",
			RootPath: "/gcp/secretmanager/secrets",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{{Label: "Secrets", Path: "/gcp/secretmanager/secrets"}},
		})
	}
	if r.logging != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "logging",
			Label:    "Cloud Logging",
			Category: "Operations",
			RootPath: "/gcp/logging/entries",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{
				{Label: "Logs explorer", Path: "/gcp/logging/entries"},
				{Label: "Logs-based metrics", Path: "/gcp/logging/metrics"},
				{Label: "Log router", Path: "/gcp/logging/sinks"},
				{Label: "Exclusions", Path: "/gcp/logging/exclusions"},
			},
		})
	}
	if r.monitoring != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "monitoring",
			Label:    "Cloud Monitoring",
			Category: "Operations",
			RootPath: "/gcp/monitoring/metrics",
			Tier:     coreui.TierFull,
			Children: []coreui.ServiceChild{
				{Label: "Metrics explorer", Path: "/gcp/monitoring/metrics"},
				{Label: "Alerting", Path: "/gcp/monitoring/alerting"},
				{Label: "Notification channels", Path: "/gcp/monitoring/channels"},
			},
		})
	}
	if r.resourcemanager != nil {
		services = append(services, coreui.ServiceDescriptor{
			ID:       "resourcemanager",
			Label:    "Resource Manager",
			Category: "Management",
			RootPath: "/gcp/resourcemanager/projects",
			Tier:     coreui.TierShape,
			Note:     "Shape only — project registry; authorization not enforced",
			Children: []coreui.ServiceChild{{Label: "Projects", Path: "/gcp/resourcemanager/projects"}},
		})
	}
	return services
}

// MountRoutes implements coreui.Registrar. Routes are mounted inside the
// authenticated group in the shared core router.
func (r *Registrar) MountRoutes(router chi.Router) {
	// Host engine liveness for the admin Runtime view. Not gated on a service
	// provider — it reports the host the emulator runs on.
	router.Get("/api/ui/v1/gcp/runtime", buildRuntimeHealthHandler())

	if r.storage != nil {
		router.Mount("/api/ui/v1/gcp/storage", storageui.BuildRouter(r.storage, r.cfg))
	}
	if r.pubsub != nil {
		router.Mount("/api/ui/v1/gcp/pubsub", pubsubui.BuildRouter(r.pubsub, r.cfg))
	}
	if r.firestore != nil {
		router.Mount("/api/ui/v1/gcp/firestore", firestoreui.BuildRouter(r.firestore, r.cfg))
	}
	if r.datastore != nil {
		router.Mount("/api/ui/v1/gcp/datastore", datastoreui.BuildRouter(r.datastore, r.cfg))
	}
	if r.compute != nil {
		router.Mount("/api/ui/v1/gcp/compute", computeui.BuildRouter(r.compute, r.cfg))
	}
	if r.run != nil {
		router.Mount("/api/ui/v1/gcp/run", runui.BuildRouter(r.run, r.cfg))
	}
	if r.scheduler != nil {
		router.Mount("/api/ui/v1/gcp/scheduler", schedulerui.BuildRouter(r.scheduler, r.cfg))
	}
	if r.tasks != nil {
		router.Mount("/api/ui/v1/gcp/tasks", tasksui.BuildRouter(r.tasks, r.cfg))
	}
	if r.workflows != nil {
		router.Mount("/api/ui/v1/gcp/workflows", workflowsui.BuildRouter(r.workflows, r.cfg))
	}
	if r.eventarc != nil {
		router.Mount("/api/ui/v1/gcp/eventarc", eventarcui.BuildRouter(r.eventarc, r.cfg))
	}
	if r.functions != nil {
		router.Mount("/api/ui/v1/gcp/functions", functionsui.BuildRouter(r.functions, r.cfg))
	}
	if r.managedkafka != nil {
		router.Mount("/api/ui/v1/gcp/managedkafka", managedkafkaui.BuildRouter(r.managedkafka, r.cfg))
	}
	if r.bigquery != nil {
		router.Mount("/api/ui/v1/gcp/bigquery", bigqueryui.BuildRouter(r.bigquery, r.cfg))
	}
	if r.dataproc != nil {
		router.Mount("/api/ui/v1/gcp/dataproc", dataprocui.BuildRouter(r.dataproc, r.cfg))
	}
	if r.iam != nil {
		router.Mount("/api/ui/v1/gcp/iam", iamui.BuildRouter(r.iam, r.cfg))
	}
	if r.kms != nil {
		router.Mount("/api/ui/v1/gcp/kms", kmsui.BuildRouter(r.kms, r.cfg))
	}
	if r.secretmanager != nil {
		router.Mount("/api/ui/v1/gcp/secretmanager", secretmanagerui.BuildRouter(r.secretmanager, r.cfg))
	}
	if r.logging != nil {
		router.Mount("/api/ui/v1/gcp/logging", loggingui.BuildRouter(r.logging, r.cfg))
	}
	if r.monitoring != nil {
		router.Mount("/api/ui/v1/gcp/monitoring", monitoringui.BuildRouter(r.monitoring, r.cfg))
	}
	if r.resourcemanager != nil {
		router.Mount("/api/ui/v1/gcp/resourcemanager", resourcemanagerui.BuildRouter(r.resourcemanager))
	}
}
