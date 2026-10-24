// Package dataproc is the transport-neutral core of the Cloud Dataproc v1
// service (dataproc.googleapis.com/v1). It owns all cluster, job, and
// long-running-operation business logic over internal/gcp/store/dataproc,
// including the Spark executor orchestration (mock / Kubernetes) for jobs.
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// gRPC transport (internal/gcp/transport/grpc/dataproc) and the REST transport
// (internal/gcp/transport/rest/dataproc) both transcode their wire format into
// this package's typed API and then call the SAME Service instance. That is the
// dual-protocol invariant: one core, one piece of state (including the
// in-flight executor registry), so the transports cannot drift.
//
// A cluster is a logical record only — the emulator never stands up a real
// multi-node cluster, exactly as EMR-on-EC2 never stands up a real YARN
// cluster. Jobs, however, run real Spark through the shared client-mode engine
// (internal/sparkhelpers + internal/k8shelpers) under JAISCLOUD_EXECUTOR_MODE.
package dataproc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/events"
	"jaiscloud/internal/gcp/eventing"
	"jaiscloud/internal/gcp/sparkgcp"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/model"
	"jaiscloud/internal/platform"
	"jaiscloud/internal/store"
)

// Service is the transport-neutral Cloud Dataproc v1 core.
type Service struct {
	store     dpstore.Store
	resources store.ResourceStore // terminal snapshots (rehydrate after k8s Job GC)

	k8sClient   kubernetes.Interface // nil = no k8s namespace provisioning
	platformCfg *platform.PlatformConfig
	namespace   string
	sparkImage  string
	gcpEmulator *sparkgcp.GCPEmulatorConfig

	// executor is the driver-submission backend. nil means mock mode: no driver
	// runs and a job settles lazily in the store.
	executor driverExecutor

	sparkSubmitPath string
	sparkSqlPath    string

	instanceID         string
	serviceAccountName string
	projectID          string // default project (from cfg.ProjectID), used as WI fallback

	// operationTTL is how long a completed operation is retained before the
	// lazy sweep removes it (keeps jc_dataproc_operations bounded; real
	// operations are GC'd after a similar TTL). Zero disables the sweep.
	operationTTL time.Duration

	// clusterReadyDelay is how long a cluster stays in a transitional state
	// (CREATING/DELETING/UPDATING/STARTING/STOPPING) before a read advances it
	// to its target state. The transition is lazy and clock-driven (no
	// goroutine), matching the emulator's KMS-rotation pattern. Zero means the
	// first read settles the cluster.
	clusterReadyDelay time.Duration

	// clusterErrorHook, when set, forces a cluster transition into ERROR
	// instead of its target state. It is a test hook (real GCP reaches ERROR on
	// provisioning failure, which the emulator cannot observe); nil disables it.
	clusterErrorHook func(project, region, name string) bool

	// jobStateDelay is how long a job stays in a transitional state
	// (PENDING/SETUP_DONE/RUNNING/CANCEL_*) before a read advances it. Like the
	// cluster state machine the transition is lazy and clock-driven (no
	// goroutine). Zero means the first read settles the next hop.
	jobStateDelay time.Duration

	// jobAttemptFailureHook, when set, forces the next mock-mode RUNNING -> DONE
	// hop to ATTEMPT_FAILURE (which then settles to ERROR). It is a test hook for
	// the mock executor, which does not run or retry a driver; the k8s executor
	// produces ATTEMPT_FAILURE naturally via its restart loop. nil disables it.
	jobAttemptFailureHook func(project, region, jobID string) bool

	// eventPublisher publishes lifecycle events (cluster/job state changes) to a
	// configured Pub/Sub topic. Nil disables Pub/Sub publishing. Publishing is
	// best-effort: a failure never fails the underlying transition.
	eventPublisher EventPublisher
	// eventsTopic is the default lifecycle events topic (a topic ID or full
	// name, project-scoped at emit time). Empty (the default) disables Pub/Sub
	// event publishing; a cluster may override it with the eventsTopicLabel
	// label. See events.go.
	eventsTopic string
	// eventDispatcher delivers lifecycle events to the Cloud Functions
	// event-delivery engine so a function whose eventTrigger names a Dataproc
	// state-change type receives them directly (independent of the Pub/Sub
	// topic). Nil disables direct delivery.
	eventDispatcher eventing.Dispatcher
	// eventBus carries cloud-neutral status events to the console's live
	// stream. Nil disables them; publishing is best-effort.
	eventBus *events.EventBus

	// blobSink stages a job's driver output/control files into the emulated
	// GCS. Nil (unit tests / mock deployments) keeps the advertised URIs but
	// does not materialize the objects. Satisfied by the GCS provider and wired
	// in main.go, so the core never imports provider/storage.
	blobSink BlobSink

	// metastoreResolver validates a cluster's Dataproc Metastore attachment and
	// formats its thrift endpoint. Satisfied by the Metastore core and wired in
	// main.go, so the core never imports service/metastore. Nil leaves the
	// attachment stored/echoed but unvalidated and not injected into jobs.
	metastoreResolver MetastoreResolver
	// hmsEndpointOverride, when set, replaces the synthesized per-service
	// endpoint with a reachable Hive Metastore address (from
	// JAISCLOUD_DATAPROC_HMS_ENDPOINT). See WithHMSEndpointOverride.
	hmsEndpointOverride string

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	cancelsMu sync.Mutex
	cancels   map[string]context.CancelFunc

	// nsPatchers holds one ownership patcher per workload namespace (the default
	// plus every per-cluster namespace), so per-cluster namespaces created after
	// startup are watched and their executor pods get ownerReferences.
	nsPatchersMu sync.Mutex
	nsPatchers   map[string]func()
}

// defaultOperationTTL is how long a completed operation is retained before the
// lazy sweep removes it. Real Dataproc operations are GC'd after a TTL; without
// a sweep, jc_dataproc_operations grows unbounded.
const defaultOperationTTL = 24 * time.Hour

// Option configures Service.
type Option func(*Service)

// WithK8s attaches a Kubernetes client and platform config for real Spark
// execution (mock mode when nil). It also installs the k8s driver executor.
func WithK8s(client kubernetes.Interface, namespace string, platformCfg *platform.PlatformConfig) Option {
	return func(s *Service) {
		s.k8sClient = client
		s.namespace = namespace
		s.platformCfg = platformCfg
		s.executor = k8sDriver{client: client}
	}
}

// mockMode reports whether no real driver backend is configured. The job state
// machine settles lazily on reads in mock mode.
func (s *Service) mockMode() bool { return s.executor == nil }

// WithSparkImage sets the container image used for spark-submit driver pods.
func WithSparkImage(image string) Option {
	return func(s *Service) { s.sparkImage = image }
}

// WithSparkSubmitPath overrides the spark-submit binary path inside the driver
// image (default "spark-submit"; the apache/spark image keeps it at
// /opt/spark/bin/spark-submit, off PATH).
func WithSparkSubmitPath(path string) Option {
	return func(s *Service) { s.sparkSubmitPath = path }
}

// WithSparkSqlPath overrides the spark-sql binary path used for sparkSqlJob
// driver pods (default: a "spark-sql" sibling of the spark-submit path, or
// "spark-sql" when that is unset).
func WithSparkSqlPath(path string) Option {
	return func(s *Service) { s.sparkSqlPath = path }
}

// WithGCPEmulator wires GCP emulator endpoint config into Spark driver pods.
func WithGCPEmulator(cfg *sparkgcp.GCPEmulatorConfig) Option {
	return func(s *Service) { s.gcpEmulator = cfg }
}

// WithInstanceID sets the instance ID stamped on Spark driver pod labels.
func WithInstanceID(id string) Option {
	return func(s *Service) { s.instanceID = id }
}

// WithServiceAccountName sets the Kubernetes service account for Spark driver
// pods (fallback when the Workload Identity mutator does not set one).
func WithServiceAccountName(sa string) Option {
	return func(s *Service) { s.serviceAccountName = sa }
}

// WithProjectID sets the default GCP project (used by the Workload Identity
// mutator; per-job requests override with their own project).
func WithProjectID(project string) Option {
	return func(s *Service) { s.projectID = project }
}

// WithOperationTTL overrides how long completed operations are retained before
// the lazy sweep removes them. Zero disables the sweep (tests use a short TTL).
func WithOperationTTL(d time.Duration) Option {
	return func(s *Service) { s.operationTTL = d }
}

// WithClusterReadyDelay overrides how long a cluster stays in a transitional
// state before a read settles it. The default is zero: the first read after the
// mutation completes the transition. Tests inject a positive delay (with a
// frozen clock) to observe CREATING/DELETING/UPDATING/STARTING/STOPPING.
func WithClusterReadyDelay(d time.Duration) Option {
	return func(s *Service) { s.clusterReadyDelay = d }
}

// WithClusterErrorHook installs a test hook that forces a settling cluster to
// ERROR instead of its target state. Production never sets it.
func WithClusterErrorHook(fn func(project, region, name string) bool) Option {
	return func(s *Service) { s.clusterErrorHook = fn }
}

// WithJobStateDelay overrides how long a job stays in a transitional state
// before a read settles the next hop. The default is zero: the first read after
// a transition advances it. Tests inject a positive delay (with a frozen clock)
// to observe PENDING/SETUP_DONE/RUNNING/CANCEL_PENDING/CANCEL_STARTED.
func WithJobStateDelay(d time.Duration) Option {
	return func(s *Service) { s.jobStateDelay = d }
}

// WithJobAttemptFailureHook installs a test hook that forces a job's next
// RUNNING -> DONE hop to ATTEMPT_FAILURE (then ERROR). The emulator's Spark
// engine does not retry, so production never sets it.
func WithJobAttemptFailureHook(fn func(project, region, jobID string) bool) Option {
	return func(s *Service) { s.jobAttemptFailureHook = fn }
}

// NewService returns a Dataproc core backed by the given store.
func NewService(s dpstore.Store, resources store.ResourceStore, opts ...Option) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	svc := &Service{
		store:        s,
		resources:    resources,
		ctx:          ctx,
		cancel:       cancel,
		sparkImage:   "spark-dataproc:devbox",
		cancels:      make(map[string]context.CancelFunc),
		nsPatchers:   make(map[string]func()),
		operationTTL: defaultOperationTTL,
	}
	for _, o := range opts {
		o(svc)
	}
	if svc.k8sClient != nil {
		ns := svc.namespace
		if ns == "" {
			ns = "jaiscloud"
		}
		// Watch the process-wide namespace plus every namespace the emulator owns
		// for Dataproc (persisted across a --dsn restart). Per-cluster namespaces
		// created after startup are registered as they are provisioned
		// (registerNamespacePatcher).
		owned, listErr := k8shelpers.ListManagedNamespaces(svc.ctx, svc.k8sClient, dataprocService)
		if listErr != nil {
			slog.Warn("dataproc: failed to list owned namespaces", "err", listErr)
		}
		svc.registerNamespacePatcher(ns)
		for _, ownedNS := range owned {
			svc.registerNamespacePatcher(ownedNS)
		}
		if err := k8shelpers.CleanupOrphans(svc.ctx, svc.k8sClient, k8shelpers.CleanupConfig{
			Namespace:       ns,
			Namespaces:      owned,
			InstanceID:      svc.instanceID,
			OrphanSelectors: []string{"spark-role in (driver,executor)"},
		}); err != nil {
			slog.Warn("dataproc: CleanupOrphans failed", "err", err)
		}
	}
	return svc
}

// Shutdown cancels the core context and drains in-flight job goroutines.
func (s *Service) Shutdown(_ context.Context) {
	s.stopNamespacePatchers()
	s.cancel()
	s.wg.Wait()
}

// Reset wipes the store and reaps every namespace the emulator owns for
// Dataproc (deleting a namespace cascades its workloads). The core's own
// in-flight goroutines are drained by Shutdown; /_jaiscloud/reset does not
// drain them (documented limitation).
func (s *Service) Reset(ctx context.Context) {
	s.store.Reset(ctx)
	s.sweepOwnedNamespaces(ctx)
	if s.executor != nil {
		s.executor.Reset(ctx)
	}
}

// randomHex returns n random hexadecimal characters.
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}

// pageParams builds the shared cursor-pagination parameter map accepted by
// internal/gcp/paging.
func pageParams(pageSize int, pageToken string) map[string]any {
	params := map[string]any{"pageSize": pageSize}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	return params
}

// invalidArgument builds the canonical InvalidArgument provider error both
// transports map onto their wire status.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// mapErr maps a dataproc store error onto a canonical provider error.
func mapErr(err error) error {
	switch {
	case errors.Is(err, dpstore.ErrNoSuchCluster):
		return model.NewProviderError("NotFound", "cluster not found", 404)
	case errors.Is(err, dpstore.ErrNoSuchJob):
		return model.NewProviderError("NotFound", "job not found", 404)
	case errors.Is(err, dpstore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	case errors.Is(err, dpstore.ErrNoSuchWorkflowTemplate):
		return model.NewProviderError("NotFound", "workflow template not found", 404)
	case errors.Is(err, dpstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	}
	return err
}
