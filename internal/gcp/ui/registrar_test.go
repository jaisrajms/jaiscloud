package ui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

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
	runui "jaiscloud/internal/gcp/ui/run"
	schedulerui "jaiscloud/internal/gcp/ui/scheduler"
	secretmanagerui "jaiscloud/internal/gcp/ui/secretmanager"
	storageui "jaiscloud/internal/gcp/ui/storage"
	tasksui "jaiscloud/internal/gcp/ui/tasks"
	workflowsui "jaiscloud/internal/gcp/ui/workflows"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
	coreui "jaiscloud/internal/ui"
)

// fakeStorage satisfies storage.ProviderInterface via an embedded (nil)
// interface; the registrar tests only exercise the catalog, never the handlers.
type fakeStorage struct{ storageui.ProviderInterface }

// fakePubSub satisfies pubsub.ProviderInterface the same way.
type fakePubSub struct{ pubsubui.ProviderInterface }

// fakeFirestore satisfies firestore.ProviderInterface the same way.
type fakeFirestore struct{ firestoreui.ProviderInterface }

// fakeDatastore satisfies datastoreui.ProviderInterface the same way.
type fakeDatastore struct{ datastoreui.ProviderInterface }

// fakeCompute satisfies compute.ProviderInterface the same way.
type fakeCompute struct{ computeui.ProviderInterface }

// fakeDataproc satisfies dataproc.ProviderInterface the same way.
type fakeDataproc struct{ dataprocui.ProviderInterface }

// fakeBigQuery satisfies bigquery.ProviderInterface the same way.
type fakeBigQuery struct{ bigqueryui.ProviderInterface }

// fakeRun satisfies run.ProviderInterface the same way.
type fakeRun struct{ runui.ProviderInterface }

// fakeScheduler satisfies scheduler.ProviderInterface the same way.
type fakeScheduler struct{ schedulerui.ProviderInterface }

// fakeIAM satisfies iam.ProviderInterface the same way.
type fakeIAM struct{ iamui.ProviderInterface }

// fakeKMS satisfies kms.ProviderInterface the same way.
type fakeKMS struct{ kmsui.ProviderInterface }

// fakeSecret satisfies secretmanager.ProviderInterface the same way.
type fakeSecret struct {
	secretmanagerui.ProviderInterface
}

// fakeLogging satisfies logging.ProviderInterface the same way.
type fakeLogging struct{ loggingui.ProviderInterface }

// fakeMonitoring satisfies monitoring.ProviderInterface the same way.
type fakeMonitoring struct{ monitoringui.ProviderInterface }

// fakeTasks satisfies tasks.ProviderInterface the same way.
type fakeTasks struct{ tasksui.ProviderInterface }

// fakeWorkflows satisfies workflows.ProviderInterface the same way.
type fakeWorkflows struct{ workflowsui.ProviderInterface }

// fakeEventarc satisfies eventarc.ProviderInterface the same way.
type fakeEventarc struct{ eventarcui.ProviderInterface }

// fakeFunctions satisfies functions.ProviderInterface the same way.
type fakeFunctions struct{ functionsui.ProviderInterface }

// fakeManagedKafka satisfies managedkafka.ProviderInterface the same way.
type fakeManagedKafka struct {
	managedkafkaui.ProviderInterface
}

func TestRegistrar_CloudIsGCP(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	if got := reg.Cloud(); got != model.CloudGCP {
		t.Fatalf("Cloud() = %q, want %q", got, model.CloudGCP)
	}
}

func TestRegistrar_NoProviders_EmptyCatalog(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	if services := reg.Services(); len(services) != 0 {
		t.Fatalf("Services() = %d entries, want 0", len(services))
	}
}

func TestRegistrar_StorageAdvertised(t *testing.T) {
	reg := NewRegistrar(fakeStorage{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	if services[0].ID != "storage" || services[0].RootPath != "/gcp/storage/buckets" {
		t.Fatalf("unexpected descriptor: %+v", services[0])
	}
}

func TestRegistrar_PubSubAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, fakePubSub{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "pubsub" || got.RootPath != "/gcp/pubsub/topics" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 2 {
		t.Fatalf("children = %+v, want topics + subscriptions", got.Children)
	}
}

func TestRegistrar_FirestoreAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, fakeFirestore{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "firestore" || got.RootPath != "/gcp/firestore/collections" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 3 ||
		got.Children[0].Path != "/gcp/firestore/collections" ||
		got.Children[1].Path != "/gcp/firestore/query" ||
		got.Children[2].Path != "/gcp/firestore/indexes" {
		t.Fatalf("children = %+v, want collections + query + indexes", got.Children)
	}
}

func TestRegistrar_DatastoreAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, fakeDatastore{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "datastore" || got.RootPath != "/gcp/datastore/kinds" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 2 ||
		got.Children[0].Path != "/gcp/datastore/kinds" ||
		got.Children[1].Path != "/gcp/datastore/query" {
		t.Fatalf("children = %+v, want kinds + query", got.Children)
	}
}

func TestRegistrar_ComputeAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, fakeCompute{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "compute" || got.RootPath != "/gcp/compute/instances" || got.Tier != "metadata" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/compute/instances" {
		t.Fatalf("children = %+v, want instances", got.Children)
	}
}

func TestRegistrar_BigQueryAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, fakeBigQuery{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "bigquery" || got.RootPath != "/gcp/bigquery/datasets" || got.Tier != "shape" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 2 {
		t.Fatalf("children = %+v, want datasets + jobs", got.Children)
	}
}

func TestRegistrar_RunAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, fakeRun{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "run" || got.RootPath != "/gcp/run/services" || got.Tier != "shape" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/run/services" {
		t.Fatalf("children = %+v, want services", got.Children)
	}
}

func TestRegistrar_SchedulerAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, fakeScheduler{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "scheduler" || got.RootPath != "/gcp/scheduler/jobs" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/scheduler/jobs" {
		t.Fatalf("children = %+v, want jobs", got.Children)
	}
}

func TestRegistrar_TasksAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeTasks{}, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "tasks" || got.RootPath != "/gcp/tasks/queues" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/tasks/queues" {
		t.Fatalf("children = %+v, want queues", got.Children)
	}
}

func TestRegistrar_WorkflowsAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeWorkflows{}, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "workflows" || got.RootPath != "/gcp/workflows" || got.Tier != "shape" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/workflows" {
		t.Fatalf("children = %+v, want workflows", got.Children)
	}
}

func TestRegistrar_EventarcAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeEventarc{}, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "eventarc" || got.RootPath != "/gcp/eventarc/triggers" || got.Tier != "shape" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 2 {
		t.Fatalf("children = %+v, want triggers + channels", got.Children)
	}
}

func TestRegistrar_IAMAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeIAM{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "iam" || got.RootPath != "/gcp/iam/service-accounts" || got.Tier != "shape" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/iam/service-accounts" {
		t.Fatalf("children = %+v, want service accounts", got.Children)
	}
}

func TestRegistrar_KMSAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeKMS{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "kms" || got.RootPath != "/gcp/kms/keyrings" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/kms/keyrings" {
		t.Fatalf("children = %+v, want key rings", got.Children)
	}
}

func TestRegistrar_SecretManagerAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeSecret{}, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "secretmanager" || got.RootPath != "/gcp/secretmanager/secrets" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/secretmanager/secrets" {
		t.Fatalf("children = %+v, want secrets", got.Children)
	}
}

func TestRegistrar_LoggingAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeLogging{}, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "logging" || got.RootPath != "/gcp/logging/entries" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 4 {
		t.Fatalf("children = %+v, want entries + metrics + sinks + exclusions", got.Children)
	}
}

func TestRegistrar_MonitoringAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeMonitoring{}, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "monitoring" || got.RootPath != "/gcp/monitoring/metrics" || got.Tier != "full" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 3 {
		t.Fatalf("children = %+v, want metrics + alerting + channels", got.Children)
	}
}

func TestRegistrar_FunctionsAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeFunctions{}, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "functions" || got.RootPath != "/gcp/functions" || got.Tier != "shape" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/functions" {
		t.Fatalf("children = %+v, want functions", got.Children)
	}
}

func TestRegistrar_DataprocAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, fakeDataproc{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "dataproc" || got.RootPath != "/gcp/dataproc/clusters" || got.Tier != "shape" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 3 {
		t.Fatalf("children = %+v, want clusters + jobs + workflow templates", got.Children)
	}
}

func TestRegistrar_ManagedKafkaAdvertised(t *testing.T) {
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, fakeManagedKafka{}, nil, &config.Config{})
	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "managedkafka" || got.RootPath != "/gcp/managedkafka/clusters" || got.Tier != "metadata" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 2 {
		t.Fatalf("children = %+v, want clusters + topics", got.Children)
	}
}

func TestRegistrar_AllAdvertised(t *testing.T) {
	reg := NewRegistrar(fakeStorage{}, fakePubSub{}, fakeFirestore{}, nil, fakeCompute{}, fakeDataproc{}, fakeBigQuery{}, fakeRun{}, fakeScheduler{}, fakeIAM{}, fakeKMS{}, fakeSecret{}, fakeLogging{}, fakeMonitoring{}, fakeTasks{}, fakeWorkflows{}, fakeEventarc{}, fakeFunctions{}, fakeManagedKafka{}, nil, &config.Config{})
	if services := reg.Services(); len(services) != 18 {
		t.Fatalf("Services() = %d entries, want 18", len(services))
	}
}

func TestRegistrar_Accounts(t *testing.T) {
	ctx := context.Background()

	// A nil core (resourcemanager disabled) contributes nothing.
	empty := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &config.Config{})
	if got := empty.Accounts(ctx); len(got) != 0 {
		t.Fatalf("Accounts() with nil core = %v, want none", got)
	}

	core := resourcemanagercore.NewService(store.NewMemoryResourceStore(),
		resourcemanagercore.WithKnownProjects("configured-proj", []string{"extra-proj"}))
	if _, _, err := core.CreateProject(ctx, resourcemanagercore.CreateProjectInput{ProjectID: "created-proj"}); err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, core, &config.Config{})
	got := reg.Accounts(ctx)
	sort.Strings(got)
	want := []string{"configured-proj", "created-proj", "extra-proj"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Accounts() = %v, want %v", got, want)
	}
}

func TestRegistrar_ResourceManagerAdvertisedAndMounted(t *testing.T) {
	core := resourcemanagercore.NewService(store.NewMemoryResourceStore())
	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, core, &config.Config{})

	services := reg.Services()
	if len(services) != 1 {
		t.Fatalf("Services() = %d entries, want 1", len(services))
	}
	got := services[0]
	if got.ID != "resourcemanager" || got.Label != "Resource Manager" || got.Category != "Management" ||
		got.RootPath != "/gcp/resourcemanager/projects" || got.Tier != "shape" {
		t.Fatalf("unexpected descriptor: %+v", got)
	}
	if len(got.Children) != 1 || got.Children[0].Path != "/gcp/resourcemanager/projects" {
		t.Fatalf("children = %+v, want projects", got.Children)
	}

	// The router is mounted under the shared UI API prefix and serves the list.
	router := chi.NewRouter()
	reg.MountRoutes(router)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/ui/v1/gcp/resourcemanager/projects", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("mounted list status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
}

// TestRegistrar_TiersPinnedToImplementationMatrix pins every descriptor's tier
// to the documented behavioural depth (docs/GCP-TESTABILITY.md §5), which the
// console renders as the per-service emulation-status badge. Shape-only services
// use the `stub` tier and every non-full service must carry a note. Changing
// this set is an intentional capability change.
func TestRegistrar_TiersPinnedToImplementationMatrix(t *testing.T) {
	stub := map[string]bool{
		"run": true, "functions": true, "workflows": true, "eventarc": true,
		"bigquery": true, "dataproc": true, "iam": true, "resourcemanager": true,
	}
	metadata := map[string]bool{"compute": true, "managedkafka": true}

	reg := NewRegistrar(
		fakeStorage{}, fakePubSub{}, fakeFirestore{}, fakeDatastore{}, fakeCompute{},
		fakeDataproc{}, fakeBigQuery{}, fakeRun{}, fakeScheduler{}, fakeIAM{},
		fakeKMS{}, fakeSecret{}, fakeLogging{}, fakeMonitoring{}, fakeTasks{},
		fakeWorkflows{}, fakeEventarc{}, fakeFunctions{}, fakeManagedKafka{},
		resourcemanagercore.NewService(store.NewMemoryResourceStore()), &config.Config{},
	)

	services := reg.Services()
	if len(services) != 20 {
		t.Fatalf("Services() = %d entries, want 20", len(services))
	}

	seen := map[string]bool{}
	for _, service := range services {
		seen[service.ID] = true
		switch {
		case stub[service.ID]:
			if service.Tier != "shape" {
				t.Errorf("%s: tier = %q, want %q", service.ID, service.Tier, "shape")
			}
			if service.Note == "" {
				t.Errorf("%s: non-full service should carry a note", service.ID)
			}
		case metadata[service.ID]:
			if service.Tier != "metadata" {
				t.Errorf("%s: tier = %q, want %q", service.ID, service.Tier, "metadata")
			}
			if service.Note == "" {
				t.Errorf("%s: non-full service should carry a note", service.ID)
			}
		default:
			if service.Tier != "full" {
				t.Errorf("%s: tier = %q, want %q (add to stub/metadata if intentional)", service.ID, service.Tier, "full")
			}
		}
	}
	for id := range stub {
		if !seen[id] {
			t.Errorf("stub service %q missing from descriptors", id)
		}
	}
	for id := range metadata {
		if !seen[id] {
			t.Errorf("metadata service %q missing from descriptors", id)
		}
	}
}

// Configuring a real engine upgrades an otherwise shape-only service to full
// with a note naming the engine, mirroring the AWS executorNote behaviour. Each
// service honours only the modes it actually supports (Dataproc, Cloud Run and
// Functions support docker and k8s; Kafka k8s/native).
func TestRegistrar_EngineModesUpgradeTier(t *testing.T) {
	engineReg := func(modes ServiceModes) (tiers, notes map[string]string) {
		t.Helper()
		reg := NewRegistrar(
			nil, nil, nil, nil, nil, fakeDataproc{}, nil, fakeRun{}, nil, nil,
			nil, nil, nil, nil, nil, nil, nil, fakeFunctions{}, fakeManagedKafka{},
			nil, &config.Config{},
		).WithServiceModes(modes)
		tiers, notes = map[string]string{}, map[string]string{}
		for _, service := range reg.Services() {
			tiers[service.ID] = service.Tier
			notes[service.ID] = service.Note
		}
		return tiers, notes
	}

	t.Run("k8s upgrades every engine service", func(t *testing.T) {
		tiers, notes := engineReg(ServiceModes{KafkaBroker: "k8s", Spark: "k8s", Lambda: "k8s", CloudRun: "k8s"})
		for _, id := range []string{"managedkafka", "dataproc", "functions", "run"} {
			if tiers[id] != "full" {
				t.Errorf("%s: tier = %q, want full with an engine configured", id, tiers[id])
			}
			if !strings.Contains(notes[id], "Engine-backed") {
				t.Errorf("%s: note = %q, want an engine-backed note", id, notes[id])
			}
		}
	})

	t.Run("docker upgrades dataproc, functions and run", func(t *testing.T) {
		// Dataproc, Functions and Cloud Run all execute real workloads under
		// docker; Managed Kafka's k8s-free engine is native, not docker.
		tiers, _ := engineReg(ServiceModes{KafkaBroker: "docker", Spark: "docker", Lambda: "docker", CloudRun: "docker"})
		for _, id := range []string{"dataproc", "functions", "run"} {
			if tiers[id] != "full" {
				t.Errorf("%s: tier = %q, want full under docker", id, tiers[id])
			}
		}
		if want := "metadata"; tiers["managedkafka"] != want {
			t.Errorf("managedkafka: tier = %q, want %q (docker unsupported)", tiers["managedkafka"], want)
		}
	})
}

// The structured Engine field drives the console's mode tag + availability
// matrix, and Tier must never depend on the orchestrator: docker and k8s are
// interchangeable implementations of one executor seam.
func TestRegistrar_EngineDescriptorAndOrchestratorParity(t *testing.T) {
	regFor := func(modes ServiceModes) *Registrar {
		return NewRegistrar(
			nil, nil, nil, nil, nil, fakeDataproc{}, nil, fakeRun{}, nil, nil,
			nil, nil, nil, nil, nil, nil, nil, fakeFunctions{}, fakeManagedKafka{},
			nil, &config.Config{},
		).WithServiceModes(modes)
	}
	descriptor := func(modes ServiceModes, id string) coreui.ServiceDescriptor {
		t.Helper()
		for _, service := range regFor(modes).Services() {
			if service.ID == id {
				return service
			}
		}
		t.Fatalf("%s not advertised", id)
		return coreui.ServiceDescriptor{}
	}

	// Dataproc honours both docker and k8s; docker must read as an active,
	// supported backend and its tier must not depend on the orchestrator (docker
	// and k8s are interchangeable implementations of one executor seam).
	dataprocDocker := descriptor(ServiceModes{Spark: "docker"}, "dataproc")
	dataprocK8s := descriptor(ServiceModes{Spark: "k8s"}, "dataproc")
	if dataprocDocker.Engine == nil {
		t.Fatal("dataproc: Engine is nil")
	}
	if !dataprocDocker.Engine.Active || dataprocDocker.Engine.Mode != "docker" {
		t.Errorf("dataproc docker: active=%v mode=%q, want active docker", dataprocDocker.Engine.Active, dataprocDocker.Engine.Mode)
	}
	if len(dataprocDocker.Engine.Modes) != 3 || dataprocDocker.Engine.Modes[1].Name != "docker" || !dataprocDocker.Engine.Modes[1].Supported {
		t.Errorf("dataproc backends = %+v, want docker supported", dataprocDocker.Engine.Modes)
	}
	if dataprocDocker.Tier != dataprocK8s.Tier || dataprocDocker.Tier != "full" {
		t.Errorf("dataproc tier docker=%q k8s=%q, want identical 'full'", dataprocDocker.Tier, dataprocK8s.Tier)
	}

	// A k8s engine reads as active with the mode reported.
	kafka := descriptor(ServiceModes{KafkaBroker: "native"}, "managedkafka")
	if kafka.Engine == nil || !kafka.Engine.Active || kafka.Engine.Mode != "native" {
		t.Errorf("managedkafka native engine = %+v, want active native", kafka.Engine)
	}

	// Same service, two supported orchestrators -> identical tier (no depth by
	// orchestrator).
	dockerTier := descriptor(ServiceModes{Lambda: "docker"}, "functions").Tier
	k8sTier := descriptor(ServiceModes{Lambda: "k8s"}, "functions").Tier
	if dockerTier != k8sTier || dockerTier != "full" {
		t.Errorf("functions tier docker=%q k8s=%q, want identical 'full'", dockerTier, k8sTier)
	}

	// Cloud Run honours both docker and k8s too; its tier must not depend on the
	// orchestrator, and docker must report as an active, supported backend.
	runDocker := descriptor(ServiceModes{CloudRun: "docker"}, "run")
	runK8s := descriptor(ServiceModes{CloudRun: "k8s"}, "run")
	if runDocker.Tier != runK8s.Tier || runDocker.Tier != "full" {
		t.Errorf("run tier docker=%q k8s=%q, want identical 'full'", runDocker.Tier, runK8s.Tier)
	}
	if runDocker.Engine == nil || !runDocker.Engine.Active || runDocker.Engine.Mode != "docker" {
		t.Errorf("run docker engine = %+v, want active docker", runDocker.Engine)
	}
	if len(runDocker.Engine.Modes) != 3 || runDocker.Engine.Modes[1].Name != "docker" || !runDocker.Engine.Modes[1].Supported {
		t.Errorf("run backends = %+v, want docker supported", runDocker.Engine.Modes)
	}
}

func TestRegistrar_AccountsExcludesDeleteRequested(t *testing.T) {
	ctx := context.Background()
	core := resourcemanagercore.NewService(store.NewMemoryResourceStore(),
		resourcemanagercore.WithKnownProjects("configured-proj", nil))
	if _, _, err := core.CreateProject(ctx, resourcemanagercore.CreateProjectInput{ProjectID: "doomed-proj"}); err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	reg := NewRegistrar(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, core, &config.Config{})
	if got := reg.Accounts(ctx); !slices.Contains(got, "doomed-proj") {
		t.Fatalf("Accounts() before delete = %v, want doomed-proj", got)
	}

	if _, _, err := core.DeleteProject(ctx, "doomed-proj"); err != nil {
		t.Fatalf("DeleteProject: %v", err)
	}
	// The picker must not offer a DELETE_REQUESTED project (it is restorable
	// from the manager, not selectable).
	if got := reg.Accounts(ctx); slices.Contains(got, "doomed-proj") {
		t.Fatalf("Accounts() after delete = %v, want doomed-proj excluded", got)
	}
}
