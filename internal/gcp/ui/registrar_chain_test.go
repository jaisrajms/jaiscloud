package ui

import (
	"testing"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/pluginchain"
	datastorecore "jaiscloud/internal/gcp/service/datastore"
	resourcemanagercore "jaiscloud/internal/gcp/service/resourcemanager"
	runcore "jaiscloud/internal/gcp/service/run"
	schedulercore "jaiscloud/internal/gcp/service/scheduler"
	tasks "jaiscloud/internal/gcp/service/tasks"
	dsstore "jaiscloud/internal/gcp/store/datastore"
	runstore "jaiscloud/internal/gcp/store/run"
	schedstore "jaiscloud/internal/gcp/store/scheduler"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
	bigqueryui "jaiscloud/internal/gcp/ui/bigquery"
	computeui "jaiscloud/internal/gcp/ui/compute"
	firestoreui "jaiscloud/internal/gcp/ui/firestore"
	iamui "jaiscloud/internal/gcp/ui/iam"
	kmsui "jaiscloud/internal/gcp/ui/kms"
	loggingui "jaiscloud/internal/gcp/ui/logging"
	monitoringui "jaiscloud/internal/gcp/ui/monitoring"
	pubsubui "jaiscloud/internal/gcp/ui/pubsub"
	secretmanagerui "jaiscloud/internal/gcp/ui/secretmanager"
	storageui "jaiscloud/internal/gcp/ui/storage"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// WithPluginChain must wrap every typed console service the registrar holds, or that
// service's console keeps reading the built-in while a plugin owns the key.
func TestWithPluginChainWrapsEveryTypedService(t *testing.T) {
	wf, _ := newWorkflows(t)
	_, fn, _ := newFunctions()
	ea, _ := newEventarc()
	_, dp, _ := newDataproc()
	_, mk, _ := newKafka()
	rm := resourcemanagercore.NewService(store.NewMemoryResourceStore())
	r := &Registrar{
		cfg:       &config.Config{},
		scheduler: schedulercore.NewService(schedstore.NewMemoryStore()),
		tasks:     tasks.NewService(tasksstore.NewMemoryStore(), store.NewMemoryResourceStore()),
		workflows: wf, functions: fn, eventarc: ea, dataproc: dp, managedkafka: mk,
		run:             runcore.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore()),
		datastore:       datastorecore.NewService(dsstore.NewMemoryStore(), "proj"),
		resourcemanager: rm,
	}
	r.WithPluginChain(pluginchain.New(fakeHost{}), map[string]provider.HandlerFunc{})

	checks := map[string]bool{}
	_, checks["scheduler"] = r.scheduler.(typedScheduler)
	_, checks["tasks"] = r.tasks.(typedTasks)
	_, checks["workflows"] = r.workflows.(typedWorkflows)
	_, checks["functions"] = r.functions.(typedFunctions)
	_, checks["eventarc"] = r.eventarc.(typedEventarc)
	_, checks["dataproc"] = r.dataproc.(typedDataproc)
	_, checks["managedkafka"] = r.managedkafka.(typedManagedKafka)
	_, checks["run"] = r.run.(typedRun)
	_, checks["datastore"] = r.datastore.(typedDatastore)
	_, checks["resourcemanager"] = r.rmAPI().(typedResourceManager)
	for svc, ok := range checks {
		if !ok {
			t.Errorf("WithPluginChain left the %s console on the built-in", svc)
		}
	}
	if len(checks) != len(typedServices) {
		t.Errorf("checked %d services, the guard lists %d typed services", len(checks), len(typedServices))
	}
}

// WithRegistry must route the console of every registry-action service the registrar holds, and
// leave a service it does not hold alone.
func TestWithRegistryRoutesEveryRegistryActionService(t *testing.T) {
	r := &Registrar{
		cfg:       &config.Config{},
		storage:   struct{ storageui.ProviderInterface }{},
		pubsub:    struct{ pubsubui.ProviderInterface }{},
		firestore: struct{ firestoreui.ProviderInterface }{},
		bigquery:  struct{ bigqueryui.ProviderInterface }{},
		compute:   struct{ computeui.ProviderInterface }{},
		iam:       struct{ iamui.ProviderInterface }{},
		kms:       struct{ kmsui.ProviderInterface }{},
		secretmanager: struct {
			secretmanagerui.ProviderInterface
		}{},
		logging:    struct{ loggingui.ProviderInterface }{},
		monitoring: struct{ monitoringui.ProviderInterface }{},
	}
	if got := r.WithRegistry(nil); got != r {
		t.Fatal("a nil registry must change nothing")
	}
	if _, ok := r.pubsub.(routedPubSub); ok {
		t.Fatal("a nil registry must leave the console on the built-in")
	}
	r.WithRegistry(provider.NewRegistry())
	checks := map[string]bool{}
	_, checks["storage"] = r.storage.(routedStorage)
	_, checks["pubsub"] = r.pubsub.(routedPubSub)
	_, checks["firestore"] = r.firestore.(routedFirestore)
	_, checks["bigquery"] = r.bigquery.(routedBigQuery)
	_, checks["compute"] = r.compute.(routedCompute)
	_, checks["iam"] = r.iam.(routedIAM)
	_, checks["kms"] = r.kms.(routedKMS)
	_, checks["secretmanager"] = r.secretmanager.(routedSecret)
	_, checks["logging"] = r.logging.(routedLogging)
	_, checks["monitoring"] = r.monitoring.(routedMonitoring)
	for svc, ok := range checks {
		if !ok {
			t.Errorf("WithRegistry left the %s console on the built-in", svc)
		}
	}
	if len(checks) != len(routedServices) {
		t.Errorf("checked %d services, the guard lists %d routed services", len(checks), len(routedServices))
	}
}
