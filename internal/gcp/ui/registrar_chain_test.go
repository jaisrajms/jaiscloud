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
