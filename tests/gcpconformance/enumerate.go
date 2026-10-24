//go:build gcp_conformance

// Package gcpconformance is an offline wire-conformance harness for the GCP
// emulator. It enumerates the emulator's own operation registry and validates
// captured HTTP responses against the official Google Discovery schemas
// vendored under discovery/.
package gcpconformance

import (
	"sort"
	"strings"

	"jaiscloud/internal/provider"

	bigqueryprovider "jaiscloud/internal/gcp/provider/bigquery"
	clouddnsprovider "jaiscloud/internal/gcp/provider/clouddns"
	cloudsqlprovider "jaiscloud/internal/gcp/provider/cloudsql"
	computeprovider "jaiscloud/internal/gcp/provider/compute"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	iamprovider "jaiscloud/internal/gcp/provider/iam"
	icebergprovider "jaiscloud/internal/gcp/provider/iceberg"
	memorystoreprovider "jaiscloud/internal/gcp/provider/memorystore"
	pubsubprovider "jaiscloud/internal/gcp/provider/pubsub"
	secretmanagerprovider "jaiscloud/internal/gcp/provider/secretmanager"
	storageprovider "jaiscloud/internal/gcp/provider/storage"
	restcontainer "jaiscloud/internal/gcp/transport/rest/container"
	restdataproc "jaiscloud/internal/gcp/transport/rest/dataproc"
	restdatastore "jaiscloud/internal/gcp/transport/rest/datastore"
	resteventarc "jaiscloud/internal/gcp/transport/rest/eventarc"
	restfunctions "jaiscloud/internal/gcp/transport/rest/functions"
	restiamcredentials "jaiscloud/internal/gcp/transport/rest/iamcredentials"
	kmsprovider "jaiscloud/internal/gcp/transport/rest/kms"
	restlogging "jaiscloud/internal/gcp/transport/rest/logging"
	restmanagedkafka "jaiscloud/internal/gcp/transport/rest/managedkafka"
	restmetastore "jaiscloud/internal/gcp/transport/rest/metastore"
	restmonitoring "jaiscloud/internal/gcp/transport/rest/monitoring"
	restresourcemanager "jaiscloud/internal/gcp/transport/rest/resourcemanager"
	restrun "jaiscloud/internal/gcp/transport/rest/run"
	restscheduler "jaiscloud/internal/gcp/transport/rest/scheduler"
	restserviceusage "jaiscloud/internal/gcp/transport/rest/serviceusage"
	resttasks "jaiscloud/internal/gcp/transport/rest/tasks"
	restworkflowexecutions "jaiscloud/internal/gcp/transport/rest/workflowexecutions"
	restworkflows "jaiscloud/internal/gcp/transport/rest/workflows"
)

// Operation is one entry in the emulator's dispatch registry, keyed by
// "<ProviderPrefix>.<Action>" (e.g. "Storage.ObjectsInsert").
type Operation struct {
	ProviderPrefix string // e.g. "Storage"
	Action         string // e.g. "ObjectsInsert"
	Service        string // wire service name, e.g. "storage" ("" if unmapped)
}

// Key returns the registry dispatch key.
func (o Operation) Key() string { return o.ProviderPrefix + "." + o.Action }

// providerPrefixes maps the registry provider prefix to the wire service name
// (the inverse of ServiceDescriptor.ProviderPrefix in internal/gcp/adapter).
var providerPrefixes = map[string]string{
	"Storage":           "storage",
	"Secret":            "secretmanager",
	"KMS":               "kms",
	"IAM":               "iam",
	"IAMCredentials":    "iamcredentials",
	"PubSub":            "pubsub",
	"Firestore":         "firestore",
	"Function":          "functions",
	"Workflow":          "workflows",
	"WorkflowExecution": "workflowexecutions",
	"Dataproc":          "dataproc",
	"Container":         "container",
	"ManagedKafka":      "managedkafka",
	"Metastore":         "metastore",
	"Iceberg":           "iceberg",
	"BigQuery":          "bigquery",
	"Eventarc":          "eventarc",
	"CloudDNS":          "clouddns",
	"Memorystore":       "memorystore",
	"CloudSQL":          "cloudsql",
	"Compute":           "compute",
	"ServiceUsage":      "serviceusage",
	"Scheduler":         "scheduler",
	"Tasks":             "tasks",
	"Run":               "run",
	"ResourceManager":   "resourcemanager",
	"Datastore":         "datastore",
	"Logging":           "logging",
	"Monitoring":        "monitoring",
}

// providers returns zero-value provider instances. Routes() only builds a map
// of bound methods, so it is safe to call without any store wiring.
func providers() []provider.Provider {
	return []provider.Provider{
		&storageprovider.Provider{},
		&secretmanagerprovider.Provider{},
		&kmsprovider.Provider{},
		&iamprovider.Provider{},
		&pubsubprovider.Provider{},
		&firestoreprovider.Provider{},
		&restfunctions.Provider{},
		&restiamcredentials.Provider{},
		&restworkflows.Provider{},
		&restworkflowexecutions.Provider{},
		&restdataproc.Provider{},
		&restcontainer.Provider{},
		&restmanagedkafka.Provider{},
		&restmetastore.Provider{},
		&icebergprovider.Provider{},
		&bigqueryprovider.Provider{},
		&resteventarc.Provider{},
		&clouddnsprovider.Provider{},
		&memorystoreprovider.Provider{},
		&cloudsqlprovider.Provider{},
		&computeprovider.Provider{},
		&restserviceusage.Provider{},
		&restscheduler.Provider{},
		&resttasks.Provider{},
		&restrun.Provider{},
		&restresourcemanager.Provider{},
		&restdatastore.Provider{},
		&restlogging.Provider{},
		&restmonitoring.Provider{},
	}
}

// Enumerate returns every operation registered in the emulator, sorted by key.
func Enumerate() []Operation {
	var ops []Operation
	for _, p := range providers() {
		for key := range p.Routes() {
			prefix, action := splitKey(key)
			ops = append(ops, Operation{
				ProviderPrefix: prefix,
				Action:         action,
				Service:        providerPrefixes[prefix],
			})
		}
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].Key() < ops[j].Key() })
	return ops
}

func splitKey(key string) (prefix, action string) {
	if i := strings.IndexByte(key, '.'); i >= 0 {
		return key[:i], key[i+1:]
	}
	return key, ""
}
