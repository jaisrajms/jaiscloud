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
	dataprocprovider "jaiscloud/internal/gcp/provider/dataproc"
	eventarcprovider "jaiscloud/internal/gcp/provider/eventarc"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	functionsprovider "jaiscloud/internal/gcp/provider/functions"
	iamprovider "jaiscloud/internal/gcp/provider/iam"
	icebergprovider "jaiscloud/internal/gcp/provider/iceberg"
	kmsprovider "jaiscloud/internal/gcp/provider/kms"
	managedkafkaprovider "jaiscloud/internal/gcp/provider/managedkafka"
	memorystoreprovider "jaiscloud/internal/gcp/provider/memorystore"
	metastoreprovider "jaiscloud/internal/gcp/provider/metastore"
	pubsubprovider "jaiscloud/internal/gcp/provider/pubsub"
	resourcemanagerprovider "jaiscloud/internal/gcp/provider/resourcemanager"
	secretmanagerprovider "jaiscloud/internal/gcp/provider/secretmanager"
	serviceusageprovider "jaiscloud/internal/gcp/provider/serviceusage"
	storageprovider "jaiscloud/internal/gcp/provider/storage"
	workflowexecutionsprovider "jaiscloud/internal/gcp/provider/workflowexecutions"
	workflowsprovider "jaiscloud/internal/gcp/provider/workflows"
	restdatastore "jaiscloud/internal/gcp/transport/rest/datastore"
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
	"PubSub":            "pubsub",
	"Firestore":         "firestore",
	"Function":          "functions",
	"Workflow":          "workflows",
	"WorkflowExecution": "workflowexecutions",
	"Dataproc":          "dataproc",
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
	"ResourceManager":   "resourcemanager",
	"Datastore":         "datastore",
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
		&functionsprovider.Provider{},
		&workflowsprovider.Provider{},
		&workflowexecutionsprovider.Provider{},
		&dataprocprovider.Provider{},
		&managedkafkaprovider.Provider{},
		&metastoreprovider.Provider{},
		&icebergprovider.Provider{},
		&bigqueryprovider.Provider{},
		&eventarcprovider.Provider{},
		&clouddnsprovider.Provider{},
		&memorystoreprovider.Provider{},
		&cloudsqlprovider.Provider{},
		&computeprovider.Provider{},
		&serviceusageprovider.Provider{},
		&resourcemanagerprovider.Provider{},
		&restdatastore.Provider{},
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
