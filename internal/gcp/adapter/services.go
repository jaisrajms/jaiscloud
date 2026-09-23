package gcp

import (
	"sort"

	"jaiscloud/internal/adapter"
	restdatastore "jaiscloud/internal/gcp/transport/rest/datastore"
)

// ServiceDescriptor captures the per-service metadata needed by the router and
// the gateway routing layer. GCP services are identified by URL path prefix
// rather than a SigV4 scope or target header.
type ServiceDescriptor struct {
	// ServiceName is the wire service name set on NormalizedRequest.Service
	// (e.g. "storage", "pubsub").
	ServiceName string

	// PathPrefixes are URL path prefixes that unambiguously identify this
	// service (e.g. "/storage/v1/" and "/upload/storage/v1/" for GCS).
	PathPrefixes []string

	// ProviderPrefix is the prefix used in the provider Registry dispatch key
	// (e.g. "Storage" → "Storage.BucketsInsert").
	ProviderPrefix string

	// Codec is a factory function that returns a new Codec for this service.
	Codec func() adapter.Codec
}

// gcpServices is the authoritative list of GCP services known to JaisCloud.
// GCS uses path-prefix detection; the /v1/projects/{project}/... services use
// segment-based detection (see detectV1Service) since they share the /v1/ prefix.
var gcpServices = []ServiceDescriptor{
	{
		ServiceName:    "storage",
		PathPrefixes:   []string{"/storage/v1/", "/upload/storage/v1/", "/resumable/upload/storage/v1/", "/download/storage/v1/"},
		ProviderPrefix: "Storage",
		Codec:          func() adapter.Codec { return &GCSCodec{} },
	},
	{
		ServiceName:    "pubsub",
		ProviderPrefix: "PubSub",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "pubsub"} },
	},
	{
		ServiceName:    "secretmanager",
		ProviderPrefix: "Secret",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "secretmanager"} },
	},
	{
		ServiceName:    "kms",
		ProviderPrefix: "KMS",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "kms"} },
	},
	{
		ServiceName:    "iam",
		ProviderPrefix: "IAM",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "iam"} },
	},
	{
		ServiceName:    "firestore",
		ProviderPrefix: "Firestore",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "firestore"} },
	},
	{
		ServiceName:    "functions",
		ProviderPrefix: "Function",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "functions"} },
	},
	{
		ServiceName:    "workflows",
		ProviderPrefix: "Workflow",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "workflows"} },
	},
	{
		ServiceName:    "workflowexecutions",
		ProviderPrefix: "WorkflowExecution",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "workflowexecutions"} },
	},
	{
		ServiceName:    "dataproc",
		ProviderPrefix: "Dataproc",
		Codec:          func() adapter.Codec { return &DataprocCodec{Service: "dataproc"} },
	},
	{
		ServiceName:    "managedkafka",
		ProviderPrefix: "ManagedKafka",
		Codec:          func() adapter.Codec { return &ManagedKafkaCodec{Service: "managedkafka"} },
	},
	{
		ServiceName:    "bigquery",
		ProviderPrefix: "BigQuery",
		Codec:          func() adapter.Codec { return &BigQueryCodec{Service: "bigquery"} },
	},
	{
		ServiceName:    "metastore",
		ProviderPrefix: "Metastore",
		Codec:          func() adapter.Codec { return &MetastoreCodec{Service: "metastore"} },
	},
	{
		ServiceName:    "eventarc",
		ProviderPrefix: "Eventarc",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "eventarc"} },
	},
	{
		ServiceName:    "iceberg",
		PathPrefixes:   []string{"/iceberg/"},
		ProviderPrefix: "Iceberg",
		Codec:          func() adapter.Codec { return &IcebergCodec{} },
	},
	{
		ServiceName:    "redis",
		ProviderPrefix: "Memorystore",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "redis"} },
	},
	{
		// Cloud SQL Admin's method paths embed the "sql/v1beta4/" service
		// prefix (unlike the shared /v1/projects/{project}/... services), so it
		// is identified by path prefix.
		ServiceName:    "sqladmin",
		PathPrefixes:   []string{"/sql/"},
		ProviderPrefix: "CloudSQL",
		Codec:          func() adapter.Codec { return &CloudSQLCodec{Service: "sqladmin"} },
	},
	{
		// Cloud DNS's method paths embed the "dns/v1/" service prefix (unlike
		// the shared /v1/projects/{project}/... services), so it is identified
		// by path prefix.
		ServiceName:    "dns",
		PathPrefixes:   []string{"/dns/v1/"},
		ProviderPrefix: "CloudDNS",
		Codec:          func() adapter.Codec { return &CloudDNSCodec{Service: "dns"} },
	},
	{
		// Compute Engine's method paths embed the "compute/v1/" service prefix
		// (unlike the shared /v1/projects/{project}/... services), so it is
		// identified by path prefix.
		ServiceName:    "compute",
		PathPrefixes:   []string{"/compute/v1/"},
		ProviderPrefix: "Compute",
		Codec:          func() adapter.Codec { return &ComputeCodec{Service: "compute"} },
	},
	{
		// Service Usage v1 shares the /v1/ prefix and is claimed by segment
		// detection (detectV1Service) on the "services" resource segment, so it
		// has no PathPrefixes. See ServiceUsageCodec.
		ServiceName:    "serviceusage",
		ProviderPrefix: "ServiceUsage",
		Codec:          func() adapter.Codec { return &ServiceUsageCodec{Service: "serviceusage"} },
	},
	{
		// Cloud Resource Manager v1 project surface shares /v1/ and is claimed
		// by segment detection (a project segment that is the last segment, with
		// or without a ':' custom verb), so it has no PathPrefixes. See
		// ResourceManagerCodec.
		ServiceName:    "resourcemanager",
		ProviderPrefix: "ResourceManager",
		Codec:          func() adapter.Codec { return &ResourceManagerCodec{Service: "resourcemanager"} },
	},
	{
		// Cloud Datastore v1 shares the /v1/ prefix; its data methods are
		// project-segment custom verbs (POST /v1/projects/{project}:lookup,
		// :runQuery, :commit, ...) claimed by segment detection
		// (detectDatastoreVerb), so it has no PathPrefixes. The codec lives with
		// the REST transport package that adapts it to the shared core.
		ServiceName:    "datastore",
		ProviderPrefix: "Datastore",
		Codec:          func() adapter.Codec { return restdatastore.NewCodec() },
	},
}

// serviceProviderMap maps wire service name → provider registry prefix.
// Built once at init time from gcpServices. Do not modify directly.
var serviceProviderMap map[string]string

func init() {
	serviceProviderMap = make(map[string]string, len(gcpServices))
	for _, svc := range gcpServices {
		serviceProviderMap[svc.ServiceName] = svc.ProviderPrefix
	}
}

// ServiceNames returns the sorted wire service names this adapter knows.
// It is the validation set for per-service transport overrides.
func ServiceNames() []string {
	names := make([]string, 0, len(gcpServices))
	for _, svc := range gcpServices {
		names = append(names, svc.ServiceName)
	}
	sort.Strings(names)
	return names
}

// grpcOnlyServices are wire services with a gRPC surface but no REST descriptor
// in gcpServices (the emulator implements them only over gRPC).
var grpcOnlyServices = []string{"logging", "monitoring"}

// KnownServiceNames returns the union of the REST service names and the
// gRPC-only services. It is the validation/enablement set for transport
// selection, so a global "grpc" default enables the gRPC-only surfaces too.
func KnownServiceNames() []string {
	names := append(ServiceNames(), grpcOnlyServices...)
	sort.Strings(names)
	return names
}
