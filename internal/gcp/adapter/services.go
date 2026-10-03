package gcp

import (
	"sort"

	"jaiscloud/internal/adapter"
	restcontainer "jaiscloud/internal/gcp/transport/rest/container"
	restdataproc "jaiscloud/internal/gcp/transport/rest/dataproc"
	restdatastore "jaiscloud/internal/gcp/transport/rest/datastore"
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
		// IAM Service Account Credentials shares the /v1/ prefix and the
		// /v1/projects/{p}/serviceAccounts/{email} resource path with IAM, so
		// it is claimed by segment detection (detectV1Service) on its unique
		// custom verbs (generateAccessToken / generateIdToken). signBlob and
		// signJwt collide with iam on a single host and stay routed to iam.
		ServiceName:    "iamcredentials",
		ProviderPrefix: "IAMCredentials",
		Codec:          func() adapter.Codec { return &JSONCodec{Service: "iamcredentials"} },
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
		// Cloud Workflow Executions v1 shares the /v1/ prefix; its execution
		// paths are claimed by segment detection (detectV1Service) on the
		// "executions" segment. The codec lives with the REST transport package
		// that adapts it to the shared core.
		ServiceName:    "workflowexecutions",
		ProviderPrefix: "WorkflowExecution",
		Codec:          func() adapter.Codec { return restworkflowexecutions.NewCodec() },
	},
	{
		ServiceName:    "dataproc",
		ProviderPrefix: "Dataproc",
		Codec:          func() adapter.Codec { return restdataproc.NewCodec() },
	},
	{
		ServiceName:    "managedkafka",
		ProviderPrefix: "ManagedKafka",
		Codec:          func() adapter.Codec { return restmanagedkafka.NewCodec() },
	},
	{
		// Google Kubernetes Engine (GKE) v1 shares the /v1/ prefix and the
		// canonical /v1/projects/{project}/locations/{location}/clusters path
		// with Managed Kafka on the single emulator origin, so it is
		// disambiguated by host (the first DNS label is "container", matching
		// container.googleapis.com) or by the "/container/" path prefix
		// Terraform/gcloud use. The codec lives with the REST transport package
		// that adapts it to the shared core. REST only — GKE's native transport
		// is gRPC, but the emulator deliberately exposes the REST metadata
		// surface (see docs/GA.md §7).
		ServiceName:    "container",
		PathPrefixes:   []string{"/container/"},
		ProviderPrefix: "Container",
		Codec:          func() adapter.Codec { return restcontainer.NewCodec() },
	},
	{
		ServiceName:    "bigquery",
		ProviderPrefix: "BigQuery",
		Codec:          func() adapter.Codec { return &BigQueryCodec{Service: "bigquery"} },
	},
	{
		ServiceName:    "metastore",
		ProviderPrefix: "Metastore",
		Codec:          func() adapter.Codec { return restmetastore.NewCodec() },
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
		// has no PathPrefixes. The codec lives with the REST transport package
		// that adapts it to the shared core.
		ServiceName:    "serviceusage",
		ProviderPrefix: "ServiceUsage",
		Codec:          func() adapter.Codec { return restserviceusage.NewCodec() },
	},
	{
		// Cloud Resource Manager v1 project surface shares /v1/ and is claimed
		// by segment detection (a project segment that is the last segment, with
		// or without a ':' custom verb), so it has no PathPrefixes. The codec
		// lives with the REST transport package that adapts it to the shared
		// core.
		ServiceName:    "resourcemanager",
		ProviderPrefix: "ResourceManager",
		Codec:          func() adapter.Codec { return restresourcemanager.NewCodec() },
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
	{
		// Cloud Scheduler v1 shares the /v1/ prefix and is claimed by segment
		// detection (detectV1Service) on the locations/{location}/jobs path, so
		// it has no PathPrefixes. The codec lives with the REST transport
		// package that adapts it to the shared core.
		ServiceName:    "scheduler",
		ProviderPrefix: "Scheduler",
		Codec:          func() adapter.Codec { return restscheduler.NewCodec() },
	},
	{
		// Cloud Tasks v2 shares the /v2/ prefix and is claimed by segment
		// detection (detectV2Service) on the locations/{location}/queues path,
		// so it has no PathPrefixes. The codec lives with the REST transport
		// package that adapts it to the shared core.
		ServiceName:    "tasks",
		ProviderPrefix: "Tasks",
		Codec:          func() adapter.Codec { return resttasks.NewCodec() },
	},
	{
		// Cloud Run Admin v2 shares the /v2/ prefix and the canonical
		// /v2/projects/{p}/locations/{l}/services path with Cloud Functions and
		// Cloud Tasks on the single emulator origin, so it is claimed by segment
		// detection (detectV2Service) on the services/revisions resource family
		// and on run-prefixed operation ids. Terraform/gcloud use the "/run/"
		// path prefix. The codec lives with the REST transport package that
		// adapts it to the shared core. REST only — gRPC is deferred (CR4).
		ServiceName:    "run",
		PathPrefixes:   []string{"/run/"},
		ProviderPrefix: "Run",
		Codec:          func() adapter.Codec { return restrun.NewCodec() },
	},
	{
		// Cloud Logging v2 shares the /v2/ namespace; its REST data methods are
		// /v2/entries:write, /v2/entries:list, /v2/{parent}/logs,
		// /v2/{logName}, and /v2/monitoredResourceDescriptors, claimed by
		// segment detection (detectV2Service), so it has no PathPrefixes. The
		// codec lives with the REST transport package that adapts it to the
		// shared core.
		ServiceName:    "logging",
		ProviderPrefix: "Logging",
		Codec:          func() adapter.Codec { return restlogging.NewCodec() },
	},
	{
		// Cloud Monitoring v3 owns the /v3/ namespace (no other emulated
		// service uses it), so it is identified by path prefix. The codec lives
		// with the REST transport package that adapts it to the shared core.
		ServiceName:    "monitoring",
		PathPrefixes:   []string{"/v3/"},
		ProviderPrefix: "Monitoring",
		Codec:          func() adapter.Codec { return restmonitoring.NewCodec() },
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
// in gcpServices. Firestore Admin is gRPC-only: its index CRUD is REST under
// the `firestore` service (projects.databases.collectionGroups.indexes) but
// exposed over gRPC as the distinct google.firestore.admin.v1.FirestoreAdmin
// wire service, so it has no REST descriptor of its own.
var grpcOnlyServices = []string{"firestoreadmin"}

// KnownServiceNames returns the union of the REST service names and the
// gRPC-only services. It is the validation/enablement set for transport
// selection, so a global "grpc" default enables the gRPC-only surfaces too.
func KnownServiceNames() []string {
	names := append(ServiceNames(), grpcOnlyServices...)
	sort.Strings(names)
	return names
}
