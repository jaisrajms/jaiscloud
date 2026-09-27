//go:build gcp_conformance

package gcpconformance

import (
	"fmt"
	"sort"

	"google.golang.org/grpc"

	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	eventarcpb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	serviceusagepb "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	workflowspb "cloud.google.com/go/workflows/apiv1/workflowspb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"

	grpcserver "jaiscloud/internal/gcp/grpc"
	grpcfirestore "jaiscloud/internal/gcp/grpc/firestore"
	grpcfirestoreadmin "jaiscloud/internal/gcp/grpc/firestoreadmin"
	grpckms "jaiscloud/internal/gcp/grpc/kms"
	grpcoperations "jaiscloud/internal/gcp/grpc/operations"
	grpcpubsub "jaiscloud/internal/gcp/grpc/pubsub"
	grpcsecretmanager "jaiscloud/internal/gcp/grpc/secretmanager"
	grpcstorage "jaiscloud/internal/gcp/grpc/storage"
	grpcstoragepb "jaiscloud/internal/gcp/grpc/storage/storagepb"
	grpcdataproc "jaiscloud/internal/gcp/transport/grpc/dataproc"
	grpcdatastore "jaiscloud/internal/gcp/transport/grpc/datastore"
	grpceventarc "jaiscloud/internal/gcp/transport/grpc/eventarc"
	grpcfunctions "jaiscloud/internal/gcp/transport/grpc/functions"
	grpclogging "jaiscloud/internal/gcp/transport/grpc/logging"
	grpcmanagedkafka "jaiscloud/internal/gcp/transport/grpc/managedkafka"
	grpcmetastore "jaiscloud/internal/gcp/transport/grpc/metastore"
	grpcmonitoring "jaiscloud/internal/gcp/transport/grpc/monitoring"
	grpcresourcemanager "jaiscloud/internal/gcp/transport/grpc/resourcemanager"
	grpcserviceusage "jaiscloud/internal/gcp/transport/grpc/serviceusage"
	grpcworkflowexecutions "jaiscloud/internal/gcp/transport/grpc/workflowexecutions"
	grpcworkflows "jaiscloud/internal/gcp/transport/grpc/workflows"
)

// GRPCService is one gRPC service the emulator registers, read back from the
// live grpc.Server rather than from a hand-maintained list.
type GRPCService struct {
	WireService string   // protobuf service name, e.g. "google.storage.v2.Storage"
	Service     string   // fidelity service name, e.g. "storage"
	Methods     []string // exact RPC method names, sorted
}

// grpcWireService maps a protobuf service name to the fidelity service it
// belongs to. It is the only hand-maintained part of the enumeration; the
// service and method names themselves come from grpc.Server.GetServiceInfo.
//
// Summary services (Pub/Sub, KMS, Secret Manager) own their IAM surface inside
// their own proto, so only the standalone google.iam.v1.IAMPolicy routing
// service is registered once (Pub/Sub + KMS, mirroring main.go).
//
// grpc.health.v1.Health and reflection are deliberately not registered here:
// they are standard gRPC infrastructure, not GCP API surface, and would not
// belong in the fidelity matrix.
var grpcWireService = map[string]string{
	"google.cloud.dataproc.v1.ClusterController":         "dataproc",
	"google.cloud.dataproc.v1.JobController":             "dataproc",
	"google.cloud.functions.v1.CloudFunctionsService":    "functions",
	"google.cloud.functions.v2.FunctionService":          "functions",
	"google.storage.v2.Storage":                          "storage",
	"google.firestore.v1.Firestore":                      "firestore",
	"google.firestore.admin.v1.FirestoreAdmin":           "firestoreadmin",
	"google.datastore.v1.Datastore":                      "datastore",
	"google.pubsub.v1.Publisher":                         "pubsub",
	"google.pubsub.v1.Subscriber":                        "pubsub",
	"google.cloud.kms.v1.KeyManagementService":           "kms",
	"google.logging.v2.LoggingServiceV2":                 "logging",
	"google.logging.v2.ConfigServiceV2":                  "logging",
	"google.logging.v2.MetricsServiceV2":                 "logging",
	"google.monitoring.v3.MetricService":                 "monitoring",
	"google.monitoring.v3.AlertPolicyService":            "monitoring",
	"google.monitoring.v3.NotificationChannelService":    "monitoring",
	"google.cloud.secretmanager.v1.SecretManagerService": "secretmanager",
	"google.cloud.workflows.executions.v1.Executions":    "workflowexecutions",
	"google.cloud.workflows.v1.Workflows":                "workflows",
	"google.cloud.managedkafka.v1.ManagedKafka":          "managedkafka",
	"google.cloud.metastore.v1.DataprocMetastore":        "metastore",
	"google.cloud.eventarc.v1.Eventarc":                  "eventarc",
	"google.api.serviceusage.v1.ServiceUsage":            "serviceusage",
	"google.cloud.resourcemanager.v3.Projects":           "resourcemanager",
	"google.iam.v1.IAMPolicy":                            "iam",
	"google.longrunning.Operations":                      "operations",
}

// EnumerateGRPC returns the emulator's gRPC surface without any network I/O.
//
// It constructs a fresh grpc.Server, registers every emulator service with a
// zero-value handler (registration only stores the implementation; no handler
// is ever invoked), and reads the exact service + method names back from
// srv.GetServiceInfo(). This means the registry of gRPC methods is derived
// from the protos the emulator actually serves, never from a hand list.
//
// This is the "runtime registration" path; it is preferred over a generated
// list because it cannot drift: a new RPC in an existing service shows up
// automatically, and a new *service* fails loudly here (no wire mapping)
// instead of being silently omitted.
//
// Panics if a registered service has no entry in grpcWireService — that is a
// programming error in this file, not a runtime condition.
func EnumerateGRPC() []GRPCService {
	reg := grpc.NewServer()

	firestorepb.RegisterFirestoreServer(reg, &grpcfirestore.Service{})
	adminpb.RegisterFirestoreAdminServer(reg, &grpcfirestoreadmin.Service{})
	datastorepb.RegisterDatastoreServer(reg, &grpcdatastore.Service{})
	pubsubpb.RegisterPublisherServer(reg, &grpcpubsub.Service{})
	pubsubpb.RegisterSubscriberServer(reg, &grpcpubsub.Service{})
	kmspb.RegisterKeyManagementServiceServer(reg, &grpckms.Service{})
	loggingpb.RegisterLoggingServiceV2Server(reg, &grpclogging.Service{})
	loggingpb.RegisterConfigServiceV2Server(reg, &grpclogging.ConfigService{})
	loggingpb.RegisterMetricsServiceV2Server(reg, &grpclogging.MetricsService{})
	monitoringpb.RegisterMetricServiceServer(reg, &grpcmonitoring.Service{})
	monitoringpb.RegisterAlertPolicyServiceServer(reg, &grpcmonitoring.Service{})
	monitoringpb.RegisterNotificationChannelServiceServer(reg, &grpcmonitoring.Service{})
	grpcstoragepb.RegisterStorageServer(reg, &grpcstorage.Service{})
	secretmanagerpb.RegisterSecretManagerServiceServer(reg, &grpcsecretmanager.Service{})
	executionspb.RegisterExecutionsServer(reg, &grpcworkflowexecutions.Service{})
	workflowspb.RegisterWorkflowsServer(reg, &grpcworkflows.Service{})
	managedkafkapb.RegisterManagedKafkaServer(reg, &grpcmanagedkafka.Service{})
	metastorepb.RegisterDataprocMetastoreServer(reg, &grpcmetastore.Service{})
	eventarcpb.RegisterEventarcServer(reg, &grpceventarc.Service{})
	serviceusagepb.RegisterServiceUsageServer(reg, &grpcserviceusage.Service{})
	resourcemanagerpb.RegisterProjectsServer(reg, &grpcresourcemanager.Service{})
	dataprocpb.RegisterClusterControllerServer(reg, &grpcdataproc.Service{})
	dataprocpb.RegisterJobControllerServer(reg, &grpcdataproc.Service{})
	functionspb.RegisterCloudFunctionsServiceServer(reg, &grpcfunctions.Service{})
	apiv2functionspb.RegisterFunctionServiceServer(reg, &grpcfunctions.ServiceV2{})
	// Pub/Sub, KMS and Eventarc share one google.iam.v1.IAMPolicy registration.
	iampb.RegisterIAMPolicyServer(reg, grpcserver.NewIAMRouter(&grpcpubsub.Service{}, &grpckms.Service{}, &grpceventarc.Service{}))
	longrunningpb.RegisterOperationsServer(reg, grpcoperations.New())

	info := reg.GetServiceInfo()
	services := make([]GRPCService, 0, len(info))
	for wire, si := range info {
		service, ok := grpcWireService[wire]
		if !ok {
			panic(fmt.Sprintf("gcpconformance: gRPC service %q has no fidelity-service mapping", wire))
		}
		methods := make([]string, 0, len(si.Methods))
		for _, m := range si.Methods {
			methods = append(methods, m.Name)
		}
		sort.Strings(methods)
		services = append(services, GRPCService{WireService: wire, Service: service, Methods: methods})
	}
	sort.Slice(services, func(i, j int) bool { return services[i].WireService < services[j].WireService })
	return services
}
