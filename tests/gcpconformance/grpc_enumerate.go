//go:build gcp_conformance

package gcpconformance

import (
	"fmt"
	"sort"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	cloudtaskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	containerpb "cloud.google.com/go/container/apiv1/containerpb"
	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	eventarcpb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"
	iamadminpb "cloud.google.com/go/iam/admin/apiv1/adminpb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	credentialspb "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	runpb "cloud.google.com/go/run/apiv2/runpb"
	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	serviceusagepb "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	workflowspb "cloud.google.com/go/workflows/apiv1/workflowspb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"

	grpcserver "jaiscloud/internal/gcp/grpc"
	grpcfirestore "jaiscloud/internal/gcp/grpc/firestore"
	grpcfirestoreadmin "jaiscloud/internal/gcp/grpc/firestoreadmin"
	grpcoperations "jaiscloud/internal/gcp/grpc/operations"
	grpcpubsub "jaiscloud/internal/gcp/grpc/pubsub"
	grpcsecretmanager "jaiscloud/internal/gcp/grpc/secretmanager"
	grpcstorage "jaiscloud/internal/gcp/grpc/storage"
	grpcstoragepb "jaiscloud/internal/gcp/grpc/storage/storagepb"
	grpccontainer "jaiscloud/internal/gcp/transport/grpc/container"
	grpcdataproc "jaiscloud/internal/gcp/transport/grpc/dataproc"
	grpcdatastore "jaiscloud/internal/gcp/transport/grpc/datastore"
	grpceventarc "jaiscloud/internal/gcp/transport/grpc/eventarc"
	grpcfunctions "jaiscloud/internal/gcp/transport/grpc/functions"
	grpciam "jaiscloud/internal/gcp/transport/grpc/iam"
	grpciamcredentials "jaiscloud/internal/gcp/transport/grpc/iamcredentials"
	grpckms "jaiscloud/internal/gcp/transport/grpc/kms"
	grpclogging "jaiscloud/internal/gcp/transport/grpc/logging"
	grpcmanagedkafka "jaiscloud/internal/gcp/transport/grpc/managedkafka"
	grpcmetastore "jaiscloud/internal/gcp/transport/grpc/metastore"
	grpcmonitoring "jaiscloud/internal/gcp/transport/grpc/monitoring"
	grpcresourcemanager "jaiscloud/internal/gcp/transport/grpc/resourcemanager"
	grpcrun "jaiscloud/internal/gcp/transport/grpc/run"
	grpcscheduler "jaiscloud/internal/gcp/transport/grpc/scheduler"
	grpcserviceusage "jaiscloud/internal/gcp/transport/grpc/serviceusage"
	grpctasks "jaiscloud/internal/gcp/transport/grpc/tasks"
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
	"google.cloud.dataproc.v1.ClusterController":            "dataproc",
	"google.cloud.dataproc.v1.JobController":                "dataproc",
	"google.cloud.dataproc.v1.WorkflowTemplateService":      "dataproc",
	"google.container.v1.ClusterManager":                    "container",
	"google.cloud.functions.v1.CloudFunctionsService":       "functions",
	"google.cloud.functions.v2.FunctionService":             "functions",
	"google.storage.v2.Storage":                             "storage",
	"google.firestore.v1.Firestore":                         "firestore",
	"google.firestore.admin.v1.FirestoreAdmin":              "firestoreadmin",
	"google.datastore.v1.Datastore":                         "datastore",
	"google.pubsub.v1.Publisher":                            "pubsub",
	"google.pubsub.v1.Subscriber":                           "pubsub",
	"google.cloud.kms.v1.KeyManagementService":              "kms",
	"google.logging.v2.LoggingServiceV2":                    "logging",
	"google.logging.v2.ConfigServiceV2":                     "logging",
	"google.logging.v2.MetricsServiceV2":                    "logging",
	"google.monitoring.v3.MetricService":                    "monitoring",
	"google.monitoring.v3.AlertPolicyService":               "monitoring",
	"google.monitoring.v3.NotificationChannelService":       "monitoring",
	"google.monitoring.v3.ServiceMonitoringService":         "monitoring",
	"google.monitoring.v3.UptimeCheckService":               "monitoring",
	"google.cloud.secretmanager.v1.SecretManagerService":    "secretmanager",
	"google.cloud.workflows.executions.v1.Executions":       "workflowexecutions",
	"google.cloud.workflows.v1.Workflows":                   "workflows",
	"google.cloud.managedkafka.v1.ManagedKafka":             "managedkafka",
	"google.cloud.metastore.v1.DataprocMetastore":           "metastore",
	"google.cloud.metastore.v1.DataprocMetastoreFederation": "metastore",
	"google.cloud.eventarc.v1.Eventarc":                     "eventarc",
	"google.api.serviceusage.v1.ServiceUsage":               "serviceusage",
	"google.cloud.resourcemanager.v3.Projects":              "resourcemanager",
	"google.cloud.run.v2.Services":                          "run",
	"google.cloud.run.v2.Revisions":                         "run",
	"google.cloud.scheduler.v1.CloudScheduler":              "scheduler",
	"google.cloud.tasks.v2.CloudTasks":                      "tasks",
	"google.iam.v1.IAMPolicy":                               "iam",
	"google.iam.admin.v1.IAM":                               "iam",
	"google.iam.credentials.v1.IAMCredentials":              "iamcredentials",
	"google.longrunning.Operations":                         "operations",
}

// grpcRuntimeUnimplemented maps the gRPC services the server registers at
// runtime but deliberately leaves out of the *graded* surface (EnumerateGRPC):
// the Dataproc Serverless controllers. cmd/jaiscloud-gcp registers them so a
// serverless client gets UNIMPLEMENTED rather than "unknown service" (the
// REST transport answers 501 for the same paths), but the fidelity matrix must
// not grade an unimplemented surface `ga`.
//
// They are still part of the *served* surface — every RPC is registered and
// answers UNIMPLEMENTED — so the published-vs-served coverage report counts
// them as served and not graded. That is the boundary GKP8 draws: a documented
// UNIMPLEMENTED is `served`/`unsupported`, never `absent`.
var grpcRuntimeUnimplemented = map[string]string{
	"google.cloud.dataproc.v1.BatchController":           "dataproc",
	"google.cloud.dataproc.v1.SessionController":         "dataproc",
	"google.cloud.dataproc.v1.SessionTemplateController": "dataproc",
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
	monitoringpb.RegisterServiceMonitoringServiceServer(reg, &grpcmonitoring.Service{})
	monitoringpb.RegisterUptimeCheckServiceServer(reg, &grpcmonitoring.Service{})
	grpcstoragepb.RegisterStorageServer(reg, &grpcstorage.Service{})
	secretmanagerpb.RegisterSecretManagerServiceServer(reg, &grpcsecretmanager.Service{})
	executionspb.RegisterExecutionsServer(reg, &grpcworkflowexecutions.Service{})
	workflowspb.RegisterWorkflowsServer(reg, &grpcworkflows.Service{})
	managedkafkapb.RegisterManagedKafkaServer(reg, &grpcmanagedkafka.Service{})
	metastorepb.RegisterDataprocMetastoreServer(reg, &grpcmetastore.Service{})
	metastorepb.RegisterDataprocMetastoreFederationServer(reg, &grpcmetastore.Service{})
	eventarcpb.RegisterEventarcServer(reg, &grpceventarc.Service{})
	serviceusagepb.RegisterServiceUsageServer(reg, &grpcserviceusage.Service{})
	resourcemanagerpb.RegisterProjectsServer(reg, &grpcresourcemanager.Service{})
	schedulerpb.RegisterCloudSchedulerServer(reg, &grpcscheduler.Service{})
	cloudtaskspb.RegisterCloudTasksServer(reg, &grpctasks.Service{})
	dataprocpb.RegisterClusterControllerServer(reg, &grpcdataproc.Service{})
	dataprocpb.RegisterJobControllerServer(reg, &grpcdataproc.Service{})
	dataprocpb.RegisterWorkflowTemplateServiceServer(reg, &grpcdataproc.Service{})
	// Deliberately NOT registered here: the Dataproc Serverless controllers
	// (Batch/Session/SessionTemplate). The emulator registers them at runtime so a
	// serverless client gets UNIMPLEMENTED rather than "unknown service", but this
	// registry enumerates the *graded* surface and the fidelity matrix is derived
	// from it — grading 14 unimplemented RPCs would be a false ga. They are
	// counted as served-but-ungraded by the coverage report instead
	// (EnumerateGRPCUnimplemented + PublishedGRPCServices).
	containerpb.RegisterClusterManagerServer(reg, &grpccontainer.Service{})
	runSvc := &grpcrun.Service{}
	runpb.RegisterServicesServer(reg, runSvc)
	runpb.RegisterRevisionsServer(reg, runSvc)
	functionspb.RegisterCloudFunctionsServiceServer(reg, &grpcfunctions.Service{})
	apiv2functionspb.RegisterFunctionServiceServer(reg, &grpcfunctions.ServiceV2{})
	credentialspb.RegisterIAMCredentialsServer(reg, &grpciamcredentials.Service{})
	iamadminpb.RegisterIAMServer(reg, &grpciam.Service{})
	// Pub/Sub, KMS, Eventarc, Metastore and Dataproc share one
	// google.iam.v1.IAMPolicy registration.
	iampb.RegisterIAMPolicyServer(reg, grpcserver.NewIAMRouter(&grpcpubsub.Service{}, &grpckms.Service{}, &grpceventarc.Service{}, &grpcmetastore.Service{}, &grpcdataproc.Service{}))
	longrunningpb.RegisterOperationsServer(reg, grpcoperations.New())

	return servicesFromInfo(reg.GetServiceInfo(), grpcWireService)
}

// EnumerateGRPCUnimplemented reads the runtime-registered unimplemented
// controllers back from a grpc.Server, the same way EnumerateGRPC reads the
// graded surface. They are registered with the zero-value Dataproc service,
// whose embedded generated Unimplemented servers answer UNIMPLEMENTED for every
// RPC; no handler is ever invoked.
func EnumerateGRPCUnimplemented() []GRPCService {
	reg := grpc.NewServer()
	svc := &grpcdataproc.Service{}
	dataprocpb.RegisterBatchControllerServer(reg, svc)
	dataprocpb.RegisterSessionControllerServer(reg, svc)
	dataprocpb.RegisterSessionTemplateControllerServer(reg, svc)
	return servicesFromInfo(reg.GetServiceInfo(), grpcRuntimeUnimplemented)
}

// PublishedGRPCServices returns every RPC declared by the proto descriptors of
// the gRPC services the emulator wires — the graded surface plus the
// runtime-registered unimplemented controllers. It returns one row per wire
// service (a fidelity service may span several, e.g. Cloud Functions v1+v2);
// callers dedupe per fidelity service.
//
// It is the gRPC half of the coverage source of truth: EnumerateGRPC says what
// is graded, this says what is *published* by the protos the emulator serves.
// A registered service exposes every method of its proto (unimplemented ones
// answer UNIMPLEMENTED), so published - graded is always served-but-ungraded,
// never an invented cell.
func PublishedGRPCServices() []GRPCService {
	wires := map[string]string{}
	for wire, service := range grpcWireService {
		wires[wire] = service
	}
	for wire, service := range grpcRuntimeUnimplemented {
		wires[wire] = service
	}

	names := make([]string, 0, len(wires))
	for wire := range wires {
		names = append(names, wire)
	}
	sort.Strings(names)

	services := make([]GRPCService, 0, len(names))
	for _, wire := range names {
		d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(wire))
		if err != nil {
			panic(fmt.Sprintf("gcpconformance: descriptor for %q: %v", wire, err))
		}
		sd, ok := d.(protoreflect.ServiceDescriptor)
		if !ok {
			panic(fmt.Sprintf("gcpconformance: %q is not a service descriptor", wire))
		}
		methods := make([]string, 0, sd.Methods().Len())
		for i := 0; i < sd.Methods().Len(); i++ {
			methods = append(methods, string(sd.Methods().Get(i).Name()))
		}
		sort.Strings(methods)
		services = append(services, GRPCService{WireService: wire, Service: wires[wire], Methods: methods})
	}
	sort.Slice(services, func(i, j int) bool { return services[i].WireService < services[j].WireService })
	return services
}

// servicesFromInfo turns grpc.Server.GetServiceInfo into GRPCService rows,
// panicking on a wire service with no fidelity mapping (a programming error in
// this file, not a runtime condition).
func servicesFromInfo(info map[string]grpc.ServiceInfo, wiring map[string]string) []GRPCService {
	services := make([]GRPCService, 0, len(info))
	for wire, si := range info {
		service, ok := wiring[wire]
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
