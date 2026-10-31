package grpcplugin

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	_ "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	_ "cloud.google.com/go/container/apiv1/containerpb"
	_ "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	_ "cloud.google.com/go/datastore/apiv1/datastorepb"
	_ "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	_ "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	_ "cloud.google.com/go/firestore/apiv1/firestorepb"
	_ "cloud.google.com/go/functions/apiv1/functionspb"
	_ "cloud.google.com/go/functions/apiv2/functionspb"
	_ "cloud.google.com/go/iam/apiv1/iampb"
	_ "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	_ "cloud.google.com/go/kms/apiv1/kmspb"
	_ "cloud.google.com/go/logging/apiv2/loggingpb"
	_ "cloud.google.com/go/longrunning/autogen/longrunningpb"
	_ "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	_ "cloud.google.com/go/metastore/apiv1/metastorepb"
	_ "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	_ "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	_ "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	_ "cloud.google.com/go/run/apiv2/runpb"
	_ "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	_ "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	_ "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	_ "cloud.google.com/go/workflows/apiv1/workflowspb"
	_ "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	gcpadapter "jaiscloud/internal/gcp/adapter"
	_ "jaiscloud/internal/gcp/grpc/storage/storagepb"
	"jaiscloud/internal/provider"
)

var update = flag.Bool("update", false, "rewrite testdata/coverage.golden")

// servedServices are the gRPC services jaiscloud-gcp registers (cmd/jaiscloud-gcp).
var servedServices = []string{
	"google.firestore.v1.Firestore", "google.firestore.admin.v1.FirestoreAdmin", "google.datastore.v1.Datastore",
	"google.cloud.workflows.executions.v1.Executions", "google.cloud.workflows.v1.Workflows",
	"google.cloud.managedkafka.v1.ManagedKafka",
	"google.cloud.metastore.v1.DataprocMetastore", "google.cloud.metastore.v1.DataprocMetastoreFederation",
	"google.cloud.eventarc.v1.Eventarc", "google.api.serviceusage.v1.ServiceUsage",
	"google.cloud.scheduler.v1.CloudScheduler", "google.cloud.tasks.v2.CloudTasks",
	"google.cloud.resourcemanager.v3.Projects", "google.iam.credentials.v1.IAMCredentials",
	"google.cloud.dataproc.v1.ClusterController", "google.cloud.dataproc.v1.JobController",
	"google.cloud.dataproc.v1.WorkflowTemplateService", "google.container.v1.ClusterManager",
	"google.cloud.run.v2.Services", "google.cloud.run.v2.Revisions",
	"google.cloud.functions.v1.CloudFunctionsService", "google.cloud.functions.v2.FunctionService",
	"google.pubsub.v1.Publisher", "google.pubsub.v1.Subscriber", "google.cloud.kms.v1.KeyManagementService",
	"google.logging.v2.LoggingServiceV2", "google.logging.v2.ConfigServiceV2", "google.logging.v2.MetricsServiceV2",
	"google.monitoring.v3.MetricService", "google.monitoring.v3.AlertPolicyService",
	"google.monitoring.v3.NotificationChannelService", "google.monitoring.v3.ServiceMonitoringService",
	"google.storage.v2.Storage", "google.cloud.secretmanager.v1.SecretManagerService",
	"google.iam.v1.IAMPolicy", "google.longrunning.Operations",
}

type noHost struct{}

func (noHost) Covers(string) bool        { return false }
func (noHost) CoveredKeys() []string     { return nil }
func (noHost) Attach(*provider.Registry) {}

// report lists, per served service, which RPCs reach a registry key and which do
// not, so the set a plugin can intercept over gRPC is a reviewed fact.
func report(t *testing.T) string {
	t.Helper()
	i := New(Config{Host: noHost{}, Adapter: gcpadapter.NewAdapter("")})
	var b strings.Builder
	for _, name := range servedServices {
		d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
		if err != nil {
			t.Fatalf("%s is not linked into the test: %v", name, err)
		}
		sd := d.(protoreflect.ServiceDescriptor)
		var mapped, unmapped []string
		for m := 0; m < sd.Methods().Len(); m++ {
			md := sd.Methods().Get(m)
			full := fmt.Sprintf("/%s/%s", name, md.Name())
			if key, ok := i.Route(full); ok {
				mapped = append(mapped, fmt.Sprintf("%s=%s", md.Name(), key))
			} else {
				unmapped = append(unmapped, string(md.Name()))
			}
		}
		sort.Strings(mapped)
		sort.Strings(unmapped)
		fmt.Fprintf(&b, "%s: %d mapped, %d without a registry key\n", name, len(mapped), len(unmapped))
		for _, m := range mapped {
			fmt.Fprintf(&b, "  + %s\n", m)
		}
		for _, m := range unmapped {
			fmt.Fprintf(&b, "  - %s\n", m)
		}
	}
	return b.String()
}

func TestCoverageIsAReviewedFact(t *testing.T) {
	got := report(t)
	const golden = "testdata/coverage.golden"
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("gRPC→registry mapping changed; review it and run `go test ./internal/gcp/grpcplugin -run Coverage -update`\n%s", got)
	}
}
