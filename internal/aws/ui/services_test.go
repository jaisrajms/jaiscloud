package ui

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/aws/key"
	"jaiscloud/internal/aws/parameter"
	"jaiscloud/internal/aws/provider/apigw"
	"jaiscloud/internal/aws/provider/cache"
	"jaiscloud/internal/aws/provider/catalog"
	"jaiscloud/internal/aws/provider/cloudwatch"
	cwlogs "jaiscloud/internal/aws/provider/cloudwatch/logs"
	"jaiscloud/internal/aws/provider/compute"
	"jaiscloud/internal/aws/provider/container"
	"jaiscloud/internal/aws/provider/dns"
	"jaiscloud/internal/aws/provider/eks"
	"jaiscloud/internal/aws/provider/elbv2"
	"jaiscloud/internal/aws/provider/emr"
	"jaiscloud/internal/aws/provider/emroneks"
	"jaiscloud/internal/aws/provider/events"
	"jaiscloud/internal/aws/provider/firehose"
	"jaiscloud/internal/aws/provider/iam"
	"jaiscloud/internal/aws/provider/kinesis"
	"jaiscloud/internal/aws/provider/lambda"
	"jaiscloud/internal/aws/provider/notification"
	"jaiscloud/internal/aws/provider/object"
	"jaiscloud/internal/aws/provider/queue"
	"jaiscloud/internal/aws/provider/rds"
	"jaiscloud/internal/aws/provider/ses"
	"jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/aws/provider/stepfunctions"
	"jaiscloud/internal/aws/provider/table"
	"jaiscloud/internal/aws/secret"
	"jaiscloud/internal/model"
)

func servicesFor(t *testing.T, cloud model.Cloud, providers *AWSProviders) ServicesResponse {
	t.Helper()
	handler := buildServicesHandler(providers, cloud)
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest("GET", "/api/ui/v1/services", nil))

	var resp ServicesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

// allProviders returns a descriptor set with every provider wired.
func allProviders() *AWSProviders {
	return &AWSProviders{
		Queue:     &queue.QueueProvider{},
		Object:    &object.ObjectProvider{},
		Table:     &table.TableProvider{},
		Function:  &lambda.FunctionProvider{},
		Logs:      &cwlogs.Provider{},
		CW:        &cloudwatch.Provider{},
		Notif:     &notification.SNSProvider{},
		IAM:       &iam.IAMProvider{},
		Key:       &key.KeyProvider{},
		Secret:    &secret.SecretProvider{},
		Param:     &parameter.ParameterProvider{},
		APIGW:     &apigw.GatewayProvider{},
		Catalog:   &catalog.GlueProvider{},
		EMR:       &emr.EMRProvider{},
		EMRC:      &emroneks.EMRContainersProvider{},
		Events:    &events.EventBridgeProvider{},
		Sfn:       &stepfunctions.Provider{},
		Compute:   &compute.ComputeProvider{},
		Container: &container.ContainerProvider{},
		EKS:       &eks.EKSProvider{},
		RDS:       &rds.RelationalProvider{},
		Cache:     &cache.CacheProvider{},
		DNS:       &dns.DNSProvider{},
		Stack:     &stack.StackProvider{},
		Kinesis:   &kinesis.Provider{},
		Firehose:  &firehose.Provider{},
		SES:       &ses.Provider{},
		ELBv2:     &elbv2.ELBv2Provider{},
	}
}

func TestServicesHandler_NonAWSCloudIsEmpty(t *testing.T) {
	for _, cloud := range []model.Cloud{model.CloudGCP, model.CloudAzure} {
		resp := servicesFor(t, cloud, &AWSProviders{Queue: &queue.QueueProvider{}})
		if len(resp.Services) != 0 {
			t.Fatalf("cloud %q: expected no services, got %d", cloud, len(resp.Services))
		}
	}
}

func TestServicesHandler_OnlyWiredProvidersListed(t *testing.T) {
	resp := servicesFor(t, model.CloudAWS, &AWSProviders{Queue: &queue.QueueProvider{}})
	if len(resp.Services) != 1 || resp.Services[0].ID != "sqs" {
		t.Fatalf("expected [sqs], got %+v", resp.Services)
	}
	if resp.Services[0].Category == "" || resp.Services[0].RootPath == "" {
		t.Fatalf("descriptor missing category/rootPath: %+v", resp.Services[0])
	}
}

func TestServicesHandler_NoProvidersIsEmpty(t *testing.T) {
	resp := servicesFor(t, model.CloudAWS, &AWSProviders{})
	if len(resp.Services) != 0 {
		t.Fatalf("expected no services, got %d", len(resp.Services))
	}
}

func TestServicesHandler_TiersPinnedToImplementationMatrix(t *testing.T) {
	// Keep in sync with DEVELOPER_GUIDE.md "Service implementation matrix".
	// Changing this set is an intentional capability change.
	metadataOnly := map[string]bool{
		"ec2": true, "ecs": true, "eks": true, "rds": true,
		"elasticache": true, "route53": true, "elbv2": true, "firehose": true,
	}
	stubs := map[string]bool{"ses": true}

	resp := servicesFor(t, model.CloudAWS, allProviders())
	if len(resp.Services) != 28 {
		t.Fatalf("expected 28 services, got %d", len(resp.Services))
	}

	seen := map[string]bool{}
	for _, service := range resp.Services {
		seen[service.ID] = true
		switch {
		case metadataOnly[service.ID]:
			if service.Tier != TierMetadata {
				t.Errorf("%s: tier = %q, want %q", service.ID, service.Tier, TierMetadata)
			}
			if service.Note == "" {
				t.Errorf("%s: metadata-only service should carry a note", service.ID)
			}
		case stubs[service.ID]:
			if service.Tier != TierShape {
				t.Errorf("%s: tier = %q, want %q", service.ID, service.Tier, TierShape)
			}
		default:
			if service.Tier != TierFull {
				t.Errorf("%s: tier = %q, want %q (add to metadataOnly/stubs if intentional)", service.ID, service.Tier, TierFull)
			}
		}
	}

	for id := range metadataOnly {
		if !seen[id] {
			t.Errorf("metadata-only service %q missing from descriptors", id)
		}
	}
	for id := range stubs {
		if !seen[id] {
			t.Errorf("stub service %q missing from descriptors", id)
		}
	}
}
