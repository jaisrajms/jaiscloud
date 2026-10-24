package ui

import (
	"net/http"

	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
	coreui "jaiscloud/internal/ui"
)

// Core descriptor types are re-exported so existing AWS UI code and tests keep
// their local names while the shared definitions live in internal/ui.
type (
	ServiceDescriptor = coreui.ServiceDescriptor
	ServiceChild      = coreui.ServiceChild
	ServicesResponse  = coreui.ServicesResponse
)

const (
	TierFull     = coreui.TierFull
	TierMetadata = coreui.TierMetadata
	TierShape    = coreui.TierShape
)

// executorNote reports the configured executor mode for an execution-backed
// service (Lambda, EMR). With the default mock executor these services accept
// the API but do not run real workloads.
func executorNote(subsystem string) string {
	mode, _ := config.ExecutorMode(subsystem, "mock")
	switch mode {
	case "", "mock":
		return "mock executor"
	default:
		return "executor: " + mode
	}
}

// buildServicesHandler reports the services this binary supports. The AWS
// service catalog is only served by an AWS build; other clouds return an
// empty list from the shared core, which owns the cloud's own registrar.
func buildServicesHandler(providers *AWSProviders, cloud model.Cloud) http.HandlerFunc {
	if cloud != model.CloudAWS {
		return func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, ServicesResponse{Services: []ServiceDescriptor{}})
		}
	}
	services := awsServiceDescriptors(providers)
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, ServicesResponse{Services: services})
	}
}

// awsServiceDescriptors returns the wired AWS service catalog, ordered by AWS
// console category so the client can group by first-seen category without
// needing its own ordering.
func awsServiceDescriptors(providers *AWSProviders) []ServiceDescriptor {
	lambdaNote := executorNote("lambda")
	sparkNote := executorNote("spark")

	descriptors := []struct {
		wired   bool
		service ServiceDescriptor
	}{
		{providers.Compute != nil, ServiceDescriptor{ID: "ec2", Label: "EC2", Category: "Compute", RootPath: "/aws/ec2/instances", Tier: TierMetadata, Note: "CRUD only", Children: []ServiceChild{{Label: "Instances", Path: "/aws/ec2/instances"}}}},
		{providers.Function != nil, ServiceDescriptor{ID: "lambda", Label: "Lambda", Category: "Compute", RootPath: "/aws/lambda", Tier: TierFull, Note: lambdaNote, Children: []ServiceChild{{Label: "Functions", Path: "/aws/lambda"}}}},
		{providers.Container != nil, ServiceDescriptor{ID: "ecs", Label: "ECS", Category: "Containers", RootPath: "/aws/ecs/clusters", Tier: TierMetadata, Note: "CRUD only", Children: []ServiceChild{{Label: "Clusters", Path: "/aws/ecs/clusters"}}}},
		{providers.EKS != nil, ServiceDescriptor{ID: "eks", Label: "EKS", Category: "Containers", RootPath: "/aws/eks/clusters", Tier: TierMetadata, Note: "CRUD only", Children: []ServiceChild{{Label: "Clusters", Path: "/aws/eks/clusters"}}}},
		{providers.Object != nil, ServiceDescriptor{ID: "s3", Label: "S3", Category: "Storage", RootPath: "/aws/s3", Tier: TierFull, Children: []ServiceChild{{Label: "Buckets", Path: "/aws/s3"}}}},
		{providers.Table != nil, ServiceDescriptor{ID: "dynamodb", Label: "DynamoDB", Category: "Database", RootPath: "/aws/dynamodb", Tier: TierFull, Children: []ServiceChild{{Label: "Tables", Path: "/aws/dynamodb"}}}},
		{providers.RDS != nil, ServiceDescriptor{ID: "rds", Label: "RDS", Category: "Database", RootPath: "/aws/rds/instances", Tier: TierMetadata, Note: "CRUD only", Children: []ServiceChild{{Label: "Instances", Path: "/aws/rds/instances"}}}},
		{providers.Cache != nil, ServiceDescriptor{ID: "elasticache", Label: "ElastiCache", Category: "Database", RootPath: "/aws/elasticache/clusters", Tier: TierMetadata, Note: "CRUD only", Children: []ServiceChild{{Label: "Clusters", Path: "/aws/elasticache/clusters"}}}},
		{providers.APIGW != nil, ServiceDescriptor{ID: "apigateway", Label: "API Gateway", Category: "Networking & Content Delivery", RootPath: "/aws/apigateway/apis", Tier: TierFull, Children: []ServiceChild{{Label: "REST APIs", Path: "/aws/apigateway/apis"}}}},
		{providers.DNS != nil, ServiceDescriptor{ID: "route53", Label: "Route 53", Category: "Networking & Content Delivery", RootPath: "/aws/route53/zones", Tier: TierMetadata, Note: "CRUD only", Children: []ServiceChild{{Label: "Hosted Zones", Path: "/aws/route53/zones"}}}},
		{providers.ELBv2 != nil, ServiceDescriptor{ID: "elbv2", Label: "Elastic Load Balancing", Category: "Networking & Content Delivery", RootPath: "/aws/elbv2/load-balancers", Tier: TierMetadata, Note: "CRUD only", Children: []ServiceChild{{Label: "Load Balancers", Path: "/aws/elbv2/load-balancers"}}}},
		{providers.EMR != nil, ServiceDescriptor{ID: "emr", Label: "EMR", Category: "Analytics", RootPath: "/aws/emr/clusters", Tier: TierFull, Note: sparkNote, Children: []ServiceChild{{Label: "Clusters", Path: "/aws/emr/clusters"}}}},
		{providers.EMRC != nil, ServiceDescriptor{ID: "emr-containers", Label: "EMR on EKS", Category: "Analytics", RootPath: "/aws/emr-containers/clusters", Tier: TierFull, Note: sparkNote, Children: []ServiceChild{{Label: "Virtual Clusters", Path: "/aws/emr-containers/clusters"}}}},
		{providers.Catalog != nil, ServiceDescriptor{ID: "glue", Label: "Glue", Category: "Analytics", RootPath: "/aws/glue/databases", Tier: TierFull, Children: []ServiceChild{{Label: "Databases", Path: "/aws/glue/databases"}, {Label: "Jobs", Path: "/aws/glue/jobs"}, {Label: "Crawlers", Path: "/aws/glue/crawlers"}}}},
		{providers.Kinesis != nil, ServiceDescriptor{ID: "kinesis", Label: "Kinesis", Category: "Analytics", RootPath: "/aws/kinesis/streams", Tier: TierFull, Children: []ServiceChild{{Label: "Data Streams", Path: "/aws/kinesis/streams"}}}},
		{providers.Firehose != nil, ServiceDescriptor{ID: "firehose", Label: "Firehose", Category: "Analytics", RootPath: "/aws/firehose/streams", Tier: TierMetadata, Note: "CRUD only", Children: []ServiceChild{{Label: "Delivery Streams", Path: "/aws/firehose/streams"}}}},
		{providers.Queue != nil, ServiceDescriptor{ID: "sqs", Label: "SQS", Category: "Application Integration", RootPath: "/aws/sqs", Tier: TierFull, Children: []ServiceChild{{Label: "Queues", Path: "/aws/sqs"}}}},
		{providers.Notif != nil, ServiceDescriptor{ID: "sns", Label: "SNS", Category: "Application Integration", RootPath: "/aws/sns", Tier: TierFull, Children: []ServiceChild{{Label: "Topics", Path: "/aws/sns"}}}},
		{providers.Events != nil, ServiceDescriptor{ID: "eventbridge", Label: "EventBridge", Category: "Application Integration", RootPath: "/aws/eventbridge/rules", Tier: TierFull, Children: []ServiceChild{{Label: "Rules", Path: "/aws/eventbridge/rules"}, {Label: "Event Buses", Path: "/aws/eventbridge/buses"}}}},
		{providers.Sfn != nil, ServiceDescriptor{ID: "sfn", Label: "Step Functions", Category: "Application Integration", RootPath: "/aws/sfn/state-machines", Tier: TierFull, Children: []ServiceChild{{Label: "State Machines", Path: "/aws/sfn/state-machines"}}}},
		{providers.CW != nil, ServiceDescriptor{ID: "cloudwatch", Label: "CloudWatch", Category: "Management & Governance", RootPath: "/aws/cloudwatch/metrics", Tier: TierFull, Children: []ServiceChild{{Label: "Metrics", Path: "/aws/cloudwatch/metrics"}, {Label: "Alarms", Path: "/aws/cloudwatch/alarms"}, {Label: "Dashboards", Path: "/aws/cloudwatch/dashboards"}}}},
		{providers.Logs != nil, ServiceDescriptor{ID: "logs", Label: "CloudWatch Logs", Category: "Management & Governance", RootPath: "/aws/logs/groups", Tier: TierFull, Children: []ServiceChild{{Label: "Log Groups", Path: "/aws/logs/groups"}, {Label: "Insights", Path: "/aws/logs/insights"}}}},
		{providers.Stack != nil, ServiceDescriptor{ID: "cloudformation", Label: "CloudFormation", Category: "Management & Governance", RootPath: "/aws/cloudformation/stacks", Tier: TierFull, Children: []ServiceChild{{Label: "Stacks", Path: "/aws/cloudformation/stacks"}}}},
		{providers.Param != nil, ServiceDescriptor{ID: "ssm", Label: "SSM", Category: "Management & Governance", RootPath: "/aws/ssm", Tier: TierFull, Children: []ServiceChild{{Label: "Parameters", Path: "/aws/ssm"}}}},
		{providers.IAM != nil, ServiceDescriptor{ID: "iam", Label: "IAM", Category: "Security, Identity & Compliance", RootPath: "/aws/iam", Tier: TierFull, Children: []ServiceChild{{Label: "Roles", Path: "/aws/iam"}}}},
		{providers.Key != nil, ServiceDescriptor{ID: "kms", Label: "KMS", Category: "Security, Identity & Compliance", RootPath: "/aws/kms", Tier: TierFull, Children: []ServiceChild{{Label: "Keys", Path: "/aws/kms"}}}},
		{providers.Secret != nil, ServiceDescriptor{ID: "secretsmanager", Label: "Secrets Manager", Category: "Security, Identity & Compliance", RootPath: "/aws/secretsmanager", Tier: TierFull, Children: []ServiceChild{{Label: "Secrets", Path: "/aws/secretsmanager"}}}},
		{providers.SES != nil, ServiceDescriptor{ID: "ses", Label: "SES", Category: "Business Applications", RootPath: "/aws/ses/identities", Tier: TierShape, Note: "limited operations", Children: []ServiceChild{{Label: "Identities", Path: "/aws/ses/identities"}}}},
	}

	services := make([]ServiceDescriptor, 0, len(descriptors))
	for _, d := range descriptors {
		if d.wired {
			services = append(services, d.service)
		}
	}
	return services
}
