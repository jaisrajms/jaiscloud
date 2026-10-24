package ui

import (
	"github.com/go-chi/chi/v5"

	apigwui "jaiscloud/internal/aws/ui/apigw"
	cfnui "jaiscloud/internal/aws/ui/cfn"
	cloudwatchui "jaiscloud/internal/aws/ui/cloudwatch"
	dynamodbui "jaiscloud/internal/aws/ui/dynamodb"
	ec2ui "jaiscloud/internal/aws/ui/ec2"
	ecsui "jaiscloud/internal/aws/ui/ecs"
	eksui "jaiscloud/internal/aws/ui/eks"
	elasticacheui "jaiscloud/internal/aws/ui/elasticache"
	elbv2ui "jaiscloud/internal/aws/ui/elbv2"
	emrui "jaiscloud/internal/aws/ui/emr"
	emroneksui "jaiscloud/internal/aws/ui/emroneks"
	eventbridgeui "jaiscloud/internal/aws/ui/eventbridge"
	firehoseui "jaiscloud/internal/aws/ui/firehose"
	glueui "jaiscloud/internal/aws/ui/glue"
	iamui "jaiscloud/internal/aws/ui/iam"
	kinesisui "jaiscloud/internal/aws/ui/kinesis"
	kmsui "jaiscloud/internal/aws/ui/kms"
	lambdaui "jaiscloud/internal/aws/ui/lambda"
	logsui "jaiscloud/internal/aws/ui/logs"
	rdsui "jaiscloud/internal/aws/ui/rds"
	route53ui "jaiscloud/internal/aws/ui/route53"
	s3ui "jaiscloud/internal/aws/ui/s3"
	secretsmanagerui "jaiscloud/internal/aws/ui/secretsmanager"
	sesui "jaiscloud/internal/aws/ui/ses"
	sfnui "jaiscloud/internal/aws/ui/sfn"
	snsui "jaiscloud/internal/aws/ui/sns"
	sqsui "jaiscloud/internal/aws/ui/sqs"
	ssmui "jaiscloud/internal/aws/ui/ssm"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
	coreui "jaiscloud/internal/ui"
)

// Registrar contributes the AWS service catalog and API routes to the shared
// UI core. It is the AWS implementation of coreui.Registrar.
type Registrar struct {
	providers *AWSProviders
	cfg       *config.Config
}

// NewRegistrar returns a registrar for the supplied AWS providers.
func NewRegistrar(providers *AWSProviders, cfg *config.Config) *Registrar {
	return &Registrar{providers: providers, cfg: cfg}
}

// Cloud implements coreui.Registrar.
func (r *Registrar) Cloud() model.Cloud { return model.CloudAWS }

// Services implements coreui.Registrar.
func (r *Registrar) Services() []coreui.ServiceDescriptor {
	return awsServiceDescriptors(r.providers)
}

// MountRoutes implements coreui.Registrar. Mounted inside the auth group, so
// the admin panel and every service router are only reachable with a session.
func (r *Registrar) MountRoutes(router chi.Router) {
	providers := r.providers
	cfg := r.cfg

	// The cloud-neutral admin panel (/api/ui/v1/admin) is mounted by the shared
	// UI core, not here — every cloud exposes the same admin surface.

	// Phase 0 service routes
	if providers.Queue != nil {
		router.Mount("/api/ui/v1/sqs", sqsui.BuildRouter(providers.Queue, cfg))
	}
	if providers.Function != nil {
		router.Mount("/api/ui/v1/lambda", lambdaui.BuildRouter(providers.Function, cfg))
	}
	if providers.Logs != nil {
		router.Mount("/api/ui/v1/logs", logsui.BuildRouter(providers.Logs, cfg))
	}

	// Phase 1a service routes
	if providers.Object != nil {
		router.Mount("/api/ui/v1/s3", s3ui.BuildRouter(providers.Object, cfg))
	}
	if providers.Table != nil {
		router.Mount("/api/ui/v1/dynamodb", dynamodbui.BuildRouter(providers.Table, cfg))
	}
	if providers.Notif != nil {
		router.Mount("/api/ui/v1/sns", snsui.BuildRouter(providers.Notif, cfg))
	}

	// Phase 3 service routes
	if providers.CW != nil {
		router.Mount("/api/ui/v1/cloudwatch", cloudwatchui.BuildRouter(providers.CW, cfg))
	}

	// Phase 5 service routes
	if providers.Events != nil {
		router.Mount("/api/ui/v1/eventbridge", eventbridgeui.BuildRouter(providers.Events, cfg))
	}
	if providers.APIGW != nil {
		router.Mount("/api/ui/v1/apigateway", apigwui.BuildRouter(providers.APIGW, cfg))
	}
	if providers.Sfn != nil {
		router.Mount("/api/ui/v1/sfn", sfnui.BuildRouter(providers.Sfn, cfg))
	}

	// Phase 4 service routes
	if providers.EMR != nil {
		router.Mount("/api/ui/v1/emr", emrui.BuildRouter(providers.EMR, cfg))
	}
	if providers.EMRC != nil {
		router.Mount("/api/ui/v1/emr-containers", emroneksui.BuildRouter(providers.EMRC, cfg))
	}
	if providers.Catalog != nil {
		router.Mount("/api/ui/v1/glue", glueui.BuildRouter(providers.Catalog, cfg))
	}

	// Phase 1b service routes
	if providers.IAM != nil {
		router.Mount("/api/ui/v1/iam", iamui.BuildRouter(providers.IAM, cfg))
	}
	if providers.Key != nil {
		router.Mount("/api/ui/v1/kms", kmsui.BuildRouter(providers.Key, cfg))
	}
	if providers.Secret != nil {
		router.Mount("/api/ui/v1/secretsmanager", secretsmanagerui.BuildRouter(providers.Secret, cfg))
	}
	if providers.Param != nil {
		router.Mount("/api/ui/v1/ssm", ssmui.BuildRouter(providers.Param, cfg))
	}

	// Phase 6 service routes
	if providers.Compute != nil {
		router.Mount("/api/ui/v1/ec2", ec2ui.BuildRouter(providers.Compute, cfg))
	}
	if providers.Container != nil {
		router.Mount("/api/ui/v1/ecs", ecsui.BuildRouter(providers.Container, cfg))
	}
	if providers.EKS != nil {
		router.Mount("/api/ui/v1/eks", eksui.BuildRouter(providers.EKS, cfg))
	}
	if providers.RDS != nil {
		router.Mount("/api/ui/v1/rds", rdsui.BuildRouter(providers.RDS, cfg))
	}
	if providers.Cache != nil {
		router.Mount("/api/ui/v1/elasticache", elasticacheui.BuildRouter(providers.Cache, cfg))
	}
	if providers.DNS != nil {
		router.Mount("/api/ui/v1/route53", route53ui.BuildRouter(providers.DNS, cfg))
	}
	if providers.Stack != nil {
		router.Mount("/api/ui/v1/cloudformation", cfnui.BuildRouter(providers.Stack, cfg))
	}

	// Phase 7 service routes
	if providers.Kinesis != nil {
		router.Mount("/api/ui/v1/kinesis", kinesisui.BuildRouter(providers.Kinesis, cfg))
	}
	if providers.Firehose != nil {
		router.Mount("/api/ui/v1/firehose", firehoseui.BuildRouter(providers.Firehose, cfg))
	}
	if providers.SES != nil {
		router.Mount("/api/ui/v1/ses", sesui.BuildRouter(providers.SES, cfg))
	}
	if providers.ELBv2 != nil {
		router.Mount("/api/ui/v1/elbv2", elbv2ui.BuildRouter(providers.ELBv2, cfg))
	}
}
