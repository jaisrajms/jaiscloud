package ui

import (
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
)

// AWSProviders holds all provider pointers injected from main.go.
// A nil provider means the service is not wired; its nav entry is hidden.
type AWSProviders struct {
	Queue     *queue.QueueProvider
	Object    *object.ObjectProvider
	Table     *table.TableProvider
	Function  *lambda.FunctionProvider
	Logs      *cwlogs.Provider
	CW        *cloudwatch.Provider
	Notif     *notification.SNSProvider
	IAM       *iam.IAMProvider
	Key       *key.KeyProvider
	Secret    *secret.SecretProvider
	Param     *parameter.ParameterProvider
	APIGW     *apigw.GatewayProvider
	Catalog   *catalog.GlueProvider
	EMR       *emr.EMRProvider
	EMRC      *emroneks.EMRContainersProvider
	Events    *events.EventBridgeProvider
	Sfn       *stepfunctions.Provider
	Compute   *compute.ComputeProvider
	Container *container.ContainerProvider
	EKS       *eks.EKSProvider
	RDS       *rds.RelationalProvider
	Cache     *cache.CacheProvider
	DNS       *dns.DNSProvider
	Stack     *stack.StackProvider
	Kinesis   *kinesis.Provider
	Firehose  *firehose.Provider
	SES       *ses.Provider
	ELBv2     *elbv2.ELBv2Provider
}
