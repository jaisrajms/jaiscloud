package ui

import (
	"context"

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

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// The console is a client like any other: when a plugin overrides a service, the
// console must show and change what the plugin serves, so every console call that
// is a registry action goes through Registry.Dispatch with origin "ui". Calls that
// are not registry actions (console-only helpers such as SQS PeekMessages) stay on
// the built-in.

// uiCaller dispatches console calls. A nil registry (unit tests) calls directly.
type uiCaller struct{ reg *provider.Registry }

func (c uiCaller) call(ctx context.Context, key string, direct func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error), nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if c.reg == nil || !c.reg.Has(key) {
		return direct(ctx, nr)
	}
	nr.Origin = model.OriginUI
	return c.reg.Dispatch(ctx, key, nr)
}

// routedQueue serves the sqs console through the registry (origin "ui"), so a plugin that
// overrides Queue also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedQueue struct {
	*queue.QueueProvider
	c uiCaller
}

func (p routedQueue) CreateQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.CreateQueue", p.QueueProvider.CreateQueue, nr)
}

func (p routedQueue) DeleteQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.DeleteQueue", p.QueueProvider.DeleteQueue, nr)
}

func (p routedQueue) ListQueues(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.ListQueues", p.QueueProvider.ListQueues, nr)
}

func (p routedQueue) GetQueueUrl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.GetQueueUrl", p.QueueProvider.GetQueueUrl, nr)
}

func (p routedQueue) GetQueueAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.GetQueueAttributes", p.QueueProvider.GetQueueAttributes, nr)
}

func (p routedQueue) PurgeQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.PurgeQueue", p.QueueProvider.PurgeQueue, nr)
}

func (p routedQueue) SendMessage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.SendMessage", p.QueueProvider.SendMessage, nr)
}

func (p routedQueue) ReceiveMessage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.ReceiveMessage", p.QueueProvider.ReceiveMessage, nr)
}

func (p routedQueue) DeleteMessage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.DeleteMessage", p.QueueProvider.DeleteMessage, nr)
}

func (p routedQueue) ListDeadLetterSourceQueues(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.ListDeadLetterSourceQueues", p.QueueProvider.ListDeadLetterSourceQueues, nr)
}

func (p routedQueue) ListQueueTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.ListQueueTags", p.QueueProvider.ListQueueTags, nr)
}

func (p routedQueue) TagQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.TagQueue", p.QueueProvider.TagQueue, nr)
}

func (p routedQueue) UntagQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.UntagQueue", p.QueueProvider.UntagQueue, nr)
}

func (p routedQueue) PeekMessages(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Queue.PeekMessages", p.QueueProvider.PeekMessages, nr)
}

func routeQueue(p *queue.QueueProvider, c uiCaller) sqsui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedQueue{p, c}
}

// routedObject serves the s3 console through the registry (origin "ui"), so a plugin that
// overrides Object also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedObject struct {
	*object.ObjectProvider
	c uiCaller
}

func (p routedObject) ListBuckets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.ListBuckets", p.ObjectProvider.ListBuckets, nr)
}

func (p routedObject) CreateBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.CreateBucket", p.ObjectProvider.CreateBucket, nr)
}

func (p routedObject) DeleteBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.DeleteBucket", p.ObjectProvider.DeleteBucket, nr)
}

func (p routedObject) ListObjectsV2(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.ListObjectsV2", p.ObjectProvider.ListObjectsV2, nr)
}

func (p routedObject) HeadObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.HeadObject", p.ObjectProvider.HeadObject, nr)
}

func (p routedObject) GetObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.GetObject", p.ObjectProvider.GetObject, nr)
}

func (p routedObject) PutObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.PutObject", p.ObjectProvider.PutObject, nr)
}

func (p routedObject) DeleteObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.DeleteObject", p.ObjectProvider.DeleteObject, nr)
}

func (p routedObject) DeleteObjects(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.DeleteObjects", p.ObjectProvider.DeleteObjects, nr)
}

func (p routedObject) CopyObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.CopyObject", p.ObjectProvider.CopyObject, nr)
}

func (p routedObject) GetBucketVersioning(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.GetBucketVersioning", p.ObjectProvider.GetBucketVersioning, nr)
}

func (p routedObject) PutBucketVersioning(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.PutBucketVersioning", p.ObjectProvider.PutBucketVersioning, nr)
}

func (p routedObject) ListObjectVersions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.ListObjectVersions", p.ObjectProvider.ListObjectVersions, nr)
}

func (p routedObject) ListMultipartUploads(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.ListMultipartUploads", p.ObjectProvider.ListMultipartUploads, nr)
}

func (p routedObject) AbortMultipartUpload(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.AbortMultipartUpload", p.ObjectProvider.AbortMultipartUpload, nr)
}

func (p routedObject) GetBucketTagging(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.GetBucketTagging", p.ObjectProvider.GetBucketTagging, nr)
}

func (p routedObject) PutBucketTagging(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.PutBucketTagging", p.ObjectProvider.PutBucketTagging, nr)
}

func (p routedObject) HeadBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Object.HeadBucket", p.ObjectProvider.HeadBucket, nr)
}

func routeObject(p *object.ObjectProvider, c uiCaller) s3ui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedObject{p, c}
}

// routedTable serves the dynamodb console through the registry (origin "ui"), so a plugin that
// overrides Table also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedTable struct {
	*table.TableProvider
	c uiCaller
}

func (p routedTable) ListTables(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Table.ListTables", p.TableProvider.ListTables, nr)
}

func (p routedTable) CreateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Table.CreateTable", p.TableProvider.CreateTable, nr)
}

func (p routedTable) DescribeTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Table.DescribeTable", p.TableProvider.DescribeTable, nr)
}

func (p routedTable) DeleteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Table.DeleteTable", p.TableProvider.DeleteTable, nr)
}

func (p routedTable) Scan(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Table.Scan", p.TableProvider.Scan, nr)
}

func (p routedTable) Query(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Table.Query", p.TableProvider.Query, nr)
}

func (p routedTable) PutItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Table.PutItem", p.TableProvider.PutItem, nr)
}

func (p routedTable) GetItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Table.GetItem", p.TableProvider.GetItem, nr)
}

func (p routedTable) DeleteItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Table.DeleteItem", p.TableProvider.DeleteItem, nr)
}

func (p routedTable) DescribeTimeToLive(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Table.DescribeTimeToLive", p.TableProvider.DescribeTimeToLive, nr)
}

func (p routedTable) ListTagsOfResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Table.ListTagsOfResource", p.TableProvider.ListTagsOfResource, nr)
}

func routeTable(p *table.TableProvider, c uiCaller) dynamodbui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedTable{p, c}
}

// routedFunction serves the lambda console through the registry (origin "ui"), so a plugin that
// overrides Function also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedFunction struct {
	*lambda.FunctionProvider
	c uiCaller
}

func (p routedFunction) ListFunctions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Function.ListFunctions", p.FunctionProvider.ListFunctions, nr)
}

func (p routedFunction) GetFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Function.GetFunction", p.FunctionProvider.GetFunction, nr)
}

func (p routedFunction) DeleteFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Function.DeleteFunction", p.FunctionProvider.DeleteFunction, nr)
}

func (p routedFunction) InvokeFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Function.InvokeFunction", p.FunctionProvider.InvokeFunction, nr)
}

func (p routedFunction) UpdateFunctionConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Function.UpdateFunctionConfiguration", p.FunctionProvider.UpdateFunctionConfiguration, nr)
}

func (p routedFunction) GetFunctionConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Function.GetFunctionConfiguration", p.FunctionProvider.GetFunctionConfiguration, nr)
}

func routeFunction(p *lambda.FunctionProvider, c uiCaller) lambdaui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedFunction{p, c}
}

// routedLogs serves the logs console through the registry (origin "ui"), so a plugin that
// overrides CloudWatchLogs also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedLogs struct {
	*cwlogs.Provider
	c uiCaller
}

func (p routedLogs) CreateLogGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatchLogs.CreateLogGroup", p.Provider.CreateLogGroup, nr)
}

func (p routedLogs) DeleteLogGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatchLogs.DeleteLogGroup", p.Provider.DeleteLogGroup, nr)
}

func (p routedLogs) DescribeLogGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatchLogs.DescribeLogGroups", p.Provider.DescribeLogGroups, nr)
}

func (p routedLogs) DescribeLogStreams(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatchLogs.DescribeLogStreams", p.Provider.DescribeLogStreams, nr)
}

func (p routedLogs) GetLogEvents(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatchLogs.GetLogEvents", p.Provider.GetLogEvents, nr)
}

func (p routedLogs) FilterLogEvents(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatchLogs.FilterLogEvents", p.Provider.FilterLogEvents, nr)
}

func (p routedLogs) PutRetentionPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatchLogs.PutRetentionPolicy", p.Provider.PutRetentionPolicy, nr)
}

func (p routedLogs) StartQuery(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatchLogs.StartQuery", p.Provider.StartQuery, nr)
}

func (p routedLogs) GetQueryResults(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatchLogs.GetQueryResults", p.Provider.GetQueryResults, nr)
}

func (p routedLogs) StopQuery(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatchLogs.StopQuery", p.Provider.StopQuery, nr)
}

func routeLogs(p *cwlogs.Provider, c uiCaller) logsui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedLogs{p, c}
}

// routedCW serves the cloudwatch console through the registry (origin "ui"), so a plugin that
// overrides CloudWatch also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedCW struct {
	*cloudwatch.Provider
	c uiCaller
}

func (p routedCW) ListMetrics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatch.ListMetrics", p.Provider.ListMetrics, nr)
}

func (p routedCW) GetMetricStatistics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatch.GetMetricStatistics", p.Provider.GetMetricStatistics, nr)
}

func (p routedCW) PutMetricAlarm(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatch.PutMetricAlarm", p.Provider.PutMetricAlarm, nr)
}

func (p routedCW) DescribeAlarms(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatch.DescribeAlarms", p.Provider.DescribeAlarms, nr)
}

func (p routedCW) DeleteAlarms(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatch.DeleteAlarms", p.Provider.DeleteAlarms, nr)
}

func (p routedCW) SetAlarmState(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatch.SetAlarmState", p.Provider.SetAlarmState, nr)
}

func (p routedCW) EnableAlarmActions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatch.EnableAlarmActions", p.Provider.EnableAlarmActions, nr)
}

func (p routedCW) DisableAlarmActions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatch.DisableAlarmActions", p.Provider.DisableAlarmActions, nr)
}

func (p routedCW) PutDashboard(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatch.PutDashboard", p.Provider.PutDashboard, nr)
}

func (p routedCW) GetDashboard(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatch.GetDashboard", p.Provider.GetDashboard, nr)
}

func (p routedCW) ListDashboards(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatch.ListDashboards", p.Provider.ListDashboards, nr)
}

func (p routedCW) DeleteDashboards(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudWatch.DeleteDashboards", p.Provider.DeleteDashboards, nr)
}

func routeCW(p *cloudwatch.Provider, c uiCaller) cloudwatchui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedCW{p, c}
}

// routedNotif serves the sns console through the registry (origin "ui"), so a plugin that
// overrides Notification also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedNotif struct {
	*notification.SNSProvider
	c uiCaller
}

func (p routedNotif) ListTopics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Notification.ListTopics", p.SNSProvider.ListTopics, nr)
}

func (p routedNotif) CreateTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Notification.CreateTopic", p.SNSProvider.CreateTopic, nr)
}

func (p routedNotif) DeleteTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Notification.DeleteTopic", p.SNSProvider.DeleteTopic, nr)
}

func (p routedNotif) GetTopicAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Notification.GetTopicAttributes", p.SNSProvider.GetTopicAttributes, nr)
}

func (p routedNotif) Subscribe(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Notification.Subscribe", p.SNSProvider.Subscribe, nr)
}

func (p routedNotif) Unsubscribe(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Notification.Unsubscribe", p.SNSProvider.Unsubscribe, nr)
}

func (p routedNotif) ListSubscriptionsByTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Notification.ListSubscriptionsByTopic", p.SNSProvider.ListSubscriptionsByTopic, nr)
}

func (p routedNotif) ListSubscriptions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Notification.ListSubscriptions", p.SNSProvider.ListSubscriptions, nr)
}

func (p routedNotif) Publish(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Notification.Publish", p.SNSProvider.Publish, nr)
}

func (p routedNotif) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Notification.ListTagsForResource", p.SNSProvider.ListTagsForResource, nr)
}

func routeNotif(p *notification.SNSProvider, c uiCaller) snsui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedNotif{p, c}
}

// routedIAM serves the iam console through the registry (origin "ui"), so a plugin that
// overrides IAM also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedIAM struct {
	*iam.IAMProvider
	c uiCaller
}

func (p routedIAM) ListRoles(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ListRoles", p.IAMProvider.ListRoles, nr)
}

func (p routedIAM) CreateRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.CreateRole", p.IAMProvider.CreateRole, nr)
}

func (p routedIAM) GetRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.GetRole", p.IAMProvider.GetRole, nr)
}

func (p routedIAM) DeleteRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.DeleteRole", p.IAMProvider.DeleteRole, nr)
}

func (p routedIAM) AttachRolePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.AttachRolePolicy", p.IAMProvider.AttachRolePolicy, nr)
}

func (p routedIAM) DetachRolePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.DetachRolePolicy", p.IAMProvider.DetachRolePolicy, nr)
}

func (p routedIAM) ListAttachedRolePolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ListAttachedRolePolicies", p.IAMProvider.ListAttachedRolePolicies, nr)
}

func (p routedIAM) ListUsers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ListUsers", p.IAMProvider.ListUsers, nr)
}

func (p routedIAM) CreateUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.CreateUser", p.IAMProvider.CreateUser, nr)
}

func (p routedIAM) GetUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.GetUser", p.IAMProvider.GetUser, nr)
}

func (p routedIAM) DeleteUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.DeleteUser", p.IAMProvider.DeleteUser, nr)
}

func (p routedIAM) ListAccessKeys(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ListAccessKeys", p.IAMProvider.ListAccessKeys, nr)
}

func (p routedIAM) CreateAccessKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.CreateAccessKey", p.IAMProvider.CreateAccessKey, nr)
}

func (p routedIAM) DeleteAccessKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.DeleteAccessKey", p.IAMProvider.DeleteAccessKey, nr)
}

func (p routedIAM) ListPolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.ListPolicies", p.IAMProvider.ListPolicies, nr)
}

func (p routedIAM) CreatePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.CreatePolicy", p.IAMProvider.CreatePolicy, nr)
}

func (p routedIAM) GetPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.GetPolicy", p.IAMProvider.GetPolicy, nr)
}

func (p routedIAM) DeletePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "IAM.DeletePolicy", p.IAMProvider.DeletePolicy, nr)
}

func routeIAM(p *iam.IAMProvider, c uiCaller) iamui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedIAM{p, c}
}

// routedKey serves the kms console through the registry (origin "ui"), so a plugin that
// overrides Key also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedKey struct {
	*key.KeyProvider
	c uiCaller
}

func (p routedKey) ListKeys(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Key.ListKeys", p.KeyProvider.ListKeys, nr)
}

func (p routedKey) CreateKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Key.CreateKey", p.KeyProvider.CreateKey, nr)
}

func (p routedKey) DescribeKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Key.DescribeKey", p.KeyProvider.DescribeKey, nr)
}

func (p routedKey) EnableKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Key.EnableKey", p.KeyProvider.EnableKey, nr)
}

func (p routedKey) DisableKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Key.DisableKey", p.KeyProvider.DisableKey, nr)
}

func (p routedKey) ScheduleKeyDeletion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Key.ScheduleKeyDeletion", p.KeyProvider.ScheduleKeyDeletion, nr)
}

func (p routedKey) CancelKeyDeletion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Key.CancelKeyDeletion", p.KeyProvider.CancelKeyDeletion, nr)
}

func (p routedKey) ListAliases(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Key.ListAliases", p.KeyProvider.ListAliases, nr)
}

func (p routedKey) CreateAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Key.CreateAlias", p.KeyProvider.CreateAlias, nr)
}

func (p routedKey) DeleteAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Key.DeleteAlias", p.KeyProvider.DeleteAlias, nr)
}

func (p routedKey) ListResourceTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Key.ListResourceTags", p.KeyProvider.ListResourceTags, nr)
}

func routeKey(p *key.KeyProvider, c uiCaller) kmsui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedKey{p, c}
}

// routedSecret serves the secretsmanager console through the registry (origin "ui"), so a plugin that
// overrides Secret also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedSecret struct {
	*secret.SecretProvider
	c uiCaller
}

func (p routedSecret) ListSecrets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.ListSecrets", p.SecretProvider.ListSecrets, nr)
}

func (p routedSecret) CreateSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.CreateSecret", p.SecretProvider.CreateSecret, nr)
}

func (p routedSecret) DescribeSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.DescribeSecret", p.SecretProvider.DescribeSecret, nr)
}

func (p routedSecret) GetSecretValue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.GetSecretValue", p.SecretProvider.GetSecretValue, nr)
}

func (p routedSecret) PutSecretValue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.PutSecretValue", p.SecretProvider.PutSecretValue, nr)
}

func (p routedSecret) UpdateSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.UpdateSecret", p.SecretProvider.UpdateSecret, nr)
}

func (p routedSecret) DeleteSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.DeleteSecret", p.SecretProvider.DeleteSecret, nr)
}

func (p routedSecret) RestoreSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.RestoreSecret", p.SecretProvider.RestoreSecret, nr)
}

func (p routedSecret) ListSecretVersionIds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.ListSecretVersionIds", p.SecretProvider.ListSecretVersionIds, nr)
}

func (p routedSecret) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Secret.TagResource", p.SecretProvider.TagResource, nr)
}

func routeSecret(p *secret.SecretProvider, c uiCaller) secretsmanagerui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedSecret{p, c}
}

// routedParam serves the ssm console through the registry (origin "ui"), so a plugin that
// overrides Parameter also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedParam struct {
	*parameter.ParameterProvider
	c uiCaller
}

func (p routedParam) DescribeParameters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Parameter.DescribeParameters", p.ParameterProvider.DescribeParameters, nr)
}

func (p routedParam) GetParameter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Parameter.GetParameter", p.ParameterProvider.GetParameter, nr)
}

func (p routedParam) GetParametersByPath(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Parameter.GetParametersByPath", p.ParameterProvider.GetParametersByPath, nr)
}

func (p routedParam) PutParameter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Parameter.PutParameter", p.ParameterProvider.PutParameter, nr)
}

func (p routedParam) DeleteParameter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Parameter.DeleteParameter", p.ParameterProvider.DeleteParameter, nr)
}

func (p routedParam) GetParameterHistory(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Parameter.GetParameterHistory", p.ParameterProvider.GetParameterHistory, nr)
}

func (p routedParam) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Parameter.ListTagsForResource", p.ParameterProvider.ListTagsForResource, nr)
}

func routeParam(p *parameter.ParameterProvider, c uiCaller) ssmui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedParam{p, c}
}

// routedAPIGW serves the apigw console through the registry (origin "ui"), so a plugin that
// overrides Gateway also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedAPIGW struct {
	*apigw.GatewayProvider
	c uiCaller
}

func (p routedAPIGW) GetRestApis(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Gateway.GetRestApis", p.GatewayProvider.GetRestApis, nr)
}

func (p routedAPIGW) CreateRestApi(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Gateway.CreateRestApi", p.GatewayProvider.CreateRestApi, nr)
}

func (p routedAPIGW) DeleteRestApi(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Gateway.DeleteRestApi", p.GatewayProvider.DeleteRestApi, nr)
}

func (p routedAPIGW) GetResources(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Gateway.GetResources", p.GatewayProvider.GetResources, nr)
}

func (p routedAPIGW) GetDeployments(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Gateway.GetDeployments", p.GatewayProvider.GetDeployments, nr)
}

func (p routedAPIGW) CreateDeployment(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Gateway.CreateDeployment", p.GatewayProvider.CreateDeployment, nr)
}

func (p routedAPIGW) GetStages(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Gateway.GetStages", p.GatewayProvider.GetStages, nr)
}

func routeAPIGW(p *apigw.GatewayProvider, c uiCaller) apigwui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedAPIGW{p, c}
}

// routedCatalog serves the glue console through the registry (origin "ui"), so a plugin that
// overrides Glue also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedCatalog struct {
	*catalog.GlueProvider
	c uiCaller
}

func (p routedCatalog) GetDatabases(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.GetDatabases", p.GlueProvider.GetDatabases, nr)
}

func (p routedCatalog) CreateDatabase(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.CreateDatabase", p.GlueProvider.CreateDatabase, nr)
}

func (p routedCatalog) DeleteDatabase(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.DeleteDatabase", p.GlueProvider.DeleteDatabase, nr)
}

func (p routedCatalog) GetTables(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.GetTables", p.GlueProvider.GetTables, nr)
}

func (p routedCatalog) CreateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.CreateTable", p.GlueProvider.CreateTable, nr)
}

func (p routedCatalog) DeleteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.DeleteTable", p.GlueProvider.DeleteTable, nr)
}

func (p routedCatalog) GetJobs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.GetJobs", p.GlueProvider.GetJobs, nr)
}

func (p routedCatalog) CreateJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.CreateJob", p.GlueProvider.CreateJob, nr)
}

func (p routedCatalog) DeleteJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.DeleteJob", p.GlueProvider.DeleteJob, nr)
}

func (p routedCatalog) StartJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.StartJobRun", p.GlueProvider.StartJobRun, nr)
}

func (p routedCatalog) GetJobRuns(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.GetJobRuns", p.GlueProvider.GetJobRuns, nr)
}

func (p routedCatalog) GetCrawlers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.GetCrawlers", p.GlueProvider.GetCrawlers, nr)
}

func (p routedCatalog) CreateCrawler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.CreateCrawler", p.GlueProvider.CreateCrawler, nr)
}

func (p routedCatalog) DeleteCrawler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.DeleteCrawler", p.GlueProvider.DeleteCrawler, nr)
}

func (p routedCatalog) StartCrawler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Glue.StartCrawler", p.GlueProvider.StartCrawler, nr)
}

func routeCatalog(p *catalog.GlueProvider, c uiCaller) glueui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedCatalog{p, c}
}

// routedEMR serves the emr console through the registry (origin "ui"), so a plugin that
// overrides EMR also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedEMR struct {
	*emr.EMRProvider
	c uiCaller
}

func (p routedEMR) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMR.ListClusters", p.EMRProvider.ListClusters, nr)
}

func (p routedEMR) DescribeCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMR.DescribeCluster", p.EMRProvider.DescribeCluster, nr)
}

func (p routedEMR) RunJobFlow(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMR.RunJobFlow", p.EMRProvider.RunJobFlow, nr)
}

func (p routedEMR) TerminateJobFlows(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMR.TerminateJobFlows", p.EMRProvider.TerminateJobFlows, nr)
}

func (p routedEMR) ListSteps(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMR.ListSteps", p.EMRProvider.ListSteps, nr)
}

func (p routedEMR) DescribeStep(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMR.DescribeStep", p.EMRProvider.DescribeStep, nr)
}

func (p routedEMR) AddJobFlowSteps(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMR.AddJobFlowSteps", p.EMRProvider.AddJobFlowSteps, nr)
}

func (p routedEMR) CancelSteps(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMR.CancelSteps", p.EMRProvider.CancelSteps, nr)
}

func routeEMR(p *emr.EMRProvider, c uiCaller) emrui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedEMR{p, c}
}

// routedEMRC serves the emroneks console through the registry (origin "ui"), so a plugin that
// overrides EMRContainers also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedEMRC struct {
	*emroneks.EMRContainersProvider
	c uiCaller
}

func (p routedEMRC) ListVirtualClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMRContainers.ListVirtualClusters", p.EMRContainersProvider.ListVirtualClusters, nr)
}

func (p routedEMRC) DescribeVirtualCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMRContainers.DescribeVirtualCluster", p.EMRContainersProvider.DescribeVirtualCluster, nr)
}

func (p routedEMRC) CreateVirtualCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMRContainers.CreateVirtualCluster", p.EMRContainersProvider.CreateVirtualCluster, nr)
}

func (p routedEMRC) DeleteVirtualCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMRContainers.DeleteVirtualCluster", p.EMRContainersProvider.DeleteVirtualCluster, nr)
}

func (p routedEMRC) ListJobRuns(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMRContainers.ListJobRuns", p.EMRContainersProvider.ListJobRuns, nr)
}

func (p routedEMRC) DescribeJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMRContainers.DescribeJobRun", p.EMRContainersProvider.DescribeJobRun, nr)
}

func (p routedEMRC) StartJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMRContainers.StartJobRun", p.EMRContainersProvider.StartJobRun, nr)
}

func (p routedEMRC) CancelJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EMRContainers.CancelJobRun", p.EMRContainersProvider.CancelJobRun, nr)
}

func routeEMRC(p *emroneks.EMRContainersProvider, c uiCaller) emroneksui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedEMRC{p, c}
}

// routedEvents serves the eventbridge console through the registry (origin "ui"), so a plugin that
// overrides EventBridge also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedEvents struct {
	*events.EventBridgeProvider
	c uiCaller
}

func (p routedEvents) ListRules(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EventBridge.ListRules", p.EventBridgeProvider.ListRules, nr)
}

func (p routedEvents) PutRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EventBridge.PutRule", p.EventBridgeProvider.PutRule, nr)
}

func (p routedEvents) DeleteRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EventBridge.DeleteRule", p.EventBridgeProvider.DeleteRule, nr)
}

func (p routedEvents) EnableRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EventBridge.EnableRule", p.EventBridgeProvider.EnableRule, nr)
}

func (p routedEvents) DisableRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EventBridge.DisableRule", p.EventBridgeProvider.DisableRule, nr)
}

func (p routedEvents) ListTargetsByRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EventBridge.ListTargetsByRule", p.EventBridgeProvider.ListTargetsByRule, nr)
}

func (p routedEvents) PutTargets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EventBridge.PutTargets", p.EventBridgeProvider.PutTargets, nr)
}

func (p routedEvents) RemoveTargets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EventBridge.RemoveTargets", p.EventBridgeProvider.RemoveTargets, nr)
}

func (p routedEvents) PutEvents(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EventBridge.PutEvents", p.EventBridgeProvider.PutEvents, nr)
}

func (p routedEvents) ListEventBuses(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EventBridge.ListEventBuses", p.EventBridgeProvider.ListEventBuses, nr)
}

func (p routedEvents) CreateEventBus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EventBridge.CreateEventBus", p.EventBridgeProvider.CreateEventBus, nr)
}

func (p routedEvents) DeleteEventBus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EventBridge.DeleteEventBus", p.EventBridgeProvider.DeleteEventBus, nr)
}

func routeEvents(p *events.EventBridgeProvider, c uiCaller) eventbridgeui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedEvents{p, c}
}

// routedSfn serves the sfn console through the registry (origin "ui"), so a plugin that
// overrides StepFunctions also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedSfn struct {
	*stepfunctions.Provider
	c uiCaller
}

func (p routedSfn) ListStateMachines(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "StepFunctions.ListStateMachines", p.Provider.ListStateMachines, nr)
}

func (p routedSfn) DescribeStateMachine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "StepFunctions.DescribeStateMachine", p.Provider.DescribeStateMachine, nr)
}

func (p routedSfn) CreateStateMachine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "StepFunctions.CreateStateMachine", p.Provider.CreateStateMachine, nr)
}

func (p routedSfn) DeleteStateMachine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "StepFunctions.DeleteStateMachine", p.Provider.DeleteStateMachine, nr)
}

func (p routedSfn) StartExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "StepFunctions.StartExecution", p.Provider.StartExecution, nr)
}

func (p routedSfn) ListExecutions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "StepFunctions.ListExecutions", p.Provider.ListExecutions, nr)
}

func (p routedSfn) StopExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "StepFunctions.StopExecution", p.Provider.StopExecution, nr)
}

func (p routedSfn) GetExecutionHistory(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "StepFunctions.GetExecutionHistory", p.Provider.GetExecutionHistory, nr)
}

func routeSfn(p *stepfunctions.Provider, c uiCaller) sfnui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedSfn{p, c}
}

// routedCompute serves the ec2 console through the registry (origin "ui"), so a plugin that
// overrides Compute also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedCompute struct {
	*compute.ComputeProvider
	c uiCaller
}

func (p routedCompute) DescribeInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Compute.DescribeInstances", p.ComputeProvider.DescribeInstances, nr)
}

func (p routedCompute) TerminateInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Compute.TerminateInstances", p.ComputeProvider.TerminateInstances, nr)
}

func (p routedCompute) StartInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Compute.StartInstances", p.ComputeProvider.StartInstances, nr)
}

func (p routedCompute) StopInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Compute.StopInstances", p.ComputeProvider.StopInstances, nr)
}

func routeCompute(p *compute.ComputeProvider, c uiCaller) ec2ui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedCompute{p, c}
}

// routedContainer serves the ecs console through the registry (origin "ui"), so a plugin that
// overrides ECS also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedContainer struct {
	*container.ContainerProvider
	c uiCaller
}

func (p routedContainer) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ECS.ListClusters", p.ContainerProvider.ListClusters, nr)
}

func (p routedContainer) DescribeClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ECS.DescribeClusters", p.ContainerProvider.DescribeClusters, nr)
}

func (p routedContainer) CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ECS.CreateCluster", p.ContainerProvider.CreateCluster, nr)
}

func (p routedContainer) DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ECS.DeleteCluster", p.ContainerProvider.DeleteCluster, nr)
}

func (p routedContainer) ListServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ECS.ListServices", p.ContainerProvider.ListServices, nr)
}

func (p routedContainer) ListTasks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ECS.ListTasks", p.ContainerProvider.ListTasks, nr)
}

func (p routedContainer) RunTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ECS.RunTask", p.ContainerProvider.RunTask, nr)
}

func (p routedContainer) StopTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ECS.StopTask", p.ContainerProvider.StopTask, nr)
}

func routeContainer(p *container.ContainerProvider, c uiCaller) ecsui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedContainer{p, c}
}

// routedEKS serves the eks console through the registry (origin "ui"), so a plugin that
// overrides EKS also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedEKS struct {
	*eks.EKSProvider
	c uiCaller
}

func (p routedEKS) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EKS.ListClusters", p.EKSProvider.ListClusters, nr)
}

func (p routedEKS) CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EKS.CreateCluster", p.EKSProvider.CreateCluster, nr)
}

func (p routedEKS) DescribeCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EKS.DescribeCluster", p.EKSProvider.DescribeCluster, nr)
}

func (p routedEKS) DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "EKS.DeleteCluster", p.EKSProvider.DeleteCluster, nr)
}

func routeEKS(p *eks.EKSProvider, c uiCaller) eksui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedEKS{p, c}
}

// routedRDS serves the rds console through the registry (origin "ui"), so a plugin that
// overrides RDS also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedRDS struct {
	*rds.RelationalProvider
	c uiCaller
}

func (p routedRDS) DescribeDBInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "RDS.DescribeDBInstances", p.RelationalProvider.DescribeDBInstances, nr)
}

func (p routedRDS) CreateDBInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "RDS.CreateDBInstance", p.RelationalProvider.CreateDBInstance, nr)
}

func (p routedRDS) DeleteDBInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "RDS.DeleteDBInstance", p.RelationalProvider.DeleteDBInstance, nr)
}

func (p routedRDS) StartDBInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "RDS.StartDBInstance", p.RelationalProvider.StartDBInstance, nr)
}

func (p routedRDS) StopDBInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "RDS.StopDBInstance", p.RelationalProvider.StopDBInstance, nr)
}

func routeRDS(p *rds.RelationalProvider, c uiCaller) rdsui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedRDS{p, c}
}

// routedCache serves the elasticache console through the registry (origin "ui"), so a plugin that
// overrides ElastiCache also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedCache struct {
	*cache.CacheProvider
	c uiCaller
}

func (p routedCache) DescribeCacheClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ElastiCache.DescribeCacheClusters", p.CacheProvider.DescribeCacheClusters, nr)
}

func (p routedCache) CreateCacheCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ElastiCache.CreateCacheCluster", p.CacheProvider.CreateCacheCluster, nr)
}

func (p routedCache) DeleteCacheCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ElastiCache.DeleteCacheCluster", p.CacheProvider.DeleteCacheCluster, nr)
}

func routeCache(p *cache.CacheProvider, c uiCaller) elasticacheui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedCache{p, c}
}

// routedDNS serves the route53 console through the registry (origin "ui"), so a plugin that
// overrides DNS also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedDNS struct {
	*dns.DNSProvider
	c uiCaller
}

func (p routedDNS) ListHostedZones(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "DNS.ListHostedZones", p.DNSProvider.ListHostedZones, nr)
}

func (p routedDNS) CreateHostedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "DNS.CreateHostedZone", p.DNSProvider.CreateHostedZone, nr)
}

func (p routedDNS) DeleteHostedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "DNS.DeleteHostedZone", p.DNSProvider.DeleteHostedZone, nr)
}

func (p routedDNS) ListResourceRecordSets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "DNS.ListResourceRecordSets", p.DNSProvider.ListResourceRecordSets, nr)
}

func routeDNS(p *dns.DNSProvider, c uiCaller) route53ui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedDNS{p, c}
}

// routedStack serves the cfn console through the registry (origin "ui"), so a plugin that
// overrides CloudFormation also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedStack struct {
	*stack.StackProvider
	c uiCaller
}

func (p routedStack) ListStacks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudFormation.ListStacks", p.StackProvider.ListStacks, nr)
}

func (p routedStack) CreateStack(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudFormation.CreateStack", p.StackProvider.CreateStack, nr)
}

func (p routedStack) DescribeStacks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudFormation.DescribeStacks", p.StackProvider.DescribeStacks, nr)
}

func (p routedStack) DeleteStack(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "CloudFormation.DeleteStack", p.StackProvider.DeleteStack, nr)
}

func routeStack(p *stack.StackProvider, c uiCaller) cfnui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedStack{p, c}
}

// routedKinesis serves the kinesis console through the registry (origin "ui"), so a plugin that
// overrides Kinesis also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedKinesis struct {
	*kinesis.Provider
	c uiCaller
}

func (p routedKinesis) ListStreams(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Kinesis.ListStreams", p.Provider.ListStreams, nr)
}

func (p routedKinesis) CreateStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Kinesis.CreateStream", p.Provider.CreateStream, nr)
}

func (p routedKinesis) DeleteStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Kinesis.DeleteStream", p.Provider.DeleteStream, nr)
}

func (p routedKinesis) DescribeStreamSummary(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Kinesis.DescribeStreamSummary", p.Provider.DescribeStreamSummary, nr)
}

func routeKinesis(p *kinesis.Provider, c uiCaller) kinesisui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedKinesis{p, c}
}

// routedFirehose serves the firehose console through the registry (origin "ui"), so a plugin that
// overrides Firehose also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedFirehose struct {
	*firehose.Provider
	c uiCaller
}

func (p routedFirehose) ListDeliveryStreams(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firehose.ListDeliveryStreams", p.Provider.ListDeliveryStreams, nr)
}

func (p routedFirehose) CreateDeliveryStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firehose.CreateDeliveryStream", p.Provider.CreateDeliveryStream, nr)
}

func (p routedFirehose) DeleteDeliveryStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firehose.DeleteDeliveryStream", p.Provider.DeleteDeliveryStream, nr)
}

func (p routedFirehose) DescribeDeliveryStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "Firehose.DescribeDeliveryStream", p.Provider.DescribeDeliveryStream, nr)
}

func routeFirehose(p *firehose.Provider, c uiCaller) firehoseui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedFirehose{p, c}
}

// routedSES serves the ses console through the registry (origin "ui"), so a plugin that
// overrides SES also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedSES struct {
	*ses.Provider
	c uiCaller
}

func (p routedSES) ListIdentities(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "SES.ListIdentities", p.Provider.ListIdentities, nr)
}

func (p routedSES) VerifyEmailIdentity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "SES.VerifyEmailIdentity", p.Provider.VerifyEmailIdentity, nr)
}

func (p routedSES) DeleteIdentity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "SES.DeleteIdentity", p.Provider.DeleteIdentity, nr)
}

func (p routedSES) GetSendQuota(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "SES.GetSendQuota", p.Provider.GetSendQuota, nr)
}

func routeSES(p *ses.Provider, c uiCaller) sesui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedSES{p, c}
}

// routedELBv2 serves the elbv2 console through the registry (origin "ui"), so a plugin that
// overrides ELBv2 also answers the console. Methods that are not registry actions
// stay on the embedded built-in.
type routedELBv2 struct {
	*elbv2.ELBv2Provider
	c uiCaller
}

func (p routedELBv2) DescribeLoadBalancers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ELBv2.DescribeLoadBalancers", p.ELBv2Provider.DescribeLoadBalancers, nr)
}

func (p routedELBv2) CreateLoadBalancer(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ELBv2.CreateLoadBalancer", p.ELBv2Provider.CreateLoadBalancer, nr)
}

func (p routedELBv2) DeleteLoadBalancer(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ELBv2.DeleteLoadBalancer", p.ELBv2Provider.DeleteLoadBalancer, nr)
}

func (p routedELBv2) DescribeTargetGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.c.call(ctx, "ELBv2.DescribeTargetGroups", p.ELBv2Provider.DescribeTargetGroups, nr)
}

func routeELBv2(p *elbv2.ELBv2Provider, c uiCaller) elbv2ui.ProviderInterface {
	if p == nil || c.reg == nil {
		return p
	}
	return routedELBv2{p, c}
}
