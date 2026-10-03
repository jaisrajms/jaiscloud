import * as s3 from '../api/s3'
import * as dynamodb from '../api/dynamodb'
import * as lambda from '../api/lambda'
import * as sns from '../api/sns'
import * as sqs from '../api/sqs'
import * as logs from '../api/logs'
import * as kms from '../api/kms'
import * as secretsmanager from '../api/secretsmanager'
import * as ssm from '../api/ssm'
import * as iam from '../api/iam'
import * as cloudwatch from '../api/cloudwatch'
import * as emr from '../api/emr'
import * as emroneks from '../api/emroneks'
import * as glue from '../api/glue'
import * as eventbridge from '../api/eventbridge'
import * as apigw from '../api/apigw'
import * as sfn from '../api/sfn'
import * as ec2 from '../api/ec2'
import * as ecs from '../api/ecs'
import * as eks from '../api/eks'
import * as rds from '../api/rds'
import * as elasticache from '../api/elasticache'
import * as route53 from '../api/route53'
import * as cfn from '../api/cfn'
import * as kinesis from '../api/kinesis'
import * as firehose from '../api/firehose'
import * as ses from '../api/ses'
import * as elbv2 from '../api/elbv2'

/** Map of resource `type` (as stored on a favourite) to the live ids. */
export type ResourceIdMap = Map<string, Set<string>>

const ids = <T>(items: T[], get: (item: T) => string): Set<string> =>
  new Set(items.map((item) => get(item)))

/**
 * Per-service loaders returning the ids that currently exist. A service with no
 * loader (or a failed request) returns null, and its favourites are left
 * untouched rather than being treated as missing.
 */
const LOOKUPS: Record<string, () => Promise<ResourceIdMap>> = {
  s3: async () => new Map([['bucket', ids((await s3.listBuckets()).items, (b) => b.name)]]),
  dynamodb: async () =>
    new Map([['table', ids((await dynamodb.listTables()).items, (t) => t.name)]]),
  lambda: async () =>
    new Map([['function', ids((await lambda.listFunctions()).items, (f) => f.name)]]),
  sns: async () => new Map([['topic', ids((await sns.listTopics()).items, (t) => t.arn)]]),
  sqs: async () =>
    new Map([['queue', ids((await sqs.listQueues({ pageSize: 1000 })).items, (q) => q.url)]]),
  logs: async () =>
    new Map([['log group', ids((await logs.listLogGroups({ pageSize: 1000 })).items, (g) => g.name)]]),
  kms: async () => new Map([['key', ids((await kms.listKeys()).items, (k) => k.keyId)]]),
  secretsmanager: async () =>
    new Map([['secret', ids((await secretsmanager.listSecrets()).items, (s) => s.name)]]),
  ssm: async () =>
    new Map([['parameter', ids((await ssm.listParameters()).items, (p) => p.name)]]),
  iam: async () => {
    const [roles, users, policies] = await Promise.all([
      iam.listRoles(),
      iam.listUsers(),
      iam.listPolicies(),
    ])
    return new Map([
      ['role', ids(roles.items, (r) => r.arn)],
      ['user', ids(users.items, (u) => u.arn)],
      ['policy', ids(policies.items, (p) => p.arn)],
    ])
  },
  cloudwatch: async () => {
    const [metrics, alarms, dashboards] = await Promise.all([
      cloudwatch.listMetrics(),
      cloudwatch.listAlarms(),
      cloudwatch.listDashboards(),
    ])
    return new Map([
      ['metric', ids(metrics.items, (m) => `${m.namespace}/${m.metricName}`)],
      ['alarm', ids(alarms.items, (a) => a.alarmName)],
      ['dashboard', ids(dashboards.items, (d) => d.dashboardName)],
    ])
  },
  emr: async () => new Map([['cluster', ids((await emr.listClusters()).items, (c) => c.id)]]),
  'emr-containers': async () =>
    new Map([['virtual cluster', ids((await emroneks.listVirtualClusters()).items, (v) => v.id)]]),
  glue: async () => {
    const [databases, jobs, crawlers] = await Promise.all([
      glue.listDatabases(),
      glue.listJobs(),
      glue.listCrawlers(),
    ])
    return new Map([
      ['database', ids(databases.items, (d) => d.name)],
      ['job', ids(jobs.items, (j) => j.name)],
      ['crawler', ids(crawlers.items, (c) => c.name)],
    ])
  },
  eventbridge: async () => {
    const [rules, buses] = await Promise.all([eventbridge.listRules(), eventbridge.listEventBuses()])
    return new Map([
      ['rule', ids(rules.items, (r) => r.name)],
      ['event bus', ids(buses.items, (b) => b.name)],
    ])
  },
  apigateway: async () =>
    new Map([['REST API', ids((await apigw.listRestAPIs()).items, (a) => a.id)]]),
  sfn: async () =>
    new Map([['state machine', ids((await sfn.listStateMachines()).items, (sm) => sm.arn)]]),
  ec2: async () => new Map([['instance', ids((await ec2.listInstances()).items, (i) => i.id)]]),
  ecs: async () => new Map([['cluster', ids((await ecs.listClusters()).items, (c) => c.name)]]),
  eks: async () => new Map([['cluster', ids((await eks.listClusters()).items, (c) => c.name)]]),
  rds: async () =>
    new Map([['database', ids((await rds.listInstances()).items, (i) => i.id)]]),
  elasticache: async () =>
    new Map([['cluster', ids((await elasticache.listClusters()).items, (c) => c.id)]]),
  route53: async () =>
    new Map([['hosted zone', ids((await route53.listZones()).items, (z) => z.id)]]),
  cloudformation: async () =>
    new Map([['stack', ids((await cfn.listStacks()).items, (s) => s.name)]]),
  kinesis: async () =>
    new Map([['stream', ids((await kinesis.listStreams()).items, (s) => s.name)]]),
  firehose: async () =>
    new Map([['delivery stream', ids((await firehose.listDeliveryStreams()).items, (s) => s.name)]]),
  ses: async () =>
    new Map([['identity', ids((await ses.listIdentities()).items, (i) => i.identity)]]),
  elbv2: async () =>
    new Map([['load balancer', ids((await elbv2.listLoadBalancers()).items, (lb) => lb.arn)]]),
}

/** Load the live ids for a service, or null when the service can't be verified. */
export async function lookupResourceIds(service: string): Promise<ResourceIdMap | null> {
  const loader = LOOKUPS[service]
  if (!loader) return null
  try {
    return await loader()
  } catch {
    return null
  }
}
