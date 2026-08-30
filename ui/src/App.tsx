import { Routes, Route, Navigate } from 'react-router-dom'
import { useMeta } from './hooks/useMeta'
import { Layout } from './components/Layout'
import { AzureRoutes } from './services/azure'
import { GCPRoutes } from './services/gcp'
import { SQSRoutes } from './services/aws/sqs'
import { LambdaRoutes } from './services/aws/lambda'
import { LogsRoutes } from './services/aws/logs'
import { S3Routes } from './services/aws/s3'
import { DynamoDBRoutes } from './services/aws/dynamodb'
import { SNSRoutes } from './services/aws/sns'
import { KMSRoutes } from './services/aws/kms'
import { SecretsManagerRoutes } from './services/aws/secretsmanager'
import { SSMRoutes } from './services/aws/ssm'
import { IAMRoutes } from './services/aws/iam'
import { CloudWatchRoutes } from './services/aws/cloudwatch'
import { EMRRoutes } from './services/aws/emr'
import { EMRContainersRoutes } from './services/aws/emroneks'
import { GlueRoutes } from './services/aws/glue'
import { EventBridgeRoutes } from './services/aws/eventbridge'
import { APIGatewayRoutes } from './services/aws/apigw'
import { SFNRoutes } from './services/aws/sfn'
import { EC2Routes } from './services/aws/ec2'
import { ECSRoutes } from './services/aws/ecs'
import { EKSRoutes } from './services/aws/eks'
import { RDSRoutes } from './services/aws/rds'
import { ElastiCacheRoutes } from './services/aws/elasticache'
import { Route53Routes } from './services/aws/route53'
import { CloudFormationRoutes } from './services/aws/cloudformation'
import { KinesisRoutes } from './services/aws/kinesis'
import { FirehoseRoutes } from './services/aws/firehose'
import { SESRoutes } from './services/aws/ses'
import { ELBv2Routes } from './services/aws/elbv2'
import { AdminPanel } from './admin/AdminPanel'

export default function App() {
  const { data: meta, isLoading } = useMeta()

  if (isLoading) {
    return (
      <div style={{ padding: '2rem', fontFamily: 'monospace' }}>
        Connecting to JaisCloud…
      </div>
    )
  }

  if (!meta) {
    return (
      <div style={{ padding: '2rem', fontFamily: 'monospace', color: '#e53' }}>
        Could not connect to JaisCloud. Is the server running on port 4567?
      </div>
    )
  }

  if (meta.cloud === 'azure') {
    return (
      <Layout>
        <Routes>
          <Route path="/" element={<Navigate to="/azure" replace />} />
          <Route path="/azure/*" element={<AzureRoutes />} />
          <Route path="/admin" element={<AdminPanel />} />
          <Route path="*" element={<Navigate to="/azure" replace />} />
        </Routes>
      </Layout>
    )
  }

  if (meta.cloud === 'gcp') {
    return (
      <Layout>
        <Routes>
          <Route path="/" element={<Navigate to="/gcp" replace />} />
          <Route path="/gcp/*" element={<GCPRoutes />} />
          <Route path="/admin" element={<AdminPanel />} />
          <Route path="*" element={<Navigate to="/gcp" replace />} />
        </Routes>
      </Layout>
    )
  }

  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Navigate to="/aws/sqs" replace />} />
        <Route path="/aws/sqs/*" element={<SQSRoutes />} />
        <Route path="/aws/lambda/*" element={<LambdaRoutes />} />
        <Route path="/aws/logs/*" element={<LogsRoutes />} />
        <Route path="/aws/s3/*" element={<S3Routes />} />
        <Route path="/aws/dynamodb/*" element={<DynamoDBRoutes />} />
        <Route path="/aws/sns/*" element={<SNSRoutes />} />
        <Route path="/aws/kms/*" element={<KMSRoutes />} />
        <Route path="/aws/secretsmanager/*" element={<SecretsManagerRoutes />} />
        <Route path="/aws/ssm/*" element={<SSMRoutes />} />
        <Route path="/aws/iam/*" element={<IAMRoutes />} />
        <Route path="/aws/cloudwatch/*" element={<CloudWatchRoutes />} />
        <Route path="/aws/emr/*" element={<EMRRoutes />} />
        <Route path="/aws/emr-containers/*" element={<EMRContainersRoutes />} />
        <Route path="/aws/glue/*" element={<GlueRoutes />} />
        <Route path="/aws/eventbridge/*" element={<EventBridgeRoutes />} />
        <Route path="/aws/apigateway/*" element={<APIGatewayRoutes />} />
        <Route path="/aws/sfn/*" element={<SFNRoutes />} />
        <Route path="/aws/ec2/*" element={<EC2Routes />} />
        <Route path="/aws/ecs/*" element={<ECSRoutes />} />
        <Route path="/aws/eks/*" element={<EKSRoutes />} />
        <Route path="/aws/rds/*" element={<RDSRoutes />} />
        <Route path="/aws/elasticache/*" element={<ElastiCacheRoutes />} />
        <Route path="/aws/route53/*" element={<Route53Routes />} />
        <Route path="/aws/cloudformation/*" element={<CloudFormationRoutes />} />
        <Route path="/aws/kinesis/*" element={<KinesisRoutes />} />
        <Route path="/aws/firehose/*" element={<FirehoseRoutes />} />
        <Route path="/aws/ses/*" element={<SESRoutes />} />
        <Route path="/aws/elbv2/*" element={<ELBv2Routes />} />
        <Route path="/admin" element={<AdminPanel />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Layout>
  )
}
