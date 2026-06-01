import { Routes, Route, Navigate } from 'react-router-dom'
import { useMeta } from './hooks/useMeta'
import { Layout } from './components/Layout'
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
        <Route path="/admin" element={<AdminPanel />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Layout>
  )
}
