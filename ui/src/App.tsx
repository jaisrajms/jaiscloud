import { Routes, Route, Navigate } from 'react-router-dom'
import { useMeta } from './hooks/useMeta'
import { Layout } from './components/Layout'
import { SQSRoutes } from './services/aws/sqs'
import { LambdaRoutes } from './services/aws/lambda'
import { LogsRoutes } from './services/aws/logs'

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
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Layout>
  )
}
