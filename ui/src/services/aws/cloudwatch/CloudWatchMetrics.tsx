import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { listMetrics, getMetricStatistics, type CWMetric, type Datapoint } from '../../../api/cloudwatch'
import { EmptyState } from '../../../components/EmptyState'

export function CloudWatchMetrics() {
  const [nsFilter, setNsFilter] = useState('')
  const [selected, setSelected] = useState<CWMetric | null>(null)
  const [statsData, setStatsData] = useState<Datapoint[] | null>(null)
  const [statsLabel, setStatsLabel] = useState('')
  const [loadingStats, setLoadingStats] = useState(false)

  const { data, isLoading, error } = useQuery({
    queryKey: ['cloudwatch', 'metrics', nsFilter],
    queryFn: () => listMetrics(nsFilter ? { namespace: nsFilter } : undefined),
  })

  async function viewStats(metric: CWMetric) {
    setSelected(metric)
    setLoadingStats(true)
    setStatsData(null)
    try {
      const now = new Date()
      const start = new Date(now.getTime() - 3 * 60 * 60 * 1000)
      const resp = await getMetricStatistics({
        namespace: metric.namespace,
        metricName: metric.metricName,
        startTime: start.toISOString(),
        endTime: now.toISOString(),
        period: 300,
        statistics: ['Sum', 'Average', 'Maximum', 'SampleCount'],
      })
      setStatsLabel(resp.label)
      setStatsData(resp.datapoints)
    } catch {
      setStatsData([])
    } finally {
      setLoadingStats(false)
    }
  }

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading metrics…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load: {(error as Error).message}</div>

  const metrics = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>CloudWatch Metrics</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{metrics.length} metric{metrics.length !== 1 ? 's' : ''}</span>
        </div>
        <input
          type="text"
          placeholder="Filter by namespace…"
          value={nsFilter}
          onChange={e => setNsFilter(e.target.value)}
          style={inputStyle}
        />
      </div>

      {metrics.length === 0 ? (
        <EmptyState title="No metrics found. Publish metric data via PutMetricData to see metrics here." />
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>
              {['Namespace', 'Metric Name', ''].map(h => (
                <th key={h} style={thStyle}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {metrics.map((m, i) => (
              <tr key={i} style={{ borderBottom: '1px solid #2d3748' }}>
                <td style={tdStyle}>{m.namespace}</td>
                <td style={tdStyle}>{m.metricName}</td>
                <td style={{ ...tdStyle, textAlign: 'right' }}>
                  <button onClick={() => viewStats(m)} style={actionBtnStyle}>View Stats</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {/* Stats drawer */}
      {selected && (
        <div style={drawerStyle}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
            <strong style={{ color: '#e2e8f0' }}>{statsLabel || selected.metricName} — last 3 hours (5m periods)</strong>
            <button onClick={() => { setSelected(null); setStatsData(null) }} style={closeBtnStyle}>✕</button>
          </div>
          {loadingStats ? (
            <div style={{ color: '#5f6b7a' }}>Loading statistics…</div>
          ) : !statsData || statsData.length === 0 ? (
            <div style={{ color: '#5f6b7a' }}>No datapoints in the last 3 hours.</div>
          ) : (
            <table style={{ ...tableStyle, marginTop: 0 }}>
              <thead>
                <tr>
                  {['Timestamp', 'Sum', 'Average', 'Maximum', 'Samples'].map(h => (
                    <th key={h} style={thStyle}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {statsData.map((dp, i) => (
                  <tr key={i} style={{ borderBottom: '1px solid #2d3748' }}>
                    <td style={tdStyle}>{dp.timestamp}</td>
                    <td style={tdStyle}>{dp.sum?.toFixed(2) ?? '—'}</td>
                    <td style={tdStyle}>{dp.average?.toFixed(2) ?? '—'}</td>
                    <td style={tdStyle}>{dp.maximum?.toFixed(2) ?? '—'}</td>
                    <td style={tdStyle}>{dp.sampleCount ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  )
}

const inputStyle: React.CSSProperties = {
  background: '#1b2a3b',
  border: '1px solid #2d3748',
  borderRadius: 4,
  color: '#e2e8f0',
  padding: '0.4rem 0.75rem',
  fontSize: '0.85em',
  width: 220,
}

const tableStyle: React.CSSProperties = {
  width: '100%',
  borderCollapse: 'collapse',
  fontSize: '0.88em',
  marginTop: '1rem',
}

const thStyle: React.CSSProperties = {
  textAlign: 'left',
  padding: '0.5rem 0.75rem',
  color: '#8892a4',
  fontWeight: 500,
  borderBottom: '1px solid #2d3748',
  fontSize: '0.8em',
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
}

const tdStyle: React.CSSProperties = {
  padding: '0.6rem 0.75rem',
  color: '#c9cdd4',
  verticalAlign: 'middle',
}

const actionBtnStyle: React.CSSProperties = {
  background: 'none',
  border: '1px solid #2d3748',
  borderRadius: 4,
  color: '#0972d3',
  cursor: 'pointer',
  padding: '0.25rem 0.75rem',
  fontSize: '0.82em',
}

const drawerStyle: React.CSSProperties = {
  marginTop: '2rem',
  padding: '1.25rem',
  background: '#1b2a3b',
  border: '1px solid #2d3748',
  borderRadius: 6,
}

const closeBtnStyle: React.CSSProperties = {
  background: 'none',
  border: 'none',
  color: '#8892a4',
  cursor: 'pointer',
  fontSize: '1rem',
}
