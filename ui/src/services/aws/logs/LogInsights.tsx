import { useState, useRef } from 'react'
import { startQuery, getQueryResults, stopQuery, type QueryResult } from '../../../api/logs'

export function LogInsights() {
  const [queryString, setQueryString] = useState('fields @timestamp, @message\n| sort @timestamp desc\n| limit 20')
  const [logGroupName, setLogGroupName] = useState('')
  const [result, setResult] = useState<QueryResult | null>(null)
  const [running, setRunning] = useState(false)
  const [error, setError] = useState('')
  const queryIdRef = useRef<string | null>(null)

  async function run() {
    if (!queryString.trim()) return
    setRunning(true)
    setError('')
    setResult(null)
    try {
      const now = Date.now()
      const { queryId } = await startQuery({
        queryString,
        logGroupName: logGroupName || undefined,
        startTime: Math.floor((now - 3 * 60 * 60 * 1000) / 1000),
        endTime: Math.floor(now / 1000),
      })
      queryIdRef.current = queryId
      // Poll for results
      let attempts = 0
      const poll = async () => {
        const res = await getQueryResults(queryId)
        if (res.status === 'Complete' || res.status === 'Failed' || res.status === 'Cancelled' || attempts >= 20) {
          setResult(res)
          setRunning(false)
        } else {
          attempts++
          setTimeout(poll, 500)
        }
      }
      await poll()
    } catch (e) {
      setError((e as Error).message)
      setRunning(false)
    }
  }

  async function stop() {
    if (queryIdRef.current) {
      try { await stopQuery(queryIdRef.current) } catch { /* ignore */ }
    }
    setRunning(false)
  }

  const columns = result?.results[0]?.map(f => f.field) ?? []

  return (
    <div>
      <div style={{ marginBottom: '1.5rem' }}>
        <h2 style={{ margin: '0 0 0.25rem', fontSize: '1.4rem', fontWeight: 600 }}>Log Insights</h2>
        <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>Run CloudWatch Logs Insights queries</span>
      </div>

      <div style={{ display: 'flex', gap: '1rem', marginBottom: '0.75rem' }}>
        <label style={{ flex: 1, ...labelStyle }}>
          Log Group
          <input
            type="text"
            value={logGroupName}
            onChange={e => setLogGroupName(e.target.value)}
            placeholder="/aws/lambda/my-function (optional)"
            style={inputStyle}
          />
        </label>
      </div>

      <label style={labelStyle}>
        Query
        <textarea
          value={queryString}
          onChange={e => setQueryString(e.target.value)}
          rows={5}
          style={{ ...inputStyle, fontFamily: 'monospace', resize: 'vertical' }}
        />
      </label>

      <div style={{ display: 'flex', gap: '0.75rem', margin: '0.75rem 0 1.5rem' }}>
        <button onClick={run} disabled={running || !queryString.trim()} style={primaryBtnStyle}>
          {running ? 'Running…' : 'Run Query'}
        </button>
        {running && (
          <button onClick={stop} style={cancelBtnStyle}>Stop</button>
        )}
      </div>

      {error && <div style={{ color: '#d13212', fontSize: '0.88em', marginBottom: '1rem' }}>{error}</div>}

      {result && (
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '0.75rem' }}>
            <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>
              Status: <strong style={{ color: result.status === 'Complete' ? '#037f0c' : '#d13212' }}>{result.status}</strong>
            </span>
            <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>
              {result.statistics.recordsScanned.toFixed(0)} records scanned, {result.statistics.recordsMatched.toFixed(0)} matched
            </span>
          </div>

          {result.results.length === 0 ? (
            <div style={{ color: '#5f6b7a', fontSize: '0.88em' }}>No results matched the query.</div>
          ) : (
            <div style={{ overflowX: 'auto' }}>
              <table style={tableStyle}>
                <thead>
                  <tr>
                    {columns.map(c => <th key={c} style={thStyle}>{c}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {result.results.map((row, i) => (
                    <tr key={i} style={{ borderBottom: '1px solid #2d3748' }}>
                      {row.map((cell, j) => (
                        <td key={j} style={{ ...tdStyle, maxWidth: 400, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                          {cell.value}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

const labelStyle: React.CSSProperties = { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '0.75rem', color: '#c9cdd4', fontSize: '0.85em' }
const inputStyle: React.CSSProperties = { background: '#0d1a26', border: '1px solid #2d3748', borderRadius: 4, color: '#e2e8f0', padding: '0.4rem 0.75rem', fontSize: '0.9em' }
const primaryBtnStyle: React.CSSProperties = { background: '#0972d3', border: '1px solid #0972d3', borderRadius: 4, color: '#fff', cursor: 'pointer', padding: '0.4rem 1rem', fontSize: '0.85em', fontWeight: 500 }
const cancelBtnStyle: React.CSSProperties = { background: 'none', border: '1px solid #2d3748', borderRadius: 4, color: '#c9cdd4', cursor: 'pointer', padding: '0.4rem 1rem', fontSize: '0.85em' }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.85em' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.5rem 0.75rem', color: '#8892a4', fontWeight: 500, borderBottom: '1px solid #2d3748', fontSize: '0.8em', textTransform: 'uppercase', letterSpacing: '0.05em', whiteSpace: 'nowrap' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 0.75rem', color: '#c9cdd4', verticalAlign: 'top', fontFamily: 'monospace' }
