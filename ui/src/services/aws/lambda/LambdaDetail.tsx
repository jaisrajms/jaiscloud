import { Fragment, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { getFunction } from '../../../api/lambda'
import { listLogStreams, getLogEvents } from '../../../api/logs'
import { LambdaTest } from './LambdaTest'

type Tab = 'configuration' | 'test' | 'logs'

export function LambdaDetail() {
  const { name: encodedName } = useParams<{ name: string }>()
  const name = encodedName ? decodeURIComponent(encodedName) : ''
  const [tab, setTab] = useState<Tab>('test')

  const { data: fn, isLoading, error } = useQuery({
    queryKey: ['lambda', 'function', name],
    queryFn: () => getFunction(name),
    enabled: !!name,
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading…</div>

  if (error || !fn) {
    return (
      <div>
        <Link to="/aws/lambda" style={{ color: '#0972d3', fontSize: '0.9em', textDecoration: 'none' }}>← Lambda</Link>
        <p style={{ color: '#d13212' }}>{error ? (error as Error).message : 'Function not found.'}</p>
      </div>
    )
  }

  const configRows: [string, string][] = [
    ['ARN', fn.arn],
    ['Runtime', fn.runtime],
    ['Handler', fn.handler],
    ['Role ARN', fn.roleArn],
    ['Timeout', `${fn.timeout}s`],
    ['Memory', `${fn.memorySize} MB`],
    ['State', fn.state],
    ['Description', fn.description || '—'],
    ['Last modified', fn.lastModified ? new Date(fn.lastModified).toLocaleString() : '—'],
  ]

  return (
    <div>
      <div style={{ marginBottom: '1.5rem' }}>
        <Link to="/aws/lambda" style={{ color: '#0972d3', fontSize: '0.85em', textDecoration: 'none' }}>
          ← Lambda Functions
        </Link>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginTop: '0.5rem', flexWrap: 'wrap' }}>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>{fn.name}</h2>
          <code style={{ fontSize: '0.75em', color: '#8d9daa', background: '#f4f5f7', padding: '0.2em 0.5em', borderRadius: 3 }}>
            {fn.runtime}
          </code>
          <span style={{
            fontSize: '0.8em', fontWeight: 500,
            color: fn.state === 'Active' ? '#1d8102' : fn.state === 'Pending' ? '#e77600' : '#d13212',
          }}>
            {fn.state}
          </span>
        </div>
      </div>

      <div style={{ display: 'flex', borderBottom: '2px solid #e7e9ec', marginBottom: '1.5rem' }}>
        {([['test', 'Test'], ['logs', 'Logs'], ['configuration', 'Configuration']] as [Tab, string][]).map(([t, label]) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            style={{
              background: 'none', border: 'none', padding: '0.6rem 1.25rem', cursor: 'pointer',
              fontSize: '0.9em', fontWeight: tab === t ? 600 : 400,
              color: tab === t ? '#e77600' : '#5f6b7a',
              borderBottom: `2px solid ${tab === t ? '#e77600' : 'transparent'}`,
              marginBottom: -2,
            }}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === 'test' && <LambdaTest name={name} />}

      {tab === 'logs' && <LambdaLogs name={name} />}

      {tab === 'configuration' && (
        <div>
          <dl style={{ display: 'grid', gridTemplateColumns: 'max-content 1fr', gap: '0 2rem', margin: '0 0 1.5rem', fontSize: '0.9em' }}>
            {configRows.map(([label, value]) => (
              <Fragment key={label}>
                <dt style={{ color: '#5f6b7a', fontWeight: 500, padding: '0.5rem 0', borderBottom: '1px solid #f4f5f7', whiteSpace: 'nowrap' }}>{label}</dt>
                <dd style={{ margin: 0, padding: '0.5rem 0', borderBottom: '1px solid #f4f5f7', wordBreak: 'break-all' }}>{value}</dd>
              </Fragment>
            ))}
          </dl>

          {fn.envVars && Object.keys(fn.envVars).length > 0 && (
            <div style={{ marginBottom: '1.5rem' }}>
              <h4 style={{ margin: '0 0 0.75rem', fontSize: '0.95rem', fontWeight: 600 }}>Environment variables</h4>
              <div style={{ border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85em' }}>
                  <thead>
                    <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                      <th style={{ padding: '0.5rem 1rem', textAlign: 'left', color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' }}>Key</th>
                      <th style={{ padding: '0.5rem 1rem', textAlign: 'left', color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' }}>Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(fn.envVars).map(([k, v]) => (
                      <tr key={k} style={{ borderBottom: '1px solid #e7e9ec' }}>
                        <td style={{ padding: '0.6rem 1rem' }}><code style={{ background: '#f4f5f7', padding: '0.2em 0.4em', borderRadius: 3 }}>{k}</code></td>
                        <td style={{ padding: '0.6rem 1rem', fontFamily: 'monospace', fontSize: '0.9em' }}>{v}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function LambdaLogs({ name }: { name: string }) {
  const logGroupName = `/aws/lambda/${name}`
  const [selectedStream, setSelectedStream] = useState<string | null>(null)

  const { data: streamsData, isLoading: streamsLoading, refetch } = useQuery({
    queryKey: ['logs', 'lambda', name, 'streams'],
    queryFn: () => listLogStreams(logGroupName, { pageSize: 20 }),
    retry: false,
  })

  const streams = streamsData?.items ?? []
  const activeStream = selectedStream ?? streams[0]?.name ?? null

  const { data: eventsData, isLoading: eventsLoading } = useQuery({
    queryKey: ['logs', 'lambda', name, 'events', activeStream],
    queryFn: () => getLogEvents(logGroupName, activeStream!, {
      limit: 200,
      startTime: Date.now() - 24 * 60 * 60 * 1000,
      endTime: Date.now(),
    }),
    enabled: !!activeStream,
  })

  function colorLine(line: string): string {
    if (/^START /.test(line)) return '#4caf78'
    if (/^END /.test(line)) return '#79b8ff'
    if (/^REPORT /.test(line)) return '#9da8b5'
    if (/^ERROR/.test(line)) return '#ff6b6b'
    if (/^WARN/.test(line)) return '#ffa94d'
    return '#e0e0e0'
  }

  if (streamsLoading) return <div style={{ color: '#5f6b7a', fontSize: '0.9em' }}>Loading log streams…</div>

  if (streams.length === 0) {
    return (
      <div style={{ color: '#5f6b7a', fontSize: '0.9em' }}>
        <p>No log streams found for <code>/aws/lambda/{name}</code>.</p>
        <p style={{ fontSize: '0.85em' }}>Invoke the function to generate logs.</p>
        <button onClick={() => refetch()} style={btnSmall}>Refresh</button>
      </div>
    )
  }

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }}>
        <label style={{ fontSize: '0.9em', fontWeight: 500 }}>Log stream</label>
        <select
          value={activeStream ?? ''}
          onChange={(e) => setSelectedStream(e.target.value)}
          style={{ border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.3rem 0.5rem', fontSize: '0.85em', flex: 1, maxWidth: 480 }}
        >
          {streams.map((s) => (
            <option key={s.name} value={s.name}>{s.name}</option>
          ))}
        </select>
        <button onClick={() => refetch()} style={btnSmall}>Refresh</button>
      </div>

      <div style={{ border: '1px solid #2d3748', borderRadius: 6, overflow: 'hidden' }}>
        <pre style={{ margin: 0, padding: '0.75rem 1rem', background: '#1e1e1e', overflow: 'auto', maxHeight: 500, fontSize: '0.8em', fontFamily: 'monospace', lineHeight: 1.6 }}>
          {eventsLoading ? (
            <span style={{ color: '#5f6b7a' }}>Loading…</span>
          ) : !eventsData || eventsData.events.length === 0 ? (
            <span style={{ color: '#5f6b7a' }}>No events in this stream.</span>
          ) : (
            eventsData.events.map((ev, i) => (
              <div key={i} style={{ color: colorLine(ev.message) }}>{ev.message}</div>
            ))
          )}
        </pre>
      </div>
    </div>
  )
}

const btnSmall: React.CSSProperties = {
  background: 'none', border: '1px solid #c9cdd4', color: '#16191f',
  borderRadius: 4, padding: '0.3rem 0.75rem', cursor: 'pointer', fontSize: '0.82em',
}
