import { useParams, Link, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { listLogStreams, type LogStream } from '../../../api/logs'

function fmtTs(ms?: number): string {
  if (!ms) return '—'
  return new Date(ms).toLocaleString()
}

export function LogGroupDetail() {
  const { name: encodedName } = useParams<{ name: string }>()
  const groupName = encodedName ? decodeURIComponent(encodedName) : ''
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['logs', 'streams', groupName],
    queryFn: () => listLogStreams(groupName),
    enabled: !!groupName,
  })

  const streams: LogStream[] = data?.items ?? []

  return (
    <div>
      <div style={{ marginBottom: '1.5rem' }}>
        <Link to="/aws/logs/groups" style={{ color: '#0972d3', fontSize: '0.85em', textDecoration: 'none' }}>
          ← Log Groups
        </Link>
        <h2 style={{ margin: '0.5rem 0 0', fontSize: '1.3rem', fontWeight: 600, fontFamily: 'monospace' }}>{groupName}</h2>
      </div>

      <h4 style={{ margin: '0 0 0.75rem', fontSize: '0.95rem', fontWeight: 600 }}>Log Streams</h4>

      {isLoading && <div style={{ color: '#5f6b7a', fontSize: '0.9em' }}>Loading streams…</div>}
      {error && <div style={{ color: '#d13212', fontSize: '0.9em' }}>{(error as Error).message}</div>}

      {!isLoading && streams.length === 0 && (
        <p style={{ color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }}>No log streams in this group.</p>
      )}

      {streams.length > 0 && (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={th}>Stream name</th>
                <th style={th}>First event</th>
                <th style={th}>Last event</th>
              </tr>
            </thead>
            <tbody>
              {streams.map((s) => (
                <tr
                  key={s.name}
                  style={{ borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }}
                  onClick={() => navigate(`/aws/logs/groups/${encodeURIComponent(groupName)}/streams/${encodeURIComponent(s.name)}`)}
                  onMouseEnter={(e) => (e.currentTarget.style.background = '#fafbfc')}
                  onMouseLeave={(e) => (e.currentTarget.style.background = '')}
                >
                  <td style={td}>
                    <span style={{ color: '#0972d3', fontFamily: 'monospace', fontSize: '0.88em' }}>{s.name}</span>
                  </td>
                  <td style={{ ...td, color: '#5f6b7a' }}>{fmtTs(s.firstEventAt)}</td>
                  <td style={{ ...td, color: '#5f6b7a' }}>{fmtTs(s.lastEventAt)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

const th: React.CSSProperties = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' }
const td: React.CSSProperties = { padding: '0.75rem 1rem' }
