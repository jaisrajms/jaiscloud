import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  listDashboards,
  getDashboard,
  putDashboard,
  deleteDashboard,
  type CWDashboard,
} from '../../../api/cloudwatch'
import { EmptyState } from '../../../components/EmptyState'

const DEFAULT_BODY = JSON.stringify({ widgets: [] }, null, 2)

export function CloudWatchDashboards() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newBody, setNewBody] = useState(DEFAULT_BODY)
  const [viewDash, setViewDash] = useState<CWDashboard | null>(null)
  const [viewBody, setViewBody] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<CWDashboard | null>(null)
  const [loadingView, setLoadingView] = useState(false)

  const qc = useQueryClient()

  const { data, isLoading, error } = useQuery({
    queryKey: ['cloudwatch', 'dashboards'],
    queryFn: listDashboards,
  })

  const createMut = useMutation({
    mutationFn: () => putDashboard({ dashboardName: newName, dashboardBody: newBody }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'dashboards'] })
      setCreateOpen(false)
      setNewName('')
      setNewBody(DEFAULT_BODY)
    },
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteDashboard(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'dashboards'] })
      setDeleteTarget(null)
    },
  })

  async function viewDashboard(d: CWDashboard) {
    setViewDash(d)
    setLoadingView(true)
    setViewBody('')
    try {
      const full = await getDashboard(d.dashboardName)
      try {
        setViewBody(JSON.stringify(JSON.parse(full.dashboardBody ?? '{}'), null, 2))
      } catch {
        setViewBody(full.dashboardBody ?? '')
      }
    } catch {
      setViewBody('Failed to load dashboard body')
    } finally {
      setLoadingView(false)
    }
  }

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading dashboards…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load: {(error as Error).message}</div>

  const dashboards = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>CloudWatch Dashboards</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{dashboards.length} dashboard{dashboards.length !== 1 ? 's' : ''}</span>
        </div>
        <button onClick={() => setCreateOpen(true)} style={primaryBtnStyle}>Create Dashboard</button>
      </div>

      {dashboards.length === 0 ? (
        <EmptyState title="No dashboards found." />
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>
              {['Name', 'Last Modified', ''].map(h => (
                <th key={h} style={thStyle}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {dashboards.map(d => (
              <tr key={d.dashboardName} style={{ borderBottom: '1px solid #2d3748' }}>
                <td style={tdStyle}>{d.dashboardName}</td>
                <td style={tdStyle}>{d.lastModified ?? '—'}</td>
                <td style={{ ...tdStyle, textAlign: 'right' }}>
                  <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                    <button onClick={() => viewDashboard(d)} style={actionBtnStyle}>View</button>
                    <button onClick={() => setDeleteTarget(d)} style={{ ...actionBtnStyle, color: '#d13212', borderColor: '#d13212' }}>Delete</button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {/* Create dialog */}
      {createOpen && (
        <div style={overlayStyle}>
          <div style={{ ...dialogStyle, minWidth: 520 }}>
            <h3 style={{ margin: '0 0 1rem', color: '#e2e8f0' }}>Create Dashboard</h3>
            <label style={labelStyle}>
              Dashboard Name *
              <input type="text" value={newName} onChange={e => setNewName(e.target.value)} style={inputStyle} placeholder="my-dashboard" />
            </label>
            <label style={labelStyle}>
              Dashboard Body (JSON)
              <textarea
                value={newBody}
                onChange={e => setNewBody(e.target.value)}
                rows={8}
                style={{ ...inputStyle, fontFamily: 'monospace', resize: 'vertical' }}
              />
            </label>
            {createMut.error && <div style={{ color: '#d13212', fontSize: '0.85em' }}>{(createMut.error as Error).message}</div>}
            <div style={{ display: 'flex', gap: '0.75rem', marginTop: '1.25rem' }}>
              <button onClick={() => createMut.mutate()} disabled={!newName} style={primaryBtnStyle}>Create</button>
              <button onClick={() => setCreateOpen(false)} style={cancelBtnStyle}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {/* View dialog */}
      {viewDash && (
        <div style={overlayStyle}>
          <div style={{ ...dialogStyle, minWidth: 560, maxWidth: 700 }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
              <h3 style={{ margin: 0, color: '#e2e8f0' }}>{viewDash.dashboardName}</h3>
              <button onClick={() => setViewDash(null)} style={closeBtnStyle}>✕</button>
            </div>
            {loadingView ? (
              <div style={{ color: '#5f6b7a' }}>Loading…</div>
            ) : (
              <pre style={{ background: '#0d1a26', border: '1px solid #2d3748', borderRadius: 4, padding: '0.75rem', color: '#c9cdd4', fontSize: '0.82em', overflow: 'auto', maxHeight: 360, margin: 0 }}>
                {viewBody}
              </pre>
            )}
          </div>
        </div>
      )}

      {/* Delete confirm */}
      {deleteTarget && (
        <div style={overlayStyle}>
          <div style={dialogStyle}>
            <h3 style={{ margin: '0 0 0.75rem', color: '#e2e8f0' }}>Delete Dashboard?</h3>
            <p style={{ color: '#8892a4', fontSize: '0.88em' }}>Delete <strong style={{ color: '#e2e8f0' }}>{deleteTarget.dashboardName}</strong>? This cannot be undone.</p>
            <div style={{ display: 'flex', gap: '0.75rem', marginTop: '1.25rem' }}>
              <button onClick={() => deleteMut.mutate(deleteTarget.dashboardName)} style={{ ...primaryBtnStyle, background: '#d13212', borderColor: '#d13212' }}>Delete</button>
              <button onClick={() => setDeleteTarget(null)} style={cancelBtnStyle}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.88em' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.5rem 0.75rem', color: '#8892a4', fontWeight: 500, borderBottom: '1px solid #2d3748', fontSize: '0.8em', textTransform: 'uppercase', letterSpacing: '0.05em' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 0.75rem', color: '#c9cdd4', verticalAlign: 'middle' }
const primaryBtnStyle: React.CSSProperties = { background: '#0972d3', border: '1px solid #0972d3', borderRadius: 4, color: '#fff', cursor: 'pointer', padding: '0.4rem 1rem', fontSize: '0.85em', fontWeight: 500 }
const cancelBtnStyle: React.CSSProperties = { background: 'none', border: '1px solid #2d3748', borderRadius: 4, color: '#c9cdd4', cursor: 'pointer', padding: '0.4rem 1rem', fontSize: '0.85em' }
const actionBtnStyle: React.CSSProperties = { background: 'none', border: '1px solid #2d3748', borderRadius: 4, color: '#0972d3', cursor: 'pointer', padding: '0.25rem 0.75rem', fontSize: '0.82em' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 50 }
const dialogStyle: React.CSSProperties = { background: '#1b2530', border: '1px solid #2d3748', borderRadius: 8, padding: '1.75rem', minWidth: 400, maxWidth: 560 }
const labelStyle: React.CSSProperties = { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '0.75rem', color: '#c9cdd4', fontSize: '0.85em' }
const inputStyle: React.CSSProperties = { background: '#0d1a26', border: '1px solid #2d3748', borderRadius: 4, color: '#e2e8f0', padding: '0.4rem 0.75rem', fontSize: '0.9em' }
const closeBtnStyle: React.CSSProperties = { background: 'none', border: 'none', color: '#8892a4', cursor: 'pointer', fontSize: '1rem' }
