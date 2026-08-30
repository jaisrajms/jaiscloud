import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { listLogGroups, createLogGroup, deleteLogGroup } from '../../../api/logs'
import { EmptyState } from '../../../components/EmptyState'

function fmtBytes(n: number): string {
  if (n === 0) return '0 B'
  if (n < 1024) return `${n} B`
  if (n < 1048576) return `${(n / 1024).toFixed(1)} KB`
  if (n < 1073741824) return `${(n / 1048576).toFixed(1)} MB`
  return `${(n / 1073741824).toFixed(2)} GB`
}

function fmtDate(ms: number): string {
  if (!ms) return '—'
  return new Date(ms).toLocaleString()
}

export function LogGroupList() {
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null)
  const [createName, setCreateName] = useState('')
  const [showCreate, setShowCreate] = useState(false)
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['logs', 'groups'],
    queryFn: () => listLogGroups(),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteLogGroup(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['logs', 'groups'] })
      setConfirmDelete(null)
    },
  })

  const createMut = useMutation({
    mutationFn: (name: string) => createLogGroup(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['logs', 'groups'] })
      setShowCreate(false)
      setCreateName('')
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading log groups…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load log groups: {(error as Error).message}</div>

  const groups = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>CloudWatch Log Groups</h2>
        <button onClick={() => setShowCreate(true)} style={btnPrimary}>Create log group</button>
      </div>

      {groups.length === 0 ? (
        <EmptyState
          title="No log groups"
          description="CloudWatch Logs lets you monitor, store, and access log files from your resources."
          cta="Create Log Group"
          onCta={() => setShowCreate(true)}
        />
      ) : (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={th}>Name</th>
                <th style={{ ...th, textAlign: 'right' }}>Retention</th>
                <th style={{ ...th, textAlign: 'right' }}>Stored</th>
                <th style={th}>Created</th>
                <th style={{ ...th, width: 80 }}></th>
              </tr>
            </thead>
            <tbody>
              {groups.map((g) => (
                <tr
                  key={g.arn || g.name}
                  style={{ borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }}
                  onClick={() => navigate(`/aws/logs/groups/${encodeURIComponent(g.name)}`)}
                  onMouseEnter={(e) => (e.currentTarget.style.background = '#fafbfc')}
                  onMouseLeave={(e) => (e.currentTarget.style.background = '')}
                >
                  <td style={td}><span style={{ color: '#0972d3', fontWeight: 500, fontFamily: 'monospace', fontSize: '0.9em' }}>{g.name}</span></td>
                  <td style={{ ...td, textAlign: 'right', color: '#5f6b7a' }}>
                    {g.retentionDays ? `${g.retentionDays}d` : 'Never expire'}
                  </td>
                  <td style={{ ...td, textAlign: 'right', color: '#5f6b7a' }}>{fmtBytes(g.storedBytes)}</td>
                  <td style={{ ...td, color: '#5f6b7a' }}>{fmtDate(g.createdAt)}</td>
                  <td style={td} onClick={(e) => e.stopPropagation()}>
                    <button onClick={() => setConfirmDelete(g.name)} style={btnSmall}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Create dialog */}
      {showCreate && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.25rem' }}>
              <h3 style={{ margin: 0, fontSize: '1.1rem' }}>Create log group</h3>
              <button onClick={() => setShowCreate(false)} style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: '1.1rem', color: '#5f6b7a' }}>✕</button>
            </div>
            <form onSubmit={(e) => { e.preventDefault(); createMut.mutate(createName) }}>
              <label style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem', fontSize: '0.9em', fontWeight: 500 }}>
                Log group name
                <input
                  required value={createName} onChange={(e) => setCreateName(e.target.value)}
                  placeholder="/aws/lambda/my-function"
                  style={{ border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.4rem 0.6rem', fontSize: '0.9em', fontFamily: 'monospace' }}
                />
              </label>
              {createMut.error && <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>{(createMut.error as Error).message}</p>}
              <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
                <button type="button" onClick={() => setShowCreate(false)} style={btnSecondary}>Cancel</button>
                <button type="submit" disabled={createMut.isPending} style={{ ...btnPrimary, opacity: createMut.isPending ? 0.6 : 1 }}>
                  {createMut.isPending ? 'Creating…' : 'Create'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Delete confirm */}
      {confirmDelete && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: '1.05rem' }}>Delete log group?</h3>
            <p style={{ margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Permanently delete <strong style={{ fontFamily: 'monospace' }}>{confirmDelete}</strong> and all its log streams?
            </p>
            {deleteMut.error && <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>{(deleteMut.error as Error).message}</p>}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setConfirmDelete(null)} style={btnSecondary}>Cancel</button>
              <button onClick={() => deleteMut.mutate(confirmDelete)} disabled={deleteMut.isPending}
                style={{ background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em', opacity: deleteMut.isPending ? 0.6 : 1 }}>
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

const th: React.CSSProperties = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' }
const td: React.CSSProperties = { padding: '0.75rem 1rem' }
const btnPrimary: React.CSSProperties = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1.25rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' }
const btnSecondary: React.CSSProperties = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' }
const btnSmall: React.CSSProperties = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const dialogStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 500, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' }
