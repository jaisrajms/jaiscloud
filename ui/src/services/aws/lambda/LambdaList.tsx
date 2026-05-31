import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { listFunctions, deleteFunction, type LambdaFunction } from '../../../api/lambda'
import { EmptyState } from '../../../components/EmptyState'

function stateColor(state: string): string {
  if (state === 'Active') return '#1d8102'
  if (state === 'Pending') return '#e77600'
  if (state === 'Inactive' || state === 'Failed') return '#d13212'
  return '#5f6b7a'
}

function fmtDate(s: string): string {
  if (!s) return '—'
  try { return new Date(s).toLocaleString() } catch { return s }
}

export function LambdaList() {
  const [confirmDelete, setConfirmDelete] = useState<LambdaFunction | null>(null)
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['lambda', 'functions'],
    queryFn: () => listFunctions(),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteFunction(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['lambda', 'functions'] })
      setConfirmDelete(null)
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading functions…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load functions: {(error as Error).message}</div>

  const functions = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>Lambda Functions</h2>
      </div>

      {functions.length === 0 ? (
        <EmptyState
          title="No functions"
          description="Lambda functions let you run code without managing infrastructure."
        />
      ) : (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={th}>Name</th>
                <th style={th}>Runtime</th>
                <th style={th}>Handler</th>
                <th style={th}>Last Modified</th>
                <th style={th}>State</th>
                <th style={{ ...th, width: 80 }}></th>
              </tr>
            </thead>
            <tbody>
              {functions.map((fn) => (
                <tr
                  key={fn.arn}
                  style={{ borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }}
                  onClick={() => navigate(`/aws/lambda/${encodeURIComponent(fn.name)}`)}
                  onMouseEnter={(e) => (e.currentTarget.style.background = '#fafbfc')}
                  onMouseLeave={(e) => (e.currentTarget.style.background = '')}
                >
                  <td style={td}><span style={{ color: '#0972d3', fontWeight: 500 }}>{fn.name}</span></td>
                  <td style={td}><code style={{ fontSize: '0.85em', background: '#f4f5f7', padding: '0.15em 0.4em', borderRadius: 3 }}>{fn.runtime}</code></td>
                  <td style={{ ...td, color: '#5f6b7a', fontFamily: 'monospace', fontSize: '0.85em' }}>{fn.handler}</td>
                  <td style={{ ...td, color: '#5f6b7a' }}>{fmtDate(fn.lastModified)}</td>
                  <td style={td}>
                    <span style={{ color: stateColor(fn.state), fontWeight: 500, fontSize: '0.85em' }}>
                      {fn.state || '—'}
                    </span>
                  </td>
                  <td style={td} onClick={(e) => e.stopPropagation()}>
                    <button onClick={() => setConfirmDelete(fn)} style={btnSmall}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {confirmDelete && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: '1.05rem' }}>Delete function?</h3>
            <p style={{ margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Permanently delete <strong>{confirmDelete.name}</strong>?
            </p>
            {deleteMut.error && (
              <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>
                {(deleteMut.error as Error).message}
              </p>
            )}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setConfirmDelete(null)} style={btnSecondary}>Cancel</button>
              <button
                onClick={() => deleteMut.mutate(confirmDelete.name)}
                disabled={deleteMut.isPending}
                style={{ background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em', opacity: deleteMut.isPending ? 0.6 : 1 }}
              >
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
const btnSmall: React.CSSProperties = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' }
const btnSecondary: React.CSSProperties = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const dialogStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 360, maxWidth: 440, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' }
