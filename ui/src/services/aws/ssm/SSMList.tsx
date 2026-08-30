import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { listParameters, putParameter, deleteParameter, getParameter, type Parameter } from '../../../api/ssm'
import { EmptyState } from '../../../components/EmptyState'

export function SSMList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newValue, setNewValue] = useState('')
  const [newType, setNewType] = useState('String')
  const [newDesc, setNewDesc] = useState('')
  const [pathFilter, setPathFilter] = useState('')
  const [viewParam, setViewParam] = useState<Parameter | null>(null)
  const [viewValue, setViewValue] = useState<string | null>(null)
  const [confirmDelete, setConfirmDelete] = useState<Parameter | null>(null)
  const qc = useQueryClient()

  const { data, isLoading, error } = useQuery({
    queryKey: ['ssm', 'parameters', pathFilter],
    queryFn: () => listParameters(pathFilter ? { path: pathFilter } : undefined),
  })

  const putMut = useMutation({
    mutationFn: () => putParameter({ name: newName, value: newValue, type: newType, description: newDesc || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['ssm', 'parameters'] })
      setCreateOpen(false)
      setNewName('')
      setNewValue('')
      setNewType('String')
      setNewDesc('')
    },
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteParameter(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['ssm', 'parameters'] })
      setConfirmDelete(null)
    },
  })

  const handleView = async (p: Parameter) => {
    setViewParam(p)
    setViewValue(null)
    try {
      const resp = await getParameter(p.name)
      setViewValue(resp.Parameter?.value ?? '(empty)')
    } catch {
      setViewValue('(could not retrieve value)')
    }
  }

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading parameters…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load: {(error as Error).message}</div>

  const params = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>SSM Parameter Store</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{params.length} parameter{params.length !== 1 ? 's' : ''}</span>
        </div>
        <button onClick={() => setCreateOpen(true)} style={btnPrimary}>Create parameter</button>
      </div>

      <div style={{ marginBottom: '1rem' }}>
        <input style={{ ...inputStyle, marginBottom: 0, maxWidth: 340 }}
          placeholder="Filter by path prefix (e.g. /myapp/)" value={pathFilter}
          onChange={e => setPathFilter(e.target.value)} />
      </div>

      {params.length === 0 ? (
        <EmptyState
          title="No parameters"
          description="Store configuration values and secrets as SSM parameters."
          cta="Create Parameter"
          onCta={() => setCreateOpen(true)}
        />
      ) : (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={th}>Name</th>
                <th style={th}>Type</th>
                <th style={th}>Version</th>
                <th style={th}>Last modified</th>
                <th style={{ ...th, textAlign: 'right' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {params.map((p, i) => (
                <tr key={p.name} style={{ borderBottom: i < params.length - 1 ? '1px solid #e7e9ec' : 'none' }}>
                  <td style={{ ...td, fontFamily: 'monospace', fontSize: '0.88em', color: '#0972d3' }}>{p.name}</td>
                  <td style={td}>
                    <span style={{
                      display: 'inline-block', padding: '1px 8px', borderRadius: 10, fontSize: '0.8em',
                      background: p.type === 'SecureString' ? '#8a611622' : '#0972d322',
                      color: p.type === 'SecureString' ? '#8a6116' : '#0972d3',
                    }}>
                      {p.type}
                    </span>
                  </td>
                  <td style={td}>{p.version}</td>
                  <td style={td}>{p.lastModifiedDate || '—'}</td>
                  <td style={{ ...td, textAlign: 'right', whiteSpace: 'nowrap' }}>
                    <button onClick={() => handleView(p)} style={btnSmall}>View</button>
                    <button onClick={() => setConfirmDelete(p)} style={{ ...btnSmall, marginLeft: 6, color: '#d13212', borderColor: '#d13212' }}>
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Create dialog */}
      {createOpen && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 1rem' }}>Create Parameter</h3>
            <label style={labelStyle}>Name *</label>
            <input style={inputStyle} value={newName} onChange={e => setNewName(e.target.value)} placeholder="/myapp/db-password" autoFocus />
            <label style={labelStyle}>Type</label>
            <select style={inputStyle} value={newType} onChange={e => setNewType(e.target.value)}>
              <option value="String">String</option>
              <option value="StringList">StringList</option>
              <option value="SecureString">SecureString</option>
            </select>
            <label style={labelStyle}>Value *</label>
            <textarea style={{ ...inputStyle, height: 80, fontFamily: 'monospace', fontSize: '0.85em', resize: 'vertical' }}
              value={newValue} onChange={e => setNewValue(e.target.value)} placeholder="parameter value" />
            <label style={labelStyle}>Description (optional)</label>
            <input style={inputStyle} value={newDesc} onChange={e => setNewDesc(e.target.value)} placeholder="My parameter description" />
            {putMut.error && (
              <div style={{ color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' }}>
                {(putMut.error as Error).message}
              </div>
            )}
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '0.5rem' }}>
              <button style={btnSecondary} onClick={() => setCreateOpen(false)}>Cancel</button>
              <button style={btnPrimary} onClick={() => putMut.mutate()} disabled={!newName || !newValue || putMut.isPending}>
                {putMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* View value dialog */}
      {viewParam && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 0.5rem' }}>{viewParam.name}</h3>
            <div style={{ color: '#5f6b7a', fontSize: '0.85em', marginBottom: '0.75rem' }}>
              Type: {viewParam.type} · Version: {viewParam.version}
            </div>
            {viewParam.description && (
              <div style={{ marginBottom: '0.75rem', fontSize: '0.9em' }}>{viewParam.description}</div>
            )}
            <label style={labelStyle}>Value</label>
            <pre style={{
              background: '#f4f5f7', borderRadius: 6, padding: '0.75rem', fontFamily: 'monospace',
              fontSize: '0.85em', overflowX: 'auto', whiteSpace: 'pre-wrap', wordBreak: 'break-all',
              margin: '0 0 1rem',
            }}>
              {viewValue === null ? 'Loading…' : viewValue}
            </pre>
            <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
              <button style={btnSecondary} onClick={() => { setViewParam(null); setViewValue(null) }}>Close</button>
            </div>
          </div>
        </div>
      )}

      {/* Confirm delete */}
      {confirmDelete && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 0.75rem' }}>Delete parameter?</h3>
            <p style={{ margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Delete <code>{confirmDelete.name}</code>? This cannot be undone.
            </p>
            {deleteMut.error && (
              <div style={{ color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' }}>
                {(deleteMut.error as Error).message}
              </div>
            )}
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
              <button style={btnSecondary} onClick={() => setConfirmDelete(null)}>Cancel</button>
              <button style={{ ...btnPrimary, background: '#d13212', borderColor: '#d13212' }}
                onClick={() => deleteMut.mutate(confirmDelete.name)}
                disabled={deleteMut.isPending}>
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

const btnPrimary: React.CSSProperties = {
  background: '#e87600', color: '#fff', border: '1px solid #e87600',
  borderRadius: 6, padding: '0.4rem 1rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em',
}
const btnSecondary: React.CSSProperties = {
  background: 'transparent', color: '#5f6b7a', border: '1px solid #ccc',
  borderRadius: 6, padding: '0.4rem 1rem', cursor: 'pointer', fontSize: '0.9em',
}
const btnSmall: React.CSSProperties = {
  background: 'transparent', color: '#0972d3', border: '1px solid #0972d3',
  borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: '0.8em',
}
const th: React.CSSProperties = {
  padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, fontSize: '0.82em',
  color: '#5f6b7a', textTransform: 'uppercase', letterSpacing: '0.05em',
}
const td: React.CSSProperties = { padding: '0.7rem 1rem', verticalAlign: 'middle' }
const overlay: React.CSSProperties = {
  position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex',
  alignItems: 'center', justifyContent: 'center', zIndex: 1000,
}
const dialog: React.CSSProperties = {
  background: '#fff', borderRadius: 10, padding: '1.5rem', minWidth: 440, maxWidth: 560,
  boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
}
const labelStyle: React.CSSProperties = { display: 'block', fontSize: '0.82em', fontWeight: 600, marginBottom: 4, color: '#2d3748' }
const inputStyle: React.CSSProperties = {
  width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #ccc', borderRadius: 6,
  marginBottom: '0.75rem', fontSize: '0.9em', boxSizing: 'border-box',
}
