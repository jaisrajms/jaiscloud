import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { listSecrets, createSecret, deleteSecret, type Secret } from '../../../api/secretsmanager'
import { EmptyState } from '../../../components/EmptyState'

export function SecretsList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newValue, setNewValue] = useState('')
  const [newDesc, setNewDesc] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<Secret | null>(null)
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['secretsmanager', 'secrets'],
    queryFn: () => listSecrets(),
  })

  const createMut = useMutation({
    mutationFn: () => createSecret({ name: newName, secretString: newValue || undefined, description: newDesc || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['secretsmanager', 'secrets'] })
      setCreateOpen(false)
      setNewName('')
      setNewValue('')
      setNewDesc('')
    },
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteSecret(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['secretsmanager', 'secrets'] })
      setConfirmDelete(null)
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading secrets…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load: {(error as Error).message}</div>

  const secrets = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>Secrets Manager</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{secrets.length} secret{secrets.length !== 1 ? 's' : ''}</span>
        </div>
        <button onClick={() => setCreateOpen(true)} style={btnPrimary}>Create secret</button>
      </div>

      {secrets.length === 0 ? (
        <EmptyState
          title="No secrets"
          description="Store and retrieve database credentials, API keys, and other secrets."
          cta="Create Secret"
          onCta={() => setCreateOpen(true)}
        />
      ) : (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={th}>Name</th>
                <th style={th}>Description</th>
                <th style={th}>Last changed</th>
                <th style={{ ...th, textAlign: 'right' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {secrets.map((s, i) => (
                <tr key={s.arn} style={{ borderBottom: i < secrets.length - 1 ? '1px solid #e7e9ec' : 'none', cursor: 'pointer' }}
                  onClick={() => navigate(encodeURIComponent(s.name))}>
                  <td style={{ ...td, fontWeight: 500, color: '#0972d3' }}>{s.name}</td>
                  <td style={td}>{s.description || <span style={{ color: '#8d9daa' }}>—</span>}</td>
                  <td style={td}>{s.lastChangedDate || <span style={{ color: '#8d9daa' }}>—</span>}</td>
                  <td style={{ ...td, textAlign: 'right' }} onClick={e => e.stopPropagation()}>
                    <button onClick={() => setConfirmDelete(s)} style={{ ...btnSmall, color: '#d13212', borderColor: '#d13212' }}>
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
            <h3 style={{ margin: '0 0 1rem' }}>Create Secret</h3>
            <label style={label}>Secret name *</label>
            <input style={input} value={newName} onChange={e => setNewName(e.target.value)} placeholder="my-secret" autoFocus />
            <label style={label}>Secret value (optional)</label>
            <textarea style={{ ...input, height: 80, resize: 'vertical', fontFamily: 'monospace', fontSize: '0.85em' }}
              value={newValue} onChange={e => setNewValue(e.target.value)}
              placeholder='{"username":"admin","password":"secret"}' />
            <label style={label}>Description (optional)</label>
            <input style={input} value={newDesc} onChange={e => setNewDesc(e.target.value)} placeholder="Database credentials" />
            {createMut.error && (
              <div style={{ color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' }}>
                {(createMut.error as Error).message}
              </div>
            )}
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '1rem' }}>
              <button style={btnSecondary} onClick={() => setCreateOpen(false)}>Cancel</button>
              <button style={btnPrimary} onClick={() => createMut.mutate()} disabled={!newName || createMut.isPending}>
                {createMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Confirm delete */}
      {confirmDelete && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 0.75rem' }}>Delete secret?</h3>
            <p style={{ margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              This will permanently delete <strong>{confirmDelete.name}</strong> and all its versions.
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
const label: React.CSSProperties = { display: 'block', fontSize: '0.82em', fontWeight: 600, marginBottom: 4, color: '#2d3748' }
const input: React.CSSProperties = {
  width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #ccc', borderRadius: 6,
  marginBottom: '0.75rem', fontSize: '0.9em', boxSizing: 'border-box',
}
