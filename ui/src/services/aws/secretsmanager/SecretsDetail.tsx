import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getSecretValue, putSecretValue, listSecretVersions } from '../../../api/secretsmanager'

export function SecretsDetail() {
  const { name } = useParams<{ name: string }>()
  const navigate = useNavigate()
  const qc = useQueryClient()
  const decodedName = decodeURIComponent(name ?? '')
  const [editOpen, setEditOpen] = useState(false)
  const [newValue, setNewValue] = useState('')
  const [showValue, setShowValue] = useState(false)

  const { data: valueData, isLoading, error } = useQuery({
    queryKey: ['secretsmanager', 'value', decodedName],
    queryFn: () => getSecretValue(decodedName),
    enabled: showValue,
    retry: false,
  })

  const { data: versionsData } = useQuery({
    queryKey: ['secretsmanager', 'versions', decodedName],
    queryFn: () => listSecretVersions(decodedName),
  })

  const putMut = useMutation({
    mutationFn: () => putSecretValue(decodedName, newValue),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['secretsmanager', 'value', decodedName] })
      void qc.invalidateQueries({ queryKey: ['secretsmanager', 'versions', decodedName] })
      setEditOpen(false)
      setNewValue('')
    },
  })

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: '1.5rem' }}>
        <button onClick={() => navigate('/aws/secretsmanager')} style={btnBack}>← Back</button>
        <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>{decodedName}</h2>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem' }}>
        {/* Secret value card */}
        <div style={card}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.75rem' }}>
            <h3 style={{ margin: 0, fontSize: '1rem', fontWeight: 600 }}>Secret Value</h3>
            <div style={{ display: 'flex', gap: 6 }}>
              <button style={btnSecondary} onClick={() => setShowValue(v => !v)}>
                {showValue ? 'Hide' : 'Reveal'}
              </button>
              <button style={btnPrimary} onClick={() => setEditOpen(true)}>Update</button>
            </div>
          </div>
          {showValue && (
            isLoading ? (
              <div style={{ color: '#5f6b7a', fontSize: '0.9em' }}>Loading…</div>
            ) : error ? (
              <div style={{ color: '#d13212', fontSize: '0.9em' }}>{(error as Error).message}</div>
            ) : (
              <pre style={{
                background: '#f4f5f7', borderRadius: 6, padding: '0.75rem',
                fontFamily: 'monospace', fontSize: '0.82em', overflowX: 'auto',
                whiteSpace: 'pre-wrap', wordBreak: 'break-all', margin: 0,
              }}>
                {valueData?.SecretString ?? '(binary)'}
              </pre>
            )
          )}
          {!showValue && (
            <div style={{ color: '#8d9daa', fontSize: '0.9em' }}>Click Reveal to view the secret value.</div>
          )}
        </div>

        {/* Versions card */}
        <div style={card}>
          <h3 style={{ margin: '0 0 0.75rem', fontSize: '1rem', fontWeight: 600 }}>Versions</h3>
          {(versionsData?.items ?? []).length === 0 ? (
            <div style={{ color: '#8d9daa', fontSize: '0.9em' }}>No versions yet.</div>
          ) : (
            <div style={{ border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden', fontSize: '0.85em' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ background: '#f4f5f7', borderBottom: '1px solid #e7e9ec' }}>
                    <th style={th}>Version ID</th>
                    <th style={th}>Stages</th>
                    <th style={th}>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {(versionsData?.items ?? []).map((v, i) => (
                    <tr key={v.versionId} style={{ borderBottom: i < (versionsData?.items ?? []).length - 1 ? '1px solid #e7e9ec' : 'none' }}>
                      <td style={td}><code style={{ fontSize: '0.9em' }}>{v.versionId.slice(0, 8)}…</code></td>
                      <td style={td}>{(v.versionStages ?? []).join(', ')}</td>
                      <td style={td}>{v.createdDate || '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* Update value dialog */}
      {editOpen && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 1rem' }}>Update secret value</h3>
            <label style={labelStyle}>New secret value</label>
            <textarea style={{ ...inputStyle, height: 120, fontFamily: 'monospace', fontSize: '0.85em' }}
              value={newValue} onChange={e => setNewValue(e.target.value)}
              placeholder='{"username":"admin","password":"new-secret"}' autoFocus />
            {putMut.error && (
              <div style={{ color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' }}>
                {(putMut.error as Error).message}
              </div>
            )}
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '0.5rem' }}>
              <button style={btnSecondary} onClick={() => setEditOpen(false)}>Cancel</button>
              <button style={btnPrimary} onClick={() => putMut.mutate()} disabled={!newValue || putMut.isPending}>
                {putMut.isPending ? 'Saving…' : 'Save'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

const card: React.CSSProperties = {
  border: '1px solid #e7e9ec', borderRadius: 8, padding: '1.25rem', background: '#fff',
}
const btnBack: React.CSSProperties = {
  background: 'transparent', color: '#5f6b7a', border: 'none', cursor: 'pointer', fontSize: '0.9em', padding: 0,
}
const btnPrimary: React.CSSProperties = {
  background: '#e87600', color: '#fff', border: '1px solid #e87600',
  borderRadius: 6, padding: '0.3rem 0.75rem', cursor: 'pointer', fontSize: '0.85em', fontWeight: 500,
}
const btnSecondary: React.CSSProperties = {
  background: 'transparent', color: '#5f6b7a', border: '1px solid #ccc',
  borderRadius: 6, padding: '0.3rem 0.75rem', cursor: 'pointer', fontSize: '0.85em',
}
const th: React.CSSProperties = {
  padding: '0.5rem 0.75rem', textAlign: 'left', fontWeight: 600, fontSize: '0.8em',
  color: '#5f6b7a', textTransform: 'uppercase',
}
const td: React.CSSProperties = { padding: '0.5rem 0.75rem', verticalAlign: 'middle' }
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
  marginBottom: '0.75rem', boxSizing: 'border-box',
}
