import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  listStateMachines,
  createStateMachine,
  deleteStateMachine,
  type StateMachine,
} from '../../../api/sfn'
import { EmptyState } from '../../../components/EmptyState'

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 1rem', verticalAlign: 'middle' }
const btnStyle: React.CSSProperties = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 440, maxWidth: 580 }

export function SFNStateMachines() {
  const qc = useQueryClient()
  const navigate = useNavigate()
  const [createOpen, setCreateOpen] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<StateMachine | null>(null)
  const [form, setForm] = useState({ name: '', definition: '', roleArn: '', type: 'STANDARD' })

  const { data, isLoading } = useQuery({
    queryKey: ['sfn', 'state-machines'],
    queryFn: () => listStateMachines(),
  })

  const createMut = useMutation({
    mutationFn: () => createStateMachine({ name: form.name, definition: form.definition || undefined, roleArn: form.roleArn || undefined, type: form.type || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sfn', 'state-machines'] })
      setCreateOpen(false)
      setForm({ name: '', definition: '', roleArn: '', type: 'STANDARD' })
    },
  })

  const deleteMut = useMutation({
    mutationFn: (arn: string) => deleteStateMachine(arn),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sfn', 'state-machines'] })
      setDeleteTarget(null)
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading state machines…</div>

  const machines = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>Step Functions</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{machines.length} state machine{machines.length !== 1 ? 's' : ''}</span>
        </div>
        <button style={btnStyle} onClick={() => setCreateOpen(true)}>Create State Machine</button>
      </div>

      {machines.length === 0 ? (
        <EmptyState title="No state machines. Create one to orchestrate workflows." />
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>{['Name', 'Type', 'Status', 'ARN', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
          </thead>
          <tbody>
            {machines.map(sm => (
              <tr
                key={sm.arn}
                style={{ borderBottom: '1px solid #2d3748', cursor: 'pointer' }}
                onClick={() => navigate(`executions?arn=${encodeURIComponent(sm.arn)}`)}
              >
                <td style={{ ...tdStyle, fontWeight: 600 }}>{sm.name}</td>
                <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }}>{sm.type || '—'}</td>
                <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }}>{sm.status || '—'}</td>
                <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.78rem', fontFamily: 'monospace', maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{sm.arn}</td>
                <td style={{ ...tdStyle, textAlign: 'right' }} onClick={e => e.stopPropagation()}>
                  <button style={{ ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }} onClick={() => setDeleteTarget(sm)}>Delete</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {createOpen && (
        <div style={overlayStyle} onClick={() => setCreateOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Create State Machine</h3>
            {[
              { key: 'name', label: 'Name *', placeholder: 'my-workflow' },
              { key: 'roleArn', label: 'Role ARN', placeholder: 'arn:aws:iam:::role/step-functions-role' },
              { key: 'definition', label: 'Definition (JSON, optional)', placeholder: '' },
            ].map(f => (
              <div key={f.key} style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }}>
                <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>{f.label}</label>
                <input style={inputStyle} placeholder={f.placeholder} value={(form as Record<string, string>)[f.key]} onChange={e => setForm(p => ({ ...p, [f.key]: e.target.value }))} />
              </div>
            ))}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1.5rem' }}>
              <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>Type</label>
              <select style={inputStyle} value={form.type} onChange={e => setForm(p => ({ ...p, type: e.target.value }))}>
                <option value="STANDARD">STANDARD</option>
                <option value="EXPRESS">EXPRESS</option>
              </select>
            </div>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setCreateOpen(false)}>Cancel</button>
              <button style={btnStyle} disabled={!form.name || createMut.isPending} onClick={() => createMut.mutate()}>
                {createMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {deleteTarget && (
        <div style={overlayStyle} onClick={() => setDeleteTarget(null)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem', fontWeight: 600 }}>Delete State Machine?</h3>
            <p style={{ color: '#b0bec5', marginBottom: '1.5rem' }}>Delete <strong>{deleteTarget.name}</strong>?</p>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setDeleteTarget(null)}>Cancel</button>
              <button style={{ ...btnStyle, background: '#d13212' }} disabled={deleteMut.isPending} onClick={() => deleteMut.mutate(deleteTarget.arn)}>
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
