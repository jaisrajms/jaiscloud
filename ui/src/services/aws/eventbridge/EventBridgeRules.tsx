import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  listRules,
  putRule,
  deleteRule,
  enableRule,
  disableRule,
  listTargets,
  putTargets,
  removeTarget,
  type Rule,
  type Target,
} from '../../../api/eventbridge'
import { EmptyState } from '../../../components/EmptyState'

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 1rem', verticalAlign: 'middle' }
const btnStyle: React.CSSProperties = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 440, maxWidth: 580 }

const stateColor = (s?: string) => s === 'ENABLED' ? '#2ecc71' : '#e74c3c'

export function EventBridgeRules() {
  const qc = useQueryClient()
  const [selectedRule, setSelectedRule] = useState<Rule | null>(null)
  const [createOpen, setCreateOpen] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<Rule | null>(null)
  const [addTargetOpen, setAddTargetOpen] = useState(false)
  const [form, setForm] = useState({ name: '', eventPattern: '', scheduleExpression: '', state: 'ENABLED', description: '' })
  const [targetForm, setTargetForm] = useState({ id: '', arn: '' })

  const { data, isLoading } = useQuery({
    queryKey: ['eventbridge', 'rules'],
    queryFn: () => listRules(),
  })

  const { data: targetsData } = useQuery({
    queryKey: ['eventbridge', 'targets', selectedRule?.name],
    queryFn: () => listTargets(selectedRule!.name),
    enabled: !!selectedRule,
  })

  const createMut = useMutation({
    mutationFn: () => putRule({ name: form.name, eventPattern: form.eventPattern || undefined, scheduleExpression: form.scheduleExpression || undefined, state: form.state, description: form.description || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['eventbridge', 'rules'] })
      setCreateOpen(false)
      setForm({ name: '', eventPattern: '', scheduleExpression: '', state: 'ENABLED', description: '' })
    },
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteRule(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['eventbridge', 'rules'] })
      if (deleteTarget?.name === selectedRule?.name) setSelectedRule(null)
      setDeleteTarget(null)
    },
  })

  const toggleMut = useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      enabled ? disableRule(name) : enableRule(name),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['eventbridge', 'rules'] }) },
  })

  const addTargetMut = useMutation({
    mutationFn: () => putTargets(selectedRule!.name, [{ id: targetForm.id, arn: targetForm.arn }]),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['eventbridge', 'targets', selectedRule?.name] })
      setAddTargetOpen(false)
      setTargetForm({ id: '', arn: '' })
    },
  })

  const removeTargetMut = useMutation({
    mutationFn: ({ rule, id }: { rule: string; id: string }) => removeTarget(rule, id),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['eventbridge', 'targets', selectedRule?.name] }) },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading rules…</div>

  const rules = data?.items ?? []
  const targets: Target[] = targetsData?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>EventBridge Rules</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{rules.length} rule{rules.length !== 1 ? 's' : ''}</span>
        </div>
        <button style={btnStyle} onClick={() => setCreateOpen(true)}>Create Rule</button>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: selectedRule ? '1fr 1fr' : '1fr', gap: '1.5rem' }}>
        <div>
          {rules.length === 0 ? (
            <EmptyState title="No rules. Create one to route events to targets." />
          ) : (
            <table style={tableStyle}>
              <thead>
                <tr>{['Name', 'State', 'Pattern / Schedule', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {rules.map(rule => (
                  <tr
                    key={rule.name}
                    style={{ borderBottom: '1px solid #2d3748', cursor: 'pointer', background: selectedRule?.name === rule.name ? '#1e2d3d' : 'transparent' }}
                    onClick={() => setSelectedRule(rule)}
                  >
                    <td style={{ ...tdStyle, fontWeight: 600 }}>{rule.name}</td>
                    <td style={tdStyle}>
                      <span style={{ color: stateColor(rule.state), fontSize: '0.8rem', fontWeight: 600 }}>{rule.state ?? '—'}</span>
                    </td>
                    <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.8rem', fontFamily: 'monospace', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {rule.scheduleExpression || rule.eventPattern || '—'}
                    </td>
                    <td style={{ ...tdStyle, textAlign: 'right', whiteSpace: 'nowrap' }} onClick={e => e.stopPropagation()}>
                      <button
                        style={{ ...btnStyle, background: 'transparent', color: rule.state === 'ENABLED' ? '#e87600' : '#2ecc71', border: `1px solid ${rule.state === 'ENABLED' ? '#e87600' : '#2ecc71'}`, padding: '0.25rem 0.6rem', fontSize: '0.78rem', marginRight: '0.4rem' }}
                        onClick={() => toggleMut.mutate({ name: rule.name, enabled: rule.state === 'ENABLED' })}
                      >
                        {rule.state === 'ENABLED' ? 'Disable' : 'Enable'}
                      </button>
                      <button style={{ ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.78rem' }} onClick={() => setDeleteTarget(rule)}>Delete</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {selectedRule && (
          <div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
              <h3 style={{ margin: 0, fontWeight: 600, fontSize: '1rem' }}>Targets for <em>{selectedRule.name}</em> ({targets.length})</h3>
              <div style={{ display: 'flex', gap: '0.5rem' }}>
                <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0', padding: '0.3rem 0.6rem', fontSize: '0.8rem' }} onClick={() => setSelectedRule(null)}>✕</button>
                <button style={{ ...btnStyle, fontSize: '0.8rem', padding: '0.3rem 0.75rem' }} onClick={() => setAddTargetOpen(true)}>Add Target</button>
              </div>
            </div>
            {targets.length === 0 ? (
              <EmptyState title="No targets for this rule." />
            ) : (
              <table style={tableStyle}>
                <thead>
                  <tr>{['ID', 'ARN', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
                </thead>
                <tbody>
                  {targets.map(t => (
                    <tr key={t.id} style={{ borderBottom: '1px solid #2d3748' }}>
                      <td style={{ ...tdStyle, fontWeight: 600 }}>{t.id}</td>
                      <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.8rem', fontFamily: 'monospace' }}>{t.arn}</td>
                      <td style={{ ...tdStyle, textAlign: 'right' }}>
                        <button style={{ ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.78rem' }} onClick={() => removeTargetMut.mutate({ rule: selectedRule.name, id: t.id })}>Remove</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}
      </div>

      {createOpen && (
        <div style={overlayStyle} onClick={() => setCreateOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Create Rule</h3>
            {[
              { key: 'name', label: 'Name *', placeholder: 'my-rule' },
              { key: 'eventPattern', label: 'Event Pattern (JSON)', placeholder: '{"source":["aws.ec2"]}' },
              { key: 'scheduleExpression', label: 'Schedule Expression', placeholder: 'rate(5 minutes)' },
              { key: 'description', label: 'Description', placeholder: '' },
            ].map(f => (
              <div key={f.key} style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }}>
                <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>{f.label}</label>
                <input style={inputStyle} placeholder={f.placeholder} value={(form as Record<string, string>)[f.key]} onChange={e => setForm(p => ({ ...p, [f.key]: e.target.value }))} />
              </div>
            ))}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1.5rem' }}>
              <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>State</label>
              <select style={inputStyle} value={form.state} onChange={e => setForm(p => ({ ...p, state: e.target.value }))}>
                <option value="ENABLED">ENABLED</option>
                <option value="DISABLED">DISABLED</option>
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
            <h3 style={{ margin: '0 0 1rem', fontWeight: 600 }}>Delete Rule?</h3>
            <p style={{ color: '#b0bec5', marginBottom: '1.5rem' }}>Delete rule <strong>{deleteTarget.name}</strong>?</p>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setDeleteTarget(null)}>Cancel</button>
              <button style={{ ...btnStyle, background: '#d13212' }} disabled={deleteMut.isPending} onClick={() => deleteMut.mutate(deleteTarget.name)}>
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}

      {addTargetOpen && selectedRule && (
        <div style={overlayStyle} onClick={() => setAddTargetOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Add Target to {selectedRule.name}</h3>
            {[
              { key: 'id', label: 'Target ID *', placeholder: 'target-1' },
              { key: 'arn', label: 'ARN *', placeholder: 'arn:aws:sqs:us-east-1:000000000000:my-queue' },
            ].map(f => (
              <div key={f.key} style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }}>
                <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>{f.label}</label>
                <input style={inputStyle} placeholder={f.placeholder} value={(targetForm as Record<string, string>)[f.key]} onChange={e => setTargetForm(p => ({ ...p, [f.key]: e.target.value }))} />
              </div>
            ))}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setAddTargetOpen(false)}>Cancel</button>
              <button style={btnStyle} disabled={!targetForm.id || !targetForm.arn || addTargetMut.isPending} onClick={() => addTargetMut.mutate()}>
                {addTargetMut.isPending ? 'Adding…' : 'Add'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
