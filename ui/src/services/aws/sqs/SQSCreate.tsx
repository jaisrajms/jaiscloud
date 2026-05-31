import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { createQueue, type CreateQueueRequest } from '../../../api/sqs'

interface Props {
  onClose: () => void
  onCreated: () => void
}

export function SQSCreate({ onClose, onCreated }: Props) {
  const [name, setName] = useState('')
  const [type, setType] = useState<'Standard' | 'FIFO'>('Standard')
  const [visibility, setVisibility] = useState(30)
  const [retention, setRetention] = useState(345600)
  const [dlqArn, setDlqArn] = useState('')
  const [dlqMaxReceive, setDlqMaxReceive] = useState(3)
  const [tagKey, setTagKey] = useState('')
  const [tagVal, setTagVal] = useState('')
  const [tags, setTags] = useState<Record<string, string>>({})

  const mut = useMutation({
    mutationFn: (req: CreateQueueRequest) => createQueue(req),
    onSuccess: () => onCreated(),
  })

  function addTag() {
    if (tagKey.trim()) {
      setTags((t) => ({ ...t, [tagKey.trim()]: tagVal }))
      setTagKey('')
      setTagVal('')
    }
  }

  function removeTag(k: string) {
    setTags((t) => { const n = { ...t }; delete n[k]; return n })
  }

  function submit(e: React.FormEvent) {
    e.preventDefault()
    const queueName = type === 'FIFO' && !name.endsWith('.fifo') ? `${name}.fifo` : name
    const req: CreateQueueRequest = {
      name: queueName,
      type,
      visibilityTimeout: visibility,
      retentionPeriod: retention,
      ...(dlqArn ? { dlqArn, dlqMaxReceive } : {}),
      ...(Object.keys(tags).length > 0 ? { tags } : {}),
    }
    mut.mutate(req)
  }

  return (
    <div style={overlay}>
      <div style={dialog} onClick={(e) => e.stopPropagation()}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.25rem' }}>
          <h3 style={{ margin: 0, fontSize: '1.1rem' }}>Create queue</h3>
          <button onClick={onClose} style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: '1.1rem', color: '#5f6b7a', lineHeight: 1 }}>✕</button>
        </div>

        <form onSubmit={submit}>
          <label style={labelStyle}>
            Queue name
            <input
              required
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder={type === 'FIFO' ? 'my-queue  (.fifo appended automatically)' : 'my-queue'}
              style={inputStyle}
            />
            {type === 'FIFO' && name && !name.endsWith('.fifo') && (
              <span style={{ fontSize: '0.78em', color: '#5f6b7a' }}>
                Will be created as <strong>{name}.fifo</strong>
              </span>
            )}
          </label>

          <label style={labelStyle}>
            Type
            <select value={type} onChange={(e) => setType(e.target.value as 'Standard' | 'FIFO')} style={inputStyle}>
              <option value="Standard">Standard</option>
              <option value="FIFO">FIFO</option>
            </select>
          </label>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem' }}>
            <label style={labelStyle}>
              Visibility timeout (s)
              <input type="number" min={0} max={43200} value={visibility}
                onChange={(e) => setVisibility(Number(e.target.value))} style={inputStyle} />
            </label>
            <label style={labelStyle}>
              Retention period (s)
              <input type="number" min={60} max={1209600} value={retention}
                onChange={(e) => setRetention(Number(e.target.value))} style={inputStyle} />
            </label>
          </div>

          <details style={{ marginTop: '0.75rem' }}>
            <summary style={{ cursor: 'pointer', color: '#0972d3', fontSize: '0.9em', marginBottom: '0.75rem' }}>
              Dead-letter queue (optional)
            </summary>
            <label style={labelStyle}>
              DLQ ARN
              <input value={dlqArn} onChange={(e) => setDlqArn(e.target.value)}
                placeholder="arn:aws:sqs:us-east-1:000000000000:my-dlq" style={inputStyle} />
            </label>
            {dlqArn && (
              <label style={labelStyle}>
                Max receive count
                <input type="number" min={1} max={1000} value={dlqMaxReceive}
                  onChange={(e) => setDlqMaxReceive(Number(e.target.value))} style={inputStyle} />
              </label>
            )}
          </details>

          <details style={{ marginTop: '0.75rem' }}>
            <summary style={{ cursor: 'pointer', color: '#0972d3', fontSize: '0.9em', marginBottom: '0.75rem' }}>
              Tags (optional)
            </summary>
            <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.5rem', alignItems: 'flex-end' }}>
              <input placeholder="Key" value={tagKey} onChange={(e) => setTagKey(e.target.value)}
                style={{ ...inputStyle, flex: 1, marginBottom: 0 }} />
              <input placeholder="Value" value={tagVal} onChange={(e) => setTagVal(e.target.value)}
                style={{ ...inputStyle, flex: 1, marginBottom: 0 }} />
              <button type="button" onClick={addTag} style={btnSecondary}>Add</button>
            </div>
            {Object.entries(tags).map(([k, v]) => (
              <div key={k} style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', fontSize: '0.85em', marginBottom: '0.3rem' }}>
                <code style={{ background: '#f4f5f7', padding: '0.2em 0.5em', borderRadius: 3 }}>{k}</code>
                <span style={{ color: '#5f6b7a' }}>=</span>
                <code style={{ background: '#f4f5f7', padding: '0.2em 0.5em', borderRadius: 3 }}>{v}</code>
                <button type="button" onClick={() => removeTag(k)}
                  style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#d13212', fontSize: '0.85em', padding: '0 0.25rem' }}>
                  Remove
                </button>
              </div>
            ))}
          </details>

          {mut.error && (
            <p style={{ color: '#d13212', margin: '1rem 0 0', fontSize: '0.9em' }}>
              {(mut.error as Error).message}
            </p>
          )}

          <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1.5rem' }}>
            <button type="button" onClick={onClose} style={btnSecondary}>Cancel</button>
            <button type="submit" disabled={mut.isPending}
              style={{ ...btnPrimary, opacity: mut.isPending ? 0.6 : 1 }}>
              {mut.isPending ? 'Creating…' : 'Create queue'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

const overlay: React.CSSProperties = {
  position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)',
  display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
}
const dialog: React.CSSProperties = {
  background: '#fff', borderRadius: 8, padding: '1.5rem',
  width: 520, maxHeight: '90vh', overflowY: 'auto',
  boxShadow: '0 4px 24px rgba(0,0,0,0.15)',
}
const labelStyle: React.CSSProperties = {
  display: 'flex', flexDirection: 'column', gap: '0.3rem',
  marginBottom: '0.75rem', fontSize: '0.9em', fontWeight: 500, color: '#16191f',
}
const inputStyle: React.CSSProperties = {
  border: '1px solid #c9cdd4', borderRadius: 4,
  padding: '0.4rem 0.6rem', fontSize: '0.9em',
  width: '100%', boxSizing: 'border-box', outline: 'none',
}
const btnSecondary: React.CSSProperties = {
  background: '#fff', border: '1px solid #c9cdd4',
  borderRadius: 4, padding: '0.45rem 1rem',
  cursor: 'pointer', fontSize: '0.9em', whiteSpace: 'nowrap',
}
const btnPrimary: React.CSSProperties = {
  background: '#e77600', color: '#fff', border: 'none',
  borderRadius: 4, padding: '0.5rem 1.25rem',
  cursor: 'pointer', fontSize: '0.9em', fontWeight: 500,
}
