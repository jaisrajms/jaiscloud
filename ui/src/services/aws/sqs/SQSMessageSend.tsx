import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { sendMessage, type SendMessageRequest } from '../../../api/sqs'

interface Props {
  queueUrl: string
  isFifo: boolean
  onSent?: () => void
}

export function SQSMessageSend({ queueUrl, isFifo, onSent }: Props) {
  const [body, setBody] = useState('')
  const [delay, setDelay] = useState(0)
  const [groupId, setGroupId] = useState('')
  const [dedupId, setDedupId] = useState('')
  const [lastMsgId, setLastMsgId] = useState<string | null>(null)
  const qc = useQueryClient()

  const mut = useMutation({
    mutationFn: (req: SendMessageRequest) => sendMessage(queueUrl, req),
    onSuccess: (data) => {
      setLastMsgId(data.messageId)
      setBody('')
      qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] })
      onSent?.()
    },
    onError: () => setLastMsgId(null),
  })

  function submit(e: React.FormEvent) {
    e.preventDefault()
    const req: SendMessageRequest = {
      body,
      ...(delay > 0 ? { delaySeconds: delay } : {}),
      ...(isFifo && groupId ? { messageGroupId: groupId } : {}),
      ...(isFifo && dedupId ? { messageDeduplicationId: dedupId } : {}),
    }
    mut.mutate(req)
  }

  return (
    <div style={{ background: '#f4f5f7', borderRadius: 8, padding: '1.25rem', marginBottom: '1.5rem' }}>
      <h4 style={{ margin: '0 0 1rem', fontSize: '1rem', fontWeight: 600 }}>Send message</h4>
      <form onSubmit={submit}>
        <label style={labelStyle}>
          Message body
          <textarea
            required
            value={body}
            onChange={(e) => setBody(e.target.value)}
            rows={5}
            style={{ ...inputStyle, fontFamily: 'monospace', resize: 'vertical', fontSize: '0.85em' }}
            placeholder='{"key": "value"}'
          />
        </label>

        <div style={{ display: 'grid', gridTemplateColumns: isFifo ? '1fr 1fr 1fr' : '160px', gap: '0.75rem' }}>
          <label style={labelStyle}>
            Delay (s)
            <input type="number" min={0} max={900} value={delay}
              onChange={(e) => setDelay(Number(e.target.value))} style={inputStyle} />
          </label>
          {isFifo && (
            <>
              <label style={labelStyle}>
                Message group ID
                <input value={groupId} onChange={(e) => setGroupId(e.target.value)}
                  style={inputStyle} placeholder="required for FIFO" />
              </label>
              <label style={labelStyle}>
                Deduplication ID
                <input value={dedupId} onChange={(e) => setDedupId(e.target.value)}
                  style={inputStyle} placeholder="leave blank for content-based" />
              </label>
            </>
          )}
        </div>

        {mut.error && (
          <p style={{ color: '#d13212', margin: '0.5rem 0', fontSize: '0.85em' }}>
            {(mut.error as Error).message}
          </p>
        )}

        {lastMsgId && !mut.isPending && (
          <p style={{ color: '#1d8102', margin: '0.5rem 0', fontSize: '0.85em' }}>
            Sent — MessageId: <code style={{ background: '#fff', padding: '0.1em 0.4em', borderRadius: 3 }}>{lastMsgId}</code>
          </p>
        )}

        <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: '0.75rem' }}>
          <button type="submit" disabled={mut.isPending}
            style={{ ...btnPrimary, opacity: mut.isPending ? 0.6 : 1 }}>
            {mut.isPending ? 'Sending…' : 'Send message'}
          </button>
        </div>
      </form>
    </div>
  )
}

const labelStyle: React.CSSProperties = {
  display: 'flex', flexDirection: 'column', gap: '0.3rem',
  marginBottom: '0.75rem', fontSize: '0.9em', fontWeight: 500,
}
const inputStyle: React.CSSProperties = {
  border: '1px solid #c9cdd4', borderRadius: 4,
  padding: '0.4rem 0.6rem', fontSize: '0.9em',
  width: '100%', boxSizing: 'border-box',
}
const btnPrimary: React.CSSProperties = {
  background: '#e77600', color: '#fff', border: 'none',
  borderRadius: 4, padding: '0.5rem 1.25rem',
  cursor: 'pointer', fontSize: '0.9em', fontWeight: 500,
}
