import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { receiveMessages, deleteMessage, type Message } from '../../../api/sqs'

interface Props {
  queueUrl: string
}

function tryPrettyJson(s: string): string {
  try { return JSON.stringify(JSON.parse(s), null, 2) } catch { return s }
}

export function SQSMessageReceive({ queueUrl }: Props) {
  const [messages, setMessages] = useState<Message[]>([])
  const [maxMessages, setMaxMessages] = useState(10)
  const [expandedId, setExpandedId] = useState<string | null>(null)
  const qc = useQueryClient()

  const receiveMut = useMutation({
    mutationFn: () => receiveMessages(queueUrl, { maxMessages }),
    onSuccess: (data) => {
      setMessages(data.messages ?? [])
      qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] })
    },
  })

  const deleteMut = useMutation({
    mutationFn: (receipt: string) => deleteMessage(queueUrl, receipt),
    onSuccess: (_, receipt) => {
      setMessages((prev) => prev.filter((m) => m.receiptHandle !== receipt))
      qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] })
    },
  })

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
        <h4 style={{ margin: 0, fontSize: '1rem', fontWeight: 600 }}>Receive messages</h4>
        <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.9em', marginLeft: 'auto' }}>
          Max:
          <input
            type="number" min={1} max={10} value={maxMessages}
            onChange={(e) => setMaxMessages(Number(e.target.value))}
            style={{ width: 56, border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.3rem 0.5rem', fontSize: '0.9em' }}
          />
        </label>
        <button
          onClick={() => receiveMut.mutate()}
          disabled={receiveMut.isPending}
          style={{
            background: '#0972d3', color: '#fff', border: 'none',
            borderRadius: 4, padding: '0.4rem 1rem',
            cursor: 'pointer', fontSize: '0.9em',
            opacity: receiveMut.isPending ? 0.6 : 1,
          }}
        >
          {receiveMut.isPending ? 'Polling…' : 'Poll for messages'}
        </button>
      </div>

      {receiveMut.error && (
        <p style={{ color: '#d13212', fontSize: '0.85em', margin: '0 0 0.75rem' }}>
          {(receiveMut.error as Error).message}
        </p>
      )}

      {receiveMut.isSuccess && messages.length === 0 && (
        <p style={{ color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }}>No messages available in the queue.</p>
      )}

      {messages.length > 0 && (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden' }}>
          {messages.map((m, i) => (
            <div key={m.messageId}
              style={{ borderBottom: i < messages.length - 1 ? '1px solid #e7e9ec' : undefined, padding: '0.75rem 1rem' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem', flexWrap: 'wrap' }}>
                <code style={{ fontSize: '0.78em', color: '#5f6b7a', flexShrink: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 300 }}>
                  {m.messageId}
                </code>
                {m.sentAt && (
                  <span style={{ fontSize: '0.75em', color: '#8d9daa', flexShrink: 0 }}>
                    {new Date(m.sentAt).toLocaleString()}
                  </span>
                )}
                <div style={{ display: 'flex', gap: '0.4rem', marginLeft: 'auto', flexShrink: 0 }}>
                  <button
                    onClick={() => setExpandedId(expandedId === m.messageId ? null : m.messageId)}
                    style={btnSmall}
                  >
                    {expandedId === m.messageId ? 'Collapse' : 'View body'}
                  </button>
                  <button
                    onClick={() => deleteMut.mutate(m.receiptHandle)}
                    disabled={deleteMut.isPending}
                    style={{ ...btnSmall, borderColor: '#d13212', color: '#d13212' }}
                  >
                    Delete
                  </button>
                </div>
              </div>

              {expandedId === m.messageId ? (
                <pre style={{
                  margin: 0, padding: '0.75rem', background: '#f4f5f7',
                  borderRadius: 4, overflow: 'auto', fontSize: '0.82em',
                  whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: 300,
                }}>
                  {tryPrettyJson(m.body)}
                </pre>
              ) : (
                <div style={{
                  fontSize: '0.85em', color: '#16191f',
                  overflow: 'hidden', textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap', fontFamily: 'monospace',
                }}>
                  {m.body}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

const btnSmall: React.CSSProperties = {
  background: 'none', border: '1px solid #c9cdd4',
  borderRadius: 4, padding: '0.2rem 0.6rem',
  cursor: 'pointer', fontSize: '0.8em',
}
