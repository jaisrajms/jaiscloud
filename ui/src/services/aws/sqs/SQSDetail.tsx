import { Fragment, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getQueue, purgeQueue, listDLQSources, getTags, peekMessages, type PeekedMessage } from '../../../api/sqs'
import { SQSMessageSend } from './SQSMessageSend'

type Tab = 'overview' | 'messages' | 'dlq' | 'tags'

const TAB_LABELS: Record<Tab, string> = {
  overview: 'Overview',
  messages: 'Messages',
  dlq: 'Dead-letter queue',
  tags: 'Tags',
}

export function SQSDetail() {
  const { queueUrl: rawParam } = useParams<{ queueUrl: string }>()
  const queueUrl = rawParam ? decodeURIComponent(rawParam) : ''
  const [tab, setTab] = useState<Tab>('overview')
  const [msgPage, setMsgPage] = useState(0)
  const [expandedMsgId, setExpandedMsgId] = useState<string | null>(null)
  const [showSend, setShowSend] = useState(false)
  const [purgeConfirm, setPurgeConfirm] = useState(false)
  const PAGE_SIZE = 50
  const qc = useQueryClient()

  const { data: queue, isLoading, error } = useQuery({
    queryKey: ['sqs', 'queue', queueUrl],
    queryFn: () => getQueue(queueUrl),
    enabled: !!queueUrl,
  })

  const { data: dlqSources } = useQuery({
    queryKey: ['sqs', 'queue', queueUrl, 'dlq-sources'],
    queryFn: () => listDLQSources(queueUrl),
    enabled: tab === 'dlq' && !!queueUrl,
  })

  const { data: tags } = useQuery({
    queryKey: ['sqs', 'queue', queueUrl, 'tags'],
    queryFn: () => getTags(queueUrl),
    enabled: tab === 'tags' && !!queueUrl,
  })

  const { data: peekData, isFetching: peekFetching, refetch: refetchPeek } = useQuery({
    queryKey: ['sqs', 'queue', queueUrl, 'peek', msgPage],
    queryFn: () => peekMessages(queueUrl, { offset: msgPage * PAGE_SIZE, limit: PAGE_SIZE }),
    enabled: tab === 'messages' && !!queueUrl,
  })

  const purgeMut = useMutation({
    mutationFn: () => purgeQueue(queueUrl),
    onSuccess: () => {
      setPurgeConfirm(false)
      void qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] })
    },
  })

  if (isLoading) {
    return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading…</div>
  }

  if (error || !queue) {
    return (
      <div>
        <Link to="/aws/sqs" style={{ color: '#0972d3', fontSize: '0.9em', textDecoration: 'none' }}>← Queues</Link>
        <p style={{ color: '#d13212' }}>{error ? (error as Error).message : 'Queue not found.'}</p>
      </div>
    )
  }

  const overviewRows: [string, string][] = [
    ['URL', queue.url],
    ['ARN', queue.arn],
    ['Type', queue.type],
    ['Messages available', queue.messagesAvailable.toLocaleString()],
    ['Messages in flight', queue.messagesInFlight.toLocaleString()],
    ['Visibility timeout', `${queue.visibilityTimeout}s`],
    ['Message retention', `${queue.retentionPeriod}s`],
    ['Max message size', `${Math.round(queue.maxMessageSize / 1024)} KB`],
    ['Dead-letter queue', queue.dlqArn || '—'],
    ['Max receive count', queue.dlqMaxReceive ? String(queue.dlqMaxReceive) : '—'],
    ['Created', queue.createdAt ? new Date(queue.createdAt).toLocaleString() : '—'],
  ]

  return (
    <div>
      {/* Breadcrumb + header */}
      <div style={{ marginBottom: '1.5rem' }}>
        <Link to="/aws/sqs" style={{ color: '#0972d3', fontSize: '0.85em', textDecoration: 'none' }}>
          ← SQS Queues
        </Link>
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginTop: '0.5rem', gap: '1rem', flexWrap: 'wrap' }}>
          <div>
            <h2 style={{ margin: '0 0 0.4rem', fontSize: '1.4rem', fontWeight: 600 }}>{queue.name}</h2>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
              <span style={{
                background: queue.type === 'FIFO' ? '#e0f0ff' : '#f4f5f7',
                color: queue.type === 'FIFO' ? '#0972d3' : '#5f6b7a',
                padding: '0.15em 0.55em', borderRadius: 3, fontSize: '0.8em', fontWeight: 500,
              }}>
                {queue.type}
              </span>
              <code style={{ fontSize: '0.75em', color: '#8d9daa' }}>{queue.arn}</code>
            </div>
          </div>
          <button
            onClick={() => setPurgeConfirm(true)}
            style={{ background: 'none', border: '1px solid #e77600', color: '#e77600', borderRadius: 4, padding: '0.4rem 0.9rem', cursor: 'pointer', fontSize: '0.85em', flexShrink: 0 }}
          >
            Purge queue
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: 'flex', borderBottom: '2px solid #e7e9ec', marginBottom: '1.5rem' }}>
        {(Object.keys(TAB_LABELS) as Tab[]).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            style={{
              background: 'none', border: 'none', padding: '0.6rem 1.25rem', cursor: 'pointer',
              fontSize: '0.9em', fontWeight: tab === t ? 600 : 400,
              color: tab === t ? '#e77600' : '#5f6b7a',
              borderBottom: `2px solid ${tab === t ? '#e77600' : 'transparent'}`,
              marginBottom: -2, whiteSpace: 'nowrap',
            }}
          >
            {TAB_LABELS[t]}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === 'overview' && (
        <dl style={{ display: 'grid', gridTemplateColumns: 'max-content 1fr', gap: '0 2rem', margin: 0, fontSize: '0.9em' }}>
          {overviewRows.map(([label, value]) => (
            <Fragment key={label}>
              <dt style={{ color: '#5f6b7a', fontWeight: 500, padding: '0.5rem 0', borderBottom: '1px solid #f4f5f7', whiteSpace: 'nowrap' }}>
                {label}
              </dt>
              <dd style={{ margin: 0, padding: '0.5rem 0', borderBottom: '1px solid #f4f5f7', wordBreak: 'break-all', color: '#16191f' }}>
                {value}
              </dd>
            </Fragment>
          ))}
        </dl>
      )}

      {tab === 'messages' && (
        <div>
          {/* Toolbar */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem', flexWrap: 'wrap' }}>
            <span style={{ fontSize: '0.88em', color: '#5f6b7a' }}>
              {peekData ? `${peekData.total.toLocaleString()} message${peekData.total !== 1 ? 's' : ''}` : '—'}
            </span>
            <button
              onClick={() => { setMsgPage(0); void refetchPeek() }}
              disabled={peekFetching}
              style={{ ...btnSmall, marginLeft: 'auto', opacity: peekFetching ? 0.6 : 1 }}
            >
              {peekFetching ? 'Loading…' : '↻ Refresh'}
            </button>
            <button
              onClick={() => setShowSend((s) => !s)}
              style={{ ...btnSmall, borderColor: '#e77600', color: '#e77600' }}
            >
              {showSend ? 'Hide send form' : '+ Send message'}
            </button>
          </div>

          {/* Optional send form */}
          {showSend && (
            <div style={{ marginBottom: '1.25rem' }}>
              <SQSMessageSend
                queueUrl={queueUrl}
                isFifo={queue.type === 'FIFO'}
                onSent={() => { void refetchPeek() }}
              />
            </div>
          )}

          {/* Messages table */}
          {!peekData || peekData.messages.length === 0 ? (
            <p style={{ color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }}>
              {peekFetching ? 'Loading messages…' : 'No messages in this queue.'}
            </p>
          ) : (
            <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.88em' }}>
                <thead>
                  <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                    <th style={th}>Status</th>
                    <th style={th}>Message ID</th>
                    <th style={th}>Body</th>
                    <th style={{ ...th, textAlign: 'right' }}>Rcv</th>
                    <th style={th}>Sent At</th>
                    {queue.type === 'FIFO' && <th style={th}>Group</th>}
                  </tr>
                </thead>
                <tbody>
                  {peekData.messages.map((m: PeekedMessage) => (
                    <Fragment key={m.messageId}>
                      <tr
                        style={{ borderBottom: expandedMsgId === m.messageId ? 'none' : '1px solid #e7e9ec', cursor: 'pointer' }}
                        onClick={() => setExpandedMsgId(expandedMsgId === m.messageId ? null : m.messageId)}
                        onMouseEnter={(e) => (e.currentTarget.style.background = '#fafbfc')}
                        onMouseLeave={(e) => (e.currentTarget.style.background = '')}
                      >
                        <td style={td}>
                          <span style={{
                            display: 'inline-block', padding: '0.1em 0.5em', borderRadius: 3,
                            fontSize: '0.8em', fontWeight: 500,
                            background: m.status === 'visible' ? '#d1fae5' : m.status === 'in-flight' ? '#fef3c7' : '#e0f2fe',
                            color: m.status === 'visible' ? '#065f46' : m.status === 'in-flight' ? '#92400e' : '#0369a1',
                          }}>
                            {m.status}
                          </span>
                        </td>
                        <td style={{ ...td, fontFamily: 'monospace', fontSize: '0.8em', color: '#5f6b7a', maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                          {m.messageId}
                        </td>
                        <td style={{ ...td, maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontFamily: 'monospace', fontSize: '0.82em' }}>
                          {m.body}
                        </td>
                        <td style={{ ...td, textAlign: 'right', color: '#5f6b7a' }}>{m.receiveCount}</td>
                        <td style={{ ...td, color: '#5f6b7a', whiteSpace: 'nowrap' }}>
                          {m.sentAt ? new Date(m.sentAt).toLocaleString() : '—'}
                        </td>
                        {queue.type === 'FIFO' && <td style={{ ...td, fontFamily: 'monospace', fontSize: '0.82em' }}>{m.groupId ?? '—'}</td>}
                      </tr>
                      {expandedMsgId === m.messageId && (
                        <tr style={{ borderBottom: '1px solid #e7e9ec' }}>
                          <td colSpan={queue.type === 'FIFO' ? 6 : 5} style={{ padding: '0 1rem 0.75rem' }}>
                            <pre style={{
                              margin: 0, padding: '0.75rem', background: '#1e1e1e', color: '#d4d4d4',
                              borderRadius: 6, overflow: 'auto', fontSize: '0.82em',
                              whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: 300,
                            }}>
                              {(() => { try { return JSON.stringify(JSON.parse(m.body), null, 2) } catch { return m.body } })()}
                            </pre>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  ))}
                </tbody>
              </table>

              {/* Pagination */}
              {peekData.total > PAGE_SIZE && (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0.6rem 1rem', background: '#f4f5f7', borderTop: '1px solid #e7e9ec', fontSize: '0.85em' }}>
                  <span style={{ color: '#5f6b7a' }}>
                    {msgPage * PAGE_SIZE + 1}–{Math.min((msgPage + 1) * PAGE_SIZE, peekData.total)} of {peekData.total.toLocaleString()}
                  </span>
                  <div style={{ display: 'flex', gap: '0.5rem' }}>
                    <button onClick={() => { setMsgPage((p) => p - 1); setExpandedMsgId(null) }} disabled={msgPage === 0} style={btnSmall}>
                      ← Prev
                    </button>
                    <button onClick={() => { setMsgPage((p) => p + 1); setExpandedMsgId(null) }} disabled={(msgPage + 1) * PAGE_SIZE >= peekData.total} style={btnSmall}>
                      Next →
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {tab === 'dlq' && (
        <div>
          {queue.dlqArn ? (
            <div style={{ marginBottom: '1.5rem', padding: '1rem', background: '#f4f5f7', borderRadius: 6, fontSize: '0.9em' }}>
              <div style={{ fontWeight: 500, marginBottom: '0.5rem', color: '#5f6b7a' }}>Dead-letter queue ARN</div>
              <code style={{ wordBreak: 'break-all' }}>{queue.dlqArn}</code>
              {queue.dlqMaxReceive && (
                <div style={{ marginTop: '0.5rem', color: '#5f6b7a' }}>
                  Max receive count: <strong>{queue.dlqMaxReceive}</strong>
                </div>
              )}
            </div>
          ) : (
            <p style={{ color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }}>
              No dead-letter queue configured.
            </p>
          )}

          {dlqSources && dlqSources.items.length > 0 && (
            <div>
              <h4 style={{ margin: '0 0 0.75rem', fontSize: '0.95rem', fontWeight: 600, color: '#16191f' }}>
                Source queues using this queue as DLQ
              </h4>
              <div style={{ border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
                  <thead>
                    <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                      <th style={th}>Name</th>
                      <th style={th}>Type</th>
                      <th style={{ ...th, textAlign: 'right' }}>Max Receive Count</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dlqSources.items.map((q) => (
                      <tr key={q.url} style={{ borderBottom: '1px solid #e7e9ec' }}>
                        <td style={td}>
                          <Link to={`/aws/sqs/${encodeURIComponent(q.url)}`} style={{ color: '#0972d3', textDecoration: 'none' }}>
                            {q.name}
                          </Link>
                        </td>
                        <td style={td}>{q.type}</td>
                        <td style={{ ...td, textAlign: 'right' }}>{q.dlqMaxReceive ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {dlqSources && dlqSources.items.length === 0 && (
            <p style={{ color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }}>
              No queues are using this queue as their dead-letter queue.
            </p>
          )}
        </div>
      )}

      {tab === 'tags' && (
        <div>
          {!tags || Object.keys(tags).length === 0 ? (
            <p style={{ color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }}>No tags on this queue.</p>
          ) : (
            <div style={{ border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden' }}>
              <table style={{ borderCollapse: 'collapse', fontSize: '0.9em', width: '100%' }}>
                <thead>
                  <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                    <th style={th}>Key</th>
                    <th style={th}>Value</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(tags).map(([k, v]) => (
                    <tr key={k} style={{ borderBottom: '1px solid #e7e9ec' }}>
                      <td style={td}><code style={{ background: '#f4f5f7', padding: '0.2em 0.5em', borderRadius: 3 }}>{k}</code></td>
                      <td style={td}>{v}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* Purge confirm dialog */}
      {purgeConfirm && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: '1.05rem' }}>Purge queue?</h3>
            <p style={{ margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              All messages in <strong>{queue.name}</strong> will be permanently deleted. This action cannot be undone.
            </p>
            {purgeMut.error && (
              <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>
                {(purgeMut.error as Error).message}
              </p>
            )}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setPurgeConfirm(false)} style={btnSecondary}>Cancel</button>
              <button
                onClick={() => purgeMut.mutate()}
                disabled={purgeMut.isPending}
                style={{ background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em', opacity: purgeMut.isPending ? 0.6 : 1 }}
              >
                {purgeMut.isPending ? 'Purging…' : 'Purge'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

const th: React.CSSProperties = { padding: '0.55rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' }
const td: React.CSSProperties = { padding: '0.7rem 1rem' }
const btnSmall: React.CSSProperties = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.3rem 0.75rem', cursor: 'pointer', fontSize: '0.83em' }
const btnSecondary: React.CSSProperties = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const dialogStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 460, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' }
