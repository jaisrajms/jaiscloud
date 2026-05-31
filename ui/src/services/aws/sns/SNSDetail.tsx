import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getTopic, listSubscriptionsByTopic, subscribe, unsubscribe, publish, type Subscription } from '../../../api/sns'

export function SNSDetail() {
  const { topicArn: encodedArn } = useParams<{ topicArn: string }>()
  const topicArn = decodeURIComponent(encodedArn ?? '')
  const navigate = useNavigate()
  const qc = useQueryClient()

  const [subOpen, setSubOpen] = useState(false)
  const [subProtocol, setSubProtocol] = useState('sqs')
  const [subEndpoint, setSubEndpoint] = useState('')
  const [publishOpen, setPublishOpen] = useState(false)
  const [publishMsg, setPublishMsg] = useState('')
  const [publishSubject, setPublishSubject] = useState('')
  const [publishResult, setPublishResult] = useState('')

  const { data: topic } = useQuery({
    queryKey: ['sns', 'topic', topicArn],
    queryFn: () => getTopic(topicArn),
  })

  const { data: subsData, isLoading: subsLoading } = useQuery({
    queryKey: ['sns', 'subscriptions', topicArn],
    queryFn: () => listSubscriptionsByTopic(topicArn),
  })

  const subscribeMut = useMutation({
    mutationFn: () => subscribe(topicArn, { protocol: subProtocol, endpoint: subEndpoint }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sns', 'subscriptions', topicArn] })
      void qc.invalidateQueries({ queryKey: ['sns', 'topics'] })
      setSubOpen(false)
      setSubEndpoint('')
    },
  })

  const unsubscribeMut = useMutation({
    mutationFn: (subArn: string) => unsubscribe(subArn),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sns', 'subscriptions', topicArn] })
    },
  })

  const publishMut = useMutation({
    mutationFn: () => publish(topicArn, { message: publishMsg, subject: publishSubject || undefined }),
    onSuccess: (res) => {
      setPublishResult(res.MessageId ?? 'sent')
      setPublishMsg('')
      setPublishSubject('')
    },
  })

  const subs = subsData?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1.5rem' }}>
        <button onClick={() => navigate('/aws/sns')} style={btnBack}>← Topics</button>
        <div style={{ flex: 1 }}>
          <h2 style={{ margin: '0 0 0.15rem', fontSize: '1.4rem', fontWeight: 600 }}>
            {topic?.name ?? topicArn.split(':').pop()}
          </h2>
          <span style={{ fontSize: '0.78em', color: '#8d9daa', fontFamily: 'monospace' }}>{topicArn}</span>
        </div>
        <button onClick={() => { setPublishOpen(true); setPublishMsg(''); setPublishSubject(''); setPublishResult('') }} style={btnPrimary}>
          Publish
        </button>
        <button onClick={() => setSubOpen(true)} style={btnSecondary}>
          Subscribe
        </button>
      </div>

      <h3 style={{ margin: '0 0 0.75rem', fontSize: '1rem', fontWeight: 600 }}>
        Subscriptions ({subs.length})
      </h3>

      {subsLoading && <div style={{ color: '#5f6b7a' }}>Loading…</div>}

      {!subsLoading && subs.length === 0 && (
        <div style={{ padding: '2.5rem', textAlign: 'center', color: '#8d9daa', border: '1px solid #e7e9ec', borderRadius: 8 }}>
          No subscriptions yet.{' '}
          <span style={{ color: '#0972d3', cursor: 'pointer' }} onClick={() => setSubOpen(true)}>
            Add one →
          </span>
        </div>
      )}

      {subs.length > 0 && (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={th}>Protocol</th>
                <th style={th}>Endpoint</th>
                <th style={{ ...th, width: 80 }}></th>
              </tr>
            </thead>
            <tbody>
              {subs.map((s: Subscription) => (
                <tr key={s.subscriptionArn} style={{ borderBottom: '1px solid #e7e9ec' }}>
                  <td style={td}>
                    <span style={{
                      display: 'inline-block', padding: '0.15em 0.55em', borderRadius: 3,
                      fontSize: '0.8em', fontWeight: 500, background: '#f4f5f7', color: '#5f6b7a',
                    }}>
                      {s.protocol}
                    </span>
                  </td>
                  <td style={{ ...td, fontFamily: 'monospace', fontSize: '0.85em', wordBreak: 'break-all' }}>
                    {s.endpoint}
                  </td>
                  <td style={td}>
                    <button
                      onClick={() => unsubscribeMut.mutate(s.subscriptionArn)}
                      disabled={unsubscribeMut.isPending}
                      style={btnDelete}
                    >
                      Remove
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Subscribe dialog */}
      {subOpen && (
        <div style={overlayStyle} onClick={() => setSubOpen(false)}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem' }}>Subscribe</h3>
            <label style={labelStyle}>Protocol</label>
            <select
              value={subProtocol}
              onChange={(e) => setSubProtocol(e.target.value)}
              style={{ ...inputStyle, marginBottom: '0.75rem' }}
            >
              {['sqs', 'lambda', 'http', 'https', 'email', 'sms'].map((p) => (
                <option key={p} value={p}>{p}</option>
              ))}
            </select>
            <label style={labelStyle}>Endpoint</label>
            <input
              autoFocus
              value={subEndpoint}
              onChange={(e) => setSubEndpoint(e.target.value)}
              style={{ ...inputStyle, marginBottom: '1rem' }}
              placeholder={subProtocol === 'sqs' ? 'arn:aws:sqs:us-east-1:000000000000:my-queue' : 'https://…'}
            />
            {subscribeMut.error && (
              <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>
                {(subscribeMut.error as Error).message}
              </p>
            )}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setSubOpen(false)} style={btnSecondary}>Cancel</button>
              <button
                onClick={() => subscribeMut.mutate()}
                disabled={!subEndpoint || subscribeMut.isPending}
                style={{ ...btnPrimary, opacity: !subEndpoint || subscribeMut.isPending ? 0.6 : 1 }}
              >
                {subscribeMut.isPending ? 'Subscribing…' : 'Subscribe'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Publish dialog */}
      {publishOpen && (
        <div style={overlayStyle} onClick={() => setPublishOpen(false)}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.25rem' }}>Publish message</h3>
            {publishResult ? (
              <div>
                <p style={{ color: '#1d6b2e', background: '#e0f9e0', padding: '0.75rem', borderRadius: 4, margin: '1rem 0' }}>
                  ✓ Published — MessageId: {publishResult}
                </p>
                <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                  <button onClick={() => { setPublishOpen(false); setPublishResult('') }} style={btnPrimary}>Done</button>
                </div>
              </div>
            ) : (
              <>
                <p style={{ margin: '0 0 0.75rem', color: '#5f6b7a', fontSize: '0.85em' }}>Topic: {topic?.name}</p>
                <label style={labelStyle}>Subject (optional)</label>
                <input
                  value={publishSubject}
                  onChange={(e) => setPublishSubject(e.target.value)}
                  style={{ ...inputStyle, marginBottom: '0.75rem' }}
                  placeholder="Subject…"
                />
                <label style={labelStyle}>Message</label>
                <textarea
                  autoFocus
                  value={publishMsg}
                  onChange={(e) => setPublishMsg(e.target.value)}
                  style={{ width: '100%', boxSizing: 'border-box', height: 120, padding: '0.5rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em', resize: 'vertical' }}
                  placeholder="Message body…"
                />
                {publishMut.error && (
                  <p style={{ color: '#d13212', margin: '0.5rem 0 0', fontSize: '0.85em' }}>
                    {(publishMut.error as Error).message}
                  </p>
                )}
                <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1rem' }}>
                  <button onClick={() => setPublishOpen(false)} style={btnSecondary}>Cancel</button>
                  <button
                    onClick={() => publishMut.mutate()}
                    disabled={!publishMsg || publishMut.isPending}
                    style={{ ...btnPrimary, opacity: !publishMsg || publishMut.isPending ? 0.6 : 1 }}
                  >
                    {publishMut.isPending ? 'Publishing…' : 'Publish'}
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

const th: React.CSSProperties = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' }
const td: React.CSSProperties = { padding: '0.75rem 1rem' }
const btnBack: React.CSSProperties = { background: 'none', border: 'none', color: '#0972d3', cursor: 'pointer', fontSize: '0.9em', padding: '0.25rem 0' }
const btnPrimary: React.CSSProperties = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.45rem 1.1rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' }
const btnSecondary: React.CSSProperties = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.45rem 0.9rem', cursor: 'pointer', fontSize: '0.88em' }
const btnDelete: React.CSSProperties = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const dialogStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 500, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' }
const labelStyle: React.CSSProperties = { display: 'block', fontSize: '0.85em', fontWeight: 500, marginBottom: '0.4rem', color: '#3d4c5e' }
const inputStyle: React.CSSProperties = { width: '100%', boxSizing: 'border-box', padding: '0.5rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em' }
