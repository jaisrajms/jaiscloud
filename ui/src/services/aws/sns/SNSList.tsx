import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { listTopics, createTopic, deleteTopic, publish, type Topic } from '../../../api/sns'
import { EmptyState } from '../../../components/EmptyState'

export function SNSList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newFIFO, setNewFIFO] = useState(false)
  const [publishTopic, setPublishTopic] = useState<Topic | null>(null)
  const [publishMsg, setPublishMsg] = useState('')
  const [publishSubject, setPublishSubject] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<Topic | null>(null)
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['sns', 'topics'],
    queryFn: () => listTopics(),
  })

  const createMut = useMutation({
    mutationFn: () => createTopic({ name: newName, fifo: newFIFO }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sns', 'topics'] })
      setCreateOpen(false)
      setNewName('')
      setNewFIFO(false)
    },
  })

  const deleteMut = useMutation({
    mutationFn: (arn: string) => deleteTopic(arn),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sns', 'topics'] })
      setConfirmDelete(null)
    },
  })

  const publishMut = useMutation({
    mutationFn: ({ arn, msg, subj }: { arn: string; msg: string; subj: string }) =>
      publish(arn, { message: msg, subject: subj || undefined }),
    onSuccess: () => {
      setPublishTopic(null)
      setPublishMsg('')
      setPublishSubject('')
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading topics…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load: {(error as Error).message}</div>

  const topics = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>SNS Topics</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{topics.length} topic{topics.length !== 1 ? 's' : ''}</span>
        </div>
        <button onClick={() => setCreateOpen(true)} style={btnPrimary}>Create topic</button>
      </div>

      {topics.length === 0 ? (
        <EmptyState
          title="No topics"
          description="SNS topics enable fan-out messaging to multiple subscribers."
          cta="Create Topic"
          onCta={() => setCreateOpen(true)}
        />
      ) : (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={th}>Name</th>
                <th style={th}>Type</th>
                <th style={{ ...th, textAlign: 'right' }}>Subscriptions</th>
                <th style={{ ...th, width: 160 }}></th>
              </tr>
            </thead>
            <tbody>
              {topics.map((t) => (
                <tr
                  key={t.arn}
                  style={{ borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }}
                  onClick={() => navigate(`/aws/sns/${encodeURIComponent(t.arn)}`)}
                  onMouseEnter={(e) => (e.currentTarget.style.background = '#fafbfc')}
                  onMouseLeave={(e) => (e.currentTarget.style.background = '')}
                >
                  <td style={td}><span style={{ color: '#0972d3', fontWeight: 500 }}>{t.name}</span></td>
                  <td style={td}>
                    <span style={{
                      display: 'inline-block', padding: '0.15em 0.55em', borderRadius: 3,
                      fontSize: '0.8em', fontWeight: 500,
                      background: t.type === 'FIFO' ? '#e0f0ff' : '#f4f5f7',
                      color: t.type === 'FIFO' ? '#0972d3' : '#5f6b7a',
                    }}>
                      {t.type}
                    </span>
                  </td>
                  <td style={{ ...td, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    {t.subscriptionCount}
                  </td>
                  <td style={{ ...td, display: 'flex', gap: '0.4rem' }} onClick={(e) => e.stopPropagation()}>
                    <button onClick={() => { setPublishTopic(t); setPublishMsg(''); setPublishSubject('') }} style={btnSmall}>
                      Publish
                    </button>
                    <button onClick={() => setConfirmDelete(t)} style={btnDelete}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Create dialog */}
      {createOpen && (
        <div style={overlayStyle} onClick={() => setCreateOpen(false)}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem' }}>Create topic</h3>
            <label style={labelStyle}>Topic name</label>
            <input
              autoFocus
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && newName && createMut.mutate()}
              style={{ ...inputStyle, marginBottom: '0.75rem' }}
              placeholder="my-topic"
            />
            <label style={{ ...labelStyle, display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
              <input type="checkbox" checked={newFIFO} onChange={(e) => setNewFIFO(e.target.checked)} />
              FIFO topic
            </label>
            {createMut.error && (
              <p style={{ color: '#d13212', margin: '0.5rem 0 0', fontSize: '0.85em' }}>
                {(createMut.error as Error).message}
              </p>
            )}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1.25rem' }}>
              <button onClick={() => setCreateOpen(false)} style={btnSecondary}>Cancel</button>
              <button
                onClick={() => createMut.mutate()}
                disabled={!newName || createMut.isPending}
                style={{ ...btnPrimary, opacity: !newName || createMut.isPending ? 0.6 : 1 }}
              >
                {createMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Publish dialog */}
      {publishTopic && (
        <div style={overlayStyle} onClick={() => setPublishTopic(null)}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.25rem' }}>Publish to topic</h3>
            <p style={{ margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.85em' }}>{publishTopic.name}</p>
            <label style={labelStyle}>Subject (optional)</label>
            <input
              value={publishSubject}
              onChange={(e) => setPublishSubject(e.target.value)}
              style={{ ...inputStyle, marginBottom: '0.75rem' }}
              placeholder="My subject"
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
              <button onClick={() => setPublishTopic(null)} style={btnSecondary}>Cancel</button>
              <button
                onClick={() => publishMut.mutate({ arn: publishTopic.arn, msg: publishMsg, subj: publishSubject })}
                disabled={!publishMsg || publishMut.isPending}
                style={{ ...btnPrimary, opacity: !publishMsg || publishMut.isPending ? 0.6 : 1 }}
              >
                {publishMut.isPending ? 'Publishing…' : 'Publish'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Delete confirm */}
      {confirmDelete && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: '1.05rem' }}>Delete topic?</h3>
            <p style={{ margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Permanently delete <strong>{confirmDelete.name}</strong> and all its subscriptions?
            </p>
            {deleteMut.error && (
              <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>
                {(deleteMut.error as Error).message}
              </p>
            )}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setConfirmDelete(null)} style={btnSecondary}>Cancel</button>
              <button
                onClick={() => deleteMut.mutate(confirmDelete.arn)}
                disabled={deleteMut.isPending}
                style={{ ...btnDanger, opacity: deleteMut.isPending ? 0.6 : 1 }}
              >
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

const th: React.CSSProperties = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' }
const td: React.CSSProperties = { padding: '0.75rem 1rem' }
const btnPrimary: React.CSSProperties = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1.25rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' }
const btnSecondary: React.CSSProperties = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' }
const btnDanger: React.CSSProperties = { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' }
const btnDelete: React.CSSProperties = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' }
const btnSmall: React.CSSProperties = { background: '#f4f5f7', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#3d4c5e' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const dialogStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 480, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' }
const labelStyle: React.CSSProperties = { display: 'block', fontSize: '0.85em', fontWeight: 500, marginBottom: '0.4rem', color: '#3d4c5e' }
const inputStyle: React.CSSProperties = { width: '100%', boxSizing: 'border-box', padding: '0.5rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em' }
