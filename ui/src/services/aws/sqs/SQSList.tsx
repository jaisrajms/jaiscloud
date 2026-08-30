import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { listQueues, deleteQueue, type Queue } from '../../../api/sqs'
import { EmptyState } from '../../../components/EmptyState'
import { SQSCreate } from './SQSCreate'

function fmtDate(iso: string): string {
  if (!iso) return '—'
  try { return new Date(iso).toLocaleString() } catch { return iso }
}

export function SQSList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState<Queue | null>(null)
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['sqs', 'queues'],
    queryFn: () => listQueues(),
  })

  const deleteMut = useMutation({
    mutationFn: (url: string) => deleteQueue(url),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sqs', 'queues'] })
      setConfirmDelete(null)
    },
  })

  if (isLoading) {
    return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading queues…</div>
  }
  if (error) {
    return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load queues: {(error as Error).message}</div>
  }

  const queues = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>SQS Queues</h2>
          {data?.total != null && (
            <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{data.total} queue{data.total !== 1 ? 's' : ''}</span>
          )}
        </div>
        <button onClick={() => setCreateOpen(true)} style={btnPrimary}>
          Create queue
        </button>
      </div>

      {queues.length === 0 ? (
        <EmptyState
          title="No queues"
          description="SQS queues let your applications communicate asynchronously."
          cta="Create Queue"
          onCta={() => setCreateOpen(true)}
        />
      ) : (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={th}>Name</th>
                <th style={th}>Type</th>
                <th style={{ ...th, textAlign: 'right' }}>Msgs Available</th>
                <th style={{ ...th, textAlign: 'right' }}>In Flight</th>
                <th style={th}>Created</th>
                <th style={{ ...th, width: 80 }}></th>
              </tr>
            </thead>
            <tbody>
              {queues.map((q) => (
                <tr
                  key={q.url}
                  style={{ borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }}
                  onClick={() => navigate(`/aws/sqs/${encodeURIComponent(q.url)}`)}
                  onMouseEnter={(e) => (e.currentTarget.style.background = '#fafbfc')}
                  onMouseLeave={(e) => (e.currentTarget.style.background = '')}
                >
                  <td style={td}>
                    <span style={{ color: '#0972d3', fontWeight: 500 }}>{q.name}</span>
                    {q.dlqArn && (
                      <span style={{ marginLeft: '0.5rem', fontSize: '0.75em', color: '#8d9daa' }}>DLQ</span>
                    )}
                  </td>
                  <td style={td}>
                    <span style={{
                      display: 'inline-block', padding: '0.15em 0.55em', borderRadius: 3,
                      fontSize: '0.8em', fontWeight: 500,
                      background: q.type === 'FIFO' ? '#e0f0ff' : '#f4f5f7',
                      color: q.type === 'FIFO' ? '#0972d3' : '#5f6b7a',
                    }}>
                      {q.type}
                    </span>
                  </td>
                  <td style={{ ...td, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    {q.messagesAvailable.toLocaleString()}
                  </td>
                  <td style={{ ...td, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    {q.messagesInFlight.toLocaleString()}
                  </td>
                  <td style={{ ...td, color: '#5f6b7a' }}>{fmtDate(q.createdAt)}</td>
                  <td style={td} onClick={(e) => e.stopPropagation()}>
                    <button
                      onClick={() => setConfirmDelete(q)}
                      style={btnDelete}
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {createOpen && (
        <SQSCreate
          onClose={() => setCreateOpen(false)}
          onCreated={() => {
            setCreateOpen(false)
            void qc.invalidateQueries({ queryKey: ['sqs', 'queues'] })
          }}
        />
      )}

      {confirmDelete && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: '1.05rem' }}>Delete queue?</h3>
            <p style={{ margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Permanently delete <strong>{confirmDelete.name}</strong>? All messages will be lost and cannot be recovered.
            </p>
            {deleteMut.error && (
              <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>
                {(deleteMut.error as Error).message}
              </p>
            )}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setConfirmDelete(null)} style={btnSecondary}>Cancel</button>
              <button
                onClick={() => deleteMut.mutate(confirmDelete.url)}
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
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const dialogStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 460, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' }
