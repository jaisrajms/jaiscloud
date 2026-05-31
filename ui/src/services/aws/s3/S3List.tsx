import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { listBuckets, createBucket, deleteBucket, type Bucket } from '../../../api/s3'
import { EmptyState } from '../../../components/EmptyState'

export function S3List() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<Bucket | null>(null)
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['s3', 'buckets'],
    queryFn: () => listBuckets(),
  })

  const createMut = useMutation({
    mutationFn: () => createBucket({ name: newName }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['s3', 'buckets'] })
      setCreateOpen(false)
      setNewName('')
    },
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteBucket(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['s3', 'buckets'] })
      setConfirmDelete(null)
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading buckets…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load: {(error as Error).message}</div>

  const buckets = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>S3 Buckets</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{buckets.length} bucket{buckets.length !== 1 ? 's' : ''}</span>
        </div>
        <button onClick={() => setCreateOpen(true)} style={btnPrimary}>Create bucket</button>
      </div>

      {buckets.length === 0 ? (
        <EmptyState
          title="No buckets"
          description="S3 buckets store your objects and files."
          cta="Create Bucket"
          onCta={() => setCreateOpen(true)}
        />
      ) : (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={th}>Name</th>
                <th style={th}>Region</th>
                <th style={th}>Versioning</th>
                <th style={{ ...th, width: 80 }}></th>
              </tr>
            </thead>
            <tbody>
              {buckets.map((b) => (
                <tr
                  key={b.name}
                  style={{ borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }}
                  onClick={() => navigate(`/aws/s3/${encodeURIComponent(b.name)}`)}
                  onMouseEnter={(e) => (e.currentTarget.style.background = '#fafbfc')}
                  onMouseLeave={(e) => (e.currentTarget.style.background = '')}
                >
                  <td style={td}><span style={{ color: '#0972d3', fontWeight: 500 }}>{b.name}</span></td>
                  <td style={{ ...td, color: '#5f6b7a' }}>{b.region}</td>
                  <td style={td}>
                    <span style={{
                      display: 'inline-block', padding: '0.15em 0.55em', borderRadius: 3,
                      fontSize: '0.8em', fontWeight: 500,
                      background: b.versioning === 'Enabled' ? '#e0f9e0' : '#f4f5f7',
                      color: b.versioning === 'Enabled' ? '#1d6b2e' : '#5f6b7a',
                    }}>
                      {b.versioning}
                    </span>
                  </td>
                  <td style={td} onClick={(e) => e.stopPropagation()}>
                    <button onClick={() => setConfirmDelete(b)} style={btnDelete}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {createOpen && (
        <div style={overlayStyle} onClick={() => setCreateOpen(false)}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem' }}>Create bucket</h3>
            <label style={labelStyle}>Bucket name</label>
            <input
              autoFocus
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && newName && createMut.mutate()}
              style={inputStyle}
              placeholder="my-bucket"
            />
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

      {confirmDelete && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: '1.05rem' }}>Delete bucket?</h3>
            <p style={{ margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Permanently delete <strong>{confirmDelete.name}</strong>? The bucket must be empty.
            </p>
            {deleteMut.error && (
              <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>
                {(deleteMut.error as Error).message}
              </p>
            )}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setConfirmDelete(null)} style={btnSecondary}>Cancel</button>
              <button
                onClick={() => deleteMut.mutate(confirmDelete.name)}
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
const labelStyle: React.CSSProperties = { display: 'block', fontSize: '0.85em', fontWeight: 500, marginBottom: '0.4rem', color: '#3d4c5e' }
const inputStyle: React.CSSProperties = { width: '100%', boxSizing: 'border-box', padding: '0.5rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em' }
