import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  listCrawlers,
  createCrawler,
  deleteCrawler,
  startCrawler,
  type Crawler,
} from '../../../api/glue'
import { EmptyState } from '../../../components/EmptyState'

const STATE_COLOR: Record<string, string> = {
  READY: '#037f0c',
  RUNNING: '#0073bb',
  STOPPING: '#8a6116',
}

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 1rem', verticalAlign: 'middle' }
const btnStyle: React.CSSProperties = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 420, maxWidth: 560 }

export function GlueCrawlers() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<Crawler | null>(null)
  const [form, setForm] = useState({ name: '', role: '', databaseName: '', s3Targets: '' })

  const { data, isLoading } = useQuery({
    queryKey: ['glue', 'crawlers'],
    queryFn: () => listCrawlers(),
  })

  const createMut = useMutation({
    mutationFn: () => createCrawler({
      name: form.name,
      role: form.role || undefined,
      databaseName: form.databaseName || undefined,
      s3Targets: form.s3Targets ? form.s3Targets.split(',').map(s => s.trim()).filter(Boolean) : undefined,
    }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'crawlers'] })
      setCreateOpen(false)
      setForm({ name: '', role: '', databaseName: '', s3Targets: '' })
    },
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteCrawler(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'crawlers'] })
      setDeleteTarget(null)
    },
  })

  const startMut = useMutation({
    mutationFn: (name: string) => startCrawler(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'crawlers'] })
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading crawlers…</div>

  const crawlers = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>Glue Crawlers</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{crawlers.length} crawler{crawlers.length !== 1 ? 's' : ''}</span>
        </div>
        <button style={btnStyle} onClick={() => setCreateOpen(true)}>Create Crawler</button>
      </div>

      {crawlers.length === 0 ? (
        <EmptyState title="No crawlers. Create one to discover and catalog data from S3." />
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>{['Name', 'Role', 'State', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
          </thead>
          <tbody>
            {crawlers.map(c => (
              <tr key={c.name} style={{ borderBottom: '1px solid #2d3748' }}>
                <td style={{ ...tdStyle, fontWeight: 600 }}>{c.name}</td>
                <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }}>{c.role || '—'}</td>
                <td style={tdStyle}>
                  <span style={{ color: STATE_COLOR[c.state ?? ''] ?? '#5f6b7a', fontWeight: 600 }}>{c.state || '—'}</span>
                </td>
                <td style={{ ...tdStyle, textAlign: 'right' }}>
                  <div style={{ display: 'flex', gap: '0.4rem', justifyContent: 'flex-end' }}>
                    <button
                      style={{ ...btnStyle, padding: '0.25rem 0.6rem', fontSize: '0.8rem' }}
                      disabled={c.state === 'RUNNING' || startMut.isPending}
                      onClick={() => startMut.mutate(c.name)}
                    >Start</button>
                    <button
                      style={{ ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }}
                      onClick={() => setDeleteTarget(c)}
                    >Delete</button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {createOpen && (
        <div style={overlayStyle} onClick={() => setCreateOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Create Crawler</h3>
            {[
              { key: 'name', label: 'Name *', placeholder: 'my-crawler' },
              { key: 'role', label: 'IAM Role', placeholder: 'AWSGlueServiceRole' },
              { key: 'databaseName', label: 'Target Database', placeholder: 'my_database' },
              { key: 's3Targets', label: 'S3 Paths (comma-separated)', placeholder: 's3://bucket/prefix' },
            ].map(f => (
              <div key={f.key} style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }}>
                <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>{f.label}</label>
                <input style={inputStyle} placeholder={f.placeholder} value={(form as Record<string, string>)[f.key]} onChange={e => setForm(p => ({ ...p, [f.key]: e.target.value }))} />
              </div>
            ))}
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
            <h3 style={{ margin: '0 0 1rem', fontWeight: 600 }}>Delete Crawler?</h3>
            <p style={{ color: '#b0bec5', marginBottom: '1.5rem' }}>Delete crawler <strong>{deleteTarget.name}</strong>?</p>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setDeleteTarget(null)}>Cancel</button>
              <button style={{ ...btnStyle, background: '#d13212' }} disabled={deleteMut.isPending} onClick={() => deleteMut.mutate(deleteTarget.name)}>
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
