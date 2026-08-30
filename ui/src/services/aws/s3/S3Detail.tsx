import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { listObjects, deleteObject, deleteObjects, downloadObjectUrl, type S3Object } from '../../../api/s3'

function fmtSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`
}

function fmtDate(s?: string): string {
  if (!s) return '—'
  try { return new Date(s).toLocaleString() } catch { return s }
}

export function S3Detail() {
  const { bucket: encodedBucket } = useParams<{ bucket: string }>()
  const bucket = decodeURIComponent(encodedBucket ?? '')
  const navigate = useNavigate()
  const qc = useQueryClient()

  const [prefix, setPrefix] = useState('')
  const [prefixInput, setPrefixInput] = useState('')
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [confirmDelete, setConfirmDelete] = useState<S3Object | null>(null)

  const { data, isLoading, error } = useQuery({
    queryKey: ['s3', 'objects', bucket, prefix],
    queryFn: () => listObjects(bucket, { prefix, delimiter: '/' }),
  })

  const deleteMut = useMutation({
    mutationFn: (key: string) => deleteObject(bucket, key),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['s3', 'objects', bucket] })
      setConfirmDelete(null)
    },
  })

  const deleteBatchMut = useMutation({
    mutationFn: (keys: string[]) => deleteObjects(bucket, keys),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['s3', 'objects', bucket] })
      setSelected(new Set())
    },
  })

  const objects = data?.items ?? []
  const prefixes = data?.commonPrefixes ?? []

  const toggleSelect = (key: string) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const navigatePrefix = (p: string) => {
    setPrefix(p)
    setPrefixInput(p)
    setSelected(new Set())
  }

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1.5rem' }}>
        <button onClick={() => navigate('/aws/s3')} style={btnBack}>← Buckets</button>
        <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>{bucket}</h2>
      </div>

      {/* Prefix navigator */}
      <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem', alignItems: 'center' }}>
        <input
          value={prefixInput}
          onChange={(e) => setPrefixInput(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && navigatePrefix(prefixInput)}
          placeholder="Filter by prefix…"
          style={{ ...inputStyle, flex: 1 }}
        />
        <button onClick={() => navigatePrefix(prefixInput)} style={btnSecondary}>Go</button>
        {prefix && (
          <button onClick={() => navigatePrefix('')} style={btnSecondary}>Clear</button>
        )}
        {selected.size > 0 && (
          <button
            onClick={() => deleteBatchMut.mutate(Array.from(selected))}
            disabled={deleteBatchMut.isPending}
            style={{ ...btnDanger, opacity: deleteBatchMut.isPending ? 0.6 : 1 }}
          >
            Delete {selected.size} selected
          </button>
        )}
      </div>

      {/* Breadcrumb */}
      {prefix && (
        <div style={{ marginBottom: '0.75rem', fontSize: '0.85em', color: '#5f6b7a' }}>
          <span
            style={{ cursor: 'pointer', color: '#0972d3' }}
            onClick={() => navigatePrefix('')}
          >
            {bucket}
          </span>
          {prefix.split('/').filter(Boolean).map((part, i, arr) => {
            const p = arr.slice(0, i + 1).join('/') + '/'
            return (
              <span key={p}>
                {' / '}
                <span
                  style={{ cursor: 'pointer', color: i < arr.length - 1 ? '#0972d3' : '#3d4c5e' }}
                  onClick={() => i < arr.length - 1 && navigatePrefix(p)}
                >
                  {part}
                </span>
              </span>
            )
          })}
        </div>
      )}

      {isLoading && <div style={{ color: '#5f6b7a', padding: '1rem' }}>Loading objects…</div>}
      {error && <div style={{ color: '#d13212', padding: '1rem' }}>Error: {(error as Error).message}</div>}

      {!isLoading && !error && prefixes.length === 0 && objects.length === 0 && (
        <div style={{ padding: '3rem', textAlign: 'center', color: '#8d9daa', border: '1px solid #e7e9ec', borderRadius: 8 }}>
          No objects{prefix ? ` with prefix "${prefix}"` : ' in this bucket'}.
        </div>
      )}

      {(prefixes.length > 0 || objects.length > 0) && (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={{ ...th, width: 36 }}>
                  <input
                    type="checkbox"
                    checked={selected.size === objects.length && objects.length > 0}
                    onChange={(e) => setSelected(e.target.checked ? new Set(objects.map((o) => o.key)) : new Set())}
                  />
                </th>
                <th style={th}>Key</th>
                <th style={{ ...th, textAlign: 'right' }}>Size</th>
                <th style={th}>Last Modified</th>
                <th style={{ ...th, width: 120 }}></th>
              </tr>
            </thead>
            <tbody>
              {prefixes.map((p) => (
                <tr
                  key={p}
                  style={{ borderBottom: '1px solid #e7e9ec', cursor: 'pointer', background: '#fffbe6' }}
                  onClick={() => navigatePrefix(p)}
                  onMouseEnter={(e) => (e.currentTarget.style.background = '#fff8e0')}
                  onMouseLeave={(e) => (e.currentTarget.style.background = '#fffbe6')}
                >
                  <td style={td} />
                  <td style={td}>
                    <span style={{ color: '#0972d3' }}>📁 {p.replace(prefix, '')}</span>
                  </td>
                  <td style={td} colSpan={3} />
                </tr>
              ))}
              {objects.map((obj) => (
                <tr
                  key={obj.key}
                  style={{ borderBottom: '1px solid #e7e9ec' }}
                  onMouseEnter={(e) => (e.currentTarget.style.background = '#fafbfc')}
                  onMouseLeave={(e) => (e.currentTarget.style.background = '')}
                >
                  <td style={td}>
                    <input
                      type="checkbox"
                      checked={selected.has(obj.key)}
                      onChange={() => toggleSelect(obj.key)}
                    />
                  </td>
                  <td style={td}>
                    <span style={{ fontFamily: 'monospace', fontSize: '0.88em' }}>
                      {obj.key.replace(prefix, '')}
                    </span>
                  </td>
                  <td style={{ ...td, textAlign: 'right', color: '#5f6b7a', fontVariantNumeric: 'tabular-nums' }}>
                    {fmtSize(obj.size)}
                  </td>
                  <td style={{ ...td, color: '#5f6b7a' }}>{fmtDate(obj.lastModified)}</td>
                  <td style={{ ...td, display: 'flex', gap: '0.4rem' }}>
                    <a
                      href={downloadObjectUrl(bucket, obj.key)}
                      download
                      style={btnSmall}
                      onClick={(e) => e.stopPropagation()}
                    >
                      Download
                    </a>
                    <button
                      onClick={(e) => { e.stopPropagation(); setConfirmDelete(obj) }}
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

      {data?.isTruncated && (
        <div style={{ marginTop: '0.75rem', textAlign: 'center', color: '#5f6b7a', fontSize: '0.85em' }}>
          More objects available — use prefix filter to narrow results.
        </div>
      )}

      {confirmDelete && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: '1.05rem' }}>Delete object?</h3>
            <p style={{ margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Permanently delete <strong style={{ wordBreak: 'break-all' }}>{confirmDelete.key}</strong>?
            </p>
            {deleteMut.error && (
              <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>
                {(deleteMut.error as Error).message}
              </p>
            )}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setConfirmDelete(null)} style={btnSecondary}>Cancel</button>
              <button
                onClick={() => deleteMut.mutate(confirmDelete.key)}
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
const td: React.CSSProperties = { padding: '0.6rem 1rem' }
const btnBack: React.CSSProperties = { background: 'none', border: 'none', color: '#0972d3', cursor: 'pointer', fontSize: '0.9em', padding: '0.25rem 0' }
const btnSecondary: React.CSSProperties = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.4rem 0.9rem', cursor: 'pointer', fontSize: '0.88em' }
const btnDanger: React.CSSProperties = { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.4rem 0.9rem', cursor: 'pointer', fontSize: '0.88em' }
const btnDelete: React.CSSProperties = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.2rem 0.55rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' }
const btnSmall: React.CSSProperties = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.2rem 0.55rem', cursor: 'pointer', fontSize: '0.8em', color: '#0972d3', textDecoration: 'none' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const dialogStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 500, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.88em' }
