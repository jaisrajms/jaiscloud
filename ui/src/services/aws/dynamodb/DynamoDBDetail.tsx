import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { scanTable, deleteItem, putItem, type ScanResponse } from '../../../api/dynamodb'

function renderValue(v: unknown): string {
  if (v === null || v === undefined) return '—'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

export function DynamoDBDetail() {
  const { table: encodedTable } = useParams<{ table: string }>()
  const table = decodeURIComponent(encodedTable ?? '')
  const navigate = useNavigate()
  const qc = useQueryClient()

  const [limit, setLimit] = useState(50)
  const [nextToken, setNextToken] = useState<string | undefined>()
  const [page, setPage] = useState(0)
  const [pages, setPages] = useState<Array<string | undefined>>([undefined])
  const [editItem, setEditItem] = useState<Record<string, unknown> | null>(null)
  const [editJson, setEditJson] = useState('')
  const [editErr, setEditErr] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<Record<string, unknown> | null>(null)
  const [jsonErr, setJsonErr] = useState('')

  const currentToken = pages[page]

  const { data, isLoading, error } = useQuery<ScanResponse>({
    queryKey: ['dynamodb', 'scan', table, currentToken, limit],
    queryFn: () => scanTable(table, { limit, nextToken: currentToken }),
  })

  const deleteMut = useMutation({
    mutationFn: (key: Record<string, unknown>) => deleteItem(table, key),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['dynamodb', 'scan', table] })
      setConfirmDelete(null)
    },
  })

  const putMut = useMutation({
    mutationFn: (item: Record<string, unknown>) => putItem(table, item),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['dynamodb', 'scan', table] })
      setEditItem(null)
    },
  })

  const items = data?.items ?? []

  // Derive columns from first few items
  const columns = Array.from(
    items.slice(0, 20).reduce((cols, item) => {
      Object.keys(item).forEach((k) => cols.add(k))
      return cols
    }, new Set<string>()),
  ).slice(0, 12)

  const goNext = () => {
    if (data?.lastEvaluatedKey) {
      const tok = JSON.stringify(data.lastEvaluatedKey)
      const next = page + 1
      if (next >= pages.length) setPages([...pages, tok])
      setPage(next)
      setNextToken(tok)
    }
  }

  const goPrev = () => {
    if (page > 0) {
      const prev = page - 1
      setPage(prev)
      setNextToken(pages[prev])
    }
  }

  const openEdit = (item: Record<string, unknown>) => {
    setEditItem(item)
    setEditJson(JSON.stringify(item, null, 2))
    setEditErr('')
  }

  const handleSave = () => {
    try {
      const parsed = JSON.parse(editJson) as Record<string, unknown>
      setEditErr('')
      putMut.mutate(parsed)
    } catch {
      setEditErr('Invalid JSON')
    }
  }

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1.5rem' }}>
        <button onClick={() => navigate('/aws/dynamodb')} style={btnBack}>← Tables</button>
        <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>{table}</h2>
        <button
          onClick={() => { setEditJson('{}'); setEditItem({}); setEditErr('') }}
          style={{ ...btnPrimary, marginLeft: 'auto' }}
        >
          Put Item
        </button>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1rem', fontSize: '0.88em', color: '#5f6b7a' }}>
        <span>
          {data ? `${data.count} / ${data.scannedCount} scanned` : '…'}
        </span>
        <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
          Rows
          <select
            value={limit}
            onChange={(e) => { setLimit(Number(e.target.value)); setPage(0); setPages([undefined]) }}
            style={{ padding: '0.2rem 0.4rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em' }}
          >
            {[25, 50, 100, 250].map((n) => <option key={n} value={n}>{n}</option>)}
          </select>
        </label>
      </div>

      {isLoading && <div style={{ color: '#5f6b7a', padding: '1rem' }}>Scanning…</div>}
      {error && <div style={{ color: '#d13212', padding: '1rem' }}>Error: {(error as Error).message}</div>}

      {!isLoading && !error && items.length === 0 && (
        <div style={{ padding: '3rem', textAlign: 'center', color: '#8d9daa', border: '1px solid #e7e9ec', borderRadius: 8 }}>
          No items in this table.
        </div>
      )}

      {items.length > 0 && (
        <div style={{ overflowX: 'auto', border: '1px solid #e7e9ec', borderRadius: 8 }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                {columns.map((col) => (
                  <th key={col} style={th}>{col}</th>
                ))}
                <th style={{ ...th, width: 80 }}></th>
              </tr>
            </thead>
            <tbody>
              {items.map((item, i) => (
                <tr
                  key={i}
                  style={{ borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }}
                  onClick={() => openEdit(item)}
                  onMouseEnter={(e) => (e.currentTarget.style.background = '#fafbfc')}
                  onMouseLeave={(e) => (e.currentTarget.style.background = '')}
                >
                  {columns.map((col) => (
                    <td key={col} style={{ ...td, maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {renderDynamo(item[col])}
                    </td>
                  ))}
                  <td style={td} onClick={(e) => e.stopPropagation()}>
                    <button onClick={() => setConfirmDelete(item)} style={btnDelete}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Pagination */}
      {(page > 0 || data?.lastEvaluatedKey) && (
        <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end', marginTop: '0.75rem' }}>
          <button onClick={goPrev} disabled={page === 0} style={btnSecondary}>← Prev</button>
          <button onClick={goNext} disabled={!data?.lastEvaluatedKey} style={btnSecondary}>Next →</button>
        </div>
      )}

      {/* Edit/Put Item dialog */}
      {editItem !== null && (
        <div style={overlayStyle} onClick={() => setEditItem(null)}>
          <div style={{ ...dialogStyle, minWidth: 480 }} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem' }}>{Object.keys(editItem).length === 0 ? 'Put Item' : 'Edit Item'}</h3>
            <p style={{ margin: '0 0 0.5rem', fontSize: '0.8em', color: '#5f6b7a' }}>Edit as DynamoDB JSON (&#123;"pk": &#123;"S": "value"&#125;, ...&#125;)</p>
            <textarea
              value={editJson}
              onChange={(e) => setEditJson(e.target.value)}
              style={{ width: '100%', boxSizing: 'border-box', height: 240, fontFamily: 'monospace', fontSize: '0.85em', padding: '0.5rem', border: '1px solid #c9cdd4', borderRadius: 4, resize: 'vertical' }}
            />
            {editErr && <p style={{ color: '#d13212', margin: '0.25rem 0 0', fontSize: '0.85em' }}>{editErr}</p>}
            {putMut.error && <p style={{ color: '#d13212', margin: '0.25rem 0 0', fontSize: '0.85em' }}>{(putMut.error as Error).message}</p>}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1rem' }}>
              <button onClick={() => setEditItem(null)} style={btnSecondary}>Cancel</button>
              <button onClick={handleSave} disabled={putMut.isPending} style={{ ...btnPrimary, opacity: putMut.isPending ? 0.6 : 1 }}>
                {putMut.isPending ? 'Saving…' : 'Save'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Delete confirm */}
      {confirmDelete && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: '1.05rem' }}>Delete item?</h3>
            <p style={{ margin: '0 0 0.5rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Enter the key for this item to delete it.
            </p>
            <textarea
              value={jsonErr || JSON.stringify(extractKey(confirmDelete), null, 2)}
              onChange={(e) => setJsonErr(e.target.value)}
              style={{ width: '100%', boxSizing: 'border-box', height: 80, fontFamily: 'monospace', fontSize: '0.82em', padding: '0.5rem', border: '1px solid #c9cdd4', borderRadius: 4 }}
            />
            {deleteMut.error && (
              <p style={{ color: '#d13212', margin: '0.25rem 0 0.75rem', fontSize: '0.85em' }}>
                {(deleteMut.error as Error).message}
              </p>
            )}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '0.75rem' }}>
              <button onClick={() => setConfirmDelete(null)} style={btnSecondary}>Cancel</button>
              <button
                onClick={() => deleteMut.mutate(confirmDelete)}
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

function renderDynamo(val: unknown): string {
  if (val === undefined || val === null) return '—'
  if (typeof val === 'object') {
    const obj = val as Record<string, unknown>
    // DynamoDB typed attribute: { S: "..." } | { N: "..." } | { BOOL: true } ...
    if ('S' in obj) return String(obj['S'])
    if ('N' in obj) return String(obj['N'])
    if ('BOOL' in obj) return String(obj['BOOL'])
    if ('NULL' in obj) return 'null'
    if ('L' in obj) return `[${(obj['L'] as unknown[]).length} items]`
    if ('M' in obj) return `{${Object.keys(obj['M'] as object).length} keys}`
    return renderValue(val)
  }
  return String(val)
}

function extractKey(item: Record<string, unknown>): Record<string, unknown> {
  // Heuristic: keep only fields that look like partition/sort key candidates
  return item
}

const th: React.CSSProperties = { padding: '0.55rem 0.75rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.78em', textTransform: 'uppercase', letterSpacing: '0.03em', whiteSpace: 'nowrap' }
const td: React.CSSProperties = { padding: '0.55rem 0.75rem' }
const btnBack: React.CSSProperties = { background: 'none', border: 'none', color: '#0972d3', cursor: 'pointer', fontSize: '0.9em', padding: '0.25rem 0' }
const btnPrimary: React.CSSProperties = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.45rem 1.1rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' }
const btnSecondary: React.CSSProperties = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.4rem 0.9rem', cursor: 'pointer', fontSize: '0.88em' }
const btnDanger: React.CSSProperties = { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.4rem 0.9rem', cursor: 'pointer', fontSize: '0.88em' }
const btnDelete: React.CSSProperties = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.2rem 0.55rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const dialogStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 520, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' }
