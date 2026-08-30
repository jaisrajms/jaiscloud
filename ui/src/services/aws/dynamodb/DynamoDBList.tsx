import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { listTables, createTable, deleteTable, type TableSummary, type CreateTableRequest } from '../../../api/dynamodb'
import { EmptyState } from '../../../components/EmptyState'

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  return `${(n / 1024 / 1024).toFixed(1)} MB`
}

export function DynamoDBList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState<TableSummary | null>(null)
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['dynamodb', 'tables'],
    queryFn: () => listTables(),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteTable(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['dynamodb', 'tables'] })
      setConfirmDelete(null)
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading tables…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load: {(error as Error).message}</div>

  const tables = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>DynamoDB Tables</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{tables.length} table{tables.length !== 1 ? 's' : ''}</span>
        </div>
        <button onClick={() => setCreateOpen(true)} style={btnPrimary}>Create table</button>
      </div>

      {tables.length === 0 ? (
        <EmptyState
          title="No tables"
          description="DynamoDB tables store your NoSQL data."
          cta="Create Table"
          onCta={() => setCreateOpen(true)}
        />
      ) : (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={th}>Name</th>
                <th style={th}>Status</th>
                <th style={{ ...th, textAlign: 'right' }}>Items</th>
                <th style={{ ...th, textAlign: 'right' }}>Size</th>
                <th style={th}>Billing</th>
                <th style={{ ...th, width: 80 }}></th>
              </tr>
            </thead>
            <tbody>
              {tables.map((t) => (
                <tr
                  key={t.name}
                  style={{ borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }}
                  onClick={() => navigate(`/aws/dynamodb/${encodeURIComponent(t.name)}`)}
                  onMouseEnter={(e) => (e.currentTarget.style.background = '#fafbfc')}
                  onMouseLeave={(e) => (e.currentTarget.style.background = '')}
                >
                  <td style={td}><span style={{ color: '#0972d3', fontWeight: 500 }}>{t.name}</span></td>
                  <td style={td}>
                    <span style={{
                      display: 'inline-block', padding: '0.15em 0.55em', borderRadius: 3,
                      fontSize: '0.8em', fontWeight: 500,
                      background: t.status === 'ACTIVE' ? '#e0f9e0' : '#fff8e0',
                      color: t.status === 'ACTIVE' ? '#1d6b2e' : '#8a6500',
                    }}>
                      {t.status}
                    </span>
                  </td>
                  <td style={{ ...td, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                    {t.itemCount.toLocaleString()}
                  </td>
                  <td style={{ ...td, textAlign: 'right', color: '#5f6b7a' }}>{fmtBytes(t.sizeBytes)}</td>
                  <td style={{ ...td, color: '#5f6b7a', fontSize: '0.85em' }}>{t.billingMode}</td>
                  <td style={td} onClick={(e) => e.stopPropagation()}>
                    <button onClick={() => setConfirmDelete(t)} style={btnDelete}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {createOpen && (
        <CreateTableDialog
          onClose={() => setCreateOpen(false)}
          onCreated={() => {
            setCreateOpen(false)
            void qc.invalidateQueries({ queryKey: ['dynamodb', 'tables'] })
          }}
        />
      )}

      {confirmDelete && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: '1.05rem' }}>Delete table?</h3>
            <p style={{ margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Permanently delete <strong>{confirmDelete.name}</strong> and all its items?
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

function CreateTableDialog({ onClose, onCreated }: { onClose: () => void; onCreated: () => void }) {
  const [form, setForm] = useState<CreateTableRequest>({
    tableName: '',
    keySchema: [{ attributeName: 'pk', keyType: 'HASH' }],
    attributeDefinitions: [{ attributeName: 'pk', attributeType: 'S' }],
    billingMode: 'PAY_PER_REQUEST',
  })
  const [withSort, setWithSort] = useState(false)

  const createMut = useMutation({
    mutationFn: (req: CreateTableRequest) => createTable(req),
    onSuccess: onCreated,
  })

  const handleCreate = () => {
    const req = { ...form }
    if (!withSort) {
      req.keySchema = req.keySchema.filter((k) => k.keyType === 'HASH')
      req.attributeDefinitions = req.attributeDefinitions.filter((a) =>
        req.keySchema.some((k) => k.attributeName === a.attributeName),
      )
    }
    createMut.mutate(req)
  }

  const pkName = form.keySchema.find((k) => k.keyType === 'HASH')?.attributeName ?? 'pk'
  const skName = form.keySchema.find((k) => k.keyType === 'RANGE')?.attributeName ?? 'sk'

  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={{ ...dialogStyle, minWidth: 420 }} onClick={(e) => e.stopPropagation()}>
        <h3 style={{ margin: '0 0 1.25rem' }}>Create table</h3>

        <label style={labelStyle}>Table name</label>
        <input
          autoFocus
          value={form.tableName}
          onChange={(e) => setForm({ ...form, tableName: e.target.value })}
          style={{ ...inputStyle, marginBottom: '1rem' }}
          placeholder="my-table"
        />

        <label style={labelStyle}>Partition key (HASH)</label>
        <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.75rem' }}>
          <input
            value={pkName}
            onChange={(e) => setForm({
              ...form,
              keySchema: form.keySchema.map((k) => k.keyType === 'HASH' ? { ...k, attributeName: e.target.value } : k),
              attributeDefinitions: form.attributeDefinitions.map((a) =>
                a.attributeName === pkName ? { ...a, attributeName: e.target.value } : a,
              ),
            })}
            style={{ ...inputStyle, flex: 1 }}
          />
          <select
            value={form.attributeDefinitions.find((a) => a.attributeName === pkName)?.attributeType ?? 'S'}
            onChange={(e) => setForm({
              ...form,
              attributeDefinitions: form.attributeDefinitions.map((a) =>
                a.attributeName === pkName ? { ...a, attributeType: e.target.value as 'S' | 'N' | 'B' } : a,
              ),
            })}
            style={{ ...inputStyle, width: 60 }}
          >
            <option value="S">S</option>
            <option value="N">N</option>
            <option value="B">B</option>
          </select>
        </div>

        <label style={{ ...labelStyle, display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer', marginBottom: '0.75rem' }}>
          <input type="checkbox" checked={withSort} onChange={(e) => {
            setWithSort(e.target.checked)
            if (e.target.checked) {
              setForm({
                ...form,
                keySchema: [...form.keySchema.filter((k) => k.keyType === 'HASH'), { attributeName: 'sk', keyType: 'RANGE' }],
                attributeDefinitions: [...form.attributeDefinitions.filter((a) => a.attributeName !== 'sk'), { attributeName: 'sk', attributeType: 'S' }],
              })
            }
          }} />
          Add sort key (RANGE)
        </label>

        {withSort && (
          <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem' }}>
            <input
              value={skName}
              onChange={(e) => setForm({
                ...form,
                keySchema: form.keySchema.map((k) => k.keyType === 'RANGE' ? { ...k, attributeName: e.target.value } : k),
                attributeDefinitions: form.attributeDefinitions.map((a) =>
                  a.attributeName === skName ? { ...a, attributeName: e.target.value } : a,
                ),
              })}
              style={{ ...inputStyle, flex: 1 }}
            />
            <select
              value={form.attributeDefinitions.find((a) => a.attributeName === skName)?.attributeType ?? 'S'}
              onChange={(e) => setForm({
                ...form,
                attributeDefinitions: form.attributeDefinitions.map((a) =>
                  a.attributeName === skName ? { ...a, attributeType: e.target.value as 'S' | 'N' | 'B' } : a,
                ),
              })}
              style={{ ...inputStyle, width: 60 }}
            >
              <option value="S">S</option>
              <option value="N">N</option>
              <option value="B">B</option>
            </select>
          </div>
        )}

        <label style={labelStyle}>Billing mode</label>
        <select
          value={form.billingMode}
          onChange={(e) => setForm({ ...form, billingMode: e.target.value as 'PAY_PER_REQUEST' | 'PROVISIONED' })}
          style={{ ...inputStyle, marginBottom: '1.25rem' }}
        >
          <option value="PAY_PER_REQUEST">On-demand (PAY_PER_REQUEST)</option>
          <option value="PROVISIONED">Provisioned</option>
        </select>

        {createMut.error && (
          <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>
            {(createMut.error as Error).message}
          </p>
        )}

        <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
          <button onClick={onClose} style={btnSecondary}>Cancel</button>
          <button
            onClick={handleCreate}
            disabled={!form.tableName || createMut.isPending}
            style={{ ...btnPrimary, opacity: !form.tableName || createMut.isPending ? 0.6 : 1 }}
          >
            {createMut.isPending ? 'Creating…' : 'Create'}
          </button>
        </div>
      </div>
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
const dialogStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 520, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' }
const labelStyle: React.CSSProperties = { display: 'block', fontSize: '0.85em', fontWeight: 500, marginBottom: '0.35rem', color: '#3d4c5e' }
const inputStyle: React.CSSProperties = { width: '100%', boxSizing: 'border-box', padding: '0.45rem 0.7rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em' }
