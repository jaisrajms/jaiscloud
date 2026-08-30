import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  listDatabases,
  createDatabase,
  deleteDatabase,
  listTables,
  createTable,
  deleteTable,
  type Database,
  type Table,
} from '../../../api/glue'
import { EmptyState } from '../../../components/EmptyState'

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 1rem', verticalAlign: 'middle' }
const btnStyle: React.CSSProperties = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 420, maxWidth: 560 }

export function GlueDatabases() {
  const qc = useQueryClient()
  const [selectedDB, setSelectedDB] = useState<Database | null>(null)
  const [createDBOpen, setCreateDBOpen] = useState(false)
  const [deleteDBTarget, setDeleteDBTarget] = useState<Database | null>(null)
  const [createTableOpen, setCreateTableOpen] = useState(false)
  const [deleteTableTarget, setDeleteTableTarget] = useState<Table | null>(null)
  const [dbForm, setDbForm] = useState({ name: '', description: '', locationUri: '' })
  const [tableForm, setTableForm] = useState({ name: '', description: '', location: '', storageType: '' })

  const { data: dbData, isLoading } = useQuery({
    queryKey: ['glue', 'databases'],
    queryFn: () => listDatabases(),
  })

  const { data: tableData } = useQuery({
    queryKey: ['glue', 'tables', selectedDB?.name],
    queryFn: () => listTables(selectedDB!.name),
    enabled: !!selectedDB,
  })

  const createDBMut = useMutation({
    mutationFn: () => createDatabase({ name: dbForm.name, description: dbForm.description || undefined, locationUri: dbForm.locationUri || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'databases'] })
      setCreateDBOpen(false)
      setDbForm({ name: '', description: '', locationUri: '' })
    },
  })

  const deleteDBMut = useMutation({
    mutationFn: (name: string) => deleteDatabase(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'databases'] })
      if (deleteDBTarget?.name === selectedDB?.name) setSelectedDB(null)
      setDeleteDBTarget(null)
    },
  })

  const createTableMut = useMutation({
    mutationFn: () => createTable(selectedDB!.name, { name: tableForm.name, description: tableForm.description || undefined, location: tableForm.location || undefined, storageType: tableForm.storageType || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'tables', selectedDB?.name] })
      setCreateTableOpen(false)
      setTableForm({ name: '', description: '', location: '', storageType: '' })
    },
  })

  const deleteTableMut = useMutation({
    mutationFn: ({ db, name }: { db: string; name: string }) => deleteTable(db, name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'tables', selectedDB?.name] })
      setDeleteTableTarget(null)
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading databases…</div>

  const databases = dbData?.items ?? []
  const tables = tableData?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>Glue Databases</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{databases.length} database{databases.length !== 1 ? 's' : ''}</span>
        </div>
        <button style={btnStyle} onClick={() => setCreateDBOpen(true)}>Create Database</button>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: selectedDB ? '1fr 1fr' : '1fr', gap: '1.5rem' }}>
        <div>
          {databases.length === 0 ? (
            <EmptyState title="No databases. Create one to store your Glue tables." />
          ) : (
            <table style={tableStyle}>
              <thead>
                <tr>{['Name', 'Description', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {databases.map(db => (
                  <tr
                    key={db.name}
                    style={{ borderBottom: '1px solid #2d3748', cursor: 'pointer', background: selectedDB?.name === db.name ? '#1e2d3d' : 'transparent' }}
                    onClick={() => setSelectedDB(db)}
                  >
                    <td style={{ ...tdStyle, fontWeight: 600 }}>{db.name}</td>
                    <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }}>{db.description || '—'}</td>
                    <td style={{ ...tdStyle, textAlign: 'right' }} onClick={e => e.stopPropagation()}>
                      <button style={{ ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }} onClick={() => setDeleteDBTarget(db)}>Delete</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {selectedDB && (
          <div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
              <h3 style={{ margin: 0, fontWeight: 600, fontSize: '1rem' }}>Tables in <em>{selectedDB.name}</em> ({tables.length})</h3>
              <div style={{ display: 'flex', gap: '0.5rem' }}>
                <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0', padding: '0.3rem 0.6rem', fontSize: '0.8rem' }} onClick={() => setSelectedDB(null)}>✕</button>
                <button style={{ ...btnStyle, fontSize: '0.8rem', padding: '0.3rem 0.75rem' }} onClick={() => setCreateTableOpen(true)}>Create Table</button>
              </div>
            </div>
            {tables.length === 0 ? (
              <EmptyState title="No tables in this database." />
            ) : (
              <table style={tableStyle}>
                <thead>
                  <tr>{['Name', 'Location', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
                </thead>
                <tbody>
                  {tables.map(t => (
                    <tr key={t.name} style={{ borderBottom: '1px solid #2d3748' }}>
                      <td style={{ ...tdStyle, fontWeight: 600 }}>{t.name}</td>
                      <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.82rem', fontFamily: 'monospace' }}>{t.location || '—'}</td>
                      <td style={{ ...tdStyle, textAlign: 'right' }}>
                        <button style={{ ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }} onClick={() => setDeleteTableTarget(t)}>Delete</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}
      </div>

      {createDBOpen && (
        <div style={overlayStyle} onClick={() => setCreateDBOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Create Database</h3>
            {[
              { key: 'name', label: 'Name *', placeholder: 'my_database' },
              { key: 'description', label: 'Description', placeholder: '' },
              { key: 'locationUri', label: 'Location URI', placeholder: 's3://bucket/prefix' },
            ].map(f => (
              <div key={f.key} style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }}>
                <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>{f.label}</label>
                <input style={inputStyle} placeholder={f.placeholder} value={(dbForm as Record<string, string>)[f.key]} onChange={e => setDbForm(p => ({ ...p, [f.key]: e.target.value }))} />
              </div>
            ))}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setCreateDBOpen(false)}>Cancel</button>
              <button style={btnStyle} disabled={!dbForm.name || createDBMut.isPending} onClick={() => createDBMut.mutate()}>
                {createDBMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {deleteDBTarget && (
        <div style={overlayStyle} onClick={() => setDeleteDBTarget(null)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem', fontWeight: 600 }}>Delete Database?</h3>
            <p style={{ color: '#b0bec5', marginBottom: '1.5rem' }}>Delete database <strong>{deleteDBTarget.name}</strong>?</p>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setDeleteDBTarget(null)}>Cancel</button>
              <button style={{ ...btnStyle, background: '#d13212' }} disabled={deleteDBMut.isPending} onClick={() => deleteDBMut.mutate(deleteDBTarget.name)}>
                {deleteDBMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}

      {createTableOpen && selectedDB && (
        <div style={overlayStyle} onClick={() => setCreateTableOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Create Table in {selectedDB.name}</h3>
            {[
              { key: 'name', label: 'Name *', placeholder: 'my_table' },
              { key: 'description', label: 'Description', placeholder: '' },
              { key: 'location', label: 'S3 Location', placeholder: 's3://bucket/prefix/' },
              { key: 'storageType', label: 'Input Format', placeholder: 'org.apache.hadoop.mapred.TextInputFormat' },
            ].map(f => (
              <div key={f.key} style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }}>
                <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>{f.label}</label>
                <input style={inputStyle} placeholder={f.placeholder} value={(tableForm as Record<string, string>)[f.key]} onChange={e => setTableForm(p => ({ ...p, [f.key]: e.target.value }))} />
              </div>
            ))}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setCreateTableOpen(false)}>Cancel</button>
              <button style={btnStyle} disabled={!tableForm.name || createTableMut.isPending} onClick={() => createTableMut.mutate()}>
                {createTableMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {deleteTableTarget && (
        <div style={overlayStyle} onClick={() => setDeleteTableTarget(null)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem', fontWeight: 600 }}>Delete Table?</h3>
            <p style={{ color: '#b0bec5', marginBottom: '1.5rem' }}>Delete table <strong>{deleteTableTarget.name}</strong>?</p>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setDeleteTableTarget(null)}>Cancel</button>
              <button style={{ ...btnStyle, background: '#d13212' }} disabled={deleteTableMut.isPending} onClick={() => deleteTableMut.mutate({ db: deleteTableTarget.databaseName, name: deleteTableTarget.name })}>
                {deleteTableMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
