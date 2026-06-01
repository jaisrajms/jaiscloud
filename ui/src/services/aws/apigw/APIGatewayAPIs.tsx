import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  listRestAPIs,
  createRestAPI,
  deleteRestAPI,
  listResources,
  listStages,
  listDeployments,
  createDeployment,
  type RestAPI,
} from '../../../api/apigw'
import { EmptyState } from '../../../components/EmptyState'

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 1rem', verticalAlign: 'middle' }
const btnStyle: React.CSSProperties = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 440, maxWidth: 580 }

type Tab = 'resources' | 'stages' | 'deployments'

export function APIGatewayAPIs() {
  const qc = useQueryClient()
  const [selectedAPI, setSelectedAPI] = useState<RestAPI | null>(null)
  const [activeTab, setActiveTab] = useState<Tab>('resources')
  const [createOpen, setCreateOpen] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<RestAPI | null>(null)
  const [deployOpen, setDeployOpen] = useState(false)
  const [form, setForm] = useState({ name: '', description: '' })
  const [deployForm, setDeployForm] = useState({ stageName: '', description: '' })

  const { data, isLoading } = useQuery({
    queryKey: ['apigw', 'apis'],
    queryFn: () => listRestAPIs(),
  })

  const { data: resourcesData } = useQuery({
    queryKey: ['apigw', 'resources', selectedAPI?.id],
    queryFn: () => listResources(selectedAPI!.id),
    enabled: !!selectedAPI && activeTab === 'resources',
  })

  const { data: stagesData } = useQuery({
    queryKey: ['apigw', 'stages', selectedAPI?.id],
    queryFn: () => listStages(selectedAPI!.id),
    enabled: !!selectedAPI && activeTab === 'stages',
  })

  const { data: deploymentsData } = useQuery({
    queryKey: ['apigw', 'deployments', selectedAPI?.id],
    queryFn: () => listDeployments(selectedAPI!.id),
    enabled: !!selectedAPI && activeTab === 'deployments',
  })

  const createMut = useMutation({
    mutationFn: () => createRestAPI({ name: form.name, description: form.description || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['apigw', 'apis'] })
      setCreateOpen(false)
      setForm({ name: '', description: '' })
    },
  })

  const deleteMut = useMutation({
    mutationFn: (id: string) => deleteRestAPI(id),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['apigw', 'apis'] })
      if (deleteTarget?.id === selectedAPI?.id) setSelectedAPI(null)
      setDeleteTarget(null)
    },
  })

  const deployMut = useMutation({
    mutationFn: () => createDeployment(selectedAPI!.id, { stageName: deployForm.stageName || undefined, description: deployForm.description || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['apigw', 'deployments', selectedAPI?.id] })
      void qc.invalidateQueries({ queryKey: ['apigw', 'stages', selectedAPI?.id] })
      setDeployOpen(false)
      setDeployForm({ stageName: '', description: '' })
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading APIs…</div>

  const apis = data?.items ?? []
  const resources = resourcesData?.items ?? []
  const stages = stagesData?.items ?? []
  const deployments = deploymentsData?.items ?? []

  const tabBtnStyle = (t: Tab): React.CSSProperties => ({
    padding: '0.4rem 1rem',
    fontSize: '0.82rem',
    border: 'none',
    borderRadius: 4,
    cursor: 'pointer',
    background: activeTab === t ? '#0073bb' : '#2d3748',
    color: activeTab === t ? '#fff' : '#b0bec5',
  })

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>API Gateway</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{apis.length} REST API{apis.length !== 1 ? 's' : ''}</span>
        </div>
        <button style={btnStyle} onClick={() => setCreateOpen(true)}>Create API</button>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: selectedAPI ? '1fr 1.4fr' : '1fr', gap: '1.5rem' }}>
        <div>
          {apis.length === 0 ? (
            <EmptyState title="No REST APIs. Create one to get started." />
          ) : (
            <table style={tableStyle}>
              <thead>
                <tr>{['Name', 'ID', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {apis.map(api => (
                  <tr
                    key={api.id}
                    style={{ borderBottom: '1px solid #2d3748', cursor: 'pointer', background: selectedAPI?.id === api.id ? '#1e2d3d' : 'transparent' }}
                    onClick={() => setSelectedAPI(api)}
                  >
                    <td style={{ ...tdStyle, fontWeight: 600 }}>{api.name}</td>
                    <td style={{ ...tdStyle, color: '#b0bec5', fontFamily: 'monospace', fontSize: '0.82rem' }}>{api.id}</td>
                    <td style={{ ...tdStyle, textAlign: 'right' }} onClick={e => e.stopPropagation()}>
                      <button style={{ ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }} onClick={() => setDeleteTarget(api)}>Delete</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {selectedAPI && (
          <div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
              <h3 style={{ margin: 0, fontWeight: 600, fontSize: '1rem' }}>{selectedAPI.name}</h3>
              <div style={{ display: 'flex', gap: '0.5rem' }}>
                <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0', padding: '0.3rem 0.6rem', fontSize: '0.8rem' }} onClick={() => setSelectedAPI(null)}>✕</button>
                {activeTab === 'deployments' && (
                  <button style={{ ...btnStyle, fontSize: '0.8rem', padding: '0.3rem 0.75rem' }} onClick={() => setDeployOpen(true)}>Deploy</button>
                )}
              </div>
            </div>

            <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem' }}>
              {(['resources', 'stages', 'deployments'] as Tab[]).map(t => (
                <button key={t} style={tabBtnStyle(t)} onClick={() => setActiveTab(t)}>{t.charAt(0).toUpperCase() + t.slice(1)}</button>
              ))}
            </div>

            {activeTab === 'resources' && (
              resources.length === 0 ? <EmptyState title="No resources defined." /> : (
                <table style={tableStyle}>
                  <thead><tr>{['Path', 'ID', 'Parent'].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr></thead>
                  <tbody>
                    {resources.map(r => (
                      <tr key={r.id} style={{ borderBottom: '1px solid #2d3748' }}>
                        <td style={{ ...tdStyle, fontWeight: 600, fontFamily: 'monospace' }}>{r.path}</td>
                        <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.82rem', fontFamily: 'monospace' }}>{r.id}</td>
                        <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.82rem', fontFamily: 'monospace' }}>{r.parentId || '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )
            )}

            {activeTab === 'stages' && (
              stages.length === 0 ? <EmptyState title="No stages. Deploy to create a stage." /> : (
                <table style={tableStyle}>
                  <thead><tr>{['Stage', 'Deployment ID', 'Description'].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr></thead>
                  <tbody>
                    {stages.map(s => (
                      <tr key={s.name} style={{ borderBottom: '1px solid #2d3748' }}>
                        <td style={{ ...tdStyle, fontWeight: 600 }}>{s.name}</td>
                        <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.82rem', fontFamily: 'monospace' }}>{s.deploymentId || '—'}</td>
                        <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }}>{s.description || '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )
            )}

            {activeTab === 'deployments' && (
              deployments.length === 0 ? <EmptyState title="No deployments yet." /> : (
                <table style={tableStyle}>
                  <thead><tr>{['ID', 'Description', 'Created'].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr></thead>
                  <tbody>
                    {deployments.map(d => (
                      <tr key={d.id} style={{ borderBottom: '1px solid #2d3748' }}>
                        <td style={{ ...tdStyle, fontFamily: 'monospace', fontSize: '0.82rem' }}>{d.id}</td>
                        <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }}>{d.description || '—'}</td>
                        <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.82rem' }}>{d.createdDate || '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )
            )}
          </div>
        )}
      </div>

      {createOpen && (
        <div style={overlayStyle} onClick={() => setCreateOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Create REST API</h3>
            {[
              { key: 'name', label: 'Name *', placeholder: 'my-api' },
              { key: 'description', label: 'Description', placeholder: '' },
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
            <h3 style={{ margin: '0 0 1rem', fontWeight: 600 }}>Delete REST API?</h3>
            <p style={{ color: '#b0bec5', marginBottom: '1.5rem' }}>Delete API <strong>{deleteTarget.name}</strong>?</p>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setDeleteTarget(null)}>Cancel</button>
              <button style={{ ...btnStyle, background: '#d13212' }} disabled={deleteMut.isPending} onClick={() => deleteMut.mutate(deleteTarget.id)}>
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}

      {deployOpen && selectedAPI && (
        <div style={overlayStyle} onClick={() => setDeployOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Deploy {selectedAPI.name}</h3>
            {[
              { key: 'stageName', label: 'Stage Name', placeholder: 'prod' },
              { key: 'description', label: 'Description', placeholder: '' },
            ].map(f => (
              <div key={f.key} style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }}>
                <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>{f.label}</label>
                <input style={inputStyle} placeholder={f.placeholder} value={(deployForm as Record<string, string>)[f.key]} onChange={e => setDeployForm(p => ({ ...p, [f.key]: e.target.value }))} />
              </div>
            ))}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setDeployOpen(false)}>Cancel</button>
              <button style={btnStyle} disabled={deployMut.isPending} onClick={() => deployMut.mutate()}>
                {deployMut.isPending ? 'Deploying…' : 'Deploy'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
