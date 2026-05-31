import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  listVirtualClusters,
  createVirtualCluster,
  deleteVirtualCluster,
  type VirtualCluster,
} from '../../../api/emroneks'
import { EmptyState } from '../../../components/EmptyState'

const STATE_COLOR: Record<string, string> = {
  RUNNING: '#037f0c',
  ARRESTED: '#d13212',
  TERMINATING: '#8a6116',
  TERMINATED: '#5f6b7a',
}

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 1rem', verticalAlign: 'middle' }
const btnStyle: React.CSSProperties = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 400, maxWidth: 520 }
const fieldStyle: React.CSSProperties = { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }
const labelStyle: React.CSSProperties = { fontSize: '0.8rem', color: '#b0bec5' }

export function EMRContainersList() {
  const [stateFilter, setStateFilter] = useState('')
  const [createOpen, setCreateOpen] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<VirtualCluster | null>(null)
  const [form, setForm] = useState({ name: '', eksClusterId: '', namespace: '' })
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['emrc', 'virtual-clusters', stateFilter],
    queryFn: () => listVirtualClusters(stateFilter ? { state: stateFilter } : undefined),
  })

  const createMut = useMutation({
    mutationFn: () => createVirtualCluster({ name: form.name, eksClusterId: form.eksClusterId || undefined, namespace: form.namespace || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emrc', 'virtual-clusters'] })
      setCreateOpen(false)
      setForm({ name: '', eksClusterId: '', namespace: '' })
    },
  })

  const deleteMut = useMutation({
    mutationFn: (id: string) => deleteVirtualCluster(id),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emrc', 'virtual-clusters'] })
      setDeleteTarget(null)
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading virtual clusters…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load: {(error as Error).message}</div>

  const vcs = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>EMR on EKS — Virtual Clusters</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{vcs.length} cluster{vcs.length !== 1 ? 's' : ''}</span>
        </div>
        <div style={{ display: 'flex', gap: '0.75rem' }}>
          <select value={stateFilter} onChange={e => setStateFilter(e.target.value)} style={{ ...inputStyle, width: 'auto' }}>
            <option value="">All states</option>
            {['RUNNING', 'ARRESTED', 'TERMINATING', 'TERMINATED'].map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <button style={btnStyle} onClick={() => setCreateOpen(true)}>Create Virtual Cluster</button>
        </div>
      </div>

      {vcs.length === 0 ? (
        <EmptyState title="No virtual clusters. Create one to run jobs on EKS." />
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>{['ID', 'Name', 'State', 'EKS Cluster', 'Namespace', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
          </thead>
          <tbody>
            {vcs.map(vc => (
              <tr key={vc.id} style={{ borderBottom: '1px solid #2d3748', cursor: 'pointer' }} onClick={() => navigate(`/aws/emr-containers/${vc.id}`)}>
                <td style={{ ...tdStyle, fontFamily: 'monospace', fontSize: '0.82rem' }}>{vc.id}</td>
                <td style={tdStyle}>{vc.name}</td>
                <td style={tdStyle}>
                  <span style={{ color: STATE_COLOR[vc.state] ?? '#5f6b7a', fontWeight: 600 }}>{vc.state}</span>
                </td>
                <td style={tdStyle}>{vc.eksCluster || '—'}</td>
                <td style={tdStyle}>{vc.namespace || '—'}</td>
                <td style={{ ...tdStyle, textAlign: 'right' }} onClick={e => e.stopPropagation()}>
                  <button
                    style={{ ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }}
                    onClick={() => setDeleteTarget(vc)}
                  >Delete</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {createOpen && (
        <div style={overlayStyle} onClick={() => setCreateOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Create Virtual Cluster</h3>
            {[
              { key: 'name', label: 'Name *', placeholder: 'my-vc' },
              { key: 'eksClusterId', label: 'EKS Cluster ID', placeholder: 'my-eks-cluster' },
              { key: 'namespace', label: 'Namespace', placeholder: 'default' },
            ].map(f => (
              <div key={f.key} style={fieldStyle}>
                <label style={labelStyle}>{f.label}</label>
                <input
                  style={inputStyle}
                  placeholder={f.placeholder}
                  value={(form as Record<string, string>)[f.key]}
                  onChange={e => setForm(prev => ({ ...prev, [f.key]: e.target.value }))}
                />
              </div>
            ))}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1rem' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setCreateOpen(false)}>Cancel</button>
              <button style={btnStyle} disabled={!form.name || createMut.isPending} onClick={() => createMut.mutate()}>
                {createMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
            {createMut.isError && <div style={{ color: '#d13212', marginTop: '0.5rem', fontSize: '0.85rem' }}>{(createMut.error as Error).message}</div>}
          </div>
        </div>
      )}

      {deleteTarget && (
        <div style={overlayStyle} onClick={() => setDeleteTarget(null)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem', fontWeight: 600 }}>Delete Virtual Cluster?</h3>
            <p style={{ color: '#b0bec5', marginBottom: '1.5rem' }}>Delete <strong>{deleteTarget.name}</strong>?</p>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setDeleteTarget(null)}>Cancel</button>
              <button style={{ ...btnStyle, background: '#d13212' }} disabled={deleteMut.isPending} onClick={() => deleteMut.mutate(deleteTarget.id)}>
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
