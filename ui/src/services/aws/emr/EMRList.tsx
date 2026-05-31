import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  listClusters,
  runJobFlow,
  terminateCluster,
  type ClusterSummary,
} from '../../../api/emr'
import { EmptyState } from '../../../components/EmptyState'

const STATE_COLOR: Record<string, string> = {
  RUNNING: '#037f0c',
  WAITING: '#0073bb',
  STARTING: '#e77600',
  BOOTSTRAPPING: '#e77600',
  TERMINATING: '#8a6116',
  TERMINATED: '#5f6b7a',
  TERMINATED_WITH_ERRORS: '#d13212',
}

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 1rem', verticalAlign: 'middle' }
const btnStyle: React.CSSProperties = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', minWidth: 160 }
const actionBtnStyle: React.CSSProperties = { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 400, maxWidth: 520 }
const fieldStyle: React.CSSProperties = { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }
const labelStyle: React.CSSProperties = { fontSize: '0.8rem', color: '#b0bec5' }

export function EMRList() {
  const [stateFilter, setStateFilter] = useState('')
  const [createOpen, setCreateOpen] = useState(false)
  const [terminateTarget, setTerminateTarget] = useState<ClusterSummary | null>(null)
  const [form, setForm] = useState({ name: '', releaseLabel: '', logUri: '', serviceRole: '', jobFlowRole: '' })
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data, isLoading, error } = useQuery({
    queryKey: ['emr', 'clusters', stateFilter],
    queryFn: () => listClusters(stateFilter ? { state: stateFilter } : undefined),
  })

  const createMut = useMutation({
    mutationFn: () => runJobFlow({ name: form.name, releaseLabel: form.releaseLabel || undefined, logUri: form.logUri || undefined, serviceRole: form.serviceRole || undefined, jobFlowRole: form.jobFlowRole || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emr', 'clusters'] })
      setCreateOpen(false)
      setForm({ name: '', releaseLabel: '', logUri: '', serviceRole: '', jobFlowRole: '' })
    },
  })

  const terminateMut = useMutation({
    mutationFn: (id: string) => terminateCluster(id),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emr', 'clusters'] })
      setTerminateTarget(null)
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading clusters…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load: {(error as Error).message}</div>

  const clusters = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>EMR Clusters</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{clusters.length} cluster{clusters.length !== 1 ? 's' : ''}</span>
        </div>
        <div style={{ display: 'flex', gap: '0.75rem' }}>
          <select value={stateFilter} onChange={e => setStateFilter(e.target.value)} style={inputStyle}>
            <option value="">All states</option>
            {['RUNNING', 'WAITING', 'STARTING', 'BOOTSTRAPPING', 'TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'].map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <button style={btnStyle} onClick={() => setCreateOpen(true)}>Create Cluster</button>
        </div>
      </div>

      {clusters.length === 0 ? (
        <EmptyState title="No clusters. Create one to get started." />
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>{['ID', 'Name', 'State', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
          </thead>
          <tbody>
            {clusters.map(c => (
              <tr key={c.id} style={{ borderBottom: '1px solid #2d3748', cursor: 'pointer' }} onClick={() => navigate(`/aws/emr/${c.id}`)}>
                <td style={{ ...tdStyle, fontFamily: 'monospace', fontSize: '0.82rem' }}>{c.id}</td>
                <td style={tdStyle}>{c.name}</td>
                <td style={tdStyle}>
                  <span style={{ color: STATE_COLOR[c.state] ?? '#5f6b7a', fontWeight: 600 }}>{c.state}</span>
                </td>
                <td style={{ ...tdStyle, textAlign: 'right' }} onClick={e => e.stopPropagation()}>
                  <button style={actionBtnStyle} onClick={() => setTerminateTarget(c)}>Terminate</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {createOpen && (
        <div style={overlayStyle} onClick={() => setCreateOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Create EMR Cluster</h3>
            {[
              { key: 'name', label: 'Name *', placeholder: 'my-cluster' },
              { key: 'releaseLabel', label: 'Release Label', placeholder: 'emr-6.10.0' },
              { key: 'logUri', label: 'Log URI', placeholder: 's3://my-bucket/logs' },
              { key: 'serviceRole', label: 'Service Role', placeholder: 'EMR_DefaultRole' },
              { key: 'jobFlowRole', label: 'Job Flow Role', placeholder: 'EMR_EC2_DefaultRole' },
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

      {terminateTarget && (
        <div style={overlayStyle} onClick={() => setTerminateTarget(null)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem', fontWeight: 600 }}>Terminate Cluster?</h3>
            <p style={{ color: '#b0bec5', marginBottom: '1.5rem' }}>Terminate <strong>{terminateTarget.name}</strong> ({terminateTarget.id})?</p>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setTerminateTarget(null)}>Cancel</button>
              <button style={{ ...btnStyle, background: '#d13212' }} disabled={terminateMut.isPending} onClick={() => terminateMut.mutate(terminateTarget.id)}>
                {terminateMut.isPending ? 'Terminating…' : 'Terminate'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
