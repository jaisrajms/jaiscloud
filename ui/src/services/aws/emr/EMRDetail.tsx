import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { describeCluster, listSteps, addSteps, cancelStep, type Step } from '../../../api/emr'
import { EmptyState } from '../../../components/EmptyState'

const STATE_COLOR: Record<string, string> = {
  RUNNING: '#037f0c',
  WAITING: '#0073bb',
  STARTING: '#e77600',
  COMPLETED: '#037f0c',
  FAILED: '#d13212',
  CANCELLED: '#5f6b7a',
  PENDING: '#e77600',
  INTERRUPTED: '#8a6116',
}

const cardStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '1.5rem', marginBottom: '1.5rem' }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 1rem', verticalAlign: 'middle' }
const btnStyle: React.CSSProperties = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 420, maxWidth: 560 }

export function EMRDetail() {
  const { id } = useParams<{ id: string }>()
  const qc = useQueryClient()
  const [addStepOpen, setAddStepOpen] = useState(false)
  const [stepName, setStepName] = useState('')
  const [cancelTarget, setCancelTarget] = useState<Step | null>(null)

  const { data: cluster, isLoading, error } = useQuery({
    queryKey: ['emr', 'cluster', id],
    queryFn: () => describeCluster(id!),
    enabled: !!id,
  })

  const { data: stepsData } = useQuery({
    queryKey: ['emr', 'steps', id],
    queryFn: () => listSteps(id!),
    enabled: !!id,
  })

  const addMut = useMutation({
    mutationFn: () => addSteps(id!, [{ Name: stepName, ActionOnFailure: 'CONTINUE', HadoopJarStep: { Jar: 'command-runner.jar', Args: ['echo', stepName] } }]),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emr', 'steps', id] })
      setAddStepOpen(false)
      setStepName('')
    },
  })

  const cancelMut = useMutation({
    mutationFn: (stepId: string) => cancelStep(id!, stepId),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emr', 'steps', id] })
      setCancelTarget(null)
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading cluster…</div>
  if (error || !cluster) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load cluster.</div>

  const steps = stepsData?.items ?? []

  return (
    <div>
      <div style={{ marginBottom: '1.5rem' }}>
        <Link to="/aws/emr" style={{ color: '#0073bb', textDecoration: 'none', fontSize: '0.85rem' }}>← Clusters</Link>
        <h2 style={{ margin: '0.5rem 0 0', fontWeight: 600, fontSize: '1.4rem' }}>{cluster.name}</h2>
        <span style={{ color: STATE_COLOR[cluster.state] ?? '#5f6b7a', fontWeight: 600 }}>{cluster.state}</span>
        {cluster.stateChangeReason && <span style={{ color: '#b0bec5', marginLeft: '0.5rem', fontSize: '0.85rem' }}>— {cluster.stateChangeReason}</span>}
      </div>

      <div style={cardStyle}>
        <h3 style={{ margin: '0 0 1rem', fontWeight: 600, fontSize: '1rem' }}>Details</h3>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem', fontSize: '0.85rem' }}>
          {[
            ['ID', cluster.id],
            ['ARN', cluster.arn || '—'],
            ['Release Label', cluster.releaseLabel || '—'],
            ['Log URI', cluster.logUri || '—'],
            ['Auto Terminate', String(cluster.autoTerminate)],
            ['Termination Protected', String(cluster.terminationProtected)],
          ].map(([k, v]) => (
            <div key={k}>
              <div style={{ color: '#b0bec5', marginBottom: '0.2rem' }}>{k}</div>
              <div style={{ fontFamily: 'monospace', wordBreak: 'break-all' }}>{v}</div>
            </div>
          ))}
        </div>
        {cluster.applications.length > 0 && (
          <div style={{ marginTop: '1rem' }}>
            <div style={{ color: '#b0bec5', fontSize: '0.8rem', marginBottom: '0.3rem' }}>Applications</div>
            <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
              {cluster.applications.map(a => (
                <span key={a} style={{ background: '#2d3748', borderRadius: 4, padding: '0.2rem 0.5rem', fontSize: '0.8rem' }}>{a}</span>
              ))}
            </div>
          </div>
        )}
      </div>

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <h3 style={{ margin: 0, fontWeight: 600, fontSize: '1rem' }}>Steps ({steps.length})</h3>
        <button style={btnStyle} onClick={() => setAddStepOpen(true)}>Add Step</button>
      </div>

      {steps.length === 0 ? (
        <EmptyState title="No steps. Add a step to run work on this cluster." />
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>{['ID', 'Name', 'State', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
          </thead>
          <tbody>
            {steps.map(s => (
              <tr key={s.id} style={{ borderBottom: '1px solid #2d3748' }}>
                <td style={{ ...tdStyle, fontFamily: 'monospace', fontSize: '0.82rem' }}>{s.id}</td>
                <td style={tdStyle}>{s.name}</td>
                <td style={tdStyle}>
                  <span style={{ color: STATE_COLOR[s.state] ?? '#5f6b7a', fontWeight: 600 }}>{s.state}</span>
                </td>
                <td style={{ ...tdStyle, textAlign: 'right' }}>
                  {['RUNNING', 'PENDING'].includes(s.state) && (
                    <button style={{ ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }} onClick={() => setCancelTarget(s)}>Cancel</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {addStepOpen && (
        <div style={overlayStyle} onClick={() => setAddStepOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Add Step</h3>
            <div style={{ marginBottom: '1rem' }}>
              <label style={{ fontSize: '0.8rem', color: '#b0bec5', display: 'block', marginBottom: '0.3rem' }}>Step Name *</label>
              <input style={inputStyle} placeholder="my-step" value={stepName} onChange={e => setStepName(e.target.value)} />
            </div>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1rem' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setAddStepOpen(false)}>Cancel</button>
              <button style={btnStyle} disabled={!stepName || addMut.isPending} onClick={() => addMut.mutate()}>
                {addMut.isPending ? 'Adding…' : 'Add Step'}
              </button>
            </div>
          </div>
        </div>
      )}

      {cancelTarget && (
        <div style={overlayStyle} onClick={() => setCancelTarget(null)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem', fontWeight: 600 }}>Cancel Step?</h3>
            <p style={{ color: '#b0bec5', marginBottom: '1.5rem' }}>Cancel step <strong>{cancelTarget.name}</strong>?</p>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setCancelTarget(null)}>Back</button>
              <button style={{ ...btnStyle, background: '#d13212' }} disabled={cancelMut.isPending} onClick={() => cancelMut.mutate(cancelTarget.id)}>
                {cancelMut.isPending ? 'Cancelling…' : 'Cancel Step'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
