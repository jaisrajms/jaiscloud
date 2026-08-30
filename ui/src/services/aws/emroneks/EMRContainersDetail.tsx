import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  describeVirtualCluster,
  listJobRuns,
  startJobRun,
  cancelJobRun,
  type JobRun,
} from '../../../api/emroneks'
import { EmptyState } from '../../../components/EmptyState'

const STATE_COLOR: Record<string, string> = {
  RUNNING: '#037f0c',
  COMPLETED: '#037f0c',
  FAILED: '#d13212',
  CANCELLED: '#5f6b7a',
  SUBMITTED: '#e77600',
  PENDING: '#e77600',
}

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 1rem', verticalAlign: 'middle' }
const btnStyle: React.CSSProperties = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 400, maxWidth: 520 }

export function EMRContainersDetail() {
  const { id } = useParams<{ id: string }>()
  const qc = useQueryClient()
  const [submitOpen, setSubmitOpen] = useState(false)
  const [cancelTarget, setCancelTarget] = useState<JobRun | null>(null)
  const [form, setForm] = useState({ name: '', releaseLabel: '', executionRoleArn: '' })

  const { data: vc, isLoading, error } = useQuery({
    queryKey: ['emrc', 'vc', id],
    queryFn: () => describeVirtualCluster(id!),
    enabled: !!id,
  })

  const { data: jobsData } = useQuery({
    queryKey: ['emrc', 'jobs', id],
    queryFn: () => listJobRuns(id!),
    enabled: !!id,
  })

  const submitMut = useMutation({
    mutationFn: () => startJobRun(id!, { name: form.name, releaseLabel: form.releaseLabel || undefined, executionRoleArn: form.executionRoleArn || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emrc', 'jobs', id] })
      setSubmitOpen(false)
      setForm({ name: '', releaseLabel: '', executionRoleArn: '' })
    },
  })

  const cancelMut = useMutation({
    mutationFn: (jobId: string) => cancelJobRun(id!, jobId),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emrc', 'jobs', id] })
      setCancelTarget(null)
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading virtual cluster…</div>
  if (error || !vc) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load virtual cluster.</div>

  const jobs = jobsData?.items ?? []

  return (
    <div>
      <div style={{ marginBottom: '1.5rem' }}>
        <Link to="/aws/emr-containers" style={{ color: '#0073bb', textDecoration: 'none', fontSize: '0.85rem' }}>← Virtual Clusters</Link>
        <h2 style={{ margin: '0.5rem 0 0', fontWeight: 600, fontSize: '1.4rem' }}>{vc.name}</h2>
        <span style={{ color: STATE_COLOR[vc.state] ?? '#5f6b7a', fontWeight: 600 }}>{vc.state}</span>
        {vc.eksCluster && <span style={{ color: '#b0bec5', marginLeft: '0.75rem', fontSize: '0.85rem' }}>EKS: {vc.eksCluster} / {vc.namespace}</span>}
      </div>

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <h3 style={{ margin: 0, fontWeight: 600, fontSize: '1rem' }}>Job Runs ({jobs.length})</h3>
        <button style={btnStyle} onClick={() => setSubmitOpen(true)}>Submit Job Run</button>
      </div>

      {jobs.length === 0 ? (
        <EmptyState title="No job runs yet. Submit a job to get started." />
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>{['ID', 'Name', 'State', 'Release Label', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
          </thead>
          <tbody>
            {jobs.map(j => (
              <tr key={j.id} style={{ borderBottom: '1px solid #2d3748' }}>
                <td style={{ ...tdStyle, fontFamily: 'monospace', fontSize: '0.82rem' }}>{j.id}</td>
                <td style={tdStyle}>{j.name}</td>
                <td style={tdStyle}>
                  <span style={{ color: STATE_COLOR[j.state] ?? '#5f6b7a', fontWeight: 600 }}>{j.state}</span>
                </td>
                <td style={tdStyle}>{j.releaseLabel || '—'}</td>
                <td style={{ ...tdStyle, textAlign: 'right' }}>
                  {['RUNNING', 'SUBMITTED', 'PENDING'].includes(j.state) && (
                    <button
                      style={{ ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }}
                      onClick={() => setCancelTarget(j)}
                    >Cancel</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {submitOpen && (
        <div style={overlayStyle} onClick={() => setSubmitOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Submit Job Run</h3>
            {[
              { key: 'name', label: 'Job Name *', placeholder: 'my-spark-job' },
              { key: 'releaseLabel', label: 'Release Label', placeholder: 'emr-6.10.0-latest' },
              { key: 'executionRoleArn', label: 'Execution Role ARN', placeholder: 'arn:aws:iam::…:role/EMRRole' },
            ].map(f => (
              <div key={f.key} style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }}>
                <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>{f.label}</label>
                <input
                  style={inputStyle}
                  placeholder={f.placeholder}
                  value={(form as Record<string, string>)[f.key]}
                  onChange={e => setForm(prev => ({ ...prev, [f.key]: e.target.value }))}
                />
              </div>
            ))}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1rem' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setSubmitOpen(false)}>Cancel</button>
              <button style={btnStyle} disabled={!form.name || submitMut.isPending} onClick={() => submitMut.mutate()}>
                {submitMut.isPending ? 'Submitting…' : 'Submit'}
              </button>
            </div>
          </div>
        </div>
      )}

      {cancelTarget && (
        <div style={overlayStyle} onClick={() => setCancelTarget(null)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem', fontWeight: 600 }}>Cancel Job Run?</h3>
            <p style={{ color: '#b0bec5', marginBottom: '1.5rem' }}>Cancel job <strong>{cancelTarget.name}</strong>?</p>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setCancelTarget(null)}>Back</button>
              <button style={{ ...btnStyle, background: '#d13212' }} disabled={cancelMut.isPending} onClick={() => cancelMut.mutate(cancelTarget.id)}>
                {cancelMut.isPending ? 'Cancelling…' : 'Cancel Job'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
