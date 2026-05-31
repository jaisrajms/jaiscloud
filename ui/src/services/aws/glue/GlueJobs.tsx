import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  listJobs,
  createJob,
  deleteJob,
  startJobRun,
  listJobRuns,
  type Job,
  type JobRun,
} from '../../../api/glue'
import { EmptyState } from '../../../components/EmptyState'

const STATE_COLOR: Record<string, string> = {
  SUCCEEDED: '#037f0c',
  FAILED: '#d13212',
  RUNNING: '#0073bb',
  STARTING: '#e77600',
  STOPPING: '#8a6116',
  STOPPED: '#5f6b7a',
  TIMEOUT: '#d13212',
}

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 1rem', verticalAlign: 'middle' }
const btnStyle: React.CSSProperties = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 420, maxWidth: 560 }

function fmtDate(s: string | undefined): string {
  if (!s) return '—'
  try { return new Date(s).toLocaleString() } catch { return s }
}

export function GlueJobs() {
  const qc = useQueryClient()
  const [selectedJob, setSelectedJob] = useState<Job | null>(null)
  const [createOpen, setCreateOpen] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<Job | null>(null)
  const [form, setForm] = useState({ name: '', role: '', command: '' })

  const { data: jobsData, isLoading } = useQuery({
    queryKey: ['glue', 'jobs'],
    queryFn: () => listJobs(),
  })

  const { data: runsData } = useQuery({
    queryKey: ['glue', 'runs', selectedJob?.name],
    queryFn: () => listJobRuns(selectedJob!.name),
    enabled: !!selectedJob,
  })

  const createMut = useMutation({
    mutationFn: () => createJob({ name: form.name, role: form.role || undefined, command: form.command || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'jobs'] })
      setCreateOpen(false)
      setForm({ name: '', role: '', command: '' })
    },
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteJob(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'jobs'] })
      if (deleteTarget?.name === selectedJob?.name) setSelectedJob(null)
      setDeleteTarget(null)
    },
  })

  const runMut = useMutation({
    mutationFn: (name: string) => startJobRun(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'runs', selectedJob?.name] })
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading jobs…</div>

  const jobs = jobsData?.items ?? []
  const runs = runsData?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>Glue Jobs</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{jobs.length} job{jobs.length !== 1 ? 's' : ''}</span>
        </div>
        <button style={btnStyle} onClick={() => setCreateOpen(true)}>Create Job</button>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: selectedJob ? '1fr 1fr' : '1fr', gap: '1.5rem' }}>
        <div>
          {jobs.length === 0 ? (
            <EmptyState title="No jobs. Create a Glue ETL job to get started." />
          ) : (
            <table style={tableStyle}>
              <thead>
                <tr>{['Name', 'Role', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {jobs.map(j => (
                  <tr
                    key={j.name}
                    style={{ borderBottom: '1px solid #2d3748', cursor: 'pointer', background: selectedJob?.name === j.name ? '#1e2d3d' : 'transparent' }}
                    onClick={() => setSelectedJob(j)}
                  >
                    <td style={{ ...tdStyle, fontWeight: 600 }}>{j.name}</td>
                    <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }}>{j.role || '—'}</td>
                    <td style={{ ...tdStyle, textAlign: 'right' }} onClick={e => e.stopPropagation()}>
                      <div style={{ display: 'flex', gap: '0.4rem', justifyContent: 'flex-end' }}>
                        <button style={{ ...btnStyle, padding: '0.25rem 0.6rem', fontSize: '0.8rem' }} onClick={() => runMut.mutate(j.name)} disabled={runMut.isPending}>Run</button>
                        <button style={{ ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }} onClick={() => setDeleteTarget(j)}>Delete</button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {selectedJob && (
          <div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
              <h3 style={{ margin: 0, fontWeight: 600, fontSize: '1rem' }}>Runs for <em>{selectedJob.name}</em> ({runs.length})</h3>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0', padding: '0.3rem 0.6rem', fontSize: '0.8rem' }} onClick={() => setSelectedJob(null)}>✕</button>
            </div>
            {runs.length === 0 ? (
              <EmptyState title="No runs yet. Click Run to start a job run." />
            ) : (
              <table style={tableStyle}>
                <thead>
                  <tr>{['Run ID', 'State', 'Started', 'Completed'].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
                </thead>
                <tbody>
                  {runs.map((r: JobRun) => (
                    <tr key={r.id} style={{ borderBottom: '1px solid #2d3748' }}>
                      <td style={{ ...tdStyle, fontFamily: 'monospace', fontSize: '0.82rem' }}>{r.id}</td>
                      <td style={tdStyle}>
                        <span style={{ color: STATE_COLOR[r.state] ?? '#5f6b7a', fontWeight: 600 }}>{r.state}</span>
                      </td>
                      <td style={{ ...tdStyle, fontSize: '0.82rem', color: '#b0bec5' }}>{fmtDate(r.startedOn)}</td>
                      <td style={{ ...tdStyle, fontSize: '0.82rem', color: '#b0bec5' }}>{fmtDate(r.completedOn)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}
      </div>

      {createOpen && (
        <div style={overlayStyle} onClick={() => setCreateOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Create Glue Job</h3>
            {[
              { key: 'name', label: 'Name *', placeholder: 'my-etl-job' },
              { key: 'role', label: 'IAM Role', placeholder: 'AWSGlueServiceRole' },
              { key: 'command', label: 'Command Script', placeholder: 'glueetl' },
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
            <h3 style={{ margin: '0 0 1rem', fontWeight: 600 }}>Delete Job?</h3>
            <p style={{ color: '#b0bec5', marginBottom: '1.5rem' }}>Delete job <strong>{deleteTarget.name}</strong>?</p>
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
