import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useSearchParams, useNavigate } from 'react-router-dom'
import {
  listExecutions,
  startExecution,
  stopExecution,
  getExecutionHistory,
  type Execution,
} from '../../../api/sfn'
import { EmptyState } from '../../../components/EmptyState'

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 1rem', verticalAlign: 'middle' }
const btnStyle: React.CSSProperties = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 440, maxWidth: 620 }

const statusColor = (s: string) => {
  if (s === 'RUNNING') return '#3498db'
  if (s === 'SUCCEEDED') return '#2ecc71'
  if (s === 'FAILED' || s === 'TIMED_OUT' || s === 'ABORTED') return '#e74c3c'
  return '#b0bec5'
}

export function SFNExecutions() {
  const qc = useQueryClient()
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const smArn = searchParams.get('arn') ?? ''

  const [startOpen, setStartOpen] = useState(false)
  const [historyExec, setHistoryExec] = useState<Execution | null>(null)
  const [form, setForm] = useState({ name: '', input: '{}' })

  const { data, isLoading } = useQuery({
    queryKey: ['sfn', 'executions', smArn],
    queryFn: () => listExecutions(smArn),
    enabled: !!smArn,
  })

  const { data: historyData } = useQuery({
    queryKey: ['sfn', 'history', historyExec?.arn],
    queryFn: () => getExecutionHistory(historyExec!.arn),
    enabled: !!historyExec,
  })

  const startMut = useMutation({
    mutationFn: () => startExecution(smArn, { name: form.name || undefined, input: form.input }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sfn', 'executions', smArn] })
      setStartOpen(false)
      setForm({ name: '', input: '{}' })
    },
  })

  const stopMut = useMutation({
    mutationFn: (arn: string) => stopExecution(arn),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['sfn', 'executions', smArn] }) },
  })

  if (!smArn) {
    return (
      <div style={{ padding: '2rem' }}>
        <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0', marginBottom: '1rem' }} onClick={() => navigate('../state-machines')}>← State Machines</button>
        <EmptyState title="No state machine selected." />
      </div>
    )
  }

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading executions…</div>

  const executions = data?.items ?? []
  const events = historyData?.events ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0', padding: '0.3rem 0.75rem', fontSize: '0.82rem', marginBottom: '0.5rem' }} onClick={() => navigate('../state-machines')}>← State Machines</button>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>Executions</h2>
          <span style={{ fontSize: '0.8rem', color: '#5f6b7a', fontFamily: 'monospace' }}>{smArn}</span>
        </div>
        <button style={btnStyle} onClick={() => setStartOpen(true)}>Start Execution</button>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: historyExec ? '1fr 1fr' : '1fr', gap: '1.5rem' }}>
        <div>
          {executions.length === 0 ? (
            <EmptyState title="No executions. Start one to run this state machine." />
          ) : (
            <table style={tableStyle}>
              <thead>
                <tr>{['Name', 'Status', 'Started', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {executions.map(exec => (
                  <tr
                    key={exec.arn}
                    style={{ borderBottom: '1px solid #2d3748', cursor: 'pointer', background: historyExec?.arn === exec.arn ? '#1e2d3d' : 'transparent' }}
                    onClick={() => setHistoryExec(exec)}
                  >
                    <td style={{ ...tdStyle, fontWeight: 600, maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{exec.name}</td>
                    <td style={tdStyle}>
                      <span style={{ color: statusColor(exec.status), fontSize: '0.8rem', fontWeight: 600 }}>{exec.status}</span>
                    </td>
                    <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.82rem' }}>
                      {exec.startDate ? new Date(exec.startDate * 1000).toLocaleString() : '—'}
                    </td>
                    <td style={{ ...tdStyle, textAlign: 'right' }} onClick={e => e.stopPropagation()}>
                      {exec.status === 'RUNNING' && (
                        <button style={{ ...btnStyle, background: 'transparent', color: '#e87600', border: '1px solid #e87600', padding: '0.25rem 0.6rem', fontSize: '0.78rem' }} onClick={() => stopMut.mutate(exec.arn)}>Stop</button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {historyExec && (
          <div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
              <h3 style={{ margin: 0, fontWeight: 600, fontSize: '1rem' }}>History ({events.length})</h3>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0', padding: '0.3rem 0.6rem', fontSize: '0.8rem' }} onClick={() => setHistoryExec(null)}>✕</button>
            </div>
            {events.length === 0 ? <EmptyState title="No events." /> : (
              <table style={tableStyle}>
                <thead><tr>{['ID', 'Type', 'Timestamp'].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr></thead>
                <tbody>
                  {events.map(ev => (
                    <tr key={ev.id} style={{ borderBottom: '1px solid #2d3748' }}>
                      <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.82rem' }}>{ev.id}</td>
                      <td style={{ ...tdStyle, fontSize: '0.85rem' }}>{ev.type}</td>
                      <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.82rem' }}>
                        {ev.timestamp ? new Date(ev.timestamp * 1000).toLocaleString() : '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}
      </div>

      {startOpen && (
        <div style={overlayStyle} onClick={() => setStartOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Start Execution</h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }}>
              <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>Execution Name (optional)</label>
              <input style={inputStyle} placeholder="my-execution" value={form.name} onChange={e => setForm(p => ({ ...p, name: e.target.value }))} />
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1.5rem' }}>
              <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>Input (JSON)</label>
              <textarea
                style={{ ...inputStyle, minHeight: 100, resize: 'vertical', fontFamily: 'monospace' }}
                value={form.input}
                onChange={e => setForm(p => ({ ...p, input: e.target.value }))}
              />
            </div>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setStartOpen(false)}>Cancel</button>
              <button style={btnStyle} disabled={startMut.isPending} onClick={() => startMut.mutate()}>
                {startMut.isPending ? 'Starting…' : 'Start'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
