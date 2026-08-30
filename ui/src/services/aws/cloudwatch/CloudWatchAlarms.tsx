import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  listAlarms,
  putAlarm,
  deleteAlarm,
  setAlarmState,
  enableAlarmActions,
  disableAlarmActions,
  type CWAlarm,
} from '../../../api/cloudwatch'
import { EmptyState } from '../../../components/EmptyState'

const STATE_COLOR: Record<string, string> = {
  OK: '#037f0c',
  ALARM: '#d13212',
  INSUFFICIENT_DATA: '#8a6116',
}

export function CloudWatchAlarms() {
  const [createOpen, setCreateOpen] = useState(false)
  const [stateFilter, setStateFilter] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<CWAlarm | null>(null)
  const [stateTarget, setStateTarget] = useState<CWAlarm | null>(null)
  const [newStateValue, setNewStateValue] = useState('OK')
  const [newStateReason, setNewStateReason] = useState('')

  // Create alarm form
  const [form, setForm] = useState({
    alarmName: '',
    namespace: '',
    metricName: '',
    statistic: 'Average',
    period: 60,
    threshold: 0,
    comparisonOperator: 'GreaterThanThreshold',
    evaluationPeriods: 1,
  })

  const qc = useQueryClient()

  const { data, isLoading, error } = useQuery({
    queryKey: ['cloudwatch', 'alarms', stateFilter],
    queryFn: () => listAlarms(stateFilter ? { stateValue: stateFilter } : undefined),
  })

  const createMut = useMutation({
    mutationFn: () => putAlarm({ ...form, actionsEnabled: true }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] })
      setCreateOpen(false)
      setForm({ alarmName: '', namespace: '', metricName: '', statistic: 'Average', period: 60, threshold: 0, comparisonOperator: 'GreaterThanThreshold', evaluationPeriods: 1 })
    },
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteAlarm(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] })
      setDeleteTarget(null)
    },
  })

  const stateMut = useMutation({
    mutationFn: () => setAlarmState({ alarmName: stateTarget!.alarmName, stateValue: newStateValue, stateReason: newStateReason }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] })
      setStateTarget(null)
    },
  })

  const enableMut = useMutation({
    mutationFn: (name: string) => enableAlarmActions([name]),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] }),
  })

  const disableMut = useMutation({
    mutationFn: (name: string) => disableAlarmActions([name]),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] }),
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading alarms…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load: {(error as Error).message}</div>

  const alarms = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>CloudWatch Alarms</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{alarms.length} alarm{alarms.length !== 1 ? 's' : ''}</span>
        </div>
        <div style={{ display: 'flex', gap: '0.75rem' }}>
          <select value={stateFilter} onChange={e => setStateFilter(e.target.value)} style={selectStyle}>
            <option value="">All states</option>
            <option value="OK">OK</option>
            <option value="ALARM">ALARM</option>
            <option value="INSUFFICIENT_DATA">INSUFFICIENT_DATA</option>
          </select>
          <button onClick={() => setCreateOpen(true)} style={primaryBtnStyle}>Create Alarm</button>
        </div>
      </div>

      {alarms.length === 0 ? (
        <EmptyState title="No alarms found." />
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>
              {['Name', 'Metric', 'State', 'Actions', ''].map(h => (
                <th key={h} style={thStyle}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {alarms.map(a => (
              <tr key={a.alarmName} style={{ borderBottom: '1px solid #2d3748' }}>
                <td style={tdStyle}>{a.alarmName}</td>
                <td style={tdStyle}>{a.namespace}/{a.metricName}</td>
                <td style={tdStyle}>
                  <span style={{ color: STATE_COLOR[a.stateValue ?? ''] ?? '#c9cdd4', fontWeight: 500 }}>
                    {a.stateValue ?? '—'}
                  </span>
                </td>
                <td style={tdStyle}>
                  <span style={{ color: a.actionsEnabled ? '#037f0c' : '#8892a4', fontSize: '0.82em' }}>
                    {a.actionsEnabled ? 'enabled' : 'disabled'}
                  </span>
                </td>
                <td style={{ ...tdStyle, textAlign: 'right' }}>
                  <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                    <button onClick={() => { setStateTarget(a); setNewStateValue('OK'); setNewStateReason('') }} style={actionBtnStyle}>Set State</button>
                    {a.actionsEnabled
                      ? <button onClick={() => disableMut.mutate(a.alarmName)} style={actionBtnStyle}>Disable</button>
                      : <button onClick={() => enableMut.mutate(a.alarmName)} style={actionBtnStyle}>Enable</button>
                    }
                    <button onClick={() => setDeleteTarget(a)} style={{ ...actionBtnStyle, color: '#d13212', borderColor: '#d13212' }}>Delete</button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {/* Create alarm dialog */}
      {createOpen && (
        <div style={overlayStyle}>
          <div style={dialogStyle}>
            <h3 style={{ margin: '0 0 1rem', color: '#e2e8f0' }}>Create Alarm</h3>
            {[
              { label: 'Alarm Name *', key: 'alarmName', type: 'text' },
              { label: 'Namespace', key: 'namespace', type: 'text' },
              { label: 'Metric Name', key: 'metricName', type: 'text' },
              { label: 'Period (s)', key: 'period', type: 'number' },
              { label: 'Threshold', key: 'threshold', type: 'number' },
              { label: 'Evaluation Periods', key: 'evaluationPeriods', type: 'number' },
            ].map(({ label, key, type }) => (
              <label key={key} style={labelStyle}>
                {label}
                <input
                  type={type}
                  value={(form as Record<string, string | number>)[key]}
                  onChange={e => setForm(f => ({ ...f, [key]: type === 'number' ? Number(e.target.value) : e.target.value }))}
                  style={inputStyle}
                />
              </label>
            ))}
            <label style={labelStyle}>
              Statistic
              <select value={form.statistic} onChange={e => setForm(f => ({ ...f, statistic: e.target.value }))} style={inputStyle}>
                {['Average', 'Sum', 'Minimum', 'Maximum', 'SampleCount'].map(s => <option key={s} value={s}>{s}</option>)}
              </select>
            </label>
            <label style={labelStyle}>
              Comparison Operator
              <select value={form.comparisonOperator} onChange={e => setForm(f => ({ ...f, comparisonOperator: e.target.value }))} style={inputStyle}>
                {['GreaterThanThreshold', 'GreaterThanOrEqualToThreshold', 'LessThanThreshold', 'LessThanOrEqualToThreshold'].map(o => (
                  <option key={o} value={o}>{o}</option>
                ))}
              </select>
            </label>
            {createMut.error && <div style={{ color: '#d13212', fontSize: '0.85em', marginTop: '0.5rem' }}>{(createMut.error as Error).message}</div>}
            <div style={{ display: 'flex', gap: '0.75rem', marginTop: '1.25rem' }}>
              <button onClick={() => createMut.mutate()} disabled={!form.alarmName} style={primaryBtnStyle}>Create</button>
              <button onClick={() => setCreateOpen(false)} style={cancelBtnStyle}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {/* Set state dialog */}
      {stateTarget && (
        <div style={overlayStyle}>
          <div style={dialogStyle}>
            <h3 style={{ margin: '0 0 1rem', color: '#e2e8f0' }}>Set Alarm State</h3>
            <p style={{ color: '#8892a4', fontSize: '0.88em', margin: '0 0 1rem' }}>{stateTarget.alarmName}</p>
            <label style={labelStyle}>
              State
              <select value={newStateValue} onChange={e => setNewStateValue(e.target.value)} style={inputStyle}>
                {['OK', 'ALARM', 'INSUFFICIENT_DATA'].map(s => <option key={s} value={s}>{s}</option>)}
              </select>
            </label>
            <label style={labelStyle}>
              Reason
              <input type="text" value={newStateReason} onChange={e => setNewStateReason(e.target.value)} style={inputStyle} placeholder="Manual state change" />
            </label>
            <div style={{ display: 'flex', gap: '0.75rem', marginTop: '1.25rem' }}>
              <button onClick={() => stateMut.mutate()} style={primaryBtnStyle}>Apply</button>
              <button onClick={() => setStateTarget(null)} style={cancelBtnStyle}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {/* Delete confirm dialog */}
      {deleteTarget && (
        <div style={overlayStyle}>
          <div style={dialogStyle}>
            <h3 style={{ margin: '0 0 0.75rem', color: '#e2e8f0' }}>Delete Alarm?</h3>
            <p style={{ color: '#8892a4', fontSize: '0.88em' }}>Delete <strong style={{ color: '#e2e8f0' }}>{deleteTarget.alarmName}</strong>? This cannot be undone.</p>
            <div style={{ display: 'flex', gap: '0.75rem', marginTop: '1.25rem' }}>
              <button onClick={() => deleteMut.mutate(deleteTarget.alarmName)} style={{ ...primaryBtnStyle, background: '#d13212', borderColor: '#d13212' }}>Delete</button>
              <button onClick={() => setDeleteTarget(null)} style={cancelBtnStyle}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.88em' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.5rem 0.75rem', color: '#8892a4', fontWeight: 500, borderBottom: '1px solid #2d3748', fontSize: '0.8em', textTransform: 'uppercase', letterSpacing: '0.05em' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 0.75rem', color: '#c9cdd4', verticalAlign: 'middle' }
const primaryBtnStyle: React.CSSProperties = { background: '#0972d3', border: '1px solid #0972d3', borderRadius: 4, color: '#fff', cursor: 'pointer', padding: '0.4rem 1rem', fontSize: '0.85em', fontWeight: 500 }
const cancelBtnStyle: React.CSSProperties = { background: 'none', border: '1px solid #2d3748', borderRadius: 4, color: '#c9cdd4', cursor: 'pointer', padding: '0.4rem 1rem', fontSize: '0.85em' }
const actionBtnStyle: React.CSSProperties = { background: 'none', border: '1px solid #2d3748', borderRadius: 4, color: '#0972d3', cursor: 'pointer', padding: '0.25rem 0.75rem', fontSize: '0.82em' }
const selectStyle: React.CSSProperties = { background: '#1b2a3b', border: '1px solid #2d3748', borderRadius: 4, color: '#e2e8f0', padding: '0.4rem 0.75rem', fontSize: '0.85em' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 50 }
const dialogStyle: React.CSSProperties = { background: '#1b2530', border: '1px solid #2d3748', borderRadius: 8, padding: '1.75rem', minWidth: 440, maxWidth: 560 }
const labelStyle: React.CSSProperties = { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '0.75rem', color: '#c9cdd4', fontSize: '0.85em' }
const inputStyle: React.CSSProperties = { background: '#0d1a26', border: '1px solid #2d3748', borderRadius: 4, color: '#e2e8f0', padding: '0.4rem 0.75rem', fontSize: '0.9em' }
