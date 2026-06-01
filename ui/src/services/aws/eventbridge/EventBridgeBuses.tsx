import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  listEventBuses,
  createEventBus,
  deleteEventBus,
  putEvents,
  type EventBus,
} from '../../../api/eventbridge'
import { EmptyState } from '../../../components/EmptyState'

const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' }
const tdStyle: React.CSSProperties = { padding: '0.6rem 1rem', verticalAlign: 'middle' }
const btnStyle: React.CSSProperties = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 440, maxWidth: 580 }

export function EventBridgeBuses() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<EventBus | null>(null)
  const [sendEventsOpen, setSendEventsOpen] = useState(false)
  const [busName, setBusName] = useState('')
  const [eventForm, setEventForm] = useState({ source: '', detailType: '', detail: '{}', bus: '' })

  const { data, isLoading } = useQuery({
    queryKey: ['eventbridge', 'buses'],
    queryFn: () => listEventBuses(),
  })

  const createMut = useMutation({
    mutationFn: () => createEventBus(busName),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['eventbridge', 'buses'] })
      setCreateOpen(false)
      setBusName('')
    },
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteEventBus(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['eventbridge', 'buses'] })
      setDeleteTarget(null)
    },
  })

  const sendEventsMut = useMutation({
    mutationFn: () => putEvents([{ source: eventForm.source, detailType: eventForm.detailType, detail: eventForm.detail, bus: eventForm.bus || undefined }]),
    onSuccess: () => {
      setSendEventsOpen(false)
      setEventForm({ source: '', detailType: '', detail: '{}', bus: '' })
    },
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading event buses…</div>

  const buses = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>Event Buses</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{buses.length} bus{buses.length !== 1 ? 'es' : ''}</span>
        </div>
        <div style={{ display: 'flex', gap: '0.75rem' }}>
          <button style={{ ...btnStyle, background: '#2d3748' }} onClick={() => setSendEventsOpen(true)}>Send Events</button>
          <button style={btnStyle} onClick={() => setCreateOpen(true)}>Create Bus</button>
        </div>
      </div>

      {buses.length === 0 ? (
        <EmptyState title="No custom event buses. The default bus is always available." />
      ) : (
        <table style={tableStyle}>
          <thead>
            <tr>{['Name', 'ARN', ''].map(h => <th key={h} style={thStyle}>{h}</th>)}</tr>
          </thead>
          <tbody>
            {buses.map(bus => (
              <tr key={bus.name} style={{ borderBottom: '1px solid #2d3748' }}>
                <td style={{ ...tdStyle, fontWeight: 600 }}>{bus.name}</td>
                <td style={{ ...tdStyle, color: '#b0bec5', fontSize: '0.82rem', fontFamily: 'monospace' }}>{bus.arn ?? '—'}</td>
                <td style={{ ...tdStyle, textAlign: 'right' }}>
                  {bus.name !== 'default' && (
                    <button style={{ ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }} onClick={() => setDeleteTarget(bus)}>Delete</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {createOpen && (
        <div style={overlayStyle} onClick={() => setCreateOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Create Event Bus</h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1.5rem' }}>
              <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>Name *</label>
              <input style={inputStyle} placeholder="my-custom-bus" value={busName} onChange={e => setBusName(e.target.value)} />
            </div>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setCreateOpen(false)}>Cancel</button>
              <button style={btnStyle} disabled={!busName || createMut.isPending} onClick={() => createMut.mutate()}>
                {createMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {deleteTarget && (
        <div style={overlayStyle} onClick={() => setDeleteTarget(null)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem', fontWeight: 600 }}>Delete Event Bus?</h3>
            <p style={{ color: '#b0bec5', marginBottom: '1.5rem' }}>Delete bus <strong>{deleteTarget.name}</strong>?</p>
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setDeleteTarget(null)}>Cancel</button>
              <button style={{ ...btnStyle, background: '#d13212' }} disabled={deleteMut.isPending} onClick={() => deleteMut.mutate(deleteTarget.name)}>
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}

      {sendEventsOpen && (
        <div style={overlayStyle} onClick={() => setSendEventsOpen(false)}>
          <div style={modalStyle} onClick={e => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1.5rem', fontWeight: 600 }}>Send Event</h3>
            {[
              { key: 'source', label: 'Source *', placeholder: 'my.app' },
              { key: 'detailType', label: 'Detail Type *', placeholder: 'StateChange' },
              { key: 'detail', label: 'Detail (JSON) *', placeholder: '{}' },
              { key: 'bus', label: 'Event Bus (optional)', placeholder: 'default' },
            ].map(f => (
              <div key={f.key} style={{ display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }}>
                <label style={{ fontSize: '0.8rem', color: '#b0bec5' }}>{f.label}</label>
                <input style={inputStyle} placeholder={f.placeholder} value={(eventForm as Record<string, string>)[f.key]} onChange={e => setEventForm(p => ({ ...p, [f.key]: e.target.value }))} />
              </div>
            ))}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button style={{ ...btnStyle, background: '#2d3748', color: '#e8eaf0' }} onClick={() => setSendEventsOpen(false)}>Cancel</button>
              <button style={btnStyle} disabled={!eventForm.source || !eventForm.detailType || sendEventsMut.isPending} onClick={() => sendEventsMut.mutate()}>
                {sendEventsMut.isPending ? 'Sending…' : 'Send'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
