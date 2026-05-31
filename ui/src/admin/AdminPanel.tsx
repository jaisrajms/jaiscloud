import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  getAdminStatus,
  resetState,
  getClock,
  setClock,
  listSnapshots,
  createSnapshot,
  revertSnapshot,
  deleteSnapshot,
  type ClockState,
  type Snapshot,
} from '../api/admin'

export function AdminPanel() {
  const qc = useQueryClient()

  const { data: status } = useQuery({
    queryKey: ['admin', 'status'],
    queryFn: getAdminStatus,
    refetchInterval: 10000,
  })

  const { data: clock } = useQuery({
    queryKey: ['admin', 'clock'],
    queryFn: getClock,
    refetchInterval: 5000,
  })

  const { data: snapshotsData } = useQuery({
    queryKey: ['admin', 'snapshots'],
    queryFn: listSnapshots,
  })

  return (
    <div>
      <h2 style={{ margin: '0 0 1.5rem', fontSize: '1.4rem', fontWeight: 600 }}>Admin Panel</h2>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.25rem' }}>
        <StatusCard status={status} onRefresh={() => void qc.invalidateQueries({ queryKey: ['admin'] })} />
        <ClockCard clock={clock} onChanged={() => void qc.invalidateQueries({ queryKey: ['admin', 'clock'] })} />
        <ResetCard />
        <ExportCard />
      </div>

      <h3 style={{ margin: '2rem 0 0.75rem', fontSize: '1.05rem', fontWeight: 600 }}>Named Snapshots</h3>
      <SnapshotsSection snapshots={snapshotsData?.snapshots ?? []} onChanged={() => void qc.invalidateQueries({ queryKey: ['admin', 'snapshots'] })} />
    </div>
  )
}

function StatusCard({ status, onRefresh }: { status: ReturnType<typeof getAdminStatus> extends Promise<infer T> ? T | undefined : never; onRefresh: () => void }) {
  return (
    <div style={card}>
      <div style={cardHeader}>
        <span style={cardTitle}>Status</span>
        <button onClick={onRefresh} style={btnSmall}>Refresh</button>
      </div>
      {status ? (
        <dl style={dl}>
          <dt style={dt}>Status</dt>
          <dd style={{ ...dd, color: status.status === 'ok' ? '#1d6b2e' : '#d13212', fontWeight: 600 }}>{status.status}</dd>
          <dt style={dt}>Cloud</dt>
          <dd style={dd}>{status.cloud}</dd>
          {status.backend && <><dt style={dt}>Backend</dt><dd style={dd}>{status.backend}</dd></>}
          <dt style={dt}>Snapshotters</dt>
          <dd style={dd}>{status.snapshotters?.length ? status.snapshotters.join(', ') : '—'}</dd>
        </dl>
      ) : (
        <p style={{ color: '#8d9daa', margin: 0 }}>Loading…</p>
      )}
    </div>
  )
}

function ClockCard({ clock, onChanged }: { clock: ClockState | undefined; onChanged: () => void }) {
  const [mode, setMode] = useState<'real' | 'fixed' | 'offset'>('real')
  const [timeStr, setTimeStr] = useState('')
  const [open, setOpen] = useState(false)

  const setMut = useMutation({
    mutationFn: (req: ClockState) => setClock(req),
    onSuccess: () => {
      onChanged()
      setOpen(false)
    },
  })

  const handleSet = () => {
    const req: ClockState = { mode }
    if (mode !== 'real') req.time = timeStr
    setMut.mutate(req)
  }

  return (
    <div style={card}>
      <div style={cardHeader}>
        <span style={cardTitle}>Clock</span>
        <button onClick={() => { setOpen(true); setMode(clock?.mode ?? 'real'); setTimeStr(clock?.time ?? '') }} style={btnSmall}>
          Set clock
        </button>
      </div>
      {clock ? (
        <dl style={dl}>
          <dt style={dt}>Mode</dt>
          <dd style={dd}>
            <span style={{
              display: 'inline-block', padding: '0.15em 0.55em', borderRadius: 3,
              fontSize: '0.82em', fontWeight: 500,
              background: clock.mode === 'real' ? '#e0f9e0' : '#fff8e0',
              color: clock.mode === 'real' ? '#1d6b2e' : '#8a6500',
            }}>
              {clock.mode}
            </span>
          </dd>
          {clock.time && <><dt style={dt}>Time</dt><dd style={{ ...dd, fontFamily: 'monospace', fontSize: '0.85em' }}>{clock.time}</dd></>}
        </dl>
      ) : (
        <p style={{ color: '#8d9daa', margin: 0 }}>Loading…</p>
      )}

      {open && (
        <div style={overlayStyle} onClick={() => setOpen(false)}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem' }}>Set clock mode</h3>
            <label style={labelStyle}>Mode</label>
            <select
              value={mode}
              onChange={(e) => setMode(e.target.value as 'real' | 'fixed' | 'offset')}
              style={{ ...inputStyle, marginBottom: '0.75rem' }}
            >
              <option value="real">real (wall clock)</option>
              <option value="fixed">fixed (frozen time)</option>
              <option value="offset">offset (shifted time)</option>
            </select>
            {mode !== 'real' && (
              <>
                <label style={labelStyle}>Time (RFC3339, e.g. 2025-01-01T00:00:00Z)</label>
                <input
                  autoFocus
                  value={timeStr}
                  onChange={(e) => setTimeStr(e.target.value)}
                  style={{ ...inputStyle, marginBottom: '1rem' }}
                  placeholder="2025-01-01T00:00:00Z"
                />
              </>
            )}
            {setMut.error && <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>{(setMut.error as Error).message}</p>}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setOpen(false)} style={btnSecondary}>Cancel</button>
              <button onClick={handleSet} disabled={setMut.isPending} style={{ ...btnPrimary, opacity: setMut.isPending ? 0.6 : 1 }}>
                {setMut.isPending ? 'Setting…' : 'Set'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function ResetCard() {
  const [confirm, setConfirm] = useState(false)
  const qc = useQueryClient()

  const resetMut = useMutation({
    mutationFn: resetState,
    onSuccess: () => {
      void qc.invalidateQueries()
      setConfirm(false)
    },
  })

  return (
    <div style={card}>
      <div style={cardHeader}>
        <span style={cardTitle}>Reset State</span>
      </div>
      <p style={{ color: '#5f6b7a', fontSize: '0.88em', margin: '0 0 1rem' }}>
        Wipe all emulator state. This is irreversible — all resources will be deleted.
      </p>
      {!confirm ? (
        <button onClick={() => setConfirm(true)} style={btnDanger}>Reset all state…</button>
      ) : (
        <div>
          <p style={{ color: '#d13212', fontWeight: 500, margin: '0 0 0.75rem', fontSize: '0.9em' }}>
            Are you sure? This will delete ALL resources.
          </p>
          {resetMut.error && <p style={{ color: '#d13212', margin: '0 0 0.5rem', fontSize: '0.85em' }}>{(resetMut.error as Error).message}</p>}
          <div style={{ display: 'flex', gap: '0.75rem' }}>
            <button onClick={() => setConfirm(false)} style={btnSecondary}>Cancel</button>
            <button
              onClick={() => resetMut.mutate()}
              disabled={resetMut.isPending}
              style={{ ...btnDanger, opacity: resetMut.isPending ? 0.6 : 1 }}
            >
              {resetMut.isPending ? 'Resetting…' : 'Yes, reset'}
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

function ExportCard() {
  return (
    <div style={card}>
      <div style={cardHeader}>
        <span style={cardTitle}>Export / Import</span>
      </div>
      <p style={{ color: '#5f6b7a', fontSize: '0.88em', margin: '0 0 1rem' }}>
        Export state as a gzip tarball or import a snapshot via the CLI.
      </p>
      <a
        href="/_jaiscloud/export"
        download="jaiscloud-state.tar.gz"
        style={{ ...btnPrimary, textDecoration: 'none', display: 'inline-block' }}
      >
        Download export
      </a>
    </div>
  )
}

function SnapshotsSection({ snapshots, onChanged }: { snapshots: Snapshot[]; onChanged: () => void }) {
  const [createOpen, setCreateOpen] = useState(false)
  const [name, setName] = useState('')
  const [desc, setDesc] = useState('')
  const [confirmRevert, setConfirmRevert] = useState<Snapshot | null>(null)
  const [confirmDel, setConfirmDel] = useState<Snapshot | null>(null)

  const createMut = useMutation({
    mutationFn: () => createSnapshot({ name, description: desc }),
    onSuccess: () => {
      onChanged()
      setCreateOpen(false)
      setName('')
      setDesc('')
    },
  })

  const revertMut = useMutation({
    mutationFn: (n: string) => revertSnapshot(n),
    onSuccess: () => {
      onChanged()
      setConfirmRevert(null)
    },
  })

  const deleteMut = useMutation({
    mutationFn: (n: string) => deleteSnapshot(n),
    onSuccess: () => {
      onChanged()
      setConfirmDel(null)
    },
  })

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: '0.75rem' }}>
        <button onClick={() => setCreateOpen(true)} style={btnSecondary}>Create snapshot</button>
      </div>

      {snapshots.length === 0 ? (
        <div style={{ padding: '2rem', textAlign: 'center', color: '#8d9daa', border: '1px solid #e7e9ec', borderRadius: 8 }}>
          No named snapshots. Create one to save the current state.
        </div>
      ) : (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={th}>Name</th>
                <th style={th}>Description</th>
                <th style={th}>Created</th>
                <th style={{ ...th, width: 160 }}></th>
              </tr>
            </thead>
            <tbody>
              {snapshots.map((s) => (
                <tr key={s.name} style={{ borderBottom: '1px solid #e7e9ec' }}>
                  <td style={td}><span style={{ fontWeight: 500 }}>{s.name}</span></td>
                  <td style={{ ...td, color: '#5f6b7a' }}>{s.description ?? '—'}</td>
                  <td style={{ ...td, color: '#8d9daa', fontSize: '0.85em' }}>
                    {s.createdAt ? new Date(s.createdAt).toLocaleString() : '—'}
                  </td>
                  <td style={{ ...td, display: 'flex', gap: '0.4rem' }}>
                    <button onClick={() => setConfirmRevert(s)} style={btnSmall}>Revert</button>
                    <button onClick={() => setConfirmDel(s)} style={btnDelete}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {createOpen && (
        <div style={overlayStyle} onClick={() => setCreateOpen(false)}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 1rem' }}>Create snapshot</h3>
            <label style={labelStyle}>Name</label>
            <input autoFocus value={name} onChange={(e) => setName(e.target.value)} style={{ ...inputStyle, marginBottom: '0.75rem' }} placeholder="my-snapshot" />
            <label style={labelStyle}>Description (optional)</label>
            <input value={desc} onChange={(e) => setDesc(e.target.value)} style={{ ...inputStyle, marginBottom: '1rem' }} placeholder="Before the big test…" />
            {createMut.error && <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>{(createMut.error as Error).message}</p>}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setCreateOpen(false)} style={btnSecondary}>Cancel</button>
              <button onClick={() => createMut.mutate()} disabled={!name || createMut.isPending} style={{ ...btnPrimary, opacity: !name || createMut.isPending ? 0.6 : 1 }}>
                {createMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {confirmRevert && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: '1.05rem' }}>Revert to snapshot?</h3>
            <p style={{ margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Current state will be replaced with <strong>{confirmRevert.name}</strong>. This cannot be undone.
            </p>
            {revertMut.error && <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>{(revertMut.error as Error).message}</p>}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setConfirmRevert(null)} style={btnSecondary}>Cancel</button>
              <button onClick={() => revertMut.mutate(confirmRevert.name)} disabled={revertMut.isPending} style={{ ...btnDanger, opacity: revertMut.isPending ? 0.6 : 1 }}>
                {revertMut.isPending ? 'Reverting…' : 'Revert'}
              </button>
            </div>
          </div>
        </div>
      )}

      {confirmDel && (
        <div style={overlayStyle}>
          <div style={dialogStyle} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: '1.05rem' }}>Delete snapshot?</h3>
            <p style={{ margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Permanently delete snapshot <strong>{confirmDel.name}</strong>?
            </p>
            {deleteMut.error && <p style={{ color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }}>{(deleteMut.error as Error).message}</p>}
            <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }}>
              <button onClick={() => setConfirmDel(null)} style={btnSecondary}>Cancel</button>
              <button onClick={() => deleteMut.mutate(confirmDel.name)} disabled={deleteMut.isPending} style={{ ...btnDanger, opacity: deleteMut.isPending ? 0.6 : 1 }}>
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

const card: React.CSSProperties = { background: '#fff', border: '1px solid #e7e9ec', borderRadius: 8, padding: '1.25rem' }
const cardHeader: React.CSSProperties = { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }
const cardTitle: React.CSSProperties = { fontWeight: 600, fontSize: '1rem' }
const dl: React.CSSProperties = { margin: 0, display: 'grid', gridTemplateColumns: 'auto 1fr', gap: '0.3rem 1rem', alignItems: 'baseline' }
const dt: React.CSSProperties = { fontSize: '0.82em', color: '#8d9daa', fontWeight: 500, textTransform: 'uppercase', letterSpacing: '0.03em', whiteSpace: 'nowrap' }
const dd: React.CSSProperties = { margin: 0, fontSize: '0.9em' }
const th: React.CSSProperties = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' }
const td: React.CSSProperties = { padding: '0.75rem 1rem' }
const btnPrimary: React.CSSProperties = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.45rem 1.1rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' }
const btnSecondary: React.CSSProperties = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.4rem 0.9rem', cursor: 'pointer', fontSize: '0.88em' }
const btnDanger: React.CSSProperties = { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.45rem 1.1rem', cursor: 'pointer', fontSize: '0.9em' }
const btnSmall: React.CSSProperties = { background: '#f4f5f7', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#3d4c5e' }
const btnDelete: React.CSSProperties = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const dialogStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 480, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' }
const labelStyle: React.CSSProperties = { display: 'block', fontSize: '0.85em', fontWeight: 500, marginBottom: '0.35rem', color: '#3d4c5e' }
const inputStyle: React.CSSProperties = { width: '100%', boxSizing: 'border-box', padding: '0.5rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em' }
