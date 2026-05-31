import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  listKeys,
  createKey,
  enableKey,
  disableKey,
  scheduleKeyDeletion,
  cancelKeyDeletion,
  type KMSKey,
} from '../../../api/kms'
import { EmptyState } from '../../../components/EmptyState'

const KEY_STATE_COLOR: Record<string, string> = {
  Enabled: '#037f0c',
  Disabled: '#d13212',
  PendingDeletion: '#8a6116',
  PendingImport: '#0073bb',
}

export function KMSList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newDesc, setNewDesc] = useState('')
  const [newUsage, setNewUsage] = useState('ENCRYPT_DECRYPT')
  const [newSpec, setNewSpec] = useState('SYMMETRIC_DEFAULT')
  const [deleteKey, setDeleteKey] = useState<KMSKey | null>(null)
  const [deleteDays, setDeleteDays] = useState(30)
  const qc = useQueryClient()

  const { data, isLoading, error } = useQuery({
    queryKey: ['kms', 'keys'],
    queryFn: () => listKeys(),
  })

  const createMut = useMutation({
    mutationFn: () => createKey({ description: newDesc || undefined, keyUsage: newUsage, keySpec: newSpec }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['kms', 'keys'] })
      setCreateOpen(false)
      setNewDesc('')
      setNewUsage('ENCRYPT_DECRYPT')
      setNewSpec('SYMMETRIC_DEFAULT')
    },
  })

  const enableMut = useMutation({
    mutationFn: (keyId: string) => enableKey(keyId),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['kms', 'keys'] }),
  })

  const disableMut = useMutation({
    mutationFn: (keyId: string) => disableKey(keyId),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['kms', 'keys'] }),
  })

  const scheduleMut = useMutation({
    mutationFn: ({ keyId, days }: { keyId: string; days: number }) => scheduleKeyDeletion(keyId, days),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['kms', 'keys'] })
      setDeleteKey(null)
    },
  })

  const cancelMut = useMutation({
    mutationFn: (keyId: string) => cancelKeyDeletion(keyId),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['kms', 'keys'] }),
  })

  if (isLoading) return <div style={{ padding: '2rem', color: '#5f6b7a' }}>Loading keys…</div>
  if (error) return <div style={{ padding: '2rem', color: '#d13212' }}>Failed to load: {(error as Error).message}</div>

  const keys = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>KMS Keys</h2>
          <span style={{ fontSize: '0.85em', color: '#5f6b7a' }}>{keys.length} key{keys.length !== 1 ? 's' : ''}</span>
        </div>
        <button onClick={() => setCreateOpen(true)} style={btnPrimary}>Create key</button>
      </div>

      {keys.length === 0 ? (
        <EmptyState
          title="No KMS keys"
          description="KMS keys encrypt and protect your data across AWS services."
          cta="Create Key"
          onCta={() => setCreateOpen(true)}
        />
      ) : (
        <div style={{ border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }}>
            <thead>
              <tr style={{ background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }}>
                <th style={th}>Key ID</th>
                <th style={th}>Description</th>
                <th style={th}>Usage</th>
                <th style={th}>Spec</th>
                <th style={th}>State</th>
                <th style={{ ...th, textAlign: 'right' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {keys.map((k, i) => (
                <tr key={k.keyId} style={{ borderBottom: i < keys.length - 1 ? '1px solid #e7e9ec' : 'none' }}>
                  <td style={td}>
                    <code style={{ fontSize: '0.85em', color: '#0972d3' }}>{k.keyId}</code>
                  </td>
                  <td style={td}>{k.description || <span style={{ color: '#8d9daa' }}>—</span>}</td>
                  <td style={td}>{k.keyUsage}</td>
                  <td style={td}>{k.keySpec}</td>
                  <td style={td}>
                    <span style={{
                      display: 'inline-block',
                      padding: '2px 8px',
                      borderRadius: 12,
                      fontSize: '0.8em',
                      fontWeight: 500,
                      background: (KEY_STATE_COLOR[k.keyState] ?? '#5f6b7a') + '22',
                      color: KEY_STATE_COLOR[k.keyState] ?? '#5f6b7a',
                    }}>
                      {k.keyState}
                    </span>
                  </td>
                  <td style={{ ...td, textAlign: 'right', whiteSpace: 'nowrap' }}>
                    {k.keyState === 'Disabled' && (
                      <button onClick={() => enableMut.mutate(k.keyId)} style={btnSmall}>Enable</button>
                    )}
                    {k.keyState === 'Enabled' && (
                      <button onClick={() => disableMut.mutate(k.keyId)} style={{ ...btnSmall, marginLeft: 6 }}>Disable</button>
                    )}
                    {k.keyState === 'PendingDeletion' && (
                      <button onClick={() => cancelMut.mutate(k.keyId)} style={{ ...btnSmall, marginLeft: 6 }}>Cancel deletion</button>
                    )}
                    {k.keyState !== 'PendingDeletion' && (
                      <button onClick={() => setDeleteKey(k)} style={{ ...btnSmall, marginLeft: 6, color: '#d13212', borderColor: '#d13212' }}>
                        Schedule deletion
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Create key dialog */}
      {createOpen && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 1rem' }}>Create KMS Key</h3>
            <label style={label}>Description (optional)</label>
            <input style={input} value={newDesc} onChange={e => setNewDesc(e.target.value)} placeholder="My encryption key" />
            <label style={label}>Key usage</label>
            <select style={input} value={newUsage} onChange={e => setNewUsage(e.target.value)}>
              <option value="ENCRYPT_DECRYPT">ENCRYPT_DECRYPT</option>
              <option value="SIGN_VERIFY">SIGN_VERIFY</option>
              <option value="GENERATE_VERIFY_MAC">GENERATE_VERIFY_MAC</option>
            </select>
            <label style={label}>Key spec</label>
            <select style={input} value={newSpec} onChange={e => setNewSpec(e.target.value)}>
              <option value="SYMMETRIC_DEFAULT">SYMMETRIC_DEFAULT</option>
              <option value="RSA_2048">RSA_2048</option>
              <option value="RSA_4096">RSA_4096</option>
              <option value="ECC_NIST_P256">ECC_NIST_P256</option>
              <option value="HMAC_256">HMAC_256</option>
            </select>
            {createMut.error && (
              <div style={{ color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' }}>
                {(createMut.error as Error).message}
              </div>
            )}
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '1rem' }}>
              <button style={btnSecondary} onClick={() => setCreateOpen(false)}>Cancel</button>
              <button style={btnPrimary} onClick={() => createMut.mutate()} disabled={createMut.isPending}>
                {createMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Schedule deletion dialog */}
      {deleteKey && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 0.75rem' }}>Schedule key deletion</h3>
            <p style={{ margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Key: <code>{deleteKey.keyId}</code>
            </p>
            <label style={label}>Waiting period (days)</label>
            <input style={input} type="number" min={7} max={30} value={deleteDays}
              onChange={e => setDeleteDays(Number(e.target.value))} />
            <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '1rem' }}>
              <button style={btnSecondary} onClick={() => setDeleteKey(null)}>Cancel</button>
              <button style={{ ...btnPrimary, background: '#d13212', borderColor: '#d13212' }}
                onClick={() => scheduleMut.mutate({ keyId: deleteKey.keyId, days: deleteDays })}
                disabled={scheduleMut.isPending}>
                {scheduleMut.isPending ? 'Scheduling…' : 'Schedule deletion'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

const btnPrimary: React.CSSProperties = {
  background: '#e87600', color: '#fff', border: '1px solid #e87600',
  borderRadius: 6, padding: '0.4rem 1rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em',
}
const btnSecondary: React.CSSProperties = {
  background: 'transparent', color: '#5f6b7a', border: '1px solid #ccc',
  borderRadius: 6, padding: '0.4rem 1rem', cursor: 'pointer', fontSize: '0.9em',
}
const btnSmall: React.CSSProperties = {
  background: 'transparent', color: '#0972d3', border: '1px solid #0972d3',
  borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: '0.8em',
}
const th: React.CSSProperties = {
  padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, fontSize: '0.82em',
  color: '#5f6b7a', textTransform: 'uppercase', letterSpacing: '0.05em',
}
const td: React.CSSProperties = { padding: '0.7rem 1rem', verticalAlign: 'middle' }
const overlay: React.CSSProperties = {
  position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex',
  alignItems: 'center', justifyContent: 'center', zIndex: 1000,
}
const dialog: React.CSSProperties = {
  background: '#fff', borderRadius: 10, padding: '1.5rem', minWidth: 420, maxWidth: 540,
  boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
}
const label: React.CSSProperties = { display: 'block', fontSize: '0.82em', fontWeight: 600, marginBottom: 4, color: '#2d3748' }
const input: React.CSSProperties = {
  width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #ccc', borderRadius: 6,
  marginBottom: '0.75rem', fontSize: '0.9em', boxSizing: 'border-box',
}
