import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  listRoles, createRole, deleteRole,
  listUsers, createUser, deleteUser, listAccessKeys, createAccessKey, deleteAccessKey,
  listPolicies, createPolicy, deletePolicy,
  type IAMRole, type IAMUser, type IAMPolicy, type AccessKey,
} from '../../../api/iam'
import { EmptyState } from '../../../components/EmptyState'

type Tab = 'roles' | 'users' | 'policies'

export function IAMList() {
  const [tab, setTab] = useState<Tab>('roles')

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <h2 style={{ margin: 0, fontSize: '1.4rem', fontWeight: 600 }}>IAM</h2>
      </div>
      <div style={{ display: 'flex', gap: 0, borderBottom: '2px solid #e7e9ec', marginBottom: '1.5rem' }}>
        {(['roles', 'users', 'policies'] as Tab[]).map(t => (
          <button key={t} onClick={() => setTab(t)} style={{
            ...tabBtn,
            borderBottom: tab === t ? '2px solid #e87600' : '2px solid transparent',
            color: tab === t ? '#e87600' : '#5f6b7a',
            fontWeight: tab === t ? 600 : 400,
            marginBottom: -2,
          }}>
            {t.charAt(0).toUpperCase() + t.slice(1)}
          </button>
        ))}
      </div>
      {tab === 'roles' && <RolesTab />}
      {tab === 'users' && <UsersTab />}
      {tab === 'policies' && <PoliciesTab />}
    </div>
  )
}

function RolesTab() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDesc, setNewDesc] = useState('')
  const [newPolicy, setNewPolicy] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<IAMRole | null>(null)
  const qc = useQueryClient()

  const { data, isLoading, error } = useQuery({
    queryKey: ['iam', 'roles'],
    queryFn: () => listRoles(),
  })

  const createMut = useMutation({
    mutationFn: () => createRole({ roleName: newName, description: newDesc || undefined, assumeRolePolicyDocument: newPolicy || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'roles'] })
      setCreateOpen(false)
      setNewName('')
      setNewDesc('')
      setNewPolicy('')
    },
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteRole(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'roles'] })
      setConfirmDelete(null)
    },
  })

  if (isLoading) return <div style={loadingStyle}>Loading roles…</div>
  if (error) return <div style={errorStyle}>Failed to load: {(error as Error).message}</div>

  const roles = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: '1rem' }}>
        <button onClick={() => setCreateOpen(true)} style={btnPrimary}>Create role</button>
      </div>
      {roles.length === 0 ? (
        <EmptyState title="No roles" description="IAM roles grant AWS service access permissions." cta="Create Role" onCta={() => setCreateOpen(true)} />
      ) : (
        <div style={tableWrap}>
          <table style={tableStyle}>
            <thead>
              <tr style={theadRow}>
                <th style={th}>Role name</th>
                <th style={th}>ARN</th>
                <th style={th}>Created</th>
                <th style={{ ...th, textAlign: 'right' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {roles.map((r, i) => (
                <tr key={r.arn} style={{ borderBottom: i < roles.length - 1 ? '1px solid #e7e9ec' : 'none' }}>
                  <td style={{ ...td, fontWeight: 500, color: '#0972d3' }}>{r.roleName}</td>
                  <td style={{ ...td, fontFamily: 'monospace', fontSize: '0.82em', color: '#5f6b7a' }}>{r.arn}</td>
                  <td style={td}>{r.createDate || '—'}</td>
                  <td style={{ ...td, textAlign: 'right' }}>
                    <button onClick={() => setConfirmDelete(r)} style={{ ...btnSmall, color: '#d13212', borderColor: '#d13212' }}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {createOpen && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 1rem' }}>Create Role</h3>
            <label style={labelStyle}>Role name *</label>
            <input style={inputStyle} value={newName} onChange={e => setNewName(e.target.value)} placeholder="my-lambda-role" autoFocus />
            <label style={labelStyle}>Description (optional)</label>
            <input style={inputStyle} value={newDesc} onChange={e => setNewDesc(e.target.value)} placeholder="Role description" />
            <label style={labelStyle}>Trust policy document (optional)</label>
            <textarea style={{ ...inputStyle, height: 120, fontFamily: 'monospace', fontSize: '0.82em', resize: 'vertical' }}
              value={newPolicy} onChange={e => setNewPolicy(e.target.value)}
              placeholder={'{\n  "Version": "2012-10-17",\n  "Statement": [...]\n}'} />
            <div style={{ fontSize: '0.78em', color: '#5f6b7a', marginBottom: '0.5rem', marginTop: '-0.25rem' }}>
              Leave blank for default Lambda trust policy.
            </div>
            {createMut.error && <div style={mutError}>{(createMut.error as Error).message}</div>}
            <div style={dialogActions}>
              <button style={btnSecondary} onClick={() => setCreateOpen(false)}>Cancel</button>
              <button style={btnPrimary} onClick={() => createMut.mutate()} disabled={!newName || createMut.isPending}>
                {createMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {confirmDelete && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 0.75rem' }}>Delete role?</h3>
            <p style={{ margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Delete role <strong>{confirmDelete.roleName}</strong>? This cannot be undone.
            </p>
            {deleteMut.error && <div style={mutError}>{(deleteMut.error as Error).message}</div>}
            <div style={dialogActions}>
              <button style={btnSecondary} onClick={() => setConfirmDelete(null)}>Cancel</button>
              <button style={{ ...btnPrimary, background: '#d13212', borderColor: '#d13212' }}
                onClick={() => deleteMut.mutate(confirmDelete.roleName)} disabled={deleteMut.isPending}>
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function UsersTab() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<IAMUser | null>(null)
  const [keysUser, setKeysUser] = useState<IAMUser | null>(null)
  const [newKey, setNewKey] = useState<AccessKey | null>(null)
  const qc = useQueryClient()

  const { data, isLoading, error } = useQuery({
    queryKey: ['iam', 'users'],
    queryFn: () => listUsers(),
  })

  const { data: keysData } = useQuery({
    queryKey: ['iam', 'access-keys', keysUser?.userName],
    queryFn: () => listAccessKeys(keysUser!.userName),
    enabled: !!keysUser,
  })

  const createMut = useMutation({
    mutationFn: () => createUser({ userName: newName }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'users'] })
      setCreateOpen(false)
      setNewName('')
    },
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteUser(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'users'] })
      setConfirmDelete(null)
    },
  })

  const createKeyMut = useMutation({
    mutationFn: () => createAccessKey(keysUser!.userName),
    onSuccess: (resp) => {
      void qc.invalidateQueries({ queryKey: ['iam', 'access-keys', keysUser?.userName] })
      setNewKey(resp.AccessKey)
    },
  })

  const deleteKeyMut = useMutation({
    mutationFn: ({ userName, keyId }: { userName: string; keyId: string }) => deleteAccessKey(userName, keyId),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'access-keys', keysUser?.userName] })
    },
  })

  if (isLoading) return <div style={loadingStyle}>Loading users…</div>
  if (error) return <div style={errorStyle}>Failed to load: {(error as Error).message}</div>

  const users = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: '1rem' }}>
        <button onClick={() => setCreateOpen(true)} style={btnPrimary}>Create user</button>
      </div>
      {users.length === 0 ? (
        <EmptyState title="No users" description="IAM users allow programmatic access to AWS services." cta="Create User" onCta={() => setCreateOpen(true)} />
      ) : (
        <div style={tableWrap}>
          <table style={tableStyle}>
            <thead>
              <tr style={theadRow}>
                <th style={th}>User name</th>
                <th style={th}>ARN</th>
                <th style={th}>Created</th>
                <th style={{ ...th, textAlign: 'right' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {users.map((u, i) => (
                <tr key={u.arn} style={{ borderBottom: i < users.length - 1 ? '1px solid #e7e9ec' : 'none' }}>
                  <td style={{ ...td, fontWeight: 500, color: '#0972d3' }}>{u.userName}</td>
                  <td style={{ ...td, fontFamily: 'monospace', fontSize: '0.82em', color: '#5f6b7a' }}>{u.arn}</td>
                  <td style={td}>{u.createDate || '—'}</td>
                  <td style={{ ...td, textAlign: 'right', whiteSpace: 'nowrap' }}>
                    <button onClick={() => setKeysUser(u)} style={btnSmall}>Access keys</button>
                    <button onClick={() => setConfirmDelete(u)} style={{ ...btnSmall, marginLeft: 6, color: '#d13212', borderColor: '#d13212' }}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {createOpen && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 1rem' }}>Create User</h3>
            <label style={labelStyle}>User name *</label>
            <input style={inputStyle} value={newName} onChange={e => setNewName(e.target.value)} placeholder="my-service-user" autoFocus />
            {createMut.error && <div style={mutError}>{(createMut.error as Error).message}</div>}
            <div style={dialogActions}>
              <button style={btnSecondary} onClick={() => setCreateOpen(false)}>Cancel</button>
              <button style={btnPrimary} onClick={() => createMut.mutate()} disabled={!newName || createMut.isPending}>
                {createMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {confirmDelete && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 0.75rem' }}>Delete user?</h3>
            <p style={{ margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Delete user <strong>{confirmDelete.userName}</strong>? This cannot be undone.
            </p>
            {deleteMut.error && <div style={mutError}>{(deleteMut.error as Error).message}</div>}
            <div style={dialogActions}>
              <button style={btnSecondary} onClick={() => setConfirmDelete(null)}>Cancel</button>
              <button style={{ ...btnPrimary, background: '#d13212', borderColor: '#d13212' }}
                onClick={() => deleteMut.mutate(confirmDelete.userName)} disabled={deleteMut.isPending}>
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}

      {keysUser && (
        <div style={overlay}>
          <div style={{ ...dialog, minWidth: 560, maxWidth: 680 }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
              <h3 style={{ margin: 0 }}>Access keys — {keysUser.userName}</h3>
              <button style={btnPrimary} onClick={() => createKeyMut.mutate()} disabled={createKeyMut.isPending}>
                {createKeyMut.isPending ? 'Creating…' : 'Create key'}
              </button>
            </div>

            {newKey && (
              <div style={{ background: '#f0f9f0', border: '1px solid #1d8102', borderRadius: 6, padding: '0.75rem', marginBottom: '1rem', fontSize: '0.85em' }}>
                <div style={{ fontWeight: 600, marginBottom: 4, color: '#1d8102' }}>Key created — save the secret now, it won't be shown again</div>
                <div><strong>Access Key ID:</strong> <code>{newKey.accessKeyId}</code></div>
                <div><strong>Secret Access Key:</strong> <code>{newKey.secretAccessKey}</code></div>
                <button style={{ ...btnSmall, marginTop: 8 }} onClick={() => setNewKey(null)}>Dismiss</button>
              </div>
            )}

            {(keysData?.items ?? []).length === 0 ? (
              <div style={{ color: '#8d9daa', fontSize: '0.9em', marginBottom: '1rem' }}>No access keys.</div>
            ) : (
              <div style={{ ...tableWrap, marginBottom: '1rem' }}>
                <table style={tableStyle}>
                  <thead>
                    <tr style={theadRow}>
                      <th style={th}>Access Key ID</th>
                      <th style={th}>Status</th>
                      <th style={th}>Created</th>
                      <th style={{ ...th, textAlign: 'right' }}>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(keysData?.items ?? []).map((k, i) => (
                      <tr key={k.accessKeyId} style={{ borderBottom: i < (keysData?.items ?? []).length - 1 ? '1px solid #e7e9ec' : 'none' }}>
                        <td style={{ ...td, fontFamily: 'monospace', fontSize: '0.85em' }}>{k.accessKeyId}</td>
                        <td style={td}>
                          <span style={{
                            display: 'inline-block', padding: '1px 8px', borderRadius: 10, fontSize: '0.8em',
                            background: k.status === 'Active' ? '#1d810222' : '#8d9daa22',
                            color: k.status === 'Active' ? '#1d8102' : '#5f6b7a',
                          }}>{k.status}</span>
                        </td>
                        <td style={td}>{k.createDate || '—'}</td>
                        <td style={{ ...td, textAlign: 'right' }}>
                          <button onClick={() => deleteKeyMut.mutate({ userName: keysUser.userName, keyId: k.accessKeyId })}
                            style={{ ...btnSmall, color: '#d13212', borderColor: '#d13212' }}
                            disabled={deleteKeyMut.isPending}>Delete</button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
              <button style={btnSecondary} onClick={() => { setKeysUser(null); setNewKey(null) }}>Close</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function PoliciesTab() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDoc, setNewDoc] = useState('')
  const [newDesc, setNewDesc] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<IAMPolicy | null>(null)
  const qc = useQueryClient()

  const { data, isLoading, error } = useQuery({
    queryKey: ['iam', 'policies'],
    queryFn: () => listPolicies({ scope: 'Local' }),
  })

  const createMut = useMutation({
    mutationFn: () => createPolicy({ policyName: newName, policyDocument: newDoc, description: newDesc || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'policies'] })
      setCreateOpen(false)
      setNewName('')
      setNewDoc('')
      setNewDesc('')
    },
  })

  const deleteMut = useMutation({
    mutationFn: (arn: string) => deletePolicy(arn),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'policies'] })
      setConfirmDelete(null)
    },
  })

  if (isLoading) return <div style={loadingStyle}>Loading policies…</div>
  if (error) return <div style={errorStyle}>Failed to load: {(error as Error).message}</div>

  const policies = data?.items ?? []

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: '1rem' }}>
        <button onClick={() => setCreateOpen(true)} style={btnPrimary}>Create policy</button>
      </div>
      {policies.length === 0 ? (
        <EmptyState title="No policies" description="IAM policies define permissions that can be attached to roles and users." cta="Create Policy" onCta={() => setCreateOpen(true)} />
      ) : (
        <div style={tableWrap}>
          <table style={tableStyle}>
            <thead>
              <tr style={theadRow}>
                <th style={th}>Policy name</th>
                <th style={th}>ARN</th>
                <th style={th}>Attachments</th>
                <th style={th}>Created</th>
                <th style={{ ...th, textAlign: 'right' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {policies.map((p, i) => (
                <tr key={p.arn} style={{ borderBottom: i < policies.length - 1 ? '1px solid #e7e9ec' : 'none' }}>
                  <td style={{ ...td, fontWeight: 500, color: '#0972d3' }}>{p.policyName}</td>
                  <td style={{ ...td, fontFamily: 'monospace', fontSize: '0.82em', color: '#5f6b7a' }}>{p.arn}</td>
                  <td style={td}>{p.attachmentCount}</td>
                  <td style={td}>{p.createDate || '—'}</td>
                  <td style={{ ...td, textAlign: 'right' }}>
                    <button onClick={() => setConfirmDelete(p)} style={{ ...btnSmall, color: '#d13212', borderColor: '#d13212' }}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {createOpen && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 1rem' }}>Create Policy</h3>
            <label style={labelStyle}>Policy name *</label>
            <input style={inputStyle} value={newName} onChange={e => setNewName(e.target.value)} placeholder="my-policy" autoFocus />
            <label style={labelStyle}>Policy document *</label>
            <textarea style={{ ...inputStyle, height: 160, fontFamily: 'monospace', fontSize: '0.82em', resize: 'vertical' }}
              value={newDoc} onChange={e => setNewDoc(e.target.value)}
              placeholder={'{\n  "Version": "2012-10-17",\n  "Statement": [\n    {\n      "Effect": "Allow",\n      "Action": "*",\n      "Resource": "*"\n    }\n  ]\n}'} />
            <label style={labelStyle}>Description (optional)</label>
            <input style={inputStyle} value={newDesc} onChange={e => setNewDesc(e.target.value)} placeholder="Policy description" />
            {createMut.error && <div style={mutError}>{(createMut.error as Error).message}</div>}
            <div style={dialogActions}>
              <button style={btnSecondary} onClick={() => setCreateOpen(false)}>Cancel</button>
              <button style={btnPrimary} onClick={() => createMut.mutate()} disabled={!newName || !newDoc || createMut.isPending}>
                {createMut.isPending ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {confirmDelete && (
        <div style={overlay}>
          <div style={dialog}>
            <h3 style={{ margin: '0 0 0.75rem' }}>Delete policy?</h3>
            <p style={{ margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.9em' }}>
              Delete policy <strong>{confirmDelete.policyName}</strong>? This cannot be undone.
            </p>
            {deleteMut.error && <div style={mutError}>{(deleteMut.error as Error).message}</div>}
            <div style={dialogActions}>
              <button style={btnSecondary} onClick={() => setConfirmDelete(null)}>Cancel</button>
              <button style={{ ...btnPrimary, background: '#d13212', borderColor: '#d13212' }}
                onClick={() => deleteMut.mutate(confirmDelete.arn)} disabled={deleteMut.isPending}>
                {deleteMut.isPending ? 'Deleting…' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

const tabBtn: React.CSSProperties = {
  background: 'none', border: 'none', padding: '0.5rem 1rem', cursor: 'pointer',
  fontSize: '0.9em', borderRadius: '4px 4px 0 0',
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
const tableWrap: React.CSSProperties = { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }
const theadRow: React.CSSProperties = { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }
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
  background: '#fff', borderRadius: 10, padding: '1.5rem', minWidth: 440, maxWidth: 560,
  boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
}
const labelStyle: React.CSSProperties = { display: 'block', fontSize: '0.82em', fontWeight: 600, marginBottom: 4, color: '#2d3748' }
const inputStyle: React.CSSProperties = {
  width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #ccc', borderRadius: 6,
  marginBottom: '0.75rem', fontSize: '0.9em', boxSizing: 'border-box',
}
const dialogActions: React.CSSProperties = { display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '0.5rem' }
const mutError: React.CSSProperties = { color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' }
const loadingStyle: React.CSSProperties = { padding: '2rem', color: '#5f6b7a' }
const errorStyle: React.CSSProperties = { padding: '2rem', color: '#d13212' }
