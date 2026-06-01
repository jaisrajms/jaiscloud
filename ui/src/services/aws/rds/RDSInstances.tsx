import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { listInstances, createInstance, deleteInstance, startInstance, stopInstance } from '../../../api/rds'

function statusColor(status: string): string {
  if (status === 'available') return 'bg-green-100 text-green-700'
  if (status === 'stopped') return 'bg-gray-100 text-gray-600'
  if (status === 'deleting') return 'bg-red-100 text-red-700'
  return 'bg-yellow-100 text-yellow-700'
}

export function RDSInstances() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [form, setForm] = useState({ id: '', engine: 'mysql', class: 'db.t3.micro', username: 'admin', password: '' })

  const { data, isLoading } = useQuery({
    queryKey: ['rds', 'instances'],
    queryFn: listInstances,
  })

  const create = useMutation({
    mutationFn: () => createInstance(form),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['rds', 'instances'] }); setCreateOpen(false) },
  })

  const del = useMutation({
    mutationFn: (id: string) => deleteInstance(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['rds', 'instances'] }),
  })

  const start = useMutation({
    mutationFn: (id: string) => startInstance(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['rds', 'instances'] }),
  })

  const stop = useMutation({
    mutationFn: (id: string) => stopInstance(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['rds', 'instances'] }),
  })

  const items = data?.items ?? []

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-semibold">RDS Instances</h1>
        <div className="flex items-center gap-2">
          <span className="text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded">metadata only</span>
          <button onClick={() => setCreateOpen(true)} className="px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600">
            Create Instance
          </button>
        </div>
      </div>

      {isLoading && <p className="text-gray-500">Loading...</p>}
      {!isLoading && items.length === 0 && (
        <div className="text-center py-16 text-gray-400">No RDS instances found</div>
      )}

      {items.length > 0 && (
        <div className="overflow-x-auto rounded border border-gray-200">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-50 text-gray-600 uppercase text-xs">
              <tr>
                <th className="px-4 py-2 text-left">Identifier</th>
                <th className="px-4 py-2 text-left">Status</th>
                <th className="px-4 py-2 text-left">Engine</th>
                <th className="px-4 py-2 text-left">Class</th>
                <th className="px-4 py-2 text-left">Endpoint</th>
                <th className="px-4 py-2 text-left">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {items.map(inst => (
                <tr key={inst.id} className="hover:bg-gray-50">
                  <td className="px-4 py-2 font-medium">{inst.id}</td>
                  <td className="px-4 py-2">
                    <span className={`text-xs px-2 py-0.5 rounded ${statusColor(inst.status)}`}>{inst.status}</span>
                  </td>
                  <td className="px-4 py-2">{inst.engine}</td>
                  <td className="px-4 py-2">{inst.class}</td>
                  <td className="px-4 py-2 font-mono text-xs">{inst.endpoint ? `${inst.endpoint}:${inst.port}` : '—'}</td>
                  <td className="px-4 py-2 space-x-1">
                    {inst.status === 'stopped' && (
                      <button onClick={() => start.mutate(inst.id)} className="text-xs px-2 py-0.5 rounded bg-green-100 text-green-700 hover:bg-green-200">Start</button>
                    )}
                    {inst.status === 'available' && (
                      <button onClick={() => stop.mutate(inst.id)} className="text-xs px-2 py-0.5 rounded bg-yellow-100 text-yellow-700 hover:bg-yellow-200">Stop</button>
                    )}
                    <button onClick={() => del.mutate(inst.id)} className="text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200">Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {createOpen && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 w-[480px]">
            <h2 className="text-lg font-semibold mb-4">Create RDS Instance</h2>
            <div className="space-y-3">
              {[
                { label: 'Identifier', key: 'id' as const, placeholder: 'my-db' },
                { label: 'Engine', key: 'engine' as const, placeholder: 'mysql' },
                { label: 'Class', key: 'class' as const, placeholder: 'db.t3.micro' },
                { label: 'Master Username', key: 'username' as const, placeholder: 'admin' },
                { label: 'Master Password', key: 'password' as const, placeholder: '••••••••' },
              ].map(f => (
                <div key={f.key}>
                  <label className="block text-xs font-medium text-gray-700 mb-1">{f.label}</label>
                  <input
                    type={f.key === 'password' ? 'password' : 'text'}
                    className="w-full border border-gray-300 rounded px-3 py-2 text-sm"
                    placeholder={f.placeholder}
                    value={form[f.key]}
                    onChange={e => setForm(p => ({ ...p, [f.key]: e.target.value }))}
                  />
                </div>
              ))}
            </div>
            <div className="flex gap-2 justify-end mt-4">
              <button onClick={() => setCreateOpen(false)} className="px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50">Cancel</button>
              <button
                onClick={() => create.mutate()}
                disabled={!form.id.trim()}
                className="px-4 py-2 text-sm rounded bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50"
              >Create</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
