import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { listLoadBalancers, createLoadBalancer, deleteLoadBalancer } from '../../../api/elbv2'

export function ELBv2LoadBalancers() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [form, setForm] = useState({ name: '', type: 'application', scheme: 'internet-facing' })

  const { data, isLoading } = useQuery({
    queryKey: ['elbv2', 'load-balancers'],
    queryFn: listLoadBalancers,
  })

  const create = useMutation({
    mutationFn: () => createLoadBalancer(form),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['elbv2', 'load-balancers'] }); setCreateOpen(false) },
  })

  const del = useMutation({
    mutationFn: (arn: string) => deleteLoadBalancer(arn),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['elbv2', 'load-balancers'] }),
  })

  const items = data?.items ?? []

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-semibold">Elastic Load Balancers</h1>
        <div className="flex items-center gap-2">
          <span className="text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded">metadata only</span>
          <button onClick={() => setCreateOpen(true)} className="px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600">
            Create Load Balancer
          </button>
        </div>
      </div>

      {isLoading && <p className="text-gray-500">Loading...</p>}
      {!isLoading && items.length === 0 && (
        <div className="text-center py-16 text-gray-400">No load balancers found</div>
      )}

      {items.length > 0 && (
        <div className="overflow-x-auto rounded border border-gray-200">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-50 text-gray-600 uppercase text-xs">
              <tr>
                <th className="px-4 py-2 text-left">Name</th>
                <th className="px-4 py-2 text-left">State</th>
                <th className="px-4 py-2 text-left">Type</th>
                <th className="px-4 py-2 text-left">Scheme</th>
                <th className="px-4 py-2 text-left">DNS Name</th>
                <th className="px-4 py-2 text-left">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {items.map(lb => (
                <tr key={lb.arn} className="hover:bg-gray-50">
                  <td className="px-4 py-2 font-medium">{lb.name}</td>
                  <td className="px-4 py-2">
                    <span className={`text-xs px-2 py-0.5 rounded ${lb.state === 'active' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'}`}>
                      {lb.state || '—'}
                    </span>
                  </td>
                  <td className="px-4 py-2">{lb.type || '—'}</td>
                  <td className="px-4 py-2">{lb.scheme || '—'}</td>
                  <td className="px-4 py-2 font-mono text-xs">{lb.dnsName || '—'}</td>
                  <td className="px-4 py-2">
                    <button onClick={() => del.mutate(lb.arn)} className="text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200">Delete</button>
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
            <h2 className="text-lg font-semibold mb-4">Create Load Balancer</h2>
            <div className="space-y-3">
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">Name</label>
                <input className="w-full border border-gray-300 rounded px-3 py-2 text-sm" placeholder="my-load-balancer" value={form.name} onChange={e => setForm(p => ({ ...p, name: e.target.value }))} />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">Type</label>
                <select className="w-full border border-gray-300 rounded px-3 py-2 text-sm" value={form.type} onChange={e => setForm(p => ({ ...p, type: e.target.value }))}>
                  <option value="application">Application</option>
                  <option value="network">Network</option>
                  <option value="gateway">Gateway</option>
                </select>
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">Scheme</label>
                <select className="w-full border border-gray-300 rounded px-3 py-2 text-sm" value={form.scheme} onChange={e => setForm(p => ({ ...p, scheme: e.target.value }))}>
                  <option value="internet-facing">Internet-facing</option>
                  <option value="internal">Internal</option>
                </select>
              </div>
            </div>
            <div className="flex gap-2 justify-end mt-4">
              <button onClick={() => setCreateOpen(false)} className="px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50">Cancel</button>
              <button
                onClick={() => create.mutate()}
                disabled={!form.name.trim()}
                className="px-4 py-2 text-sm rounded bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50"
              >Create</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
