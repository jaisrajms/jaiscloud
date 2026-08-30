import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { listInstances, terminateInstance, startInstance, stopInstance, type Instance } from '../../../api/ec2'

function stateColor(state: string): string {
  if (state === 'running') return 'text-green-600'
  if (state === 'stopped') return 'text-gray-500'
  if (state === 'terminated') return 'text-red-500'
  return 'text-yellow-500'
}

export function EC2Instances() {
  const qc = useQueryClient()
  const [selected, setSelected] = useState<Instance | null>(null)

  const { data, isLoading } = useQuery({
    queryKey: ['ec2', 'instances'],
    queryFn: () => listInstances(),
  })

  const terminate = useMutation({
    mutationFn: (id: string) => terminateInstance(id),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['ec2', 'instances'] }); setSelected(null) },
  })
  const start = useMutation({
    mutationFn: (id: string) => startInstance(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['ec2', 'instances'] }),
  })
  const stop = useMutation({
    mutationFn: (id: string) => stopInstance(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['ec2', 'instances'] }),
  })

  const items = data?.items ?? []

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-semibold">EC2 Instances</h1>
        <span className="text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded">metadata only</span>
      </div>

      {isLoading && <p className="text-gray-500">Loading...</p>}

      {!isLoading && items.length === 0 && (
        <div className="text-center py-16 text-gray-400">No instances found</div>
      )}

      {items.length > 0 && (
        <div className="overflow-x-auto rounded border border-gray-200">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-50 text-gray-600 uppercase text-xs">
              <tr>
                <th className="px-4 py-2 text-left">Instance ID</th>
                <th className="px-4 py-2 text-left">State</th>
                <th className="px-4 py-2 text-left">Type</th>
                <th className="px-4 py-2 text-left">Image ID</th>
                <th className="px-4 py-2 text-left">Private IP</th>
                <th className="px-4 py-2 text-left">Public IP</th>
                <th className="px-4 py-2 text-left">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {items.map(inst => (
                <tr key={inst.id} className="hover:bg-gray-50">
                  <td className="px-4 py-2 font-mono text-xs">{inst.id}</td>
                  <td className={`px-4 py-2 font-medium ${stateColor(inst.state)}`}>{inst.state}</td>
                  <td className="px-4 py-2">{inst.instanceType}</td>
                  <td className="px-4 py-2 font-mono text-xs">{inst.imageId}</td>
                  <td className="px-4 py-2 font-mono text-xs">{inst.privateIp || '—'}</td>
                  <td className="px-4 py-2 font-mono text-xs">{inst.publicIp || '—'}</td>
                  <td className="px-4 py-2 space-x-1">
                    {inst.state === 'stopped' && (
                      <button
                        onClick={() => start.mutate(inst.id)}
                        className="text-xs px-2 py-0.5 rounded bg-green-100 text-green-700 hover:bg-green-200"
                      >Start</button>
                    )}
                    {inst.state === 'running' && (
                      <button
                        onClick={() => stop.mutate(inst.id)}
                        className="text-xs px-2 py-0.5 rounded bg-yellow-100 text-yellow-700 hover:bg-yellow-200"
                      >Stop</button>
                    )}
                    {inst.state !== 'terminated' && (
                      <button
                        onClick={() => setSelected(inst)}
                        className="text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200"
                      >Terminate</button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {selected && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 w-96">
            <h2 className="text-lg font-semibold mb-2">Terminate Instance</h2>
            <p className="text-sm text-gray-600 mb-4">Terminate <span className="font-mono">{selected.id}</span>? This cannot be undone.</p>
            <div className="flex gap-2 justify-end">
              <button onClick={() => setSelected(null)} className="px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50">Cancel</button>
              <button
                onClick={() => terminate.mutate(selected.id)}
                className="px-4 py-2 text-sm rounded bg-red-600 text-white hover:bg-red-700"
              >Terminate</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
