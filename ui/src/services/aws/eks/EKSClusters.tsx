import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { listClusters, createCluster, deleteCluster } from '../../../api/eks'

export function EKSClusters() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [clusterName, setClusterName] = useState('')

  const { data, isLoading } = useQuery({
    queryKey: ['eks', 'clusters'],
    queryFn: listClusters,
  })

  const create = useMutation({
    mutationFn: (name: string) => createCluster(name),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['eks', 'clusters'] }); setCreateOpen(false); setClusterName('') },
  })

  const del = useMutation({
    mutationFn: (name: string) => deleteCluster(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['eks', 'clusters'] }),
  })

  const items = data?.items ?? []

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-semibold">EKS Clusters</h1>
        <div className="flex items-center gap-2">
          <span className="text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded">metadata only</span>
          <button onClick={() => setCreateOpen(true)} className="px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600">
            Create Cluster
          </button>
        </div>
      </div>

      {isLoading && <p className="text-gray-500">Loading...</p>}
      {!isLoading && items.length === 0 && (
        <div className="text-center py-16 text-gray-400">No EKS clusters found</div>
      )}

      {items.length > 0 && (
        <div className="overflow-x-auto rounded border border-gray-200">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-50 text-gray-600 uppercase text-xs">
              <tr>
                <th className="px-4 py-2 text-left">Name</th>
                <th className="px-4 py-2 text-left">Status</th>
                <th className="px-4 py-2 text-left">Version</th>
                <th className="px-4 py-2 text-left">Created</th>
                <th className="px-4 py-2 text-left">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {items.map(c => (
                <tr key={c.name} className="hover:bg-gray-50">
                  <td className="px-4 py-2 font-medium">{c.name}</td>
                  <td className="px-4 py-2">
                    <span className={`text-xs px-2 py-0.5 rounded ${c.status === 'ACTIVE' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'}`}>
                      {c.status || '—'}
                    </span>
                  </td>
                  <td className="px-4 py-2">{c.version || '—'}</td>
                  <td className="px-4 py-2 text-gray-500">{c.createdAt ? new Date(c.createdAt).toLocaleString() : '—'}</td>
                  <td className="px-4 py-2">
                    <button
                      onClick={() => del.mutate(c.name)}
                      className="text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200"
                    >Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {createOpen && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 w-96">
            <h2 className="text-lg font-semibold mb-4">Create EKS Cluster</h2>
            <input
              className="w-full border border-gray-300 rounded px-3 py-2 text-sm mb-4"
              placeholder="Cluster name"
              value={clusterName}
              onChange={e => setClusterName(e.target.value)}
            />
            <div className="flex gap-2 justify-end">
              <button onClick={() => setCreateOpen(false)} className="px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50">Cancel</button>
              <button
                onClick={() => create.mutate(clusterName)}
                disabled={!clusterName.trim()}
                className="px-4 py-2 text-sm rounded bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50"
              >Create</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
