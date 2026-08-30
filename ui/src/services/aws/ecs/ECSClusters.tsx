import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { listClusters, createCluster, deleteCluster, listTasks, listServices } from '../../../api/ecs'

export function ECSClusters() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [clusterName, setClusterName] = useState('')
  const [expanded, setExpanded] = useState<string | null>(null)

  const { data, isLoading } = useQuery({
    queryKey: ['ecs', 'clusters'],
    queryFn: listClusters,
  })

  const tasksQuery = useQuery({
    queryKey: ['ecs', 'tasks', expanded],
    queryFn: () => listTasks(expanded!),
    enabled: !!expanded,
  })

  const servicesQuery = useQuery({
    queryKey: ['ecs', 'services', expanded],
    queryFn: () => listServices(expanded!),
    enabled: !!expanded,
  })

  const create = useMutation({
    mutationFn: (name: string) => createCluster(name),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['ecs', 'clusters'] }); setCreateOpen(false); setClusterName('') },
  })

  const del = useMutation({
    mutationFn: (name: string) => deleteCluster(name),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['ecs', 'clusters'] }); setExpanded(null) },
  })

  const items = data?.items ?? []

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-semibold">ECS Clusters</h1>
        <div className="flex items-center gap-2">
          <span className="text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded">metadata only</span>
          <button
            onClick={() => setCreateOpen(true)}
            className="px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600"
          >Create Cluster</button>
        </div>
      </div>

      {isLoading && <p className="text-gray-500">Loading...</p>}
      {!isLoading && items.length === 0 && (
        <div className="text-center py-16 text-gray-400">No ECS clusters found</div>
      )}

      <div className="space-y-2">
        {items.map(c => (
          <div key={c.name} className="border border-gray-200 rounded-lg overflow-hidden">
            <div
              className="flex items-center justify-between px-4 py-3 cursor-pointer hover:bg-gray-50"
              onClick={() => setExpanded(expanded === c.name ? null : c.name)}
            >
              <div className="flex items-center gap-3">
                <span className="font-medium">{c.name}</span>
                <span className={`text-xs px-2 py-0.5 rounded ${c.status === 'ACTIVE' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-600'}`}>{c.status}</span>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={e => { e.stopPropagation(); del.mutate(c.name) }}
                  className="text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200"
                >Delete</button>
                <span className="text-gray-400">{expanded === c.name ? '▲' : '▼'}</span>
              </div>
            </div>

            {expanded === c.name && (
              <div className="border-t border-gray-100 bg-gray-50 px-4 py-3 space-y-3">
                <div>
                  <p className="text-xs font-semibold text-gray-500 uppercase mb-1">Tasks ({tasksQuery.data?.total ?? 0})</p>
                  {tasksQuery.data?.items.map(t => (
                    <p key={t.arn} className="text-xs font-mono text-gray-600">{t.arn}</p>
                  ))}
                  {tasksQuery.data?.items.length === 0 && <p className="text-xs text-gray-400">No tasks</p>}
                </div>
                <div>
                  <p className="text-xs font-semibold text-gray-500 uppercase mb-1">Services ({servicesQuery.data?.total ?? 0})</p>
                  {servicesQuery.data?.items.map(s => (
                    <p key={s.arn} className="text-xs font-mono text-gray-600">{s.arn}</p>
                  ))}
                  {servicesQuery.data?.items.length === 0 && <p className="text-xs text-gray-400">No services</p>}
                </div>
              </div>
            )}
          </div>
        ))}
      </div>

      {createOpen && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 w-96">
            <h2 className="text-lg font-semibold mb-4">Create ECS Cluster</h2>
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
