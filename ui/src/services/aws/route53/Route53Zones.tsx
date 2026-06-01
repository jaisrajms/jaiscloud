import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { listZones, createZone, deleteZone, listRecords } from '../../../api/route53'

export function Route53Zones() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [zoneName, setZoneName] = useState('')
  const [expandedId, setExpandedId] = useState<string | null>(null)

  const { data, isLoading } = useQuery({
    queryKey: ['route53', 'zones'],
    queryFn: listZones,
  })

  const recordsQuery = useQuery({
    queryKey: ['route53', 'records', expandedId],
    queryFn: () => listRecords(expandedId!),
    enabled: !!expandedId,
  })

  const create = useMutation({
    mutationFn: (name: string) => createZone(name),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['route53', 'zones'] }); setCreateOpen(false); setZoneName('') },
  })

  const del = useMutation({
    mutationFn: (id: string) => deleteZone(id),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['route53', 'zones'] }); setExpandedId(null) },
  })

  const items = data?.items ?? []

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-semibold">Route 53 Hosted Zones</h1>
        <div className="flex items-center gap-2">
          <span className="text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded">metadata only</span>
          <button onClick={() => setCreateOpen(true)} className="px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600">
            Create Zone
          </button>
        </div>
      </div>

      {isLoading && <p className="text-gray-500">Loading...</p>}
      {!isLoading && items.length === 0 && (
        <div className="text-center py-16 text-gray-400">No hosted zones found</div>
      )}

      <div className="space-y-2">
        {items.map(z => (
          <div key={z.id} className="border border-gray-200 rounded-lg overflow-hidden">
            <div
              className="flex items-center justify-between px-4 py-3 cursor-pointer hover:bg-gray-50"
              onClick={() => setExpandedId(expandedId === z.id ? null : z.id)}
            >
              <div className="flex items-center gap-3">
                <span className="font-medium">{z.name}</span>
                <span className="text-xs text-gray-500 font-mono">{z.id}</span>
                {z.private && <span className="text-xs bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded">Private</span>}
              </div>
              <div className="flex items-center gap-3">
                <span className="text-sm text-gray-500">{z.recordCount} records</span>
                <button
                  onClick={e => { e.stopPropagation(); del.mutate(z.id) }}
                  className="text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200"
                >Delete</button>
                <span className="text-gray-400">{expandedId === z.id ? '▲' : '▼'}</span>
              </div>
            </div>

            {expandedId === z.id && (
              <div className="border-t border-gray-100 bg-gray-50 px-4 py-3">
                <p className="text-xs font-semibold text-gray-500 uppercase mb-2">Record Sets</p>
                {recordsQuery.isLoading && <p className="text-xs text-gray-400">Loading records...</p>}
                {recordsQuery.data?.items.length === 0 && <p className="text-xs text-gray-400">No records found</p>}
                {recordsQuery.data?.items.map((r, i) => (
                  <div key={i} className="text-xs font-mono flex gap-4 py-0.5">
                    <span className="text-blue-700 w-32 truncate">{r.name}</span>
                    <span className="text-purple-700 w-10">{r.type}</span>
                    <span className="text-gray-500 w-12">TTL {r.ttl}</span>
                    <span className="text-gray-700">{r.records?.join(', ')}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>

      {createOpen && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 w-96">
            <h2 className="text-lg font-semibold mb-4">Create Hosted Zone</h2>
            <input
              className="w-full border border-gray-300 rounded px-3 py-2 text-sm mb-4"
              placeholder="example.com"
              value={zoneName}
              onChange={e => setZoneName(e.target.value)}
            />
            <div className="flex gap-2 justify-end">
              <button onClick={() => setCreateOpen(false)} className="px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50">Cancel</button>
              <button
                onClick={() => create.mutate(zoneName)}
                disabled={!zoneName.trim()}
                className="px-4 py-2 text-sm rounded bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50"
              >Create</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
