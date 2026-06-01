import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { listStacks, createStack, deleteStack } from '../../../api/cfn'

function statusColor(status: string): string {
  if (status.includes('COMPLETE') && !status.includes('ROLLBACK')) return 'bg-green-100 text-green-700'
  if (status.includes('FAILED') || status.includes('ROLLBACK')) return 'bg-red-100 text-red-700'
  if (status.includes('IN_PROGRESS')) return 'bg-yellow-100 text-yellow-700'
  return 'bg-gray-100 text-gray-600'
}

const DEFAULT_TEMPLATE = JSON.stringify({ AWSTemplateFormatVersion: '2010-09-09', Resources: {} }, null, 2)

export function CFNStacks() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [form, setForm] = useState({ name: '', templateBody: DEFAULT_TEMPLATE })

  const { data, isLoading } = useQuery({
    queryKey: ['cfn', 'stacks'],
    queryFn: listStacks,
  })

  const create = useMutation({
    mutationFn: () => createStack({ name: form.name, templateBody: form.templateBody }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['cfn', 'stacks'] }); setCreateOpen(false) },
  })

  const del = useMutation({
    mutationFn: (name: string) => deleteStack(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['cfn', 'stacks'] }),
  })

  const items = data?.items ?? []

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-semibold">CloudFormation Stacks</h1>
        <button onClick={() => setCreateOpen(true)} className="px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600">
          Create Stack
        </button>
      </div>

      {isLoading && <p className="text-gray-500">Loading...</p>}
      {!isLoading && items.length === 0 && (
        <div className="text-center py-16 text-gray-400">No stacks found</div>
      )}

      {items.length > 0 && (
        <div className="overflow-x-auto rounded border border-gray-200">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-50 text-gray-600 uppercase text-xs">
              <tr>
                <th className="px-4 py-2 text-left">Name</th>
                <th className="px-4 py-2 text-left">Status</th>
                <th className="px-4 py-2 text-left">Description</th>
                <th className="px-4 py-2 text-left">Created</th>
                <th className="px-4 py-2 text-left">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {items.map(s => (
                <tr key={s.name} className="hover:bg-gray-50">
                  <td className="px-4 py-2 font-medium">{s.name}</td>
                  <td className="px-4 py-2">
                    <span className={`text-xs px-2 py-0.5 rounded ${statusColor(s.status)}`}>{s.status}</span>
                  </td>
                  <td className="px-4 py-2 text-gray-500 truncate max-w-xs">{s.description || '—'}</td>
                  <td className="px-4 py-2 text-gray-500">{s.createdAt ? new Date(s.createdAt).toLocaleString() : '—'}</td>
                  <td className="px-4 py-2">
                    <button onClick={() => del.mutate(s.name)} className="text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200">Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {createOpen && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 w-[560px]">
            <h2 className="text-lg font-semibold mb-4">Create Stack</h2>
            <div className="space-y-3">
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">Stack Name</label>
                <input
                  className="w-full border border-gray-300 rounded px-3 py-2 text-sm"
                  placeholder="my-stack"
                  value={form.name}
                  onChange={e => setForm(p => ({ ...p, name: e.target.value }))}
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">Template Body (JSON/YAML)</label>
                <textarea
                  rows={8}
                  className="w-full border border-gray-300 rounded px-3 py-2 text-xs font-mono"
                  value={form.templateBody}
                  onChange={e => setForm(p => ({ ...p, templateBody: e.target.value }))}
                />
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
