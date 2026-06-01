import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { listIdentities, verifyEmailIdentity, deleteIdentity } from '../../../api/ses'

export function SESIdentities() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [identityInput, setIdentityInput] = useState('')

  const { data, isLoading } = useQuery({
    queryKey: ['ses', 'identities'],
    queryFn: listIdentities,
  })

  const verify = useMutation({
    mutationFn: () => verifyEmailIdentity(identityInput),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['ses', 'identities'] }); setCreateOpen(false); setIdentityInput('') },
  })

  const del = useMutation({
    mutationFn: (identity: string) => deleteIdentity(identity),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['ses', 'identities'] }),
  })

  const items = data?.items ?? []

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-semibold">SES Identities</h1>
        <button onClick={() => setCreateOpen(true)} className="px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600">
          Verify Identity
        </button>
      </div>

      {isLoading && <p className="text-gray-500">Loading...</p>}
      {!isLoading && items.length === 0 && (
        <div className="text-center py-16 text-gray-400">No SES identities found</div>
      )}

      {items.length > 0 && (
        <div className="overflow-x-auto rounded border border-gray-200">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-50 text-gray-600 uppercase text-xs">
              <tr>
                <th className="px-4 py-2 text-left">Identity</th>
                <th className="px-4 py-2 text-left">Type</th>
                <th className="px-4 py-2 text-left">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {items.map(id => (
                <tr key={id.identity} className="hover:bg-gray-50">
                  <td className="px-4 py-2 font-medium">{id.identity}</td>
                  <td className="px-4 py-2">
                    <span className="text-xs px-2 py-0.5 rounded bg-blue-100 text-blue-700">{id.type}</span>
                  </td>
                  <td className="px-4 py-2">
                    <button onClick={() => del.mutate(id.identity)} className="text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200">Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {createOpen && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 w-[420px]">
            <h2 className="text-lg font-semibold mb-4">Verify Email Identity</h2>
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">Email Address or Domain</label>
              <input className="w-full border border-gray-300 rounded px-3 py-2 text-sm" placeholder="user@example.com or example.com" value={identityInput} onChange={e => setIdentityInput(e.target.value)} />
            </div>
            <div className="flex gap-2 justify-end mt-4">
              <button onClick={() => setCreateOpen(false)} className="px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50">Cancel</button>
              <button
                onClick={() => verify.mutate()}
                disabled={!identityInput.trim()}
                className="px-4 py-2 text-sm rounded bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50"
              >Verify</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
