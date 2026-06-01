import { api } from './client'

const BASE = '/api/ui/v1/cloudformation'

export interface Stack { name: string; status: string; stackId: string; description: string; createdAt: string }
export interface ListStacksResponse { items: Stack[]; total: number }

export const listStacks = () => api.get<ListStacksResponse>(`${BASE}/stacks`)

export const createStack = (req: { name: string; templateBody?: string; templateUrl?: string }) =>
  api.post<unknown>(`${BASE}/stacks`, req)

export const deleteStack = (name: string) =>
  api.delete<void>(`${BASE}/stacks/${encodeURIComponent(name)}`)
