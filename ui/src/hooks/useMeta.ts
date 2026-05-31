import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'

export interface Meta {
  cloud: string
  region: string
  accountId: string
  mode: 'memory' | 'postgres' | 'ephemeral'
  version: string
  uiVersion: string
  instanceId: string
}

export function useMeta() {
  return useQuery<Meta>({
    queryKey: ['meta'],
    queryFn: () => api.get<Meta>('/api/ui/v1/meta'),
    staleTime: 60_000,
    retry: 3,
  })
}
