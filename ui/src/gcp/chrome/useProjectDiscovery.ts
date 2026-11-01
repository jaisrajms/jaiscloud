import { useQuery } from '@tanstack/react-query'
import { listProjectDiscovery } from '../../api/gcp/projects'

/**
 * Configured, registered and observed projects. `refetchInterval` lets the home
 * page follow live workload activity while the picker can stay cheap.
 */
export function useProjectDiscovery(refetchInterval: number | false = false) {
  return useQuery({
    queryKey: ['gcp', 'project-discovery'],
    queryFn: listProjectDiscovery,
    staleTime: 10_000,
    refetchInterval,
    retry: false,
  })
}
