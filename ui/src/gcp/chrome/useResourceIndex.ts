import { useQuery } from '@tanstack/react-query'
import { useAccount } from '../../context/AccountContext'
import { loadResourceIndex } from '../common/resourceSearch'

/**
 * The console resource index backing the global search's resource results.
 *
 * It fans out over ~20 list endpoints, so it is fetched lazily (`enabled` is
 * false until the query is long enough to use it) and cached per project — the
 * `account` query param every UI API call carries is what scopes it. The
 * staleTime is short so a resource created just before a search still shows up;
 * there is no mutation-driven invalidation (per-service mutations would have to
 * learn the index key). A failure is not retried: the loaders are already
 * best-effort and a retry would only re-run the fan-out.
 */
export function useResourceIndex(enabled: boolean) {
  const { accountId } = useAccount()
  return useQuery({
    queryKey: ['gcp', 'resource-index', accountId],
    queryFn: loadResourceIndex,
    enabled,
    staleTime: 15_000,
    retry: false,
    refetchOnWindowFocus: false,
  })
}
