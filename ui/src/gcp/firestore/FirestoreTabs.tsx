import { useLocation, useNavigate } from 'react-router-dom'
import { GcpTabs } from '../common/GcpTabs'

const TABS = [
  { label: 'Collections', value: '/gcp/firestore/collections' },
  { label: 'Query', value: '/gcp/firestore/query' },
  { label: 'Indexes', value: '/gcp/firestore/indexes' },
]

/**
 * Firestore sub-navigation (Collections | Query), mirroring the console's
 * per-service tabs. Rendered on every Firestore page so the query runner is a
 * first-class peer of browsing rather than a hidden nav child.
 */
export function FirestoreTabs() {
  const location = useLocation()
  const navigate = useNavigate()
  const value = location.pathname.startsWith('/gcp/firestore/query')
    ? '/gcp/firestore/query'
    : location.pathname.startsWith('/gcp/firestore/indexes')
      ? '/gcp/firestore/indexes'
      : '/gcp/firestore/collections'
  return (
    <GcpTabs
      aria-label="Firestore sections"
      tabs={TABS}
      value={value}
      onChange={(next) => navigate(String(next))}
    />
  )
}
