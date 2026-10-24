import { useLocation, useNavigate } from 'react-router-dom'
import { GcpTabs } from '../common/GcpTabs'

const TABS = [
  { label: 'Kinds', value: '/gcp/datastore/kinds' },
  { label: 'Query', value: '/gcp/datastore/query' },
]

/**
 * Datastore sub-navigation (Kinds | Query), mirroring the console's per-service
 * tabs so the GQL runner is a first-class peer of browsing.
 */
export function DatastoreTabs() {
  const location = useLocation()
  const navigate = useNavigate()
  const value = location.pathname.startsWith('/gcp/datastore/query')
    ? '/gcp/datastore/query'
    : '/gcp/datastore/kinds'
  return (
    <GcpTabs
      aria-label="Datastore sections"
      tabs={TABS}
      value={value}
      onChange={(next) => navigate(String(next))}
    />
  )
}
