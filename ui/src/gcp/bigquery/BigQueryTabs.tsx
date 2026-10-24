import { useLocation, useNavigate } from 'react-router-dom'
import { GcpTabs } from '../common/GcpTabs'

const TABS = [
  { label: 'Datasets', value: '/gcp/bigquery/datasets' },
  { label: 'Jobs', value: '/gcp/bigquery/jobs' },
  { label: 'Query', value: '/gcp/bigquery/query' },
]

/**
 * BigQuery sub-navigation (Datasets | Jobs | Query), mirroring the console's
 * per-service tabs so the SQL runner is a first-class peer of browsing.
 */
export function BigQueryTabs() {
  const location = useLocation()
  const navigate = useNavigate()
  let value = '/gcp/bigquery/datasets'
  if (location.pathname.startsWith('/gcp/bigquery/query')) {
    value = '/gcp/bigquery/query'
  } else if (location.pathname.startsWith('/gcp/bigquery/jobs')) {
    value = '/gcp/bigquery/jobs'
  }
  return (
    <GcpTabs
      aria-label="BigQuery sections"
      tabs={TABS}
      value={value}
      onChange={(next) => navigate(String(next))}
    />
  )
}
