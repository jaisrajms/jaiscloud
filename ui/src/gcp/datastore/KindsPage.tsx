import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, Stack } from '@mui/material'
import { Link as RouterLink } from 'react-router-dom'
import { listKinds, type Kind } from '../../api/gcp/datastore'
import { useAccount } from '../../context/AccountContext'
import { DatastoreTabs } from './DatastoreTabs'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** Datastore kinds (SELECT __key__ FROM __kind__). Clicking a kind browses its
 * entities. */
export function KindsPage() {
  const { accountId } = useAccount()
  const [filter, setFilter] = useState('')

  const kinds = useQuery({
    queryKey: ['gcp', 'datastore', 'kinds', accountId],
    queryFn: listKinds,
  })

  const rows = filterRows(kinds.data?.kinds ?? [], filter, (kind) => kind.name)

  const columns: GcpColumn<Kind>[] = [
    {
      key: 'name',
      header: 'Kind',
      sortable: true,
      sortValue: (kind) => kind.name,
      render: (kind) => (
        <Link
          component={RouterLink}
          to={`/gcp/datastore/kinds/${encodeURIComponent(kind.name)}`}
        >
          {kind.name}
        </Link>
      ),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (kind) => (
        <Link
          component={RouterLink}
          to={`/gcp/datastore/query?kind=${encodeURIComponent(kind.name)}`}
          aria-label={`Query kind ${kind.name}`}
        >
          Query
        </Link>
      ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="datastore"
        title="Datastore"
        subtitle={`Kinds · project ${accountId || '—'}`}
      >
        <DatastoreTabs />
      </GcpPageHeader>

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter kinds"
        onRefresh={() => void kinds.refetch()}
        refreshing={kinds.isFetching}
      />

      <GcpDataTable
        aria-label="Kinds"
        columns={columns}
        rows={rows}
        getRowKey={(kind) => kind.name}
        loading={kinds.isLoading}
        error={kinds.isError ? 'Failed to load kinds.' : null}
        emptyMessage={
          filter ? 'No kinds match the filter.' : 'No entities in this project yet.'
        }
        renderDetail={(kind) => <GcpRowDetail row={kind} />}
        detailTitle={(kind) => kind.name}
      />
    </Stack>
  )
}
