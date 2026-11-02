import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Chip, Stack, Typography } from '@mui/material'
import {
  listMonitoredResourceDescriptors,
  type MonitoredResourceDescriptor,
} from '../../api/gcp/monitoring'
import { useAccount } from '../../context/AccountContext'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** The monitored-resource descriptor catalog (monitoredResourceDescriptors). */
export function MonitoredResourcesPage() {
  const { accountId } = useAccount()
  const [filter, setFilter] = useState('')

  const descriptors = useQuery({
    queryKey: ['gcp', 'monitoring', 'monitoredResourceDescriptors', accountId],
    queryFn: () => listMonitoredResourceDescriptors(),
  })

  const rows = filterRows(descriptors.data?.resourceDescriptors ?? [], filter, (d) =>
    [d.type ?? '', d.displayName ?? '', d.description ?? ''].join(' '),
  )

  const columns: GcpColumn<MonitoredResourceDescriptor>[] = [
    {
      key: 'type',
      header: 'Type',
      sortable: true,
      sortValue: (d) => d.type ?? '',
      render: (d) => <Typography variant="body2">{d.type || '—'}</Typography>,
    },
    {
      key: 'displayName',
      header: 'Display name',
      sortable: true,
      sortValue: (d) => d.displayName ?? '',
      render: (d) => d.displayName || '—',
    },
    {
      key: 'launchStage',
      header: 'Launch stage',
      sortable: true,
      sortValue: (d) => d.launchStage ?? '',
      render: (d) => (d.launchStage ? <Chip size="small" label={d.launchStage} /> : '—'),
    },
    {
      key: 'labels',
      header: 'Labels',
      sortable: false,
      render: (d) => `${d.labels?.length ?? 0}`,
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="monitoring"
        title="Cloud Monitoring"
        subtitle={`Monitored resources · project ${accountId || '—'}`}
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter resource types"
        onRefresh={() => void descriptors.refetch()}
        refreshing={descriptors.isFetching}
      />

      <GcpDataTable
        aria-label="Monitored resources"
        columns={columns}
        rows={rows}
        getRowKey={(d) => d.type ?? d.name ?? ''}
        loading={descriptors.isLoading}
        error={descriptors.isError ? 'Failed to load monitored resource descriptors.' : null}
        emptyMessage={
          filter ? 'No resource types match the filter.' : 'No monitored resource descriptors.'
        }
        renderDetail={(d) => (
          <Stack spacing={1}>
            <Typography variant="body2">{d.description || 'No description.'}</Typography>
            {(d.labels ?? []).map((label) => (
              <Typography key={label.key} variant="body2" color="text.secondary">
                {label.key}: {label.description || ''}
              </Typography>
            ))}
          </Stack>
        )}
        detailTitle={(d) => d.type || 'Descriptor'}
      />
    </Stack>
  )
}
