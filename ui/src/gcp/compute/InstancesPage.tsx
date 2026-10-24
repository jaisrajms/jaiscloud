import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Chip, IconButton, Link, Stack, Tooltip } from '@mui/material'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import StopIcon from '@mui/icons-material/Stop'
import { Link as RouterLink } from 'react-router-dom'
import {
  deleteInstance,
  listInstances,
  startInstance,
  stopInstance,
  type Instance,
} from '../../api/gcp/compute'
import { useAccount } from '../../context/AccountContext'
import { shortDate, statusColor } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function target(instance: Instance): { zone: string; instance: string } {
  return { zone: instance.zone, instance: instance.name }
}

/** Compute Engine instances across every zone, with start / stop / delete. */
export function InstancesPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'compute'] })

  const instances = useQuery({
    queryKey: ['gcp', 'compute', 'instances', accountId],
    queryFn: listInstances,
  })

  const start = useMutation({
    mutationFn: ({ zone, instance }: { zone: string; instance: string }) =>
      startInstance(zone, instance),
    onSuccess: invalidate,
  })
  const stop = useMutation({
    mutationFn: ({ zone, instance }: { zone: string; instance: string }) =>
      stopInstance(zone, instance),
    onSuccess: invalidate,
  })
  const remove = useMutation({
    mutationFn: ({ zone, instance }: { zone: string; instance: string }) =>
      deleteInstance(zone, instance),
    onSuccess: invalidate,
  })

  const busy = start.isPending || stop.isPending || remove.isPending

  const rows = filterRows(instances.data?.instances ?? [], filter, (instance) =>
    `${instance.name} ${instance.zone} ${instance.status} ${instance.machineType} ${
      instance.internalIp ?? ''
    } ${instance.externalIp ?? ''} ${instance.creationTimestamp ?? ''}`,
  )

  const columns: GcpColumn<Instance>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (instance) => instance.name,
      render: (instance) => (
        <Link
          component={RouterLink}
          to={`/gcp/compute/instances/${encodeURIComponent(instance.zone)}/${encodeURIComponent(instance.name)}`}
        >
          {instance.name}
        </Link>
      ),
    },
    {
      key: 'zone',
      header: 'Zone',
      sortable: true,
      sortValue: (instance) => instance.zone,
      render: (instance) => instance.zone || '—',
    },
    {
      key: 'status',
      header: 'Status',
      sortable: true,
      sortValue: (instance) => instance.status,
      render: (instance) => (
        <Chip size="small" label={instance.status || 'UNKNOWN'} color={statusColor(instance.status)} />
      ),
    },
    {
      key: 'machineType',
      header: 'Machine type',
      sortable: true,
      sortValue: (instance) => instance.machineType,
      render: (instance) => instance.machineType || '—',
    },
    {
      key: 'internalIp',
      header: 'Internal IP',
      sortable: true,
      sortValue: (instance) => instance.internalIp ?? '',
      render: (instance) => instance.internalIp || '—',
    },
    {
      key: 'externalIp',
      header: 'External IP',
      sortable: true,
      sortValue: (instance) => instance.externalIp ?? '',
      render: (instance) => instance.externalIp || '—',
    },
    {
      key: 'created',
      header: 'Created',
      sortable: true,
      sortValue: (instance) => instance.creationTimestamp ?? '',
      render: (instance) => shortDate(instance.creationTimestamp),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (instance) => (
        <>
          <Tooltip title="Start instance">
            <span>
              <IconButton
                size="small"
                disabled={busy || instance.status === 'RUNNING'}
                onClick={() => start.mutate(target(instance))}
                aria-label={`Start ${instance.name}`}
              >
                <PlayArrowIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
          <Tooltip title="Stop instance">
            <span>
              <IconButton
                size="small"
                disabled={busy || instance.status !== 'RUNNING'}
                onClick={() => stop.mutate(target(instance))}
                aria-label={`Stop ${instance.name}`}
              >
                <StopIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
          <Tooltip title="Delete instance">
            <span>
              <IconButton
                size="small"
                disabled={busy}
                onClick={() => remove.mutate(target(instance))}
                aria-label={`Delete ${instance.name}`}
              >
                <DeleteOutlineIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
        </>
      ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="compute"
        title="Compute Engine"
        subtitle={`Instances · project ${accountId || '—'}`}
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter instances"
        onRefresh={() => void instances.refetch()}
        refreshing={instances.isFetching}
      />

      {start.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Start failed.
        </Alert>
      )}
      {stop.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Stop failed.
        </Alert>
      )}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Compute Engine instances"
        columns={columns}
        rows={rows}
        getRowKey={(instance) => `${instance.zone}/${instance.name}`}
        loading={instances.isLoading}
        error={instances.isError ? 'Failed to load instances.' : null}
        emptyMessage={filter ? 'No instances match the filter.' : 'No instances in this project.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(instance) => <GcpRowDetail row={instance} />}
        detailTitle={(instance) => instance.name}
      />
    </Stack>
  )
}
