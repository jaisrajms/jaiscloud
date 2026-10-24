import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Chip, IconButton, Link, Stack, Tooltip } from '@mui/material'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { deleteService, listServices, type RunService } from '../../api/gcp/run'
import { useAccount } from '../../context/AccountContext'
import { lastSegment, shortDate } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function target(service: RunService) {
  return { region: service.region, service: service.id }
}

/** Cloud Run services across every region, with delete. */
export function ServicesPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [selected, setSelected] = useState<string[]>([])
  const [filter, setFilter] = useState('')
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'run'] })

  const services = useQuery({
    queryKey: ['gcp', 'run', 'services', accountId],
    queryFn: listServices,
  })

  const remove = useMutation({
    mutationFn: ({ region, service }: { region: string; service: string }) =>
      deleteService(region, service),
    onSuccess: invalidate,
  })

  const rows = filterRows(services.data?.services ?? [], filter, (service) =>
    `${service.id} ${service.region}`,
  )

  const columns: GcpColumn<RunService>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (service) => service.id,
      render: (service) => (
        <Link
          component={RouterLink}
          to={`/gcp/run/services/${encodeURIComponent(service.region)}/${encodeURIComponent(service.id)}`}
        >
          {service.id}
        </Link>
      ),
    },
    {
      key: 'region',
      header: 'Region',
      sortable: true,
      sortValue: (service) => service.region,
      render: (service) => service.region || '—',
    },
    {
      key: 'latestRevision',
      header: 'Latest revision',
      sortable: true,
      sortValue: (service) =>
        service.latestReadyRevision ? lastSegment(service.latestReadyRevision) : null,
      render: (service) =>
        service.latestReadyRevision ? (
          <Link
            component={RouterLink}
            to={`/gcp/run/services/${encodeURIComponent(service.region)}/${encodeURIComponent(service.id)}/revisions/${encodeURIComponent(lastSegment(service.latestReadyRevision))}`}
          >
            {lastSegment(service.latestReadyRevision)}
          </Link>
        ) : (
          '—'
        ),
    },
    {
      key: 'updated',
      header: 'Updated',
      sortable: true,
      sortValue: (service) => service.updateTime ?? null,
      render: (service) => shortDate(service.updateTime),
    },
    {
      key: 'ready',
      header: 'Ready',
      sortable: true,
      sortValue: (service) => (service.ready ? 'Ready' : 'Not ready'),
      render: (service) => (
        <Chip
          size="small"
          label={service.ready ? 'Ready' : 'Not ready'}
          color={service.ready ? 'success' : 'warning'}
        />
      ),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (service) => (
        <Tooltip title="Delete service">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(target(service))}
              aria-label={`Delete ${service.id}`}
            >
              <DeleteOutlineIcon fontSize="small" />
            </IconButton>
          </span>
        </Tooltip>
      ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="run"
        title="Cloud Run"
        subtitle={`Services · project ${accountId || '—'}`}
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter services"
        onRefresh={() => void services.refetch()}
        refreshing={services.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Cloud Run services"
        columns={columns}
        rows={rows}
        getRowKey={(service) => `${service.region}/${service.id}`}
        loading={services.isLoading}
        error={services.isError ? 'Failed to load services.' : null}
        emptyMessage={
          filter ? 'No services match the filter.' : 'No Cloud Run services in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(service) => <GcpRowDetail row={service} />}
        detailTitle={(service) => service.id}
      />
    </Stack>
  )
}
