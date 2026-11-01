import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  IconButton,
  Link,
  MenuItem,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import { Link as RouterLink } from 'react-router-dom'
import {
  deleteServiceLevelObjective,
  listServiceLevelObjectives,
  listServices,
  type ServiceLevelObjective,
} from '../../api/gcp/monitoring'
import { useAccount } from '../../context/AccountContext'
import { SLODialog } from './SLODialog'
import { formatGoal, periodLabel, sliLabel } from './sloUtil'
import { resourceID } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** Service Monitoring SLOs: pick a service, then list/create/edit/delete its
 * service level objectives. */
export function SLOPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [service, setService] = useState('')
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<ServiceLevelObjective | undefined>(undefined)
  const [filter, setFilter] = useState('')

  // A different project has a different service set; drop a stale selection.
  useEffect(() => {
    setService('')
  }, [accountId])

  const services = useQuery({
    queryKey: ['gcp', 'monitoring', 'services', accountId],
    queryFn: () => listServices(),
  })

  const serviceOptions = services.data?.services ?? []
  const activeService = service || (serviceOptions[0] ? resourceID(serviceOptions[0].name) : '')

  const slos = useQuery({
    queryKey: ['gcp', 'monitoring', 'slos', accountId, activeService],
    queryFn: () => listServiceLevelObjectives(activeService),
    enabled: Boolean(activeService),
  })

  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: ['gcp', 'monitoring', 'slos'] })
  }

  const remove = useMutation({
    mutationFn: (slo: ServiceLevelObjective) =>
      deleteServiceLevelObjective(activeService, resourceID(slo.name)),
    onSuccess: invalidate,
  })

  const rows = filterRows(slos.data?.serviceLevelObjectives ?? [], filter, (slo) =>
    `${slo.displayName ?? ''} ${resourceID(slo.name)}`,
  )

  const columns: GcpColumn<ServiceLevelObjective>[] = [
    {
      key: 'displayName',
      header: 'Display name',
      sortable: true,
      sortValue: (slo) => slo.displayName || resourceID(slo.name),
      render: (slo) => (
        <Link
          component={RouterLink}
          to={`/gcp/monitoring/slo/${encodeURIComponent(activeService)}/${encodeURIComponent(
            resourceID(slo.name),
          )}`}
        >
          {slo.displayName || resourceID(slo.name)}
        </Link>
      ),
    },
    {
      key: 'goal',
      header: 'Goal',
      sortable: true,
      sortValue: (slo) => slo.goal ?? 0,
      render: (slo) => formatGoal(slo.goal),
    },
    {
      key: 'period',
      header: 'Period',
      sortable: true,
      sortValue: (slo) => periodLabel(slo),
      render: (slo) => periodLabel(slo),
    },
    {
      key: 'sli',
      header: 'SLI',
      sortable: true,
      sortValue: (slo) => sliLabel(slo.serviceLevelIndicator),
      render: (slo) => sliLabel(slo.serviceLevelIndicator),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (slo) => (
        <>
          <Tooltip title="Edit SLO">
            <IconButton
              size="small"
              onClick={() => {
                setEditing(slo)
                setDialogOpen(true)
              }}
            >
              <EditOutlinedIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete SLO">
            <IconButton
              size="small"
              onClick={() => remove.mutate(slo)}
              disabled={remove.isPending}
            >
              <DeleteOutlineIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </>
      ),
    },
  ]

  return (
    <Box>
      <GcpPageHeader
        id="monitoring"
        title="Service level objectives"
        subtitle={`SLOs · ${activeService || 'no service selected'} · project ${accountId || '—'}`}
        actions={
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            disabled={!activeService}
            onClick={() => {
              setEditing(undefined)
              setDialogOpen(true)
            }}
          >
            Create SLO
          </Button>
        }
      />

      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 1.5, flexWrap: 'wrap', rowGap: 1 }}
      >
        <TextField
          select
          size="small"
          label="Service"
          value={activeService}
          onChange={(event) => setService(event.target.value)}
          disabled={services.isLoading || serviceOptions.length === 0}
          sx={{ minWidth: 280 }}
        >
          {serviceOptions.map((option) => {
            const id = resourceID(option.name)
            return (
              <MenuItem key={option.name} value={id}>
                {option.displayName || id}
              </MenuItem>
            )
          })}
        </TextField>
        <Typography variant="body2" color="text.secondary">
          {serviceOptions.length === 0
            ? 'No services'
            : `${serviceOptions.length} service${serviceOptions.length === 1 ? '' : 's'}`}
        </Typography>
      </Stack>

      {services.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Failed to load Service Monitoring services.
        </Alert>
      )}
      {!services.isLoading && !services.isError && serviceOptions.length === 0 && (
        <Alert severity="info" sx={{ mb: 2 }}>
          This project has no Service Monitoring services. Create one with the
          Cloud Monitoring API (<code>projects.services.create</code>) or{' '}
          <code>gcloud monitoring services create</code>, then refresh.
        </Alert>
      )}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Could not delete the SLO: {(remove.error as Error).message}
        </Alert>
      )}

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter SLOs"
        onRefresh={() => {
          void services.refetch()
          void slos.refetch()
        }}
        refreshing={services.isFetching || slos.isFetching}
      />

      <GcpDataTable
        aria-label="Service level objectives"
        columns={columns}
        rows={rows}
        getRowKey={(slo) => slo.name}
        loading={services.isLoading || slos.isLoading}
        error={slos.isError ? 'Failed to load service level objectives.' : null}
        emptyMessage={
          !activeService
            ? 'Choose a service to see its SLOs.'
            : filter
              ? 'No SLOs match the filter.'
              : 'No SLOs in this service.'
        }
        renderDetail={(slo) => (
          <GcpRowDetail
            row={{
              displayName: slo.displayName || resourceID(slo.name),
              goal: formatGoal(slo.goal),
              period: periodLabel(slo),
              sli: sliLabel(slo.serviceLevelIndicator),
              name: slo.name,
            }}
          />
        )}
        detailTitle={(slo) => slo.displayName || resourceID(slo.name)}
      />

      <SLODialog
        open={dialogOpen}
        onClose={() => setDialogOpen(false)}
        service={activeService}
        initial={editing}
      />
    </Box>
  )
}
