import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Button, Chip, IconButton, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import { deleteMetric, listMetrics, type LogMetric } from '../../api/gcp/logging'
import { useAccount } from '../../context/AccountContext'
import { MetricDialog } from './MetricDialog'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

const ellipsisSx = {
  maxWidth: 360,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
} as const

/** Logs-based metrics: list, create, edit and delete. */
export function MetricsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<LogMetric | undefined>(undefined)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const metrics = useQuery({
    queryKey: ['gcp', 'logging', 'metrics', accountId],
    queryFn: () => listMetrics(),
  })

  const remove = useMutation({
    mutationFn: (name: string) => deleteMetric(name),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'logging', 'metrics'] }),
  })

  const rows = filterRows(metrics.data?.metrics ?? [], filter, (metric) => metric.name)

  const columns: GcpColumn<LogMetric>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (metric) => metric.name,
      render: (metric) => metric.name,
    },
    {
      key: 'filter',
      header: 'Filter',
      sortable: true,
      sortValue: (metric) => metric.filter,
      render: (metric) => <Box sx={ellipsisSx}>{metric.filter}</Box>,
    },
    {
      key: 'kind',
      header: 'Kind',
      sortable: true,
      sortValue: (metric) => metric.metricDescriptor?.metricKind ?? '',
      render: (metric) => metric.metricDescriptor?.metricKind || '—',
    },
    {
      key: 'valueType',
      header: 'Value type',
      sortable: true,
      sortValue: (metric) => metric.metricDescriptor?.valueType ?? '',
      render: (metric) => metric.metricDescriptor?.valueType || '—',
    },
    {
      key: 'status',
      header: 'Status',
      sortable: true,
      sortValue: (metric) => (metric.disabled ? 'DISABLED' : 'ENABLED'),
      render: (metric) => (
        <Chip
          size="small"
          label={metric.disabled ? 'DISABLED' : 'ENABLED'}
          color={metric.disabled ? 'default' : 'success'}
        />
      ),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (metric) => (
        <>
          <Tooltip title="Edit metric">
            <IconButton
              size="small"
              onClick={() => {
                setEditing(metric)
                setDialogOpen(true)
              }}
            >
              <EditOutlinedIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete metric">
            <IconButton size="small" onClick={() => remove.mutate(metric.name)} disabled={remove.isPending}>
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
        id="logging"
        title="Logs-based metrics"
        subtitle={`Count log entries matching a filter · project ${accountId || '—'}`}
        actions={
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => {
              setEditing(undefined)
              setDialogOpen(true)
            }}
          >
            Create metric
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter metrics"
        onRefresh={() => void metrics.refetch()}
        refreshing={metrics.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Logs-based metrics"
        columns={columns}
        rows={rows}
        getRowKey={(metric) => metric.name}
        loading={metrics.isLoading}
        error={metrics.isError ? 'Failed to load metrics.' : null}
        emptyMessage={
          filter ? 'No logs-based metrics match the filter.' : 'No logs-based metrics in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(metric) => <GcpRowDetail row={metric} />}
        detailTitle={(metric) => metric.name}
      />

      <MetricDialog open={dialogOpen} onClose={() => setDialogOpen(false)} initial={editing} />
    </Box>
  )
}
