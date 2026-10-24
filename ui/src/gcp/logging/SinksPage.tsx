import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Button, Chip, IconButton, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import { deleteSink, listSinks, type LogSink } from '../../api/gcp/logging'
import { useAccount } from '../../context/AccountContext'
import { SinkDialog } from './SinkDialog'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

const ellipsisSx = {
  maxWidth: 320,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
} as const

/** Log router sinks: list, create, edit and delete. */
export function SinksPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<LogSink | undefined>(undefined)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const sinks = useQuery({
    queryKey: ['gcp', 'logging', 'sinks', accountId],
    queryFn: () => listSinks(),
  })

  const remove = useMutation({
    mutationFn: (name: string) => deleteSink(name),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'logging', 'sinks'] }),
  })

  const rows = filterRows(sinks.data?.sinks ?? [], filter, (sink) => sink.name)

  const columns: GcpColumn<LogSink>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (sink) => sink.name,
      render: (sink) => sink.name,
    },
    {
      key: 'destination',
      header: 'Destination',
      sortable: true,
      sortValue: (sink) => sink.destination,
      render: (sink) => <Box sx={ellipsisSx}>{sink.destination}</Box>,
    },
    {
      key: 'filter',
      header: 'Filter',
      sortable: true,
      sortValue: (sink) => sink.filter ?? '',
      render: (sink) => <Box sx={ellipsisSx}>{sink.filter || '—'}</Box>,
    },
    {
      key: 'status',
      header: 'Status',
      sortable: true,
      sortValue: (sink) => (sink.disabled ? 'DISABLED' : 'ENABLED'),
      render: (sink) => (
        <Chip
          size="small"
          label={sink.disabled ? 'DISABLED' : 'ENABLED'}
          color={sink.disabled ? 'default' : 'success'}
        />
      ),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (sink) => (
        <>
          <Tooltip title="Edit sink">
            <IconButton
              size="small"
              onClick={() => {
                setEditing(sink)
                setDialogOpen(true)
              }}
            >
              <EditOutlinedIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete sink">
            <IconButton size="small" onClick={() => remove.mutate(sink.name)} disabled={remove.isPending}>
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
        title="Log router"
        subtitle={`Sinks route log entries to a destination · project ${accountId || '—'}`}
        actions={
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => {
              setEditing(undefined)
              setDialogOpen(true)
            }}
          >
            Create sink
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter sinks"
        onRefresh={() => void sinks.refetch()}
        refreshing={sinks.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Log router sinks"
        columns={columns}
        rows={rows}
        getRowKey={(sink) => sink.name}
        loading={sinks.isLoading}
        error={sinks.isError ? 'Failed to load sinks.' : null}
        emptyMessage={filter ? 'No sinks match the filter.' : 'No sinks in this project.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(sink) => <GcpRowDetail row={sink} />}
        detailTitle={(sink) => sink.name}
      />

      <SinkDialog open={dialogOpen} onClose={() => setDialogOpen(false)} initial={editing} />
    </Box>
  )
}
