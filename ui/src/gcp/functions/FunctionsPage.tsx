import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Button, Chip, IconButton, Link, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { deleteFunction, listFunctions, type GcpFunction } from '../../api/gcp/functions'
import { useAccount } from '../../context/AccountContext'
import { FunctionDialog } from './FunctionDialog'
import { functionStateColor, memoryLabel, shortDate, triggerSummary } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** Cloud Functions across every location, with create and delete. */
export function FunctionsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<string[]>([])
  const [filter, setFilter] = useState('')

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'functions'] })

  const functions = useQuery({
    queryKey: ['gcp', 'functions', 'list', accountId],
    queryFn: listFunctions,
  })

  const remove = useMutation({
    mutationFn: (fn: GcpFunction) => deleteFunction(fn.location, fn.id),
    onSuccess: invalidate,
  })

  const rows = filterRows(functions.data?.functions ?? [], filter, (fn) =>
    `${fn.id} ${fn.location} ${fn.runtime ?? ''}`,
  )

  const columns: GcpColumn<GcpFunction>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (fn) => fn.id,
      render: (fn) => (
        <Link
          component={RouterLink}
          to={`/gcp/functions/${encodeURIComponent(fn.location)}/${encodeURIComponent(fn.id)}`}
        >
          {fn.id}
        </Link>
      ),
    },
    {
      key: 'location',
      header: 'Location',
      sortable: true,
      sortValue: (fn) => fn.location,
      render: (fn) => fn.location || '—',
    },
    {
      key: 'runtime',
      header: 'Runtime',
      sortable: true,
      sortValue: (fn) => fn.runtime ?? null,
      render: (fn) => fn.runtime || '—',
    },
    {
      key: 'trigger',
      header: 'Trigger',
      sortable: true,
      sortValue: (fn) => triggerSummary(fn),
      render: (fn) => (
        <Box sx={{ maxWidth: 320, overflowWrap: 'anywhere' }}>{triggerSummary(fn)}</Box>
      ),
    },
    {
      key: 'memory',
      header: 'Memory',
      sortable: true,
      sortValue: (fn) => fn.availableMemoryMB ?? null,
      render: (fn) => memoryLabel(fn.availableMemoryMB),
    },
    {
      key: 'status',
      header: 'Status',
      sortable: true,
      sortValue: (fn) => fn.status ?? null,
      render: (fn) => (
        <Chip size="small" label={fn.status || '—'} color={functionStateColor(fn.status)} />
      ),
    },
    {
      key: 'updated',
      header: 'Updated',
      sortable: true,
      sortValue: (fn) => fn.updateTime ?? null,
      render: (fn) => shortDate(fn.updateTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (fn) => (
        <Tooltip title="Delete function">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(fn)}
              aria-label={`Delete ${fn.id}`}
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
        id="functions"
        title="Cloud Functions"
        subtitle={`Functions · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create function
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter functions"
        onRefresh={() => void functions.refetch()}
        refreshing={functions.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(remove.error as Error).message}
        </Alert>
      )}

      <GcpDataTable
        aria-label="Cloud Functions"
        columns={columns}
        rows={rows}
        getRowKey={(fn) => `${fn.location}/${fn.id}`}
        loading={functions.isLoading}
        error={functions.isError ? 'Failed to load functions.' : null}
        emptyMessage={filter ? 'No functions match the filter.' : 'No functions in this project.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(fn) => <GcpRowDetail row={fn} />}
        detailTitle={(fn) => fn.id}
      />

      <FunctionDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Stack>
  )
}
