import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Button, Chip, IconButton, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import { deleteExclusion, listExclusions, type LogExclusion } from '../../api/gcp/logging'
import { useAccount } from '../../context/AccountContext'
import { ExclusionDialog } from './ExclusionDialog'
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

/** Resource-level log exclusions: list, create, edit and delete. */
export function ExclusionsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<LogExclusion | undefined>(undefined)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const exclusions = useQuery({
    queryKey: ['gcp', 'logging', 'exclusions', accountId],
    queryFn: () => listExclusions(),
  })

  const remove = useMutation({
    mutationFn: (name: string) => deleteExclusion(name),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'logging', 'exclusions'] }),
  })

  const rows = filterRows(exclusions.data?.exclusions ?? [], filter, (exclusion) => exclusion.name)

  const columns: GcpColumn<LogExclusion>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (exclusion) => exclusion.name,
      render: (exclusion) => exclusion.name,
    },
    {
      key: 'filter',
      header: 'Filter',
      sortable: true,
      sortValue: (exclusion) => exclusion.filter,
      render: (exclusion) => <Box sx={ellipsisSx}>{exclusion.filter}</Box>,
    },
    {
      key: 'description',
      header: 'Description',
      sortable: true,
      sortValue: (exclusion) => exclusion.description ?? '',
      render: (exclusion) => exclusion.description || '—',
    },
    {
      key: 'status',
      header: 'Status',
      sortable: true,
      sortValue: (exclusion) => (exclusion.disabled ? 'DISABLED' : 'ENABLED'),
      render: (exclusion) => (
        <Chip
          size="small"
          label={exclusion.disabled ? 'DISABLED' : 'ENABLED'}
          color={exclusion.disabled ? 'default' : 'success'}
        />
      ),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (exclusion) => (
        <>
          <Tooltip title="Edit exclusion">
            <IconButton
              size="small"
              onClick={() => {
                setEditing(exclusion)
                setDialogOpen(true)
              }}
            >
              <EditOutlinedIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete exclusion">
            <IconButton
              size="small"
              onClick={() => remove.mutate(exclusion.name)}
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
        id="logging"
        title="Exclusions"
        subtitle={`Excluded entries are not ingested · project ${accountId || '—'}`}
        actions={
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => {
              setEditing(undefined)
              setDialogOpen(true)
            }}
          >
            Create exclusion
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter exclusions"
        onRefresh={() => void exclusions.refetch()}
        refreshing={exclusions.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Log exclusions"
        columns={columns}
        rows={rows}
        getRowKey={(exclusion) => exclusion.name}
        loading={exclusions.isLoading}
        error={exclusions.isError ? 'Failed to load exclusions.' : null}
        emptyMessage={filter ? 'No exclusions match the filter.' : 'No exclusions in this project.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(exclusion) => <GcpRowDetail row={exclusion} />}
        detailTitle={(exclusion) => exclusion.name}
      />

      <ExclusionDialog open={dialogOpen} onClose={() => setDialogOpen(false)} initial={editing} />
    </Box>
  )
}
