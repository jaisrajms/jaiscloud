import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, Chip, IconButton, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { deleteIndex, listIndexes, type FirestoreIndex } from '../../api/gcp/firestore'
import { useAccount } from '../../context/AccountContext'
import { FirestoreTabs } from './FirestoreTabs'
import { IndexFormDialog } from './IndexFormDialog'
import { formatIndexFields } from './indexes'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** scopeLabel renders the query scope in the console's wording. */
function scopeLabel(scope?: string): string {
  if (scope === 'COLLECTION_GROUP') return 'Collection group'
  if (scope === 'COLLECTION') return 'Collection'
  return scope || '—'
}

/** Firestore composite indexes across every collection group. */
export function IndexesPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const indexes = useQuery({
    queryKey: ['gcp', 'firestore', 'indexes', accountId],
    queryFn: () => listIndexes(),
  })

  const remove = useMutation({
    mutationFn: (index: FirestoreIndex) => deleteIndex(index.collectionGroup, index.id),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'firestore', 'indexes'] }),
  })

  const rows = filterRows(indexes.data?.indexes ?? [], filter, (index) =>
    `${index.collectionGroup} ${formatIndexFields(index.fields)} ${index.queryScope ?? ''} ${index.state ?? ''}`,
  )

  const columns: GcpColumn<FirestoreIndex>[] = [
    {
      key: 'collectionGroup',
      header: 'Collection group',
      sortable: true,
      sortValue: (index) => index.collectionGroup,
      render: (index) => index.collectionGroup,
    },
    {
      key: 'fields',
      header: 'Fields',
      render: (index) => formatIndexFields(index.fields),
    },
    {
      key: 'scope',
      header: 'Query scope',
      sortable: true,
      sortValue: (index) => index.queryScope ?? '',
      render: (index) => scopeLabel(index.queryScope),
    },
    {
      key: 'state',
      header: 'State',
      sortable: true,
      sortValue: (index) => index.state ?? '',
      render: (index) =>
        index.state ? <Chip label={index.state} size="small" variant="outlined" /> : '—',
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (index) => (
        <Tooltip title="Delete index">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(index)}
              aria-label={`Delete index on ${index.collectionGroup}`}
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
        id="firestore"
        title="Firestore"
        subtitle={`Indexes · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create index
          </Button>
        }
      >
        <FirestoreTabs />
      </GcpPageHeader>

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter indexes"
        onRefresh={() => void indexes.refetch()}
        refreshing={indexes.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Indexes"
        columns={columns}
        rows={rows}
        getRowKey={(index) => index.name}
        loading={indexes.isLoading}
        error={indexes.isError ? 'Failed to load indexes.' : null}
        emptyMessage={
          filter
            ? 'No indexes match the filter.'
            : 'No composite indexes in this project. Create one to support multi-field queries.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(index) => <GcpRowDetail row={index} />}
        detailTitle={(index) => index.collectionGroup}
      />

      <IndexFormDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Stack>
  )
}
