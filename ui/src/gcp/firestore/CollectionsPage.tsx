import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Button, Link, Stack } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import { Link as RouterLink } from 'react-router-dom'
import { listCollections, type Collection } from '../../api/gcp/firestore'
import { useAccount } from '../../context/AccountContext'
import { CreateDocumentDialog } from './CreateDocumentDialog'
import { FirestoreTabs } from './FirestoreTabs'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'
import { encodeFirestorePath } from './util'

/** Firestore root collections. Clicking a collection browses its documents. */
export function CollectionsPage() {
  const { accountId } = useAccount()
  const [createOpen, setCreateOpen] = useState(false)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const collections = useQuery({
    queryKey: ['gcp', 'firestore', 'collections', accountId],
    queryFn: () => listCollections(),
  })

  const rows = filterRows(collections.data?.collections ?? [], filter, (collection) => collection.id)

  const columns: GcpColumn<Collection>[] = [
    {
      key: 'id',
      header: 'Collection ID',
      sortable: true,
      sortValue: (collection) => collection.id,
      render: (collection) => (
        <Link
          component={RouterLink}
          to={`/gcp/firestore/collections/${encodeFirestorePath(collection.id)}`}
        >
          {collection.id}
        </Link>
      ),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (collection) => (
        <Link
          component={RouterLink}
          to={`/gcp/firestore/query?collection=${encodeURIComponent(collection.id)}`}
          aria-label={`Query collection ${collection.id}`}
        >
          Query
        </Link>
      ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="firestore"
        title="Firestore"
        subtitle={`Collections · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create document
          </Button>
        }
      >
        <FirestoreTabs />
      </GcpPageHeader>

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter collections"
        onRefresh={() => void collections.refetch()}
        refreshing={collections.isFetching}
      />

      <GcpDataTable
        aria-label="Collections"
        columns={columns}
        rows={rows}
        getRowKey={(collection) => collection.id}
        loading={collections.isLoading}
        error={collections.isError ? 'Failed to load collections.' : null}
        emptyMessage={
          filter
            ? 'No collections match the filter.'
            : 'No collections in this project. Create a document to add one.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(collection) => <GcpRowDetail row={collection} />}
        detailTitle={(collection) => collection.id}
      />

      <CreateDocumentDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Stack>
  )
}
