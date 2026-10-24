import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, Chip, IconButton, Link, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { deleteDocument, listDocuments, type FirestoreDocument } from '../../api/gcp/firestore'
import { useAccount } from '../../context/AccountContext'
import { CreateDocumentDialog } from './CreateDocumentDialog'
import { FirestoreTabs } from './FirestoreTabs'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'
import {
  collectionId,
  decodeFirestorePath,
  encodeFirestoreId,
  encodeFirestorePath,
  isNestedCollection,
  parentDocument,
} from './util'

function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** Documents in a Firestore collection. */
export function DocumentsPage() {
  const { collection: collectionParam = '' } = useParams()
  const collection = decodeFirestorePath(collectionParam)
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const documents = useQuery({
    queryKey: ['gcp', 'firestore', 'documents', collection, accountId],
    queryFn: () => listDocuments(collection),
    enabled: Boolean(collection),
  })

  const remove = useMutation({
    mutationFn: (document: string) => deleteDocument(collection, document),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'firestore'] }),
  })

  const rows = filterRows(documents.data?.documents ?? [], filter, (document) =>
    `${document.id} ${document.updateTime ?? ''}`,
  )

  // A nested collection (e.g. `users/alice/orders`) backs onto its parent
  // document; a root collection backs onto the collections list.
  const parent = parentDocument(collection)
  const backTo = parent
    ? `/gcp/firestore/collections/${encodeFirestorePath(parent.collection)}/documents/${encodeFirestoreId(parent.document)}`
    : '/gcp/firestore/collections'
  const backAriaLabel = parent ? `Back to document ${parent.document}` : 'Back to collections'
  const subtitle = isNestedCollection(collection)
    ? `Firestore subcollection · ${collection} · project ${accountId || '—'}`
    : `Firestore collection · project ${accountId || '—'}`

  const columns: GcpColumn<FirestoreDocument>[] = [
    {
      key: 'id',
      header: 'Document ID',
      sortable: true,
      sortValue: (document) => document.id,
      render: (document) => (
        <Stack direction="row" spacing={0.75} sx={{ alignItems: 'center' }}>
          <Link
            component={RouterLink}
            to={`/gcp/firestore/collections/${encodeFirestorePath(collection)}/documents/${encodeFirestoreId(document.id)}`}
          >
            {document.id}
          </Link>
          {document.missing && (
            <Tooltip title="No document exists here; it only has subcollections below it.">
              <Chip label="missing" size="small" variant="outlined" />
            </Tooltip>
          )}
        </Stack>
      ),
    },
    {
      key: 'updated',
      header: 'Updated',
      sortable: true,
      sortValue: (document) => document.updateTime ?? '',
      render: (document) => shortDate(document.updateTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (document) => (
        <Tooltip title="Delete document">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(document.id)}
              aria-label={`Delete ${document.id}`}
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
        title={collectionId(collection)}
        subtitle={subtitle}
        backTo={backTo}
        backAriaLabel={backAriaLabel}
        actions={
          <Stack direction="row" spacing={1}>
            <Button
              component={RouterLink}
              to={`/gcp/firestore/query?collection=${encodeURIComponent(collection)}`}
            >
              Query this collection
            </Button>
            <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
              Create document
            </Button>
          </Stack>
        }
      >
        <FirestoreTabs />
      </GcpPageHeader>

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter documents"
        onRefresh={() => void documents.refetch()}
        refreshing={documents.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Documents"
        columns={columns}
        rows={rows}
        getRowKey={(document) => document.id}
        loading={documents.isLoading}
        error={documents.isError ? 'Failed to load documents.' : null}
        emptyMessage={filter ? 'No documents match the filter.' : 'No documents in this collection.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(document) => <GcpRowDetail row={document} />}
        detailTitle={(document) => document.id}
      />

      <CreateDocumentDialog
        open={createOpen}
        onClose={() => setCreateOpen(false)}
        collection={collection}
      />
    </Stack>
  )
}
