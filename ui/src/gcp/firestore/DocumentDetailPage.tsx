import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Divider,
  IconButton,
  Link,
  Stack,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import SaveOutlinedIcon from '@mui/icons-material/SaveOutlined'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import { APIError } from '../../api/client'
import {
  deleteDocument,
  getDocument,
  listSubcollections,
  updateDocument,
  type Collection,
} from '../../api/gcp/firestore'
import { useAccount } from '../../context/AccountContext'
import { JsonEditor } from './JsonEditor'
import { parseJsonObject, decodeFirestoreId, decodeFirestorePath, encodeFirestorePath } from './util'
import { CreateDocumentDialog } from './CreateDocumentDialog'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageTitle } from '../common/PageTitle'

function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** A single Firestore document: edit the fields as JSON and save or delete. */
export function DocumentDetailPage() {
  const { collection: collectionParam = '', document: documentParam = '' } = useParams()
  const collection = decodeFirestorePath(collectionParam)
  const documentId = decodeFirestoreId(documentParam)
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [text, setText] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)
  const [startCollectionOpen, setStartCollectionOpen] = useState(false)

  const query = useQuery({
    queryKey: ['gcp', 'firestore', 'document', collection, documentId, accountId],
    queryFn: () => getDocument(collection, documentId),
    enabled: Boolean(collection && documentId),
  })

  const subcollections = useQuery({
    queryKey: ['gcp', 'firestore', 'subcollections', collection, documentId, accountId],
    queryFn: () => listSubcollections(collection, documentId),
    // Load unconditionally: a missing parent document (404 on GET) still has
    // subcollections, and they are what let the user browse below it.
    enabled: Boolean(collection && documentId),
  })

  // Seed the editor whenever a fresh document snapshot arrives.
  useEffect(() => {
    if (query.data) {
      setText(JSON.stringify(query.data.fields ?? {}, null, 2))
      setError(null)
      setSaved(false)
    }
  }, [query.data])

  const save = useMutation({
    mutationFn: (fields: Record<string, unknown>) =>
      updateDocument(collection, documentId, { fields, updateTime: query.data?.updateTime }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'firestore'] })
      setSaved(true)
    },
  })

  const remove = useMutation({
    mutationFn: () => deleteDocument(collection, documentId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'firestore'] })
      navigate(`/gcp/firestore/collections/${encodeFirestorePath(collection)}`)
    },
  })

  const submit = () => {
    const parsed = parseJsonObject(text)
    setError(parsed.error ?? null)
    setSaved(false)
    if (!parsed.error && parsed.value) save.mutate(parsed.value)
  }

  const conflicted =
    save.error instanceof APIError &&
    (save.error.code === 'FailedPrecondition' || save.error.status === 409 || save.error.status === 412)

  // A missing document (Firestore showMissing) has subcollections but no fields
  // of its own: the GET returns 404, yet the Subcollections section still works.
  const documentMissing = query.error instanceof APIError && query.error.status === 404

  const subcollectionColumns: GcpColumn<Collection>[] = [
    {
      key: 'id',
      header: 'Subcollection',
      sortable: true,
      sortValue: (sub) => sub.id,
      render: (sub) => (
        <Link
          component={RouterLink}
          to={`/gcp/firestore/collections/${encodeFirestorePath(
            `${collection}/${documentId}/${sub.id}`,
          )}`}
        >
          {sub.id}
        </Link>
      ),
    },
  ]

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton
          component={RouterLink}
          to={`/gcp/firestore/collections/${encodeFirestorePath(collection)}`}
          aria-label="Back to documents"
        >
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="firestore">{documentId}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            {collection} · project {accountId || '—'}
          </Typography>
        </Box>
        <Button
          variant="contained"
          startIcon={<SaveOutlinedIcon />}
          disabled={save.isPending || query.isLoading || !query.data}
          onClick={submit}
        >
          Save
        </Button>
        <Button
          color="error"
          startIcon={<DeleteOutlineIcon />}
          disabled={remove.isPending || !query.data}
          onClick={() => remove.mutate()}
        >
          Delete
        </Button>
      </Stack>

      {query.isError &&
        (documentMissing ? (
          <Alert severity="info" sx={{ mb: 2 }}>
            This document is missing: it has subcollections below it but no fields of its own.
          </Alert>
        ) : (
          <Alert severity="error" sx={{ mb: 2 }}>
            Failed to load the document.
          </Alert>
        ))}
      {query.isLoading && <CircularProgress size={24} />}
      {conflicted && (
        <Alert
          severity="warning"
          sx={{ mb: 2 }}
          action={
            <Button color="inherit" size="small" onClick={() => void query.refetch()}>
              Reload
            </Button>
          }
        >
          This document changed since it was loaded. Reload before saving to avoid overwriting.
        </Alert>
      )}
      {save.isError && !conflicted && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Save failed: {(save.error as Error).message}
        </Alert>
      )}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}
      {saved && !save.isPending && (
        <Alert severity="success" sx={{ mb: 2 }}>
          Document saved.
        </Alert>
      )}

      {query.data && (
        <Box sx={{ maxWidth: 900 }}>
          <Stack spacing={0.5} sx={{ mb: 2 }}>
            <Typography variant="caption" color="text.secondary">
              Full resource name
            </Typography>
            <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
              {query.data.name}
            </Typography>
            <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
              Created {shortDate(query.data.createTime)} · updated {shortDate(query.data.updateTime)}
            </Typography>
            <Divider sx={{ mt: 1 }} />
          </Stack>
          <JsonEditor
            label="Fields (Firestore value encoding)"
            value={text}
            onChange={setText}
            error={error}
            minRows={16}
            disabled={save.isPending}
          />
          <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
            Saving replaces all fields with the encoded object above, guarded by the loaded
            updateTime.
          </Typography>
        </Box>
      )}

      <Box sx={{ mt: 4 }}>
        <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 1 }}>
          <Typography variant="h6" sx={{ flexGrow: 1 }}>
            Subcollections
          </Typography>
            <Button startIcon={<AddIcon />} onClick={() => setStartCollectionOpen(true)}>
              Start collection
            </Button>
          </Stack>
        <GcpDataTable
          aria-label="Subcollections"
          columns={subcollectionColumns}
          rows={subcollections.data?.collections ?? []}
          getRowKey={(sub) => sub.id}
          loading={subcollections.isLoading}
          error={subcollections.isError ? 'Failed to load subcollections.' : null}
          emptyMessage="No subcollections in this document."
        />
      </Box>

      <CreateDocumentDialog
        open={startCollectionOpen}
        onClose={() => setStartCollectionOpen(false)}
        collectionPrefix={`${collection}/${documentId}`}
        onCreated={(createdCollection) => {
          // The dialog already invalidates the firestore queries on success.
          navigate(`/gcp/firestore/collections/${encodeFirestorePath(createdCollection)}`)
        }}
      />
    </Box>
  )
}
