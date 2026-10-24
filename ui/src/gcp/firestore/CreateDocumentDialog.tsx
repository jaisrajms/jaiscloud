import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Stack,
  TextField,
  Typography,
} from '@mui/material'
import { createDocument } from '../../api/gcp/firestore'
import { JsonEditor } from './JsonEditor'
import { parseJsonObject, validateCollectionPath } from './util'

const EMPTY = '{\n  \n}'

export interface CreateDocumentDialogProps {
  open: boolean
  onClose: () => void
  /** When set, the collection is a fixed full collection path and cannot be
   * edited (may be nested, e.g. `users/alice/orders`). */
  collection?: string
  /** Parent document path used to create a new subcollection; the id typed in
   * the dialog is appended to it. Mutually exclusive with `collection`. */
  collectionPrefix?: string
  onCreated?: (collection: string, documentId: string) => void
}

/** Create a document. Firestore collections are implicit, so a collection id
 * supplied here is created on the first write. With `collectionPrefix` the
 * dialog creates a subcollection of a document. */
export function CreateDocumentDialog({
  open,
  onClose,
  collection,
  collectionPrefix,
  onCreated,
}: CreateDocumentDialogProps) {
  const queryClient = useQueryClient()
  const [collectionId, setCollectionId] = useState(collection ?? '')
  const [documentId, setDocumentId] = useState('')
  const [text, setText] = useState(EMPTY)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (open) {
      setCollectionId(collection ?? '')
      setDocumentId('')
      setText(EMPTY)
      setError(null)
    }
  }, [open, collection, collectionPrefix])

  // When creating a subcollection, the typed id is appended to the parent
  // document path; otherwise the field already holds the (fixed or root) path.
  const collectionPath =
    collection ?? (collectionPrefix ? `${collectionPrefix}/${collectionId}` : collectionId)
  const canCreate = Boolean(collection) || collectionId.length > 0

  const create = useMutation({
    mutationFn: () => {
      const parsed = parseJsonObject(text)
      if (parsed.error || !parsed.value) throw new Error(parsed.error ?? 'invalid document')
      return createDocument(collectionPath, {
        documentId: documentId || undefined,
        fields: parsed.value,
      })
    },
    onSuccess: (doc) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'firestore'] })
      onCreated?.(doc.collection, doc.id)
      onClose()
    },
  })

  const submit = () => {
    const parsed = parseJsonObject(text)
    setError(parsed.error ?? null)
    if (parsed.error) return
    if (!collection) {
      if (collectionPrefix) {
        // The typed id is a single subcollection segment appended to the
        // parent document path.
        if (!collectionId) {
          setError('Subcollection ID is required')
          return
        }
        if (collectionId.includes('/')) {
          setError('Subcollection ID must not contain "/"')
          return
        }
      } else {
        // A root create may target a nested path directly, e.g.
        // `users/alice/orders`.
        const pathError = validateCollectionPath(collectionId)
        if (pathError) {
          setError(pathError)
          return
        }
      }
    }
    create.mutate()
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Create document</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {create.isError && (
            <Alert severity="error">
              Could not create the document: {(create.error as Error).message}
            </Alert>
          )}
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              autoFocus
              label={collectionPrefix ? 'Subcollection ID' : 'Collection ID'}
              value={collectionId}
              onChange={(e) => setCollectionId(e.target.value)}
              disabled={Boolean(collection)}
              placeholder={collectionPrefix ? 'orders' : 'users or users/alice/orders'}
              helperText={
                !collection && !collectionPrefix
                  ? 'A nested path like users/alice/orders creates a subcollection document.'
                  : undefined
              }
              fullWidth
            />
            <TextField
              label="Document ID (optional)"
              value={documentId}
              onChange={(e) => setDocumentId(e.target.value)}
              placeholder="auto-generated"
              fullWidth
            />
          </Stack>
          <Typography variant="caption" color="text.secondary">
            Fields use Firestore&apos;s typed encoding, e.g. {'{'} <code>name</code>:{' '}
            {'{'} <code>stringValue</code>: &quot;Ada&quot; {'}'}, <code>age</code>:{' '}
            {'{'} <code>integerValue</code>: &quot;42&quot; {'}'} {'}'}. Values keep their exact
            type when saved.
          </Typography>
          <JsonEditor label="Fields" value={text} onChange={setText} error={error} />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={!canCreate || create.isPending} onClick={submit}>
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
