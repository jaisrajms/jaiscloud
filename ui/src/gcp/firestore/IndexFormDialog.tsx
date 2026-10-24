import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  MenuItem,
  Stack,
  TextField,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import {
  createIndex,
  type CreateIndexRequest,
  type FirestoreQueryScope,
} from '../../api/gcp/firestore'
import { indexFieldFromRow, type IndexFieldMode, type IndexFieldRow } from './indexes'

const FIELD_MODES: { value: IndexFieldMode; label: string }[] = [
  { value: 'ASCENDING', label: 'Ascending' },
  { value: 'DESCENDING', label: 'Descending' },
  { value: 'CONTAINS', label: 'Array contains' },
]

const QUERY_SCOPES: { value: FirestoreQueryScope; label: string }[] = [
  { value: 'COLLECTION', label: 'Collection' },
  { value: 'COLLECTION_GROUP', label: 'Collection group' },
]

function newRow(): IndexFieldRow {
  return { fieldPath: '', mode: 'ASCENDING' }
}

export interface IndexFormDialogProps {
  open: boolean
  onClose: () => void
}

/** Create a Firestore composite index. Composite indexes require at least two
 * fields; the final `__name__` tiebreaker is added automatically by the
 * server-side core. */
export function IndexFormDialog({ open, onClose }: IndexFormDialogProps) {
  const queryClient = useQueryClient()
  const [collectionGroup, setCollectionGroup] = useState('')
  const [queryScope, setQueryScope] = useState<FirestoreQueryScope>('COLLECTION')
  const [rows, setRows] = useState<IndexFieldRow[]>([newRow(), newRow()])
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (open) {
      setCollectionGroup('')
      setQueryScope('COLLECTION')
      setRows([newRow(), newRow()])
      setError(null)
    }
  }, [open])

  const updateRow = (index: number, patch: Partial<IndexFieldRow>) => {
    setRows((prev) => prev.map((row, i) => (i === index ? { ...row, ...patch } : row)))
  }

  const create = useMutation({
    mutationFn: (body: CreateIndexRequest) => createIndex(body),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'firestore', 'indexes'] })
      onClose()
    },
  })

  const submit = () => {
    const group = collectionGroup.trim()
    if (!group) {
      setError('Collection group is required')
      return
    }
    if (group.includes('/')) {
      setError('Collection group must be a single segment')
      return
    }
    if (rows.length < 2) {
      setError('A composite index requires at least 2 fields')
      return
    }
    const fields = []
    for (const row of rows) {
      const field = indexFieldFromRow(row)
      if (!field) {
        setError('Each field needs a field path')
        return
      }
      fields.push(field)
    }
    setError(null)
    create.mutate({ collectionGroup: group, queryScope, fields })
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Create composite index</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {(error || create.isError) && (
            <Alert severity="error">
              {error ?? `Could not create the index: ${(create.error as Error).message}`}
            </Alert>
          )}
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              autoFocus
              label="Collection group"
              value={collectionGroup}
              onChange={(e) => setCollectionGroup(e.target.value)}
              placeholder="cities"
              helperText="The collection id the index applies to."
              fullWidth
            />
            <TextField
              select
              label="Query scope"
              value={queryScope}
              onChange={(e) => setQueryScope(e.target.value as FirestoreQueryScope)}
              fullWidth
            >
              {QUERY_SCOPES.map((option) => (
                <MenuItem key={option.value} value={option.value}>
                  {option.label}
                </MenuItem>
              ))}
            </TextField>
          </Stack>

          <Typography variant="subtitle2">Fields</Typography>
          {rows.map((row, index) => (
            <Stack key={index} direction="row" spacing={1} sx={{ alignItems: 'center' }}>
              <TextField
                label={`Field ${index + 1}`}
                value={row.fieldPath}
                onChange={(e) => updateRow(index, { fieldPath: e.target.value })}
                placeholder="name"
                fullWidth
              />
              <TextField
                select
                label="Mode"
                value={row.mode}
                onChange={(e) => updateRow(index, { mode: e.target.value as IndexFieldMode })}
                sx={{ minWidth: 170 }}
              >
                {FIELD_MODES.map((option) => (
                  <MenuItem key={option.value} value={option.value}>
                    {option.label}
                  </MenuItem>
                ))}
              </TextField>
              <IconButton
                aria-label={`Remove field ${index + 1}`}
                disabled={rows.length <= 2}
                onClick={() => setRows((prev) => prev.filter((_, i) => i !== index))}
              >
                <DeleteOutlineIcon fontSize="small" />
              </IconButton>
            </Stack>
          ))}
          <Button
            startIcon={<AddIcon />}
            onClick={() => setRows((prev) => [...prev, newRow()])}
            sx={{ alignSelf: 'flex-start' }}
          >
            Add field
          </Button>

          <Typography variant="caption" color="text.secondary">
            A composite index needs at least two fields. The engine adds the{' '}
            <code>__name__</code> tiebreaker automatically if you do not.
          </Typography>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={create.isPending} onClick={submit}>
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
