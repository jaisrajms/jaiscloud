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
import {
  upsertEntity,
  type DatastoreValue,
  type KeyElement,
  type KeyRef,
} from '../../api/gcp/datastore'
import { GcpCodeEditor } from '../common/GcpCodeEditor'

export interface CreateEntityDialogProps {
  open: boolean
  kind: string
  onClose: () => void
  onCreated: (key: KeyRef) => void
}

/** Build the final key element from a typed id-or-name: an all-digit value is a
 * numeric id, anything else a name, and an empty value leaves the element
 * incomplete so the server allocates a numeric ID. */
function finalElement(kind: string, idOrName: string): KeyElement {
  const trimmed = idOrName.trim()
  if (trimmed === '') return { kind }
  if (/^-?\d+$/.test(trimmed)) return { kind, id: trimmed }
  return { kind, name: trimmed }
}

/** Create an entity of a kind. The key is optional (auto-allocated ID), and the
 * properties are edited as Datastore value JSON. */
export function CreateEntityDialog({ open, kind, onClose, onCreated }: CreateEntityDialogProps) {
  const queryClient = useQueryClient()
  const [idOrName, setIdOrName] = useState('')
  const [text, setText] = useState('{}')
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (open) {
      setIdOrName('')
      setText('{}')
      setError(null)
    }
  }, [open])

  const create = useMutation({
    mutationFn: (properties: Record<string, DatastoreValue>) =>
      upsertEntity({ key: { path: [finalElement(kind, idOrName)] }, properties }),
    onSuccess: (entity) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'datastore'] })
      onCreated(entity.key)
    },
  })

  const submit = () => {
    let parsed: unknown
    try {
      parsed = JSON.parse(text)
    } catch {
      setError('Properties must be valid JSON.')
      return
    }
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      setError('Properties must be a JSON object of Datastore values.')
      return
    }
    setError(null)
    create.mutate(parsed as Record<string, DatastoreValue>)
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Create entity · {kind}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          <TextField
            size="small"
            label="ID or name (optional)"
            value={idOrName}
            onChange={(event) => setIdOrName(event.target.value)}
            helperText="A numeric value becomes a numeric key ID; otherwise a name key. Leave blank for a server-allocated ID."
            fullWidth
          />
          <GcpCodeEditor
            label="Properties (Datastore value encoding)"
            value={text}
            onChange={setText}
            language="json"
            minRows={12}
            error={error}
          />
          <Typography variant="caption" color="text.secondary">
            Values use the Datastore encoding, e.g. {'{"name": {"stringValue": "Ada"}}'}.
          </Typography>
          {create.isError && <Alert severity="error">Create failed: {(create.error as Error).message}</Alert>}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={create.isPending}>
          Cancel
        </Button>
        <Button variant="contained" onClick={submit} disabled={create.isPending}>
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
