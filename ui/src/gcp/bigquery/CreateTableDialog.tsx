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
import { createTable } from '../../api/gcp/bigquery'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { parseJsonObject } from './util'

export interface CreateTableDialogProps {
  open: boolean
  onClose: () => void
  dataset: string
  onCreated?: (tableId: string) => void
}

const EMPTY_SCHEMA = `{
  "fields": [
    { "name": "id", "type": "STRING", "mode": "REQUIRED" }
  ]
}`

/** Create a BigQuery table in a dataset. */
export function CreateTableDialog({ open, onClose, dataset, onCreated }: CreateTableDialogProps) {
  const queryClient = useQueryClient()
  const [tableId, setTableId] = useState('')
  const [schemaText, setSchemaText] = useState(EMPTY_SCHEMA)
  const [friendlyName, setFriendlyName] = useState('')
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (open) {
      setTableId('')
      setSchemaText(EMPTY_SCHEMA)
      setFriendlyName('')
      setError(null)
    }
  }, [open])

  const create = useMutation({
    mutationFn: (schema: Record<string, unknown> | undefined) =>
      createTable(dataset, {
        tableId,
        schema,
        friendlyName: friendlyName || undefined,
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'bigquery'] })
      onCreated?.(tableId)
      onClose()
    },
  })

  const submit = () => {
    const parsed = parseJsonObject(schemaText)
    setError(parsed.error ?? null)
    if (parsed.error) return
    create.mutate(parsed.value)
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Create table</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {create.isError && (
            <Alert severity="error">
              Could not create the table: {(create.error as Error).message}
            </Alert>
          )}
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              autoFocus
              label="Table ID"
              value={tableId}
              onChange={(e) => setTableId(e.target.value)}
              placeholder="events"
              fullWidth
            />
            <TextField
              label="Friendly name (optional)"
              value={friendlyName}
              onChange={(e) => setFriendlyName(e.target.value)}
              fullWidth
            />
          </Stack>
          <Typography variant="caption" color="text.secondary">
            Schema is the BigQuery TableSchema object whose <code>fields</code> array lists columns
            (<code>name</code>, <code>type</code>, <code>mode</code>).
          </Typography>
          <GcpCodeEditor
            label="Schema"
            value={schemaText}
            onChange={setSchemaText}
            error={error}
            language="json"
            minRows={10}
            ariaLabel="Schema"
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={!tableId || create.isPending} onClick={submit}>
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
