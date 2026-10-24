import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  Stack,
  Switch,
  TextField,
  Typography,
} from '@mui/material'
import {
  insertRows,
  type InsertErrorsEntry,
  type InsertRow,
  type InsertRowsRequest,
} from '../../api/gcp/bigquery'
import { useGcpSnackbar } from '../common/SnackbarProvider'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { parseInsertRows, rowTemplate, type SchemaField } from './util'

export interface InsertRowsDialogProps {
  open: boolean
  onClose: () => void
  dataset: string
  table: string
  fields: SchemaField[]
}

/** Stream rows into a table via tabledata.insertAll. */
export function InsertRowsDialog({ open, onClose, dataset, table, fields }: InsertRowsDialogProps) {
  const queryClient = useQueryClient()
  const { notify } = useGcpSnackbar()
  const [text, setText] = useState('')
  const [insertId, setInsertId] = useState('')
  const [skipInvalidRows, setSkipInvalidRows] = useState(false)
  const [ignoreUnknownValues, setIgnoreUnknownValues] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [insertErrors, setInsertErrors] = useState<InsertErrorsEntry[]>([])

  useEffect(() => {
    if (open) {
      setText(JSON.stringify(rowTemplate(fields), null, 2))
      setInsertId('')
      setSkipInvalidRows(false)
      setIgnoreUnknownValues(false)
      setError(null)
      setInsertErrors([])
    }
    // Reset only when the dialog opens; fields are stable for a table.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  const insert = useMutation({
    mutationFn: (body: InsertRowsRequest) => insertRows(dataset, table, body),
    onSuccess: (response) => {
      // Some rows may have been inserted even when others were rejected, so
      // refresh the preview before reporting row-level errors.
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'bigquery', 'rows', dataset, table] })
      const errors = response.insertErrors ?? []
      if (errors.length > 0) {
        setInsertErrors(errors)
        notify('Some rows were rejected.', { severity: 'warning' })
        return
      }
      notify('Rows inserted.')
      onClose()
    },
  })

  const submit = () => {
    const parsed = parseInsertRows(text)
    setError(parsed.error ?? null)
    setInsertErrors([])
    if (parsed.error || !parsed.rows) return
    const rows: InsertRow[] = parsed.rows
    const only = rows.length === 1 ? rows[0] : undefined
    if (insertId && only) {
      rows[0] = { ...only, insertId }
    }
    insert.mutate({ rows, skipInvalidRows, ignoreUnknownValues })
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Insert rows</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {(insert.isError || error) && (
            <Alert severity="error">
              {error ?? `Could not insert rows: ${(insert.error as Error).message}`}
            </Alert>
          )}
          {insertErrors.length > 0 && (
            <Alert severity="warning">
              <Typography variant="subtitle2">Rejected rows</Typography>
              {insertErrors.map((entry) => (
                <Typography key={entry.index} variant="body2">
                  Row {entry.index + 1}:{' '}
                  {entry.errors
                    .map((e) => [e.location, e.message || e.reason].filter(Boolean).join(': '))
                    .join('; ')}
                </Typography>
              ))}
            </Alert>
          )}
          <Typography variant="caption" color="text.secondary">
            Enter one row object or a JSON array of row objects for <code>{dataset}.{table}</code>.
            Each row is a single tabledata.insertAll row.
          </Typography>
          <GcpCodeEditor
            label="Rows"
            value={text}
            onChange={setText}
            language="json"
            minRows={12}
            ariaLabel="Rows"
          />
          <TextField
            label="Insert ID (optional)"
            value={insertId}
            onChange={(e) => setInsertId(e.target.value)}
            helperText="Best-effort duplicate suppression; applied only when a single row is provided."
            fullWidth
          />
          <Stack direction="row" spacing={3}>
            <FormControlLabel
              control={
                <Switch
                  checked={skipInvalidRows}
                  onChange={(e) => setSkipInvalidRows(e.target.checked)}
                />
              }
              label="Skip invalid rows"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={ignoreUnknownValues}
                  onChange={(e) => setIgnoreUnknownValues(e.target.checked)}
                />
              }
              label="Ignore unknown values"
            />
          </Stack>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
        <Button variant="contained" disabled={insert.isPending} onClick={submit}>
          Insert
        </Button>
      </DialogActions>
    </Dialog>
  )
}
