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
} from '@mui/material'
import { createSink, updateSink, type LogSink } from '../../api/gcp/logging'

export interface SinkDialogProps {
  open: boolean
  onClose: () => void
  initial?: LogSink
}

/** Create or edit a log router sink. */
export function SinkDialog({ open, onClose, initial }: SinkDialogProps) {
  const queryClient = useQueryClient()
  const [name, setName] = useState('')
  const [destination, setDestination] = useState('')
  const [filter, setFilter] = useState('')
  const [description, setDescription] = useState('')
  const [disabled, setDisabled] = useState(false)
  const [includeChildren, setIncludeChildren] = useState(false)

  useEffect(() => {
    if (open) {
      setName(initial?.name ?? '')
      setDestination(initial?.destination ?? '')
      setFilter(initial?.filter ?? '')
      setDescription(initial?.description ?? '')
      setDisabled(initial?.disabled ?? false)
      setIncludeChildren(initial?.includeChildren ?? false)
    }
  }, [open, initial])

  const save = useMutation({
    mutationFn: () => {
      const body = { name, destination, filter, description, disabled, includeChildren }
      return initial ? updateSink(initial.name, body) : createSink(body)
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'logging', 'sinks'] })
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{initial ? 'Edit sink' : 'Create sink'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not save the sink: {(save.error as Error).message}</Alert>
          )}
          <TextField
            autoFocus
            label="Sink name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={Boolean(initial)}
            placeholder="audit-export"
            fullWidth
          />
          <TextField
            label="Destination"
            value={destination}
            onChange={(e) => setDestination(e.target.value)}
            placeholder="storage.googleapis.com/my-bucket"
            fullWidth
          />
          <TextField
            label="Filter (optional)"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            multiline
            minRows={2}
            fullWidth
          />
          <TextField
            label="Description (optional)"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            fullWidth
          />
          <FormControlLabel
            control={<Switch checked={disabled} onChange={(e) => setDisabled(e.target.checked)} />}
            label="Disabled"
          />
          <FormControlLabel
            control={
              <Switch checked={includeChildren} onChange={(e) => setIncludeChildren(e.target.checked)} />
            }
            label="Include children"
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={!name || !destination || save.isPending}
          onClick={() => save.mutate()}
        >
          Save
        </Button>
      </DialogActions>
    </Dialog>
  )
}
