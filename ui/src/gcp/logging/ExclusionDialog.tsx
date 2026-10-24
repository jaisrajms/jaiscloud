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
import { createExclusion, updateExclusion, type LogExclusion } from '../../api/gcp/logging'

export interface ExclusionDialogProps {
  open: boolean
  onClose: () => void
  initial?: LogExclusion
}

/** Create or edit a resource-level log exclusion. */
export function ExclusionDialog({ open, onClose, initial }: ExclusionDialogProps) {
  const queryClient = useQueryClient()
  const [name, setName] = useState('')
  const [filter, setFilter] = useState('')
  const [description, setDescription] = useState('')
  const [disabled, setDisabled] = useState(false)

  useEffect(() => {
    if (open) {
      setName(initial?.name ?? '')
      setFilter(initial?.filter ?? '')
      setDescription(initial?.description ?? '')
      setDisabled(initial?.disabled ?? false)
    }
  }, [open, initial])

  const save = useMutation({
    mutationFn: () => {
      const body = { name, filter, description, disabled }
      return initial ? updateExclusion(initial.name, body) : createExclusion(body)
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'logging', 'exclusions'] })
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{initial ? 'Edit exclusion' : 'Create exclusion'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not save the exclusion: {(save.error as Error).message}</Alert>
          )}
          <TextField
            autoFocus
            label="Exclusion name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={Boolean(initial)}
            placeholder="noisy-debug"
            fullWidth
          />
          <TextField
            label="Filter"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="severity=DEBUG"
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
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={!name || !filter || save.isPending}
          onClick={() => save.mutate()}
        >
          Save
        </Button>
      </DialogActions>
    </Dialog>
  )
}
