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
} from '@mui/material'
import {
  createQueue,
  updateQueue,
  type QueueInput,
  type TaskQueue,
} from '../../api/gcp/tasks'

export interface QueueDialogProps {
  open: boolean
  onClose: () => void
  /** When set, the dialog edits this queue; otherwise it creates a new one. */
  queue?: TaskQueue
  onSaved?: (queue: TaskQueue) => void
}

function emptyInput(): QueueInput {
  return {
    name: '',
    location: 'us-central1',
    maxDispatchesPerSecond: 500,
    maxBurstSize: 100,
    maxConcurrentDispatches: 1000,
    maxAttempts: 100,
    maxRetryDuration: '',
    minBackoff: '100ms',
    maxBackoff: '1h',
    maxDoublings: 16,
  }
}

function inputFromQueue(queue: TaskQueue): QueueInput {
  return {
    name: queue.name,
    location: queue.location,
    maxDispatchesPerSecond: queue.maxDispatchesPerSecond ?? 0,
    maxBurstSize: queue.maxBurstSize ?? 0,
    maxConcurrentDispatches: queue.maxConcurrentDispatches ?? 0,
    maxAttempts: queue.maxAttempts ?? 0,
    maxRetryDuration: queue.maxRetryDuration ?? '',
    minBackoff: queue.minBackoff ?? '',
    maxBackoff: queue.maxBackoff ?? '',
    maxDoublings: queue.maxDoublings ?? 0,
  }
}

/** Create or edit a Cloud Tasks queue. */
export function QueueDialog({ open, onClose, queue, onSaved }: QueueDialogProps) {
  const queryClient = useQueryClient()
  const editing = Boolean(queue)
  const [input, setInput] = useState<QueueInput>(emptyInput())

  useEffect(() => {
    if (open) {
      setInput(queue ? inputFromQueue(queue) : emptyInput())
    }
  }, [open, queue])

  const set = <K extends keyof QueueInput>(key: K, value: QueueInput[K]) =>
    setInput((cur) => ({ ...cur, [key]: value }))

  const save = useMutation({
    mutationFn: () =>
      editing && queue ? updateQueue(queue.location, queue.name, input) : createQueue(input),
    onSuccess: (saved) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'tasks'] })
      onSaved?.(saved)
      onClose()
    },
  })

  const canSave = Boolean(input.name && input.location) && !save.isPending

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{editing ? `Edit ${queue?.name}` : 'Create queue'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not save the queue: {(save.error as Error).message}</Alert>
          )}
          <TextField
            autoFocus={!editing}
            label="Name"
            value={input.name}
            disabled={editing}
            onChange={(e) => set('name', e.target.value)}
            placeholder="my-queue"
            fullWidth
          />
          <TextField
            label="Location"
            value={input.location}
            disabled={editing}
            onChange={(e) => set('location', e.target.value)}
            placeholder="us-central1"
            fullWidth
          />
          <TextField
            label="Max dispatches per second"
            type="number"
            value={input.maxDispatchesPerSecond}
            onChange={(e) => set('maxDispatchesPerSecond', Number(e.target.value))}
            slotProps={{ htmlInput: { min: 0, max: 500 } }}
            fullWidth
          />
          <TextField
            label="Max burst size"
            type="number"
            value={input.maxBurstSize}
            onChange={(e) => set('maxBurstSize', Number(e.target.value))}
            slotProps={{ htmlInput: { min: 0 } }}
            fullWidth
          />
          <TextField
            label="Max concurrent dispatches"
            type="number"
            value={input.maxConcurrentDispatches}
            onChange={(e) => set('maxConcurrentDispatches', Number(e.target.value))}
            slotProps={{ htmlInput: { min: 0 } }}
            fullWidth
          />
          <TextField
            label="Max attempts"
            type="number"
            value={input.maxAttempts}
            onChange={(e) => set('maxAttempts', Number(e.target.value))}
            slotProps={{ htmlInput: { min: -1 } }}
            fullWidth
          />
          <TextField
            label="Min backoff"
            value={input.minBackoff}
            onChange={(e) => set('minBackoff', e.target.value)}
            placeholder="100ms"
            fullWidth
          />
          <TextField
            label="Max backoff"
            value={input.maxBackoff}
            onChange={(e) => set('maxBackoff', e.target.value)}
            placeholder="1h"
            fullWidth
          />
          <TextField
            label="Max retry duration (optional)"
            value={input.maxRetryDuration}
            onChange={(e) => set('maxRetryDuration', e.target.value)}
            placeholder="0s (unbounded)"
            fullWidth
          />
          <TextField
            label="Max doublings"
            type="number"
            value={input.maxDoublings}
            onChange={(e) => set('maxDoublings', Number(e.target.value))}
            slotProps={{ htmlInput: { min: 0 } }}
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={!canSave} onClick={() => save.mutate()}>
          {editing ? 'Save' : 'Create'}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
