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
import { batchTasks, type TaskInput } from '../../api/gcp/tasks'

export interface BatchTasksDialogProps {
  open: boolean
  onClose: () => void
  location: string
  queue: string
}

/**
 * Batch create/delete tasks. Creates are a JSON array of the create-task body;
 * deletes are task names, one per line. The console loops the typed core (the
 * REST LRO envelope is a provider concern).
 */
export function BatchTasksDialog({ open, onClose, location, queue }: BatchTasksDialogProps) {
  const queryClient = useQueryClient()
  const [createText, setCreateText] = useState('[]')
  const [deleteText, setDeleteText] = useState('')
  const [error, setError] = useState('')
  const [result, setResult] = useState('')

  useEffect(() => {
    if (!open) return
    setCreateText('[]')
    setDeleteText('')
    setError('')
    setResult('')
  }, [open])

  const run = useMutation({
    mutationFn: (body: { create?: TaskInput[]; delete?: string[] }) =>
      batchTasks(location, queue, body),
    onSuccess: (resp) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'tasks'] })
      setResult(
        `Created ${resp.created?.length ?? 0}, deleted ${resp.deleted?.length ?? 0} task(s).`,
      )
    },
  })

  const handleRun = () => {
    setError('')
    setResult('')
    let create: TaskInput[] = []
    try {
      const parsed = JSON.parse(createText) as unknown
      if (!Array.isArray(parsed)) throw new Error('Creates must be a JSON array.')
      create = parsed as TaskInput[]
    } catch (err) {
      setError((err as Error).message)
      return
    }
    const del = deleteText
      .split('\n')
      .map((s) => s.trim())
      .filter(Boolean)
    if (create.length === 0 && del.length === 0) {
      setError('Provide at least one create entry or delete name.')
      return
    }
    run.mutate({ create, delete: del })
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Batch tasks · {queue}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {run.isError && <Alert severity="error">{(run.error as Error).message}</Alert>}
          {error && <Alert severity="error">{error}</Alert>}
          {result && <Alert severity="success">{result}</Alert>}
          <TextField
            label="Create (JSON array of create-task bodies)"
            value={createText}
            onChange={(e) => setCreateText(e.target.value)}
            multiline
            minRows={6}
            slotProps={{ input: { sx: { fontFamily: 'monospace' } } }}
            fullWidth
          />
          <TextField
            label="Delete task names (one per line)"
            value={deleteText}
            onChange={(e) => setDeleteText(e.target.value)}
            multiline
            minRows={3}
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>{result ? 'Done' : 'Cancel'}</Button>
        <Button variant="contained" disabled={run.isPending || Boolean(result)} onClick={handleRun}>
          Run batch
        </Button>
      </DialogActions>
    </Dialog>
  )
}
