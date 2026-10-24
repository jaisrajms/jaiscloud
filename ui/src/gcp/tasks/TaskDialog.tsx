import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  MenuItem,
  Stack,
  TextField,
} from '@mui/material'
import { createTask, type TaskInput } from '../../api/gcp/tasks'
import { GcpCodeEditor } from '../common/GcpCodeEditor'

export interface TaskDialogProps {
  open: boolean
  onClose: () => void
  location: string
  queue: string
  onSaved?: () => void
}

const TARGETS = [
  { value: 'http', label: 'HTTP' },
  { value: 'appengine', label: 'App Engine HTTP' },
]

const METHODS = ['POST', 'GET', 'HEAD', 'PUT', 'DELETE', 'PATCH', 'OPTIONS']
const APPENGINE_METHODS = ['POST', 'GET', 'HEAD', 'PUT', 'DELETE']

function emptyInput(): TaskInput {
  return {
    name: '',
    target: 'http',
    httpUrl: '',
    httpMethod: 'POST',
    httpBody: '',
    appEngineUri: '',
    appEngineMethod: 'POST',
    scheduleTime: '',
    dispatchDeadline: '10m',
  }
}

/** Create a task on a Cloud Tasks queue. */
export function TaskDialog({ open, onClose, location, queue, onSaved }: TaskDialogProps) {
  const queryClient = useQueryClient()
  const [input, setInput] = useState<TaskInput>(emptyInput())

  useEffect(() => {
    if (open) {
      setInput(emptyInput())
    }
  }, [open])

  const set = <K extends keyof TaskInput>(key: K, value: TaskInput[K]) =>
    setInput((cur) => ({ ...cur, [key]: value }))

  const save = useMutation({
    mutationFn: () => createTask(location, queue, input),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'tasks'] })
      onSaved?.()
      onClose()
    },
  })

  const canSave =
    (input.target === 'http' ? Boolean(input.httpUrl) : true) && !save.isPending

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Create task</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not create the task: {(save.error as Error).message}</Alert>
          )}
          <TextField
            autoFocus
            label="Name (optional)"
            value={input.name}
            onChange={(e) => set('name', e.target.value)}
            placeholder="auto-generated"
            fullWidth
          />
          <TextField
            select
            label="Target"
            value={input.target}
            onChange={(e) => set('target', e.target.value)}
            fullWidth
          >
            {TARGETS.map((t) => (
              <MenuItem key={t.value} value={t.value}>
                {t.label}
              </MenuItem>
            ))}
          </TextField>

          {input.target === 'http' && (
            <>
              <TextField
                label="URL"
                value={input.httpUrl}
                onChange={(e) => set('httpUrl', e.target.value)}
                placeholder="https://example.com/hook"
                fullWidth
              />
              <TextField
                select
                label="HTTP method"
                value={input.httpMethod}
                onChange={(e) => set('httpMethod', e.target.value)}
                fullWidth
              >
                {METHODS.map((m) => (
                  <MenuItem key={m} value={m}>
                    {m}
                  </MenuItem>
                ))}
              </TextField>
              <GcpCodeEditor
                label="Body (optional)"
                value={input.httpBody}
                onChange={(value) => set('httpBody', value)}
                language="json"
                minRows={2}
              />
            </>
          )}

          {input.target === 'appengine' && (
            <>
              <TextField
                label="Relative URI"
                value={input.appEngineUri}
                onChange={(e) => set('appEngineUri', e.target.value)}
                placeholder="/hook"
                fullWidth
              />
              <TextField
                select
                label="HTTP method"
                value={input.appEngineMethod}
                onChange={(e) => set('appEngineMethod', e.target.value)}
                fullWidth
              >
                {APPENGINE_METHODS.map((m) => (
                  <MenuItem key={m} value={m}>
                    {m}
                  </MenuItem>
                ))}
              </TextField>
            </>
          )}

          <TextField
            label="Schedule time (RFC3339, optional)"
            value={input.scheduleTime}
            onChange={(e) => set('scheduleTime', e.target.value)}
            placeholder="2026-01-01T00:00:00Z"
            fullWidth
          />
          <TextField
            label="Dispatch deadline"
            value={input.dispatchDeadline}
            onChange={(e) => set('dispatchDeadline', e.target.value)}
            placeholder="10m"
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={!canSave} onClick={() => save.mutate()}>
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
