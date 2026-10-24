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
import {
  createJob,
  updateJob,
  type JobInput,
  type SchedulerJob,
} from '../../api/gcp/scheduler'
import { GcpCodeEditor } from '../common/GcpCodeEditor'

export interface JobDialogProps {
  open: boolean
  onClose: () => void
  /** When set, the dialog edits this job; otherwise it creates a new one. */
  job?: SchedulerJob
  onSaved?: (job: SchedulerJob) => void
}

const TARGETS = [
  { value: 'http', label: 'HTTP' },
  { value: 'pubsub', label: 'Pub/Sub' },
  { value: 'appengine', label: 'App Engine HTTP' },
]

function emptyInput(): JobInput {
  return {
    name: '',
    location: 'us-central1',
    schedule: '0 * * * *',
    timeZone: 'UTC',
    target: 'http',
    httpMethod: 'POST',
    retryCount: 0,
  }
}

function inputFromJob(job: SchedulerJob): JobInput {
  return {
    name: job.name,
    location: job.location,
    schedule: job.schedule ?? '',
    timeZone: job.timeZone ?? '',
    description: job.description,
    target: job.target || 'http',
    httpUri: job.httpUri,
    httpMethod: job.httpMethod,
    httpBody: job.httpBody,
    httpHeaders: job.httpHeaders,
    pubsubTopic: job.pubsubTopic,
    pubsubData: job.pubsubData,
    appEngineUri: job.appEngineUri,
    appEngineMethod: job.appEngineMethod,
    retryCount: job.retryCount ?? 0,
    attemptDeadline: job.attemptDeadline,
  }
}

/** Create or edit a Cloud Scheduler job. */
export function JobDialog({ open, onClose, job, onSaved }: JobDialogProps) {
  const queryClient = useQueryClient()
  const editing = Boolean(job)
  const [input, setInput] = useState<JobInput>(emptyInput())

  useEffect(() => {
    if (open) {
      setInput(job ? inputFromJob(job) : emptyInput())
    }
  }, [open, job])

  const set = <K extends keyof JobInput>(key: K, value: JobInput[K]) =>
    setInput((cur) => ({ ...cur, [key]: value }))

  const save = useMutation({
    mutationFn: () =>
      editing && job ? updateJob(job.location, job.name, input) : createJob(input),
    onSuccess: (saved) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'scheduler'] })
      onSaved?.(saved)
      onClose()
    },
  })

  const canSave = Boolean(input.name && input.location && input.schedule) && !save.isPending

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{editing ? `Edit ${job?.name}` : 'Create job'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not save the job: {(save.error as Error).message}</Alert>
          )}
          <TextField
            autoFocus={!editing}
            label="Name"
            value={input.name}
            disabled={editing}
            onChange={(e) => set('name', e.target.value)}
            placeholder="nightly-report"
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
            label="Schedule (cron)"
            value={input.schedule}
            onChange={(e) => set('schedule', e.target.value)}
            placeholder="0 * * * *"
            fullWidth
          />
          <TextField
            label="Time zone"
            value={input.timeZone}
            onChange={(e) => set('timeZone', e.target.value)}
            placeholder="UTC"
            fullWidth
          />
          <TextField
            label="Description (optional)"
            value={input.description ?? ''}
            onChange={(e) => set('description', e.target.value)}
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
                label="URI"
                value={input.httpUri ?? ''}
                onChange={(e) => set('httpUri', e.target.value)}
                placeholder="https://example.com/hook"
                fullWidth
              />
              <TextField
                label="HTTP method"
                value={input.httpMethod ?? ''}
                onChange={(e) => set('httpMethod', e.target.value)}
                placeholder="POST"
                fullWidth
              />
              <GcpCodeEditor
                label="Body (optional)"
                value={input.httpBody ?? ''}
                onChange={(value) => set('httpBody', value)}
                language="json"
                minRows={2}
              />
            </>
          )}

          {input.target === 'pubsub' && (
            <>
              <TextField
                label="Topic"
                value={input.pubsubTopic ?? ''}
                onChange={(e) => set('pubsubTopic', e.target.value)}
                placeholder="projects/p/topics/t"
                fullWidth
              />
              <TextField
                label="Message data (optional)"
                value={input.pubsubData ?? ''}
                onChange={(e) => set('pubsubData', e.target.value)}
                multiline
                minRows={2}
                fullWidth
              />
            </>
          )}

          {input.target === 'appengine' && (
            <>
              <TextField
                label="Relative URI"
                value={input.appEngineUri ?? ''}
                onChange={(e) => set('appEngineUri', e.target.value)}
                placeholder="/hook"
                fullWidth
              />
              <TextField
                label="HTTP method"
                value={input.appEngineMethod ?? ''}
                onChange={(e) => set('appEngineMethod', e.target.value)}
                placeholder="POST"
                fullWidth
              />
            </>
          )}

          <TextField
            label="Retry count"
            type="number"
            value={input.retryCount ?? 0}
            onChange={(e) => set('retryCount', Number(e.target.value))}
            slotProps={{ htmlInput: { min: 0, max: 5 } }}
            fullWidth
          />
          <TextField
            label="Attempt deadline (optional)"
            value={input.attemptDeadline ?? ''}
            onChange={(e) => set('attemptDeadline', e.target.value)}
            placeholder="60s"
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
