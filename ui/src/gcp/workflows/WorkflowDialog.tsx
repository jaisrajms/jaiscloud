import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  TextField,
} from '@mui/material'
import {
  createWorkflow,
  updateWorkflow,
  type Workflow,
} from '../../api/gcp/workflows'
import { callLogLevelOptions, formatKV, parseKV } from './util'
import { GcpCodeEditor } from '../common/GcpCodeEditor'

export interface WorkflowDialogProps {
  open: boolean
  onClose: () => void
  /** When set, the dialog edits this workflow; otherwise it creates a new one. */
  workflow?: Workflow
  onSaved?: (workflow: Workflow) => void
}

interface FormState {
  id: string
  location: string
  description: string
  serviceAccount: string
  callLogLevel: string
  sourceContents: string
  labels: string
  userEnvVars: string
}

function emptyInput(): FormState {
  return {
    id: '',
    location: 'us-central1',
    description: '',
    serviceAccount: '',
    callLogLevel: 'LOG_ERRORS_ONLY',
    sourceContents: 'main:\n  steps:\n    - done:\n        return: "hello"\n',
    labels: '',
    userEnvVars: '',
  }
}

function inputFromWorkflow(workflow: Workflow): FormState {
  return {
    id: workflow.id,
    location: workflow.location,
    description: workflow.description ?? '',
    serviceAccount: workflow.serviceAccount ?? '',
    callLogLevel: workflow.callLogLevel ?? '',
    sourceContents: workflow.sourceContents ?? '',
    labels: formatKV(workflow.labels),
    userEnvVars: formatKV(workflow.userEnvVars),
  }
}

/** Create or edit a workflow. */
export function WorkflowDialog({ open, onClose, workflow, onSaved }: WorkflowDialogProps) {
  const queryClient = useQueryClient()
  const editing = Boolean(workflow)
  const [input, setInput] = useState<FormState>(emptyInput())

  useEffect(() => {
    if (open) {
      setInput(workflow ? inputFromWorkflow(workflow) : emptyInput())
    }
  }, [open, workflow])

  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
    setInput((cur) => ({ ...cur, [key]: value }))

  const save = useMutation({
    mutationFn: () =>
      editing && workflow
        ? updateWorkflow(workflow.location, workflow.id, {
            description: input.description,
            serviceAccount: input.serviceAccount,
            sourceContents: input.sourceContents,
            callLogLevel: input.callLogLevel,
            labels: parseKV(input.labels),
            userEnvVars: parseKV(input.userEnvVars),
          })
        : createWorkflow({
            id: input.id,
            location: input.location,
            description: input.description,
            serviceAccount: input.serviceAccount,
            sourceContents: input.sourceContents,
            callLogLevel: input.callLogLevel,
            labels: parseKV(input.labels),
            userEnvVars: parseKV(input.userEnvVars),
          }),
    onSuccess: (saved) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'workflows'] })
      onSaved?.(saved)
      onClose()
    },
  })

  const canSave = Boolean(input.id && input.location) && !save.isPending

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>{editing ? `Edit ${workflow?.id}` : 'Create workflow'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not save the workflow: {(save.error as Error).message}</Alert>
          )}
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              autoFocus={!editing}
              label="Workflow ID"
              value={input.id}
              disabled={editing}
              onChange={(e) => set('id', e.target.value)}
              placeholder="my-workflow"
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
          </Stack>
          <TextField
            label="Description"
            value={input.description}
            onChange={(e) => set('description', e.target.value)}
            fullWidth
          />
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              label="Service account"
              value={input.serviceAccount}
              onChange={(e) => set('serviceAccount', e.target.value)}
              placeholder="default compute service account"
              fullWidth
            />
            <FormControl fullWidth>
              <InputLabel id="workflow-call-log-level">Call log level</InputLabel>
              <Select
                labelId="workflow-call-log-level"
                label="Call log level"
                value={input.callLogLevel}
                onChange={(e) => set('callLogLevel', e.target.value)}
              >
                {callLogLevelOptions.map((option) => (
                  <MenuItem key={option.value || 'unspecified'} value={option.value}>
                    {option.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Stack>
          <GcpCodeEditor
            label="Source (YAML)"
            value={input.sourceContents}
            onChange={(value) => set('sourceContents', value)}
            language="yaml"
            minRows={8}
          />
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              label="Labels (key=value per line)"
              value={input.labels}
              onChange={(e) => set('labels', e.target.value)}
              multiline
              minRows={2}
              fullWidth
            />
            <TextField
              label="Environment variables (key=value per line)"
              value={input.userEnvVars}
              onChange={(e) => set('userEnvVars', e.target.value)}
              multiline
              minRows={2}
              fullWidth
            />
          </Stack>
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
