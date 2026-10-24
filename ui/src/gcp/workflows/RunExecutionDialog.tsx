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
import { runWorkflow, type Execution } from '../../api/gcp/workflows'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { callLogLevelOptions, parseKV } from './util'

export interface RunExecutionDialogProps {
  open: boolean
  onClose: () => void
  location: string
  workflow: string
  onRun?: (execution: Execution) => void
}

/** Trigger a workflow execution with a JSON argument. */
export function RunExecutionDialog({ open, onClose, location, workflow, onRun }: RunExecutionDialogProps) {
  const queryClient = useQueryClient()
  const [argument, setArgument] = useState('')
  const [callLogLevel, setCallLogLevel] = useState('')
  const [labels, setLabels] = useState('')

  useEffect(() => {
    if (open) {
      setArgument('')
      setCallLogLevel('')
      setLabels('')
    }
  }, [open])

  const run = useMutation({
    mutationFn: () =>
      runWorkflow(location, workflow, {
        argument,
        callLogLevel,
        labels: parseKV(labels),
      }),
    onSuccess: (execution) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'workflows'] })
      onRun?.(execution)
      onClose()
    },
  })

  const argumentInvalid = (() => {
    if (!argument.trim()) return false
    try {
      JSON.parse(argument)
      return false
    } catch {
      return true
    }
  })()

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Run {workflow}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {run.isError && (
            <Alert severity="error">Could not run the workflow: {(run.error as Error).message}</Alert>
          )}
          <GcpCodeEditor
            label="Argument (JSON, optional)"
            value={argument}
            onChange={setArgument}
            error={argumentInvalid ? 'Argument must be valid JSON' : null}
            language="json"
            minRows={3}
          />
          <FormControl fullWidth>
            <InputLabel id="execution-call-log-level">Call log level</InputLabel>
            <Select
              labelId="execution-call-log-level"
              label="Call log level"
              value={callLogLevel}
              onChange={(e) => setCallLogLevel(e.target.value)}
            >
              {callLogLevelOptions.map((option) => (
                <MenuItem key={option.value || 'unspecified'} value={option.value}>
                  {option.label}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <TextField
            label="Labels (key=value per line)"
            value={labels}
            onChange={(e) => setLabels(e.target.value)}
            multiline
            minRows={2}
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={run.isPending || argumentInvalid}
          onClick={() => run.mutate()}
        >
          Run
        </Button>
      </DialogActions>
    </Dialog>
  )
}
