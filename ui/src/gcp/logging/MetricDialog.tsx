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
  MenuItem,
  Stack,
  Switch,
  TextField,
} from '@mui/material'
import { createMetric, updateMetric, type LogMetric } from '../../api/gcp/logging'

export interface MetricDialogProps {
  open: boolean
  onClose: () => void
  initial?: LogMetric
}

/** Create or edit a logs-based metric. */
export function MetricDialog({ open, onClose, initial }: MetricDialogProps) {
  const queryClient = useQueryClient()
  const [name, setName] = useState('')
  const [filter, setFilter] = useState('')
  const [description, setDescription] = useState('')
  const [valueType, setValueType] = useState('INT64')
  const [metricKind, setMetricKind] = useState('DELTA')
  const [disabled, setDisabled] = useState(false)

  useEffect(() => {
    if (open) {
      setName(initial?.name ?? '')
      setFilter(initial?.filter ?? '')
      setDescription(initial?.description ?? '')
      setValueType(initial?.metricDescriptor?.valueType ?? 'INT64')
      setMetricKind(initial?.metricDescriptor?.metricKind ?? 'DELTA')
      setDisabled(initial?.disabled ?? false)
    }
  }, [open, initial])

  const save = useMutation({
    mutationFn: () => {
      const body = {
        name,
        filter,
        description,
        disabled,
        metricDescriptor: { metricKind, valueType },
      }
      return initial ? updateMetric(initial.name, body) : createMetric(body)
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'logging', 'metrics'] })
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{initial ? 'Edit metric' : 'Create logs-based metric'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not save the metric: {(save.error as Error).message}</Alert>
          )}
          <TextField
            autoFocus
            label="Metric ID"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={Boolean(initial)}
            placeholder="error_count"
            fullWidth
          />
          <TextField
            label="Filter"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="severity=ERROR"
            multiline
            minRows={2}
            fullWidth
          />
          <TextField
            label="Description"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            fullWidth
          />
          <Stack direction="row" spacing={2}>
            <TextField
              select
              label="Metric kind"
              value={metricKind}
              onChange={(e) => setMetricKind(e.target.value)}
              fullWidth
            >
              {['DELTA', 'GAUGE', 'CUMULATIVE'].map((k) => (
                <MenuItem key={k} value={k}>
                  {k}
                </MenuItem>
              ))}
            </TextField>
            <TextField
              select
              label="Value type"
              value={valueType}
              onChange={(e) => setValueType(e.target.value)}
              fullWidth
            >
              {['INT64', 'DOUBLE', 'DISTRIBUTION', 'BOOL', 'STRING'].map((v) => (
                <MenuItem key={v} value={v}>
                  {v}
                </MenuItem>
              ))}
            </TextField>
          </Stack>
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
