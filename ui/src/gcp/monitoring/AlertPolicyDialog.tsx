import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
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
import {
  createAlertPolicy,
  listNotificationChannels,
  updateAlertPolicy,
  type AlertPolicy,
} from '../../api/gcp/monitoring'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { resourceID } from './util'

export interface AlertPolicyDialogProps {
  open: boolean
  onClose: () => void
  initial?: AlertPolicy
}

/** Create or edit a Cloud Monitoring alerting policy. */
export function AlertPolicyDialog({ open, onClose, initial }: AlertPolicyDialogProps) {
  const queryClient = useQueryClient()
  const [displayName, setDisplayName] = useState('')
  const [content, setContent] = useState('')
  const [combiner, setCombiner] = useState('OR')
  const [enabled, setEnabled] = useState(true)
  const [conditions, setConditions] = useState('[]')
  const [channels, setChannels] = useState<string[]>([])
  const [jsonError, setJsonError] = useState('')

  const channelList = useQuery({
    queryKey: ['gcp', 'monitoring', 'channels'],
    queryFn: () => listNotificationChannels(),
    enabled: open,
  })

  useEffect(() => {
    if (open) {
      setDisplayName(initial?.displayName ?? '')
      setContent((initial?.documentation?.content as string) ?? '')
      setCombiner(initial?.combiner ?? 'OR')
      setEnabled(initial?.enabled ?? true)
      setConditions(JSON.stringify(initial?.conditions ?? [], null, 2))
      setChannels(initial?.notificationChannels ?? [])
      setJsonError('')
    }
  }, [open, initial])

  const save = useMutation({
    mutationFn: () => {
      let parsedConditions: Record<string, unknown>[] = []
      if (conditions.trim()) {
        const parsed = JSON.parse(conditions)
        if (!Array.isArray(parsed)) throw new Error('conditions must be a JSON array')
        parsedConditions = parsed as Record<string, unknown>[]
      }
      const body = {
        displayName,
        documentation: content ? { content } : undefined,
        combiner,
        enabled,
        conditions: parsedConditions,
        notificationChannels: channels,
      }
      return initial ? updateAlertPolicy(resourceID(initial.name), body) : createAlertPolicy(body)
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'monitoring', 'policies'] })
      onClose()
    },
  })

  const submit = () => {
    try {
      JSON.parse(conditions || '[]')
      setJsonError('')
    } catch (e) {
      setJsonError((e as Error).message)
      return
    }
    save.mutate()
  }

  const availableChannels = channelList.data?.notificationChannels ?? []

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{initial ? 'Edit alert policy' : 'Create alert policy'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not save the policy: {(save.error as Error).message}</Alert>
          )}
          <TextField
            autoFocus
            label="Display name"
            value={displayName}
            onChange={(e) => setDisplayName(e.target.value)}
            placeholder="High error rate"
            fullWidth
          />
          <TextField
            label="Documentation (optional)"
            value={content}
            onChange={(e) => setContent(e.target.value)}
            multiline
            minRows={2}
            fullWidth
          />
          <TextField
            select
            label="Combiner"
            value={combiner}
            onChange={(e) => setCombiner(e.target.value)}
            fullWidth
          >
            {['OR', 'AND', 'AND_WITH_MATCHING_RESOURCE'].map((c) => (
              <MenuItem key={c} value={c}>
                {c}
              </MenuItem>
            ))}
          </TextField>
          <GcpCodeEditor
            label="Conditions (JSON array, optional)"
            value={conditions}
            onChange={setConditions}
            error={jsonError ? `Invalid conditions JSON: ${jsonError}` : null}
            language="json"
            minRows={3}
          />
          <TextField
            select
            label="Notification channels (optional)"
            value={channels}
            onChange={(e) => setChannels(e.target.value as unknown as string[])}
            slotProps={{ select: { multiple: true } }}
            fullWidth
          >
            {availableChannels.map((c) => (
              <MenuItem key={c.name} value={c.name}>
                {c.displayName || resourceID(c.name)}
              </MenuItem>
            ))}
          </TextField>
          <FormControlLabel
            control={<Switch checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />}
            label="Enabled"
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={!displayName || save.isPending} onClick={submit}>
          Save
        </Button>
      </DialogActions>
    </Dialog>
  )
}
