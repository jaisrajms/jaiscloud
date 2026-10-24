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
  createNotificationChannel,
  listNotificationChannelDescriptors,
  updateNotificationChannel,
  type NotificationChannel,
} from '../../api/gcp/monitoring'
import { resourceID } from './util'

export interface ChannelDialogProps {
  open: boolean
  onClose: () => void
  initial?: NotificationChannel
}

function parseLines(input: string): Record<string, string> {
  const out: Record<string, string> = {}
  for (const raw of input.split('\n')) {
    const line = raw.trim()
    const idx = line.indexOf('=')
    if (idx <= 0) continue
    out[line.slice(0, idx).trim()] = line.slice(idx + 1).trim()
  }
  return out
}

/** Create or edit a Cloud Monitoring notification channel. */
export function ChannelDialog({ open, onClose, initial }: ChannelDialogProps) {
  const queryClient = useQueryClient()
  const [type, setType] = useState('')
  const [displayName, setDisplayName] = useState('')
  const [description, setDescription] = useState('')
  const [labels, setLabels] = useState('')
  const [enabled, setEnabled] = useState(true)

  const descriptors = useQuery({
    queryKey: ['gcp', 'monitoring', 'channelDescriptors'],
    queryFn: () => listNotificationChannelDescriptors(),
    enabled: open,
  })

  useEffect(() => {
    if (open) {
      setType(initial?.type ?? '')
      setDisplayName(initial?.displayName ?? '')
      setDescription(initial?.description ?? '')
      setLabels(
        Object.entries(initial?.labels ?? {})
          .map(([k, v]) => `${k}=${v}`)
          .join('\n'),
      )
      setEnabled(initial?.enabled ?? true)
    }
  }, [open, initial])

  const save = useMutation({
    mutationFn: () => {
      const body = {
        type,
        displayName,
        description,
        labels: parseLines(labels),
        enabled,
      }
      return initial
        ? updateNotificationChannel(resourceID(initial.name), body)
        : createNotificationChannel(body)
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'monitoring', 'channels'] })
      onClose()
    },
  })

  const options = descriptors.data?.channelDescriptors ?? []

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{initial ? 'Edit notification channel' : 'Create notification channel'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">
              Could not save the channel: {(save.error as Error).message}
            </Alert>
          )}
          {options.length > 0 ? (
            <TextField
              select
              label="Type"
              value={type}
              onChange={(e) => setType(e.target.value)}
              fullWidth
            >
              {options.map((d) => (
                <MenuItem key={d.type} value={d.type}>
                  {d.displayName || d.type}
                </MenuItem>
              ))}
            </TextField>
          ) : (
            <TextField
              label="Type"
              value={type}
              onChange={(e) => setType(e.target.value)}
              placeholder="email"
              helperText="e.g. email, pubsub, webhook_tokenauth"
              fullWidth
            />
          )}
          <TextField
            label="Display name"
            value={displayName}
            onChange={(e) => setDisplayName(e.target.value)}
            fullWidth
          />
          <TextField
            label="Description (optional)"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            fullWidth
          />
          <TextField
            label="Labels (one key=value per line)"
            value={labels}
            onChange={(e) => setLabels(e.target.value)}
            placeholder="email_address=ops@example.com"
            multiline
            minRows={2}
            fullWidth
          />
          <FormControlLabel
            control={<Switch checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />}
            label="Enabled"
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={!type || !displayName || save.isPending}
          onClick={() => save.mutate()}
        >
          Save
        </Button>
      </DialogActions>
    </Dialog>
  )
}
