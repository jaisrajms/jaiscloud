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
  Typography,
} from '@mui/material'
import {
  createChannel,
  updateChannel,
  type ChannelInput,
  type EventarcChannel,
} from '../../api/gcp/eventarc'
import { GcpCodeEditor } from '../common/GcpCodeEditor'

export interface ChannelDialogProps {
  open: boolean
  onClose: () => void
  /** When set, the dialog edits this channel; otherwise it creates a new one. */
  channel?: EventarcChannel
  onSaved?: (channel: EventarcChannel) => void
}

function emptyInput(): ChannelInput {
  return {
    name: '',
    location: 'us-central1',
    provider: '',
    cryptoKeyName: '',
  }
}

function inputFromChannel(channel: EventarcChannel): ChannelInput {
  return {
    name: channel.name,
    location: channel.location,
    provider: channel.provider ?? '',
    cryptoKeyName: channel.cryptoKeyName ?? '',
    labels: channel.labels,
  }
}

/** Create or edit an Eventarc channel, with a raw-JSON escape hatch. */
export function ChannelDialog({ open, onClose, channel, onSaved }: ChannelDialogProps) {
  const queryClient = useQueryClient()
  const editing = Boolean(channel)
  const [input, setInput] = useState<ChannelInput>(emptyInput())
  const [rawMode, setRawMode] = useState(false)
  const [rawText, setRawText] = useState('{}')
  const [rawError, setRawError] = useState('')

  useEffect(() => {
    if (!open) return
    setInput(channel ? inputFromChannel(channel) : emptyInput())
    setRawMode(false)
    setRawText(channel ? JSON.stringify(channel.config ?? {}, null, 2) : '{}')
    setRawError('')
  }, [open, channel])

  const set = <K extends keyof ChannelInput>(key: K, value: ChannelInput[K]) =>
    setInput((cur) => ({ ...cur, [key]: value }))

  const save = useMutation({
    mutationFn: (body: ChannelInput) =>
      editing && channel ? updateChannel(channel.location, channel.name, body) : createChannel(body),
    onSuccess: (saved) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'eventarc'] })
      onSaved?.(saved)
      onClose()
    },
  })

  const handleSave = () => {
    if (rawMode) {
      try {
        const parsed: unknown = JSON.parse(rawText)
        if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
          setRawError('Config must be a JSON object.')
          return
        }
        setRawError('')
        save.mutate({ ...input, config: parsed as Record<string, unknown> })
      } catch (err) {
        setRawError(`Invalid JSON: ${(err as Error).message}`)
      }
      return
    }
    save.mutate(input)
  }

  const canSave =
    (rawMode ? rawText.trim().length > 0 : Boolean(input.name && input.location)) && !save.isPending

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{editing ? `Edit ${channel?.name}` : 'Create channel'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not save the channel: {(save.error as Error).message}</Alert>
          )}
          <TextField
            autoFocus={!editing}
            label="Name"
            value={input.name}
            disabled={editing}
            onChange={(e) => set('name', e.target.value)}
            placeholder="my-channel"
            fullWidth
          />
          <TextField
            label="Location (region)"
            value={input.location}
            disabled={editing}
            onChange={(e) => set('location', e.target.value)}
            placeholder="us-central1"
            fullWidth
          />

          <FormControlLabel
            control={<Switch checked={rawMode} onChange={(e) => setRawMode(e.target.checked)} />}
            label="Edit raw JSON (advanced)"
          />

          {rawMode ? (
            <>
              <Typography variant="body2" color="text.secondary">
                The JSON below is sent verbatim as the channel body; the structured fields are ignored.
              </Typography>
              <GcpCodeEditor
                label="Channel config JSON"
                value={rawText}
                onChange={setRawText}
                error={rawError || null}
                language="json"
                minRows={8}
              />
            </>
          ) : (
            <>
              <TextField
                label="Provider (optional)"
                value={input.provider}
                onChange={(e) => set('provider', e.target.value)}
                placeholder="projects/my-project/locations/us-central1/providers/some.saas"
                fullWidth
              />
              <TextField
                label="Crypto key name (optional)"
                value={input.cryptoKeyName}
                onChange={(e) => set('cryptoKeyName', e.target.value)}
                placeholder="projects/my-project/locations/us-central1/keyRings/kr/cryptoKeys/k"
                fullWidth
              />
            </>
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={!canSave} onClick={handleSave}>
          {editing ? 'Save' : 'Create'}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
