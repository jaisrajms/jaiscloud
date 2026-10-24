import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  FormControlLabel,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  Switch,
  TextField,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import {
  createTrigger,
  updateTrigger,
  type EventFilter,
  type EventarcTrigger,
  type TriggerInput,
} from '../../api/gcp/eventarc'
import { GcpCodeEditor } from '../common/GcpCodeEditor'

const DESTINATION_TYPES = ['cloudFunction', 'cloudRun', 'workflow']

export interface TriggerDialogProps {
  open: boolean
  onClose: () => void
  /** When set, the dialog edits this trigger; otherwise it creates a new one. */
  trigger?: EventarcTrigger
  onSaved?: (trigger: EventarcTrigger) => void
}

function destinationPlaceholder(type: string): string {
  switch (type) {
    case 'cloudFunction':
      return 'projects/my-project/locations/us-central1/functions/my-function'
    case 'cloudRun':
      return 'projects/my-project/locations/us-central1/services/my-service'
    case 'workflow':
      return 'projects/my-project/locations/us-central1/workflows/my-workflow'
    default:
      return ''
  }
}

function defaultFilter(): EventFilter {
  return { attribute: 'type', operator: '', value: '' }
}

function emptyInput(): TriggerInput {
  return {
    name: '',
    location: 'us-central1',
    destinationType: 'cloudFunction',
    destination: '',
    destinationRegion: '',
    serviceAccount: '',
    channel: '',
    eventDataContentType: '',
    eventFilters: [defaultFilter()],
    transportPubsubTopic: '',
  }
}

function inputFromTrigger(trigger: EventarcTrigger): TriggerInput {
  return {
    name: trigger.name,
    location: trigger.location,
    destinationType: trigger.destinationType ?? 'cloudFunction',
    destination: trigger.destination ?? '',
    destinationRegion: trigger.destinationRegion ?? '',
    serviceAccount: trigger.serviceAccount ?? '',
    channel: trigger.channel ?? '',
    eventDataContentType: trigger.eventDataContentType ?? '',
    eventFilters: trigger.eventFilters?.length ? trigger.eventFilters : [defaultFilter()],
    transportPubsubTopic: trigger.transportPubsubTopic ?? '',
    labels: trigger.labels,
  }
}

/** Create or edit an Eventarc trigger, with a raw-JSON escape hatch. */
export function TriggerDialog({ open, onClose, trigger, onSaved }: TriggerDialogProps) {
  const queryClient = useQueryClient()
  const editing = Boolean(trigger)
  const [input, setInput] = useState<TriggerInput>(emptyInput())
  const [rawMode, setRawMode] = useState(false)
  const [rawText, setRawText] = useState('{}')
  const [rawError, setRawError] = useState('')

  useEffect(() => {
    if (!open) return
    setInput(trigger ? inputFromTrigger(trigger) : emptyInput())
    // A gke/httpEndpoint destination has a nested shape the structured form
    // does not model, so editing one starts in raw mode.
    setRawMode(Boolean(trigger && !DESTINATION_TYPES.includes(trigger.destinationType ?? '')))
    setRawText(trigger ? JSON.stringify(trigger.config ?? {}, null, 2) : '{}')
    setRawError('')
  }, [open, trigger])

  const set = <K extends keyof TriggerInput>(key: K, value: TriggerInput[K]) =>
    setInput((cur) => ({ ...cur, [key]: value }))

  const setFilter = (index: number, patch: Partial<EventFilter>) =>
    setInput((cur) => ({
      ...cur,
      eventFilters: cur.eventFilters.map((f, i) => (i === index ? { ...f, ...patch } : f)),
    }))

  const addFilter = () =>
    setInput((cur) => ({ ...cur, eventFilters: [...cur.eventFilters, defaultFilter()] }))

  const removeFilter = (index: number) =>
    setInput((cur) => ({ ...cur, eventFilters: cur.eventFilters.filter((_, i) => i !== index) }))

  const save = useMutation({
    mutationFn: (body: TriggerInput) =>
      editing && trigger ? updateTrigger(trigger.location, trigger.name, body) : createTrigger(body),
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

  const structuredValid =
    Boolean(input.name && input.location && input.destinationType && input.destination) &&
    (input.destinationType !== 'cloudRun' || Boolean(input.destinationRegion)) &&
    input.eventFilters.some((f) => f.attribute === 'type' && f.value !== '')
  const canSave = (rawMode ? rawText.trim().length > 0 : structuredValid) && !save.isPending

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>{editing ? `Edit ${trigger?.name}` : 'Create trigger'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not save the trigger: {(save.error as Error).message}</Alert>
          )}
          <TextField
            autoFocus={!editing}
            label="Name"
            value={input.name}
            disabled={editing}
            onChange={(e) => set('name', e.target.value)}
            placeholder="my-trigger"
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
            control={
              <Switch
                checked={rawMode}
                disabled={Boolean(trigger && !DESTINATION_TYPES.includes(trigger.destinationType ?? ''))}
                onChange={(e) => setRawMode(e.target.checked)}
              />
            }
            label="Edit raw JSON (advanced)"
          />

          {rawMode ? (
            <>
              <Typography variant="body2" color="text.secondary">
                The JSON below is sent verbatim as the trigger body; the structured fields are ignored.
                Use this for a GKE or HTTP-endpoint destination.
              </Typography>
              <GcpCodeEditor
                label="Trigger config JSON"
                value={rawText}
                onChange={setRawText}
                error={rawError || null}
                language="json"
                minRows={12}
              />
            </>
          ) : (
            <>
              <FormControl fullWidth>
                <InputLabel id="eventarc-destination-type">Destination type</InputLabel>
                <Select
                  labelId="eventarc-destination-type"
                  label="Destination type"
                  value={input.destinationType}
                  onChange={(e) => set('destinationType', e.target.value)}
                >
                  {DESTINATION_TYPES.map((type) => (
                    <MenuItem key={type} value={type}>
                      {type}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <TextField
                label="Destination"
                value={input.destination}
                onChange={(e) => set('destination', e.target.value)}
                placeholder={destinationPlaceholder(input.destinationType)}
                fullWidth
              />
              {input.destinationType === 'cloudRun' && (
                <TextField
                  label="Destination region"
                  value={input.destinationRegion}
                  onChange={(e) => set('destinationRegion', e.target.value)}
                  placeholder={input.location || 'us-central1'}
                  fullWidth
                />
              )}
              <TextField
                label="Service account (optional)"
                value={input.serviceAccount}
                onChange={(e) => set('serviceAccount', e.target.value)}
                placeholder="my-sa@my-project.iam.gserviceaccount.com"
                fullWidth
              />
              <TextField
                label="Channel (optional)"
                value={input.channel}
                onChange={(e) => set('channel', e.target.value)}
                placeholder="projects/my-project/locations/us-central1/channels/my-channel"
                fullWidth
              />
              <TextField
                label="Transport Pub/Sub topic (optional)"
                value={input.transportPubsubTopic}
                onChange={(e) => set('transportPubsubTopic', e.target.value)}
                placeholder="projects/my-project/topics/my-topic"
                fullWidth
              />
              <TextField
                label="Event data content type (optional)"
                value={input.eventDataContentType}
                onChange={(e) => set('eventDataContentType', e.target.value)}
                placeholder="application/json"
                fullWidth
              />

              <Box>
                <Stack direction="row" sx={{ alignItems: 'center', justifyContent: 'space-between' }}>
                  <Typography variant="subtitle2">Event filters</Typography>
                  <Button size="small" startIcon={<AddIcon />} onClick={addFilter}>
                    Add filter
                  </Button>
                </Stack>
                <Typography variant="caption" color="text.secondary">
                  A filter with attribute &quot;type&quot; is required.
                </Typography>
                <Stack spacing={1} sx={{ mt: 1 }}>
                  {input.eventFilters.map((filter, index) => (
                    <Stack key={index} direction="row" spacing={1} sx={{ alignItems: 'center' }}>
                      <TextField
                        label="Attribute"
                        value={filter.attribute}
                        onChange={(e) => setFilter(index, { attribute: e.target.value })}
                        placeholder="type"
                        size="small"
                        fullWidth
                      />
                      <TextField
                        label="Operator"
                        value={filter.operator ?? ''}
                        onChange={(e) => setFilter(index, { operator: e.target.value })}
                        placeholder="match-path-pattern"
                        size="small"
                        sx={{ maxWidth: 120 }}
                      />
                      <TextField
                        label="Value"
                        value={filter.value}
                        onChange={(e) => setFilter(index, { value: e.target.value })}
                        placeholder="google.cloud.pubsub.topic.v1.messagePublished"
                        size="small"
                        fullWidth
                      />
                      <IconButton
                        size="small"
                        onClick={() => removeFilter(index)}
                        aria-label={`Remove filter ${index + 1}`}
                      >
                        <DeleteOutlineIcon fontSize="small" />
                      </IconButton>
                    </Stack>
                  ))}
                </Stack>
              </Box>
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
