import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Chip,
  MenuItem,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import {
  createNotification,
  deleteNotification,
  listNotifications,
  NOTIFICATION_EVENT_TYPES,
} from '../../api/gcp/storage'
import { listTopics } from '../../api/gcp/pubsub'
import { validateEventTypes } from './util'

/** parses `key=value` lines, throwing on a line without `=`. */
function parseKeyValueLines(text: string): Record<string, string> {
  const out: Record<string, string> = {}
  for (const raw of text.split('\n')) {
    const line = raw.trim()
    if (!line || line.startsWith('#')) continue
    const at = line.indexOf('=')
    if (at <= 0) throw new Error(`Expected key=value, got "${line}"`)
    out[line.slice(0, at).trim()] = line.slice(at + 1).trim()
  }
  return out
}

/**
 * Bucket notification configs: a Pub/Sub target, the documented event-type set,
 * payload format, object prefix and custom attributes.
 */
export function NotificationsSection({ bucket }: { bucket: string }) {
  const queryClient = useQueryClient()
  const [topic, setTopic] = useState('')
  const [eventTypes, setEventTypes] = useState<string[]>(['OBJECT_FINALIZE'])
  const [payloadFormat, setPayloadFormat] = useState('JSON_API_V1')
  const [prefix, setPrefix] = useState('')
  const [attributes, setAttributes] = useState('')
  const [error, setError] = useState('')

  const notifications = useQuery({
    queryKey: ['gcp', 'storage', 'notifications', bucket],
    queryFn: () => listNotifications(bucket),
  })
  const topics = useQuery({ queryKey: ['gcp', 'pubsub', 'topics'], queryFn: () => listTopics() })

  const invalidate = () =>
    void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'notifications', bucket] })

  const create = useMutation({
    mutationFn: () =>
      createNotification(bucket, {
        topic,
        payloadFormat,
        eventTypes,
        objectNamePrefix: prefix.trim() || undefined,
        customAttributes: attributes.trim() ? parseKeyValueLines(attributes) : undefined,
      }),
    onSuccess: () => {
      setTopic('')
      setPrefix('')
      setAttributes('')
      invalidate()
    },
  })

  const remove = useMutation({
    mutationFn: (id: string) => deleteNotification(bucket, id),
    onSuccess: invalidate,
  })

  const handleCreate = () => {
    setError('')
    if (!topic.trim()) {
      setError('Select a Pub/Sub topic.')
      return
    }
    const invalid = validateEventTypes(eventTypes)
    if (invalid) {
      setError(invalid)
      return
    }
    try {
      if (attributes.trim()) parseKeyValueLines(attributes)
      create.mutate()
    } catch (err) {
      setError((err as Error).message)
    }
  }

  return (
    <Stack spacing={2}>
      <Typography variant="subtitle1">Notification configs</Typography>
      <Typography variant="body2" color="text.secondary">
        GCS publishes object events to a Pub/Sub topic. The topic must exist in the project.
      </Typography>

      {(create.isError || error || remove.isError || notifications.isError) && (
        <Alert severity="error">
          {error ||
            (create.error as Error | null)?.message ||
            (remove.error as Error | null)?.message ||
            'Failed to load notification configs.'}
        </Alert>
      )}

      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} sx={{ alignItems: 'flex-start' }}>
        <TextField
          select
          label="Pub/Sub topic"
          value={topic}
          onChange={(e) => setTopic(e.target.value)}
          helperText={topics.isError ? 'Failed to load topics.' : 'Topic in this project.'}
          sx={{ minWidth: 280 }}
          fullWidth
        >
          {(topics.data?.topics ?? []).map((t) => (
            <MenuItem key={t.fullName ?? t.name} value={t.fullName ?? t.name}>
              {t.name}
            </MenuItem>
          ))}
        </TextField>
        <TextField
          select
          label="Event types"
          value={eventTypes}
          onChange={(e) =>
            setEventTypes(
              typeof e.target.value === 'string' ? e.target.value.split(',') : e.target.value,
            )
          }
          slotProps={{ select: { multiple: true } }}
          sx={{ minWidth: 240 }}
          fullWidth
        >
          {NOTIFICATION_EVENT_TYPES.map((et) => (
            <MenuItem key={et} value={et}>
              {et}
            </MenuItem>
          ))}
        </TextField>
        <TextField
          select
          label="Payload format"
          value={payloadFormat}
          onChange={(e) => setPayloadFormat(e.target.value)}
          sx={{ minWidth: 180 }}
          fullWidth
        >
          <MenuItem value="JSON_API_V1">JSON_API_V1</MenuItem>
          <MenuItem value="NONE">NONE</MenuItem>
        </TextField>
      </Stack>
      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
        <TextField
          label="Object name prefix (optional)"
          value={prefix}
          onChange={(e) => setPrefix(e.target.value)}
          placeholder="logs/"
          fullWidth
        />
        <TextField
          label="Custom attributes (key=value per line)"
          value={attributes}
          onChange={(e) => setAttributes(e.target.value)}
          multiline
          minRows={2}
          fullWidth
        />
      </Stack>
      <Stack direction="row" sx={{ justifyContent: 'flex-end' }}>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          disabled={create.isPending}
          onClick={handleCreate}
        >
          Add notification
        </Button>
      </Stack>

      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>ID</TableCell>
            <TableCell>Topic</TableCell>
            <TableCell>Events</TableCell>
            <TableCell>Prefix</TableCell>
            <TableCell align="right">Actions</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {(notifications.data?.items ?? []).length === 0 && !notifications.isLoading && (
            <TableRow>
              <TableCell colSpan={5} align="center" sx={{ py: 3 }}>
                No notification configs.
              </TableCell>
            </TableRow>
          )}
          {(notifications.data?.items ?? []).map((n) => (
            <TableRow key={n.id}>
              <TableCell>{n.id || '—'}</TableCell>
              <TableCell sx={{ wordBreak: 'break-all' }}>{n.topic}</TableCell>
              <TableCell>
                <Stack direction="row" spacing={0.5} sx={{ flexWrap: 'wrap', rowGap: 0.5 }}>
                  {(n.eventTypes ?? []).map((et) => (
                    <Chip key={et} size="small" variant="outlined" label={et} />
                  ))}
                </Stack>
              </TableCell>
              <TableCell>{n.objectNamePrefix || '—'}</TableCell>
              <TableCell align="right">
                <Button
                  size="small"
                  color="error"
                  startIcon={<DeleteOutlineIcon />}
                  disabled={remove.isPending || !n.id}
                  onClick={() => n.id && remove.mutate(n.id)}
                >
                  Delete
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </Stack>
  )
}
