import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Stack,
  TextField,
} from '@mui/material'
import { updateSecret } from '../../api/gcp/secretmanager'
import { formatKeyValueLines, formatTopicList, parseKeyValueLines, parseTopicList } from './util'

export interface EditSecretDialogProps {
  open: boolean
  secret: string
  labels?: Record<string, string>
  topics?: string[]
  rotationPeriod?: string
  nextRotationTime?: string
  onClose: () => void
}

/**
 * Edit a secret's labels, event-notification topics and rotation schedule;
 * absent fields are preserved. The rotation block is only sent when it changed,
 * so saving a label edit cannot reset a schedule whose next rotation time is
 * already less than five minutes away.
 */
export function EditSecretDialog({
  open,
  secret,
  labels,
  topics,
  rotationPeriod,
  nextRotationTime,
  onClose,
}: EditSecretDialogProps) {
  const queryClient = useQueryClient()
  const [labelText, setLabelText] = useState('')
  const [topicText, setTopicText] = useState('')
  const [rotation, setRotation] = useState('')
  const [nextRotation, setNextRotation] = useState('')

  useEffect(() => {
    if (open) {
      setLabelText(formatKeyValueLines(labels))
      setTopicText(formatTopicList(topics))
      setRotation(rotationPeriod ?? '')
      setNextRotation(nextRotationTime ?? '')
    }
  }, [open, labels, topics, rotationPeriod, nextRotationTime])

  const rotationChanged =
    rotation !== (rotationPeriod ?? '') || nextRotation !== (nextRotationTime ?? '')
  // Clearing both schedule fields removes the schedule: the API needs an empty
  // `rotation` object for that, which an absent block cannot express.
  const removeRotation = Boolean(rotationPeriod || nextRotationTime) && !rotation && !nextRotation

  const update = useMutation({
    mutationFn: () =>
      updateSecret(secret, {
        labels: parseKeyValueLines(labelText),
        topics: parseTopicList(topicText),
        ...(removeRotation
          ? { removeRotation: true }
          : rotationChanged
            ? {
                rotationPeriod: rotation || undefined,
                nextRotationTime: nextRotation || undefined,
              }
            : {}),
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'secretmanager'] })
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Edit secret</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {update.isError && (
            <Alert severity="error">Could not update: {(update.error as Error).message}</Alert>
          )}
          <TextField
            label="Labels (one key=value per line)"
            value={labelText}
            onChange={(e) => setLabelText(e.target.value)}
            multiline
            minRows={3}
            fullWidth
          />
          <TextField
            label="Pub/Sub topics (comma-separated)"
            value={topicText}
            onChange={(e) => setTopicText(e.target.value)}
            placeholder="projects/my-project/topics/secret-events"
            helperText="Up to 10 topics. A rotation schedule requires at least one."
            fullWidth
          />
          <TextField
            label="Rotation period"
            value={rotation}
            onChange={(e) => setRotation(e.target.value)}
            placeholder="7776000s"
            helperText="Whole seconds, at least 3600s."
            fullWidth
          />
          <TextField
            label="Next rotation time"
            value={nextRotation}
            onChange={(e) => setNextRotation(e.target.value)}
            placeholder="2026-11-01T00:00:00Z"
            helperText="RFC 3339. Required with a rotation period; must be at least 5 minutes in the future when changed."
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={update.isPending} onClick={() => update.mutate()}>
          Save
        </Button>
      </DialogActions>
    </Dialog>
  )
}
