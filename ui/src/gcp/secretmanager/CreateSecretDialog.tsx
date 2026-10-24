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
import { createSecret } from '../../api/gcp/secretmanager'
import { parseKeyValueLines, parseTopicList } from './util'

export interface CreateSecretDialogProps {
  open: boolean
  onClose: () => void
}

/** Create a Secret Manager secret. */
export function CreateSecretDialog({ open, onClose }: CreateSecretDialogProps) {
  const queryClient = useQueryClient()
  const [secretId, setSecretId] = useState('')
  const [labels, setLabels] = useState('')
  const [topics, setTopics] = useState('')
  const [rotationPeriod, setRotationPeriod] = useState('')
  const [nextRotationTime, setNextRotationTime] = useState('')

  useEffect(() => {
    if (open) {
      setSecretId('')
      setLabels('')
      setTopics('')
      setRotationPeriod('')
      setNextRotationTime('')
    }
  }, [open])

  const create = useMutation({
    mutationFn: () =>
      createSecret({
        secretId,
        labels: parseKeyValueLines(labels),
        topics: parseTopicList(topics),
        rotationPeriod: rotationPeriod || undefined,
        nextRotationTime: nextRotationTime || undefined,
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'secretmanager'] })
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Create secret</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {create.isError && (
            <Alert severity="error">
              Could not create the secret: {(create.error as Error).message}
            </Alert>
          )}
          <TextField
            autoFocus
            label="Secret ID"
            value={secretId}
            onChange={(e) => setSecretId(e.target.value)}
            placeholder="db-password"
            fullWidth
          />
          <TextField
            label="Labels (one key=value per line, optional)"
            value={labels}
            onChange={(e) => setLabels(e.target.value)}
            multiline
            minRows={2}
            fullWidth
          />
          <TextField
            label="Pub/Sub topics (comma-separated, optional)"
            value={topics}
            onChange={(e) => setTopics(e.target.value)}
            placeholder="projects/my-project/topics/secret-events"
            helperText="Up to 10 topics. A rotation schedule requires at least one."
            fullWidth
          />
          <TextField
            label="Rotation period (optional)"
            value={rotationPeriod}
            onChange={(e) => setRotationPeriod(e.target.value)}
            placeholder="7776000s"
            helperText="Whole seconds, at least 3600s."
            fullWidth
          />
          <TextField
            label="Next rotation time (optional)"
            value={nextRotationTime}
            onChange={(e) => setNextRotationTime(e.target.value)}
            placeholder="2026-11-01T00:00:00Z"
            helperText="RFC 3339, at least 5 minutes in the future. Required with a rotation period."
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={!secretId || create.isPending}
          onClick={() => create.mutate()}
        >
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
