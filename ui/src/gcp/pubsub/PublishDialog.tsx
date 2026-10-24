import { useState } from 'react'
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
import { publishToTopic } from '../../api/gcp/pubsub'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { encodeMessageData, parseAttributes } from './util'

/** Publish one message to a topic: plain-text payload + optional attributes. */
export function PublishDialog({
  topic,
  open,
  onClose,
}: {
  topic: string
  open: boolean
  onClose: () => void
}) {
  const queryClient = useQueryClient()
  const [payload, setPayload] = useState('')
  const [attributes, setAttributes] = useState('')
  const [error, setError] = useState('')
  const [published, setPublished] = useState<string[]>([])

  const publish = useMutation({
    mutationFn: () => {
      const attrs = parseAttributes(attributes)
      return publishToTopic(topic, [{ data: encodeMessageData(payload), attributes: attrs }])
    },
    onSuccess: (resp) => {
      setPublished(resp.messageIds ?? [])
      setPayload('')
      setAttributes('')
      setError('')
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'pubsub', 'topics'] })
    },
  })

  const onPublish = () => {
    try {
      parseAttributes(attributes)
    } catch {
      setError('Attributes must be valid JSON.')
      return
    }
    setError('')
    publish.mutate()
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Publish message to {topic}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {publish.isError && <Alert severity="error">Publish failed.</Alert>}
          {published.length > 0 && (
            <Alert severity="success">Published message ID: {published.join(', ')}</Alert>
          )}
          {error && <Alert severity="error">{error}</Alert>}
          <GcpCodeEditor
            autoFocus
            label="Message"
            value={payload}
            onChange={setPayload}
            language="text"
            minRows={4}
          />
          <TextField
            label='Attributes (JSON, e.g. {"k":"v"})'
            value={attributes}
            onChange={(e) => setAttributes(e.target.value)}
            size="small"
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
        <Button variant="contained" disabled={!payload || publish.isPending} onClick={onPublish}>
          Publish
        </Button>
      </DialogActions>
    </Dialog>
  )
}
