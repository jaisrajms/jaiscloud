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
import {
  createTopic,
  updateTopic,
  type ManagedKafkaTopic,
  type TopicWriteInput,
} from '../../api/gcp/managedkafka'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { formatKV, parseKV } from './util'

export interface TopicDialogProps {
  open: boolean
  onClose: () => void
  location: string
  cluster: string
  /** The topic to edit; omitted for a create. */
  topic?: ManagedKafkaTopic
  onSaved?: (topic: ManagedKafkaTopic) => void
}

/** Create or update a Managed Kafka topic. */
export function TopicDialog({ open, onClose, location, cluster, topic, onSaved }: TopicDialogProps) {
  const queryClient = useQueryClient()
  const editing = Boolean(topic)
  const [id, setId] = useState('')
  const [partitions, setPartitions] = useState(1)
  const [replication, setReplication] = useState(1)
  const [configsText, setConfigsText] = useState('')

  useEffect(() => {
    if (!open) return
    setId(topic?.id ?? '')
    setPartitions(topic?.partitionCount || 1)
    setReplication(topic?.replicationFactor || 1)
    setConfigsText(formatKV(topic?.configs))
  }, [open, topic])

  const save = useMutation({
    mutationFn: (input: TopicWriteInput) =>
      editing
        ? updateTopic(location, cluster, topic!.id, input)
        : createTopic(location, cluster, input),
    onSuccess: (saved) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'managedkafka'] })
      onSaved?.(saved)
      onClose()
    },
  })

  const canSave = Boolean((editing || id.trim()) && location && cluster) && !save.isPending

  const handleSave = () => {
    if (editing) {
      // Kafka allows a topic to grow (never shrink); the core rejects a
      // decrease. replicationFactor is immutable, so it is echoed unchanged.
      save.mutate({
        partitionCount: partitions,
        replicationFactor: replication,
        configs: parseKV(configsText),
      })
      return
    }
    save.mutate({
      id: id.trim(),
      partitionCount: partitions,
      replicationFactor: replication,
      configs: parseKV(configsText),
    })
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{editing ? `Edit ${topic?.id}` : 'Create topic'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">
              Could not {editing ? 'update' : 'create'} the topic: {(save.error as Error).message}
            </Alert>
          )}
          <TextField
            autoFocus={!editing}
            label="Topic id"
            value={id}
            onChange={(e) => setId(e.target.value)}
            disabled={editing}
            placeholder="orders"
            fullWidth
          />
          <Stack direction="row" spacing={2}>
            <TextField
              label="Partitions"
              type="number"
              value={partitions}
              onChange={(e) => setPartitions(Number(e.target.value))}
              helperText={editing ? 'Can grow, never shrink' : undefined}
              fullWidth
            />
            <TextField
              label="Replication factor"
              type="number"
              value={replication}
              onChange={(e) => setReplication(Number(e.target.value))}
              disabled={editing}
              fullWidth
            />
          </Stack>
          <GcpCodeEditor
            label="Configs (key=value per line)"
            value={configsText}
            onChange={setConfigsText}
            language="text"
            minRows={3}
          />
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
