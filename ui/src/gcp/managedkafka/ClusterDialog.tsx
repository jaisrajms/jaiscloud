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
  createCluster,
  updateCluster,
  type ClusterWriteInput,
  type ManagedKafkaCluster,
} from '../../api/gcp/managedkafka'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { formatKV, parseKV } from './util'

export interface ClusterDialogProps {
  open: boolean
  onClose: () => void
  /** The cluster to edit; omitted for a create. */
  cluster?: ManagedKafkaCluster
  onSaved?: (cluster: ManagedKafkaCluster) => void
}

/** A minimal capacity config the create form starts from. */
const DEFAULT_CONFIG = `{
  "capacityConfig": {
    "vcpuCount": 3,
    "memoryBytes": "3221225472"
  }
}`

/** Create or update a Managed Kafka cluster. */
export function ClusterDialog({ open, onClose, cluster, onSaved }: ClusterDialogProps) {
  const queryClient = useQueryClient()
  const editing = Boolean(cluster)
  const [id, setId] = useState('')
  const [location, setLocation] = useState('us-central1')
  const [labelsText, setLabelsText] = useState('')
  const [configText, setConfigText] = useState(DEFAULT_CONFIG)
  const [parseError, setParseError] = useState('')

  useEffect(() => {
    if (!open) return
    setId(cluster?.id ?? '')
    setLocation(cluster?.location ?? 'us-central1')
    setLabelsText(formatKV(cluster?.labels))
    setConfigText(cluster?.config != null ? JSON.stringify(cluster.config, null, 2) : DEFAULT_CONFIG)
    setParseError('')
  }, [open, cluster])

  const save = useMutation({
    mutationFn: (input: ClusterWriteInput) =>
      editing
        ? updateCluster(cluster!.location, cluster!.id, input)
        : createCluster(input),
    onSuccess: (saved) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'managedkafka'] })
      onSaved?.(saved)
      onClose()
    },
  })

  const canSave = Boolean((editing || id.trim()) && location.trim()) && !save.isPending

  const handleSave = () => {
    setParseError('')
    let config: unknown
    if (configText.trim()) {
      try {
        config = JSON.parse(configText)
      } catch (err) {
        setParseError(`Config is not valid JSON: ${(err as Error).message}`)
        return
      }
    }
    const body: ClusterWriteInput = { labels: parseKV(labelsText), config }
    if (editing) {
      save.mutate(body)
    } else {
      save.mutate({ ...body, id: id.trim(), location: location.trim() })
    }
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>{editing ? `Edit ${cluster?.id}` : 'Create cluster'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">
              Could not {editing ? 'update' : 'create'} the cluster: {(save.error as Error).message}
            </Alert>
          )}
          <Stack direction="row" spacing={2}>
            <TextField
              autoFocus={!editing}
              label="Cluster name"
              value={id}
              onChange={(e) => setId(e.target.value)}
              disabled={editing}
              placeholder="my-cluster"
              fullWidth
            />
            <TextField
              label="Location"
              value={location}
              onChange={(e) => setLocation(e.target.value)}
              disabled={editing}
              placeholder="us-central1"
              fullWidth
            />
          </Stack>
          <GcpCodeEditor
            label="Labels (key=value per line)"
            value={labelsText}
            onChange={setLabelsText}
            language="text"
            minRows={2}
          />
          <GcpCodeEditor
            label="Cluster config (JSON)"
            value={configText}
            onChange={setConfigText}
            error={parseError || null}
            language="json"
            minRows={10}
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
