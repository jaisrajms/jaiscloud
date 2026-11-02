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
import { updateCluster, type DataprocCluster, type DataprocClusterPatch } from '../../api/gcp/dataproc'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { buildClusterUpdateMask, parseJsonObject, parseKeyValueLines } from './util'

export interface ClusterEditDialogProps {
  open: boolean
  onClose: () => void
  cluster: DataprocCluster
}

const labelLines = (labels?: Record<string, string>) =>
  labels
    ? Object.entries(labels)
        .map(([k, v]) => `${k}=${v}`)
        .join('\n')
    : ''

/**
 * Edit a cluster's labels and config. Only the fields the user touched go into
 * the update mask; an untouched form never sends a patch, because an empty mask
 * applies everything in the core.
 */
export function ClusterEditDialog({ open, onClose, cluster }: ClusterEditDialogProps) {
  const queryClient = useQueryClient()
  const originalLabels = labelLines(cluster.labels)
  const [labelsText, setLabelsText] = useState(originalLabels)
  const [configText, setConfigText] = useState('')
  const [error, setError] = useState('')

  useEffect(() => {
    if (!open) return
    setLabelsText(labelLines(cluster.labels))
    setConfigText(
      cluster.config ? JSON.stringify(cluster.config, null, 2) : '',
    )
    setError('')
  }, [open, cluster])

  const save = useMutation({
    mutationFn: (patch: DataprocClusterPatch) => updateCluster(cluster.region, cluster.id, patch),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })
      onClose()
    },
  })

  const handleSave = () => {
    setError('')
    let labels: Record<string, string> | undefined
    try {
      labels = labelsText.trim() ? parseKeyValueLines(labelsText) : {}
    } catch (err) {
      setError((err as Error).message)
      return
    }
    // A cluster with no config is GKE-backed; virtualClusterConfig is
    // recreate-only, so the editor must not invent one.
    if (!cluster.config) {
      if (labelsText === originalLabels) {
        setError('No changes to save.')
        return
      }
      save.mutate({ labels, updateMask: ['labels'] })
      return
    }
    const parsed = parseJsonObject(configText)
    if (parsed.error) {
      setError(parsed.error)
      return
    }
    const labelsChanged = labelsText !== originalLabels
    const mask = buildClusterUpdateMask(labelsChanged, cluster.config, parsed.value)
    if (mask.length === 0) {
      setError('No changes to save.')
      return
    }
    const patch: DataprocClusterPatch = { updateMask: mask }
    if (labelsChanged) patch.labels = labels
    if (mask.some((path) => path === 'config' || path.startsWith('config.'))) {
      patch.config = parsed.value
    }
    save.mutate(patch)
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Edit {cluster.id}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">
              Could not update the cluster: {(save.error as Error).message}
            </Alert>
          )}
          {error && <Alert severity="error">{error}</Alert>}
          <TextField
            label="Labels (key=value per line)"
            value={labelsText}
            onChange={(e) => setLabelsText(e.target.value)}
            multiline
            minRows={2}
            fullWidth
          />
          {cluster.config ? (
            <GcpCodeEditor
              label="Cluster config (JSON)"
              value={configText}
              onChange={setConfigText}
              language="json"
              minRows={12}
            />
          ) : (
            <Alert severity="info">
              This is a Dataproc-on-GKE virtual cluster. Its virtual cluster config is
              recreate-only, so only labels can be edited.
            </Alert>
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={save.isPending} onClick={handleSave}>
          Save
        </Button>
      </DialogActions>
    </Dialog>
  )
}
