import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  MenuItem,
  Stack,
  TextField,
} from '@mui/material'
import {
  createCluster,
  type DataprocCluster,
  type DataprocClusterInput,
} from '../../api/gcp/dataproc'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { buildVirtualClusterConfig, parseJsonObject } from './util'

export interface ClusterDialogProps {
  open: boolean
  onClose: () => void
  onCreated?: (cluster: DataprocCluster) => void
}

type Placement = 'gce' | 'gke'

/** A minimal GCE ClusterConfig the create form starts from. */
const DEFAULT_GCE_CONFIG = `{
  "gceClusterConfig": {
    "zoneUri": ""
  },
  "masterConfig": {
    "numInstances": 1
  },
  "workerConfig": {
    "numInstances": 2
  }
}`

/** Create a Dataproc cluster: a GCE config or a GKE virtual cluster. */
export function ClusterDialog({ open, onClose, onCreated }: ClusterDialogProps) {
  const queryClient = useQueryClient()
  const [name, setName] = useState('')
  const [region, setRegion] = useState('us-central1')
  const [placement, setPlacement] = useState<Placement>('gce')
  const [gceText, setGceText] = useState(DEFAULT_GCE_CONFIG)
  const [gkeTarget, setGkeTarget] = useState('')
  const [gkeNamespace, setGkeNamespace] = useState('')
  const [gkeNodePool, setGkeNodePool] = useState('')
  const [parseError, setParseError] = useState('')

  useEffect(() => {
    if (!open) return
    setName('')
    setRegion('us-central1')
    setPlacement('gce')
    setGceText(DEFAULT_GCE_CONFIG)
    setGkeTarget('')
    setGkeNamespace('')
    setGkeNodePool('')
    setParseError('')
  }, [open])

  const save = useMutation({
    mutationFn: (input: DataprocClusterInput) => createCluster(input),
    onSuccess: (created) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })
      onCreated?.(created)
      onClose()
    },
  })

  const gkeValid =
    gkeNamespace.trim() !== '' && (gkeTarget.trim() !== '' || gkeNodePool.trim() !== '')
  const canSave = Boolean(name.trim() && region.trim()) && !save.isPending

  const handleSave = () => {
    setParseError('')
    const base = { region: region.trim(), name: name.trim() }
    if (placement === 'gce') {
      const { value, error } = parseJsonObject(gceText)
      if (error) {
        setParseError(error)
        return
      }
      save.mutate({ ...base, config: value })
      return
    }
    if (!gkeValid) return
    save.mutate({
      ...base,
      virtualClusterConfig: buildVirtualClusterConfig({
        gkeClusterTarget: gkeTarget,
        kubernetesNamespace: gkeNamespace,
        nodePool: gkeNodePool,
      }),
    })
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Create cluster</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">
              Could not create the cluster: {(save.error as Error).message}
            </Alert>
          )}
          <TextField
            autoFocus
            label="Name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="my-cluster"
            fullWidth
          />
          <TextField
            label="Region"
            value={region}
            onChange={(e) => setRegion(e.target.value)}
            placeholder="us-central1"
            fullWidth
          />
          <TextField
            select
            label="Cluster placement"
            value={placement}
            onChange={(e) => setPlacement(e.target.value as Placement)}
            fullWidth
          >
            <MenuItem value="gce">Compute Engine (GCE) cluster</MenuItem>
            <MenuItem value="gke">GKE virtual cluster</MenuItem>
          </TextField>

          {placement === 'gce' ? (
            <GcpCodeEditor
              label="Cluster config (JSON)"
              value={gceText}
              onChange={setGceText}
              error={parseError || null}
              language="json"
              minRows={12}
            />
          ) : (
            <>
              <TextField
                label="GKE cluster target"
                value={gkeTarget}
                onChange={(e) => setGkeTarget(e.target.value)}
                placeholder="projects/my-project/locations/us-central1/clusters/gke-1"
                fullWidth
              />
              <TextField
                label="Kubernetes namespace"
                value={gkeNamespace}
                onChange={(e) => setGkeNamespace(e.target.value)}
                placeholder="dataproc"
                fullWidth
              />
              <TextField
                label="Node pool target (optional)"
                value={gkeNodePool}
                onChange={(e) => setGkeNodePool(e.target.value)}
                placeholder="projects/my-project/locations/us-central1/clusters/gke-1/nodePools/default"
                fullWidth
              />
            </>
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={!canSave || (placement === 'gke' && !gkeValid)}
          onClick={handleSave}
        >
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
