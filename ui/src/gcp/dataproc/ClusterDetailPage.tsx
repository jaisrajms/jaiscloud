import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  IconButton,
  Stack,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import StopIcon from '@mui/icons-material/Stop'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import { APIError } from '../../api/client'
import { deleteCluster, getCluster, startCluster, stopCluster } from '../../api/gcp/dataproc'
import { useAccount } from '../../context/AccountContext'
import { ClusterEditDialog } from './ClusterEditDialog'
import { JobDialog } from './JobDialog'
import {
  ConfirmDialog,
  CopyButton,
  Detail,
  GsUriList,
  JsonBlock,
  StatusTimeline,
} from './common'
import { clusterStateColor, collectGsUris, shortDate } from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** A single Dataproc cluster: overview, config and status history. */
export function ClusterDetailPage() {
  const { region = '', cluster: clusterName = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [editing, setEditing] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [submittingJob, setSubmittingJob] = useState(false)

  const detail = useQuery({
    queryKey: ['gcp', 'dataproc', 'cluster', region, clusterName, accountId],
    queryFn: () => getCluster(region, clusterName),
    enabled: Boolean(region && clusterName),
  })

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })

  const remove = useMutation({
    mutationFn: () => deleteCluster(region, clusterName),
    onSuccess: () => {
      invalidate()
      navigate('/gcp/dataproc/clusters')
    },
  })
  const start = useMutation({ mutationFn: () => startCluster(region, clusterName), onSuccess: invalidate })
  const stop = useMutation({ mutationFn: () => stopCluster(region, clusterName), onSuccess: invalidate })

  const cluster = detail.data
  const busy = remove.isPending || start.isPending || stop.isPending
  const actionError = remove.error ?? start.error ?? stop.error
  // Cluster config carries gs:// executable files only on initializationActions;
  // filter to that field so a future keyed URI cannot be mislabelled here.
  const initActions = collectGsUris(cluster?.config).filter((r) => r.key === 'executableFile')
  // A job keeps its cluster name after the cluster is deleted, so a job -> cluster
  // link can land here on a 404. Say that plainly instead of a generic failure.
  const gone = detail.isError && detail.error instanceof APIError && detail.error.status === 404

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}>
        <IconButton component={RouterLink} to="/gcp/dataproc/clusters" aria-label="Back to clusters">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <Stack direction="row" spacing={0.5} sx={{ alignItems: 'center' }}>
            <GcpPageTitle id="dataproc">{clusterName}</GcpPageTitle>
            <CopyButton value={clusterName} label="cluster name" />
          </Stack>
          <Typography variant="body2" color="text.secondary">
            Dataproc · {region || '—'}
          </Typography>
        </Box>
        {cluster && (
          <>
            <Button startIcon={<AddIcon />} disabled={busy} onClick={() => setSubmittingJob(true)}>
              Submit job
            </Button>
            <Button component={RouterLink} to={`/gcp/dataproc/jobs?cluster=${encodeURIComponent(clusterName)}`}>
              Jobs
            </Button>
            {cluster.status === 'STOPPED' ? (
              <Button startIcon={<PlayArrowIcon />} disabled={busy} onClick={() => start.mutate()}>
                Start
              </Button>
            ) : (
              <Button
                startIcon={<StopIcon />}
                disabled={busy || cluster.status !== 'RUNNING'}
                onClick={() => stop.mutate()}
              >
                Stop
              </Button>
            )}
            <Button
              startIcon={<EditOutlinedIcon />}
              disabled={busy}
              onClick={() => setEditing(true)}
            >
              Edit
            </Button>
            <Button
              color="error"
              startIcon={<DeleteOutlineIcon />}
              disabled={busy}
              onClick={() => {
                remove.reset()
                setDeleting(true)
              }}
            >
              Delete
            </Button>
          </>
        )}
      </Stack>

      {gone && (
        <Alert severity="info" sx={{ mb: 2 }}>
          This cluster no longer exists. A Dataproc job may still reference it by name and UUID.
        </Alert>
      )}
      {detail.isError && !gone && <Alert severity="error">Failed to load the cluster.</Alert>}
      {actionError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(actionError as Error).message}
        </Alert>
      )}
      {detail.isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      )}

      {cluster && (
        <>
          <ClusterEditDialog open={editing} onClose={() => setEditing(false)} cluster={cluster} />
          <JobDialog
            open={submittingJob}
            onClose={() => setSubmittingJob(false)}
            defaultRegion={region}
            defaultCluster={clusterName}
          />
          <ConfirmDialog
            open={deleting}
            title="Delete cluster"
            confirmLabel="Delete cluster"
            body={
              <>
                Delete cluster <strong>{clusterName}</strong>? Its record is removed after the emulator
                finishes the delete operation. This cannot be undone.
              </>
            }
            busy={remove.isPending}
            error={remove.error?.message}
            onConfirm={() => remove.mutate()}
            onClose={() => {
              remove.reset()
              setDeleting(false)
            }}
          />
          <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2 }}>
            <Chip size="small" label={cluster.status || '—'} color={clusterStateColor(cluster.status)} />
            {cluster.gkeBacked && <Chip size="small" variant="outlined" label="Dataproc on GKE" />}
          </Stack>
          {cluster.statusDetail && (
            <Alert severity={cluster.status === 'ERROR' ? 'error' : 'info'} sx={{ mb: 2 }}>
              {cluster.statusDetail}
            </Alert>
          )}

          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(3, 1fr)' },
              gap: 2,
              mb: 3,
            }}
          >
            <Detail label="Region">{cluster.region}</Detail>
            {cluster.namespace && (
              <Detail label="Kubernetes namespace">
                {cluster.namespace}
                {cluster.namespaceOwned ? ' · created by emulator' : ''}
              </Detail>
            )}
            {cluster.gkeBacked &&
              cluster.kubernetesNamespace &&
              cluster.kubernetesNamespace !== cluster.namespace && (
                <Detail label="Requested namespace">{cluster.kubernetesNamespace}</Detail>
              )}
            <Detail label="Cluster UUID">
              {cluster.clusterUuid || '—'} <CopyButton value={cluster.clusterUuid} label="cluster UUID" />
            </Detail>
            <Detail label="Created">{shortDate(cluster.createTime)}</Detail>
            <Detail label="Updated">{shortDate(cluster.updateTime)}</Detail>
          </Box>

          {cluster.labels && Object.keys(cluster.labels).length > 0 && (
            <>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Labels
              </Typography>
              <Stack direction="row" spacing={1} sx={{ flexWrap: 'wrap', rowGap: 1, mb: 3 }}>
                {Object.entries(cluster.labels).map(([k, v]) => (
                  <Chip key={k} size="small" variant="outlined" label={`${k}=${v}`} />
                ))}
              </Stack>
            </>
          )}

          {initActions.length > 0 && (
            <>
              <Divider sx={{ mb: 2 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Initialization actions
              </Typography>
              <GsUriList resources={initActions} />
            </>
          )}

          {cluster.config != null && (
            <>
              <Divider sx={{ mb: 2 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Cluster config
              </Typography>
              <JsonBlock value={cluster.config} />
            </>
          )}

          {cluster.virtualClusterConfig != null && (
            <>
              <Divider sx={{ my: 3 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Virtual cluster config
              </Typography>
              <JsonBlock value={cluster.virtualClusterConfig} />
            </>
          )}

          <Divider sx={{ my: 3 }} />
          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Status timeline
          </Typography>
          <StatusTimeline
            history={cluster.statusHistory ?? []}
            current={{
              state: cluster.status,
              detail: cluster.statusDetail,
              stateStartTime: cluster.statusStartTime,
            }}
            colorFor={clusterStateColor}
          />
        </>
      )}
    </Box>
  )
}
