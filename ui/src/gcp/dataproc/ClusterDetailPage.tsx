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
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import StopIcon from '@mui/icons-material/Stop'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import { deleteCluster, getCluster, startCluster, stopCluster } from '../../api/gcp/dataproc'
import { useAccount } from '../../context/AccountContext'
import { Detail, JsonBlock } from './common'
import { clusterStateColor, shortDate } from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** A single Dataproc cluster: overview, config and status history. */
export function ClusterDetailPage() {
  const { region = '', cluster: clusterName = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()

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

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}>
        <IconButton component={RouterLink} to="/gcp/dataproc/clusters" aria-label="Back to clusters">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="dataproc">{clusterName}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Dataproc · {region || '—'}
          </Typography>
        </Box>
        {cluster && (
          <>
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
              color="error"
              startIcon={<DeleteOutlineIcon />}
              disabled={busy}
              onClick={() => remove.mutate()}
            >
              Delete
            </Button>
          </>
        )}
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the cluster.</Alert>}
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
          <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2 }}>
            <Chip size="small" label={cluster.status || '—'} color={clusterStateColor(cluster.status)} />
            {cluster.gkeBacked && <Chip size="small" variant="outlined" label="Dataproc on GKE" />}
          </Stack>
          {cluster.statusDetail && (
            <Alert severity="info" sx={{ mb: 2 }}>
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
            <Detail label="Cluster UUID">{cluster.clusterUuid}</Detail>
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

          {(cluster.statusHistory?.length ?? 0) > 0 && (
            <>
              <Divider sx={{ my: 3 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Status history
              </Typography>
              <Stack spacing={1}>
                {cluster.statusHistory?.map((event, i) => (
                  <Stack key={`${event.state}-${i}`} direction="row" spacing={2} sx={{ alignItems: 'baseline' }}>
                    <Chip size="small" label={event.state} color={clusterStateColor(event.state)} />
                    <Typography variant="body2" color="text.secondary">
                      {shortDate(event.stateStartTime)}
                    </Typography>
                    {event.detail && <Typography variant="body2">{event.detail}</Typography>}
                  </Stack>
                ))}
              </Stack>
            </>
          )}
        </>
      )}
    </Box>
  )
}
