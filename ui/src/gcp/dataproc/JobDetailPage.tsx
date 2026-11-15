import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  IconButton,
  Link,
  Stack,
  Tooltip,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import CancelOutlinedIcon from '@mui/icons-material/CancelOutlined'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { cancelJob, getJob, getJobDriverOutput } from '../../api/gcp/dataproc'
import { useAccount } from '../../context/AccountContext'
import {
  ConfirmDialog,
  CopyButton,
  Detail,
  GsUriLink,
  GsUriList,
  JsonBlock,
  StatusTimeline,
} from './common'
import {
  collectGsUris,
  gcsBucketPath,
  jobStateColor,
  jobTypeLabel,
  parseGsUri,
  shortDate,
} from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** True for a job that has reached a terminal state (dataproc.v1 Job.done). */
function isTerminal(state?: string): boolean {
  return ['DONE', 'ERROR', 'CANCELLED'].includes(state ?? '')
}

/** A single Dataproc job: overview, type-specific body and status history. */
export function JobDetailPage() {
  const { region = '', job: jobID = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [logsOpen, setLogsOpen] = useState(false)
  const [canceling, setCanceling] = useState(false)

  const detail = useQuery({
    queryKey: ['gcp', 'dataproc', 'job', region, jobID, accountId],
    queryFn: () => getJob(region, jobID),
    enabled: Boolean(region && jobID),
  })

  const logs = useQuery({
    queryKey: ['gcp', 'dataproc', 'job', region, jobID, 'driver-output', accountId],
    queryFn: () => getJobDriverOutput(region, jobID),
    enabled: logsOpen,
    retry: false,
  })

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })

  const cancel = useMutation({
    mutationFn: () => cancelJob(region, jobID),
    onSuccess: () => {
      invalidate()
      setCanceling(false)
    },
  })

  const job = detail.data
  const resources = collectGsUris(job?.typeJob)
  const outputGs = parseGsUri(job?.driverOutputResourceUri)
  const syntheticOutput =
    logs.data?.startsWith(`Dataproc job ${jobID} finished with state `) ?? false

  const downloadLogs = () => {
    if (logs.data === undefined) return
    const blob = new Blob([logs.data], { type: 'text/plain;charset=utf-8' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${jobID}-driver-output.txt`
    document.body.appendChild(a)
    a.click()
    a.remove()
    // Some browsers cancel the download if the blob URL is revoked in the same
    // turn as the click; release it on the next task.
    setTimeout(() => URL.revokeObjectURL(url), 0)
  }

  const errorState = job?.status === 'ERROR' || job?.status === 'ATTEMPT_FAILURE'

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}>
        <IconButton component={RouterLink} to="/gcp/dataproc/jobs" aria-label="Back to jobs">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <Stack direction="row" spacing={0.5} sx={{ alignItems: 'center' }}>
            <GcpPageTitle id="dataproc">{jobID}</GcpPageTitle>
            <CopyButton value={jobID} label="job ID" />
          </Stack>
          <Typography variant="body2" color="text.secondary">
            Dataproc job · {region || '—'}
          </Typography>
        </Box>
        {job && !isTerminal(job.status) && (
          <Button
            color="error"
            startIcon={<CancelOutlinedIcon />}
            disabled={cancel.isPending}
            onClick={() => {
              cancel.reset()
              setCanceling(true)
            }}
          >
            Cancel
          </Button>
        )}
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the job.</Alert>}
      {cancel.error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(cancel.error as Error).message}
        </Alert>
      )}
      {detail.isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      )}

      {job && (
        <>
          <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2 }}>
            <Chip size="small" label={job.status || '—'} color={jobStateColor(job.status)} />
            {job.substate && <Chip size="small" variant="outlined" label={job.substate} />}
          </Stack>
          {job.statusDetail && (
            <Alert severity={errorState ? 'error' : 'info'} sx={{ mb: 2 }}>
              {job.statusDetail}
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
            <Detail label="Region">{job.region}</Detail>
            <Detail label="Cluster">
              {job.clusterName ? (
                <Link
                  component={RouterLink}
                  to={`/gcp/dataproc/clusters/${encodeURIComponent(job.region)}/${encodeURIComponent(job.clusterName)}`}
                >
                  {job.clusterName}
                </Link>
              ) : (
                '—'
              )}
            </Detail>
            <Detail label="Type">{jobTypeLabel(job.type)}</Detail>
            <Detail label="Job UUID">
              {job.jobUuid || '—'} <CopyButton value={job.jobUuid} label="job UUID" />
            </Detail>
            <Detail label="Created">{shortDate(job.createTime)}</Detail>
          </Box>

          {job.labels && Object.keys(job.labels).length > 0 && (
            <>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Labels
              </Typography>
              <Stack direction="row" spacing={1} sx={{ flexWrap: 'wrap', rowGap: 1, mb: 3 }}>
                {Object.entries(job.labels).map(([k, v]) => (
                  <Chip key={k} size="small" variant="outlined" label={`${k}=${v}`} />
                ))}
              </Stack>
            </>
          )}

          {resources.length > 0 && (
            <>
              <Divider sx={{ mb: 2 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Job resources
              </Typography>
              <GsUriList resources={resources} />
            </>
          )}

          <Divider sx={{ mb: 2 }} />
          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Job details
          </Typography>
          <JsonBlock value={job.typeJob} />

          {(job.driverOutputResourceUri || job.driverControlFilesUri) && (
            <>
              <Divider sx={{ my: 3 }} />
              <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 1 }}>
                <Typography variant="subtitle1">Driver output</Typography>
                <Tooltip title={isTerminal(job.status) ? '' : 'Available when the job finishes'}>
                  <span>
                    <Button
                      size="small"
                      disabled={!isTerminal(job.status) || !job.driverOutputResourceUri}
                      onClick={() => setLogsOpen(true)}
                    >
                      View driver output
                    </Button>
                  </span>
                </Tooltip>
              </Stack>
              <Box
                sx={{
                  display: 'grid',
                  gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)' },
                  gap: 2,
                }}
              >
                <Detail label="Output URI">
                  <GsUriLink uri={job.driverOutputResourceUri} />
                </Detail>
                <Detail label="Control files URI">
                  <GsUriLink uri={job.driverControlFilesUri} />
                </Detail>
              </Box>
            </>
          )}

          <Divider sx={{ my: 3 }} />
          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Status timeline
          </Typography>
          <StatusTimeline
            history={job.statusHistory ?? []}
            current={{
              state: job.status,
              detail: job.statusDetail,
              substate: job.substate,
              stateStartTime: job.statusStartTime,
            }}
            colorFor={jobStateColor}
          />

          <ConfirmDialog
            open={canceling}
            title="Cancel job"
            confirmLabel="Cancel job"
            body={
              <>
                Cancel job <strong>{jobID}</strong>? The driver is stopped and the job becomes
                CANCELLED.
              </>
            }
            busy={cancel.isPending}
            error={cancel.error?.message}
            onConfirm={() => cancel.mutate()}
            onClose={() => {
              cancel.reset()
              setCanceling(false)
            }}
          />

          <Dialog open={logsOpen} onClose={() => setLogsOpen(false)} fullWidth maxWidth="lg">
            <DialogTitle>Driver output</DialogTitle>
            <DialogContent dividers>
              {logs.isLoading && (
                <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
                  <CircularProgress />
                </Box>
              )}
              {logs.isError && (
                <Alert severity="error">
                  Could not load the driver output: {(logs.error as Error).message}
                </Alert>
              )}
              {syntheticOutput && (
                <Alert severity="info" sx={{ mb: 2 }}>
                  This is the emulator&apos;s synthetic mock-mode output (no Spark driver ran),
                  not a real driver log.
                </Alert>
              )}
              {logs.data !== undefined && (
                <Box
                  component="pre"
                  sx={{
                    m: 0,
                    fontSize: 12,
                    whiteSpace: 'pre-wrap',
                    overflowWrap: 'anywhere',
                    maxHeight: 480,
                    overflow: 'auto',
                  }}
                >
                  {logs.data || 'No output was captured.'}
                </Box>
              )}
            </DialogContent>
            <DialogActions>
              {outputGs && (
                <Button
                  component={RouterLink}
                  to={gcsBucketPath(outputGs.bucket, outputGs.object)}
                  target="_blank"
                  rel="noreferrer"
                >
                  Open in Cloud Storage
                </Button>
              )}
              <Button onClick={downloadLogs} disabled={logs.data === undefined}>
                Download
              </Button>
              <Button onClick={() => setLogsOpen(false)}>Close</Button>
            </DialogActions>
          </Dialog>
        </>
      )}
    </Box>
  )
}
