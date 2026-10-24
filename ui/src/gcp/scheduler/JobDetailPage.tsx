import { useState } from 'react'
import type { ReactNode } from 'react'
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
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import PauseIcon from '@mui/icons-material/Pause'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import PlayCircleOutlineIcon from '@mui/icons-material/PlayCircleOutlineOutlined'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import {
  deleteJob,
  getJob,
  pauseJob,
  resumeJob,
  runJob,
} from '../../api/gcp/scheduler'
import { useAccount } from '../../context/AccountContext'
import { JobDialog } from './JobDialog'
import { shortDate, stateColor, targetLabel } from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** A small label/value row for the job overview. */
function Detail({ label, children }: { label: string; children: ReactNode }) {
  return (
    <Stack spacing={0.5}>
      <Typography variant="caption" color="text.secondary">
        {label}
      </Typography>
      <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
        {children || '—'}
      </Typography>
    </Stack>
  )
}

/** A single Cloud Scheduler job: overview plus state actions. */
export function JobDetailPage() {
  const { location = '', job: jobName = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [editOpen, setEditOpen] = useState(false)

  const key = ['gcp', 'scheduler', 'job', location, jobName, accountId]

  const detail = useQuery({
    queryKey: key,
    queryFn: () => getJob(location, jobName),
    enabled: Boolean(location && jobName),
  })

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'scheduler'] })

  const remove = useMutation({
    mutationFn: () => deleteJob(location, jobName),
    onSuccess: () => {
      invalidate()
      navigate('/gcp/scheduler/jobs')
    },
  })
  const pause = useMutation({ mutationFn: () => pauseJob(location, jobName), onSuccess: invalidate })
  const resume = useMutation({ mutationFn: () => resumeJob(location, jobName), onSuccess: invalidate })
  const run = useMutation({ mutationFn: () => runJob(location, jobName), onSuccess: invalidate })

  const job = detail.data
  const busy = remove.isPending || pause.isPending || resume.isPending || run.isPending
  const actionError = remove.error ?? pause.error ?? resume.error ?? run.error

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton component={RouterLink} to="/gcp/scheduler/jobs" aria-label="Back to jobs">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="scheduler">{jobName}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Cloud Scheduler · {location || '—'}
          </Typography>
        </Box>
        {job && (
          <>
            <Button
              startIcon={<PlayCircleOutlineIcon />}
              disabled={busy}
              onClick={() => run.mutate()}
            >
              Run now
            </Button>
            {job.state === 'PAUSED' ? (
              <Button startIcon={<PlayArrowIcon />} disabled={busy} onClick={() => resume.mutate()}>
                Resume
              </Button>
            ) : (
              <Button
                startIcon={<PauseIcon />}
                disabled={busy || job.state !== 'ENABLED'}
                onClick={() => pause.mutate()}
              >
                Pause
              </Button>
            )}
            <Button startIcon={<EditOutlinedIcon />} onClick={() => setEditOpen(true)}>
              Edit
            </Button>
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

      {detail.isError && <Alert severity="error">Failed to load the job.</Alert>}
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

      {job && (
        <>
          <Box sx={{ mb: 2 }}>
            <Chip size="small" label={job.state || '—'} color={stateColor(job.state)} />
          </Box>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(3, 1fr)' },
              gap: 2,
            }}
          >
            <Detail label="Schedule">{job.schedule}</Detail>
            <Detail label="Time zone">{job.timeZone}</Detail>
            <Detail label="Target">{targetLabel(job)}</Detail>
            <Detail label="Next run">{shortDate(job.scheduleTime)}</Detail>
            <Detail label="Last attempt">{shortDate(job.lastAttemptTime)}</Detail>
            <Detail label="Updated">{shortDate(job.userUpdateTime)}</Detail>
            <Detail label="Retry count">{job.retryCount ?? 0}</Detail>
            <Detail label="Attempt deadline">{job.attemptDeadline}</Detail>
            <Detail label="Description">{job.description}</Detail>
          </Box>

          {job.target === 'http' && (
            <>
              <Divider sx={{ my: 3 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                HTTP target
              </Typography>
              <Box
                sx={{
                  display: 'grid',
                  gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)' },
                  gap: 2,
                }}
              >
                <Detail label="URI">{job.httpUri}</Detail>
                <Detail label="Method">{job.httpMethod}</Detail>
              </Box>
              {job.httpBody && <Detail label="Body">{job.httpBody}</Detail>}
            </>
          )}

          {job.target === 'pubsub' && (
            <>
              <Divider sx={{ my: 3 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Pub/Sub target
              </Typography>
              <Detail label="Topic">{job.pubsubTopic}</Detail>
              {job.pubsubData && <Detail label="Data">{job.pubsubData}</Detail>}
            </>
          )}

          {job.target === 'appengine' && (
            <>
              <Divider sx={{ my: 3 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                App Engine HTTP target
              </Typography>
              <Box
                sx={{
                  display: 'grid',
                  gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)' },
                  gap: 2,
                }}
              >
                <Detail label="Relative URI">{job.appEngineUri}</Detail>
                <Detail label="Method">{job.appEngineMethod}</Detail>
              </Box>
            </>
          )}

          {job.lastStatus && (
            <>
              <Divider sx={{ my: 3 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Last status
              </Typography>
              <Detail label="Code">{String(job.lastStatus.code)}</Detail>
              <Detail label="Message">{job.lastStatus.message}</Detail>
            </>
          )}

          <JobDialog open={editOpen} onClose={() => setEditOpen(false)} job={job} />
        </>
      )}
    </Box>
  )
}
