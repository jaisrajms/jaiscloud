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
import CancelOutlinedIcon from '@mui/icons-material/CancelOutlined'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { cancelJob, getJob } from '../../api/gcp/dataproc'
import { useAccount } from '../../context/AccountContext'
import { Detail, JsonBlock } from './common'
import { jobStateColor, jobTypeLabel, shortDate } from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** True for a job that has reached a terminal state and cannot be cancelled. */
function isTerminal(state?: string): boolean {
  return ['DONE', 'ERROR', 'CANCELLED', 'ATTEMPT_FAILURE'].includes(state ?? '')
}

/** A single Dataproc job: overview, type-specific body and status history. */
export function JobDetailPage() {
  const { region = '', job: jobID = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()

  const detail = useQuery({
    queryKey: ['gcp', 'dataproc', 'job', region, jobID, accountId],
    queryFn: () => getJob(region, jobID),
    enabled: Boolean(region && jobID),
  })

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })

  const cancel = useMutation({ mutationFn: () => cancelJob(region, jobID), onSuccess: invalidate })

  const job = detail.data

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}>
        <IconButton component={RouterLink} to="/gcp/dataproc/jobs" aria-label="Back to jobs">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="dataproc">{jobID}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Dataproc job · {region || '—'}
          </Typography>
        </Box>
        {job && !isTerminal(job.status) && (
          <Button
            color="error"
            startIcon={<CancelOutlinedIcon />}
            disabled={cancel.isPending}
            onClick={() => cancel.mutate()}
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
            <Alert severity="info" sx={{ mb: 2 }}>
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
            <Detail label="Cluster">{job.clusterName}</Detail>
            <Detail label="Type">{jobTypeLabel(job.type)}</Detail>
            <Detail label="Job UUID">{job.jobUuid}</Detail>
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

          <Divider sx={{ mb: 2 }} />
          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Job details
          </Typography>
          <JsonBlock value={job.typeJob} />

          {(job.driverOutputResourceUri || job.driverControlFilesUri) && (
            <>
              <Divider sx={{ my: 3 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Driver output
              </Typography>
              <Box
                sx={{
                  display: 'grid',
                  gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)' },
                  gap: 2,
                }}
              >
                <Detail label="Output URI">{job.driverOutputResourceUri}</Detail>
                <Detail label="Control files URI">{job.driverControlFilesUri}</Detail>
              </Box>
            </>
          )}

          {(job.statusHistory?.length ?? 0) > 0 && (
            <>
              <Divider sx={{ my: 3 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Status history
              </Typography>
              <Stack spacing={1}>
                {job.statusHistory?.map((event, i) => (
                  <Stack key={`${event.state}-${i}`} direction="row" spacing={2} sx={{ alignItems: 'baseline' }}>
                    <Chip size="small" label={event.state} color={jobStateColor(event.state)} />
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
