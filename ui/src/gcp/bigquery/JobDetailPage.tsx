import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Button, CircularProgress, IconButton, Stack, Typography } from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import { cancelJob, deleteJob, getJob } from '../../api/gcp/bigquery'
import { useAccount } from '../../context/AccountContext'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { GcpPageTitle } from '../common/PageTitle'

/** One BigQuery job: the full wire object, with cancel and delete. */
export function JobDetailPage() {
  const { job = '' } = useParams()
  const { accountId } = useAccount()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'bigquery'] })

  const detail = useQuery({
    queryKey: ['gcp', 'bigquery', 'job', job, accountId],
    queryFn: () => getJob(job),
    enabled: Boolean(job),
  })

  const cancel = useMutation({ mutationFn: () => cancelJob(job), onSuccess: invalidate })
  const remove = useMutation({
    mutationFn: () => deleteJob(job),
    onSuccess: () => {
      invalidate()
      navigate('/gcp/bigquery/jobs')
    },
  })

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton component={RouterLink} to="/gcp/bigquery/jobs" aria-label="Back to jobs">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="bigquery">{job}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            BigQuery job · project {accountId || '—'}
          </Typography>
        </Box>
        <Button
          variant="outlined"
          disabled={cancel.isPending}
          onClick={() => cancel.mutate()}
        >
          Cancel
        </Button>
        <Button
          variant="contained"
          color="error"
          disabled={remove.isPending}
          onClick={() => remove.mutate()}
        >
          Delete
        </Button>
      </Stack>

      {(detail.isError || cancel.isError || remove.isError) && (
        <Alert severity="error" sx={{ mb: 2 }}>
          A job action failed.
        </Alert>
      )}

      {detail.isLoading && <CircularProgress size={24} />}
      {detail.data && (
        <GcpCodeEditor
          value={JSON.stringify(detail.data, null, 2)}
          readOnly
          language="json"
          minRows={20}
          ariaLabel="Job JSON"
        />
      )}
    </Box>
  )
}
