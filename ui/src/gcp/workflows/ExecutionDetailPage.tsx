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
import CancelOutlinedIcon from '@mui/icons-material/CancelOutlined'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { cancelExecution, getExecution } from '../../api/gcp/workflows'
import { useAccount } from '../../context/AccountContext'
import { executionStateColor, shortDate } from './util'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { GcpPageTitle } from '../common/PageTitle'

/** A small label/value row for the execution overview. */
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

/** A read-only code block for JSON argument/result/error payloads. */
function CodeBlock({ value }: { value?: string }) {
  if (!value) return <Typography variant="body2">—</Typography>
  return <GcpCodeEditor value={value} readOnly language="json" minRows={4} ariaLabel="JSON" />
}

/** A single workflow execution: state, argument, result and error. */
export function ExecutionDetailPage() {
  const { location = '', workflow: workflowID = '', execution: executionID = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()

  const detail = useQuery({
    queryKey: ['gcp', 'workflows', 'execution', location, workflowID, executionID, accountId],
    queryFn: () => getExecution(location, workflowID, executionID),
    enabled: Boolean(location && workflowID && executionID),
  })

  const cancel = useMutation({
    mutationFn: () => cancelExecution(location, workflowID, executionID),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'workflows'] }),
  })

  const execution = detail.data
  const backTo = `/gcp/workflows/${encodeURIComponent(location)}/${encodeURIComponent(workflowID)}`

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}>
        <IconButton component={RouterLink} to={backTo} aria-label="Back to workflow">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="workflows">{executionID}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Workflow execution · {workflowID} · {location || '—'}
          </Typography>
        </Box>
        {execution?.state === 'ACTIVE' && (
          <Button
            startIcon={<CancelOutlinedIcon />}
            disabled={cancel.isPending}
            onClick={() => cancel.mutate()}
          >
            Cancel
          </Button>
        )}
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the execution.</Alert>}
      {cancel.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(cancel.error as Error).message}
        </Alert>
      )}
      {detail.isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      )}

      {execution && (
        <>
          <Box sx={{ mb: 2 }}>
            <Chip
              size="small"
              label={execution.state || '—'}
              color={executionStateColor(execution.state)}
            />
          </Box>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(3, 1fr)' },
              gap: 2,
            }}
          >
            <Detail label="Started">{shortDate(execution.startTime)}</Detail>
            <Detail label="Ended">{shortDate(execution.endTime)}</Detail>
            <Detail label="Duration">{execution.duration}</Detail>
            <Detail label="Workflow revision">{execution.workflowRevisionId}</Detail>
            <Detail label="Call log level">{execution.callLogLevel}</Detail>
            <Detail label="Labels">
              {execution.labels ? Object.entries(execution.labels).map(([k, v]) => `${k}=${v}`).join(', ') : ''}
            </Detail>
          </Box>

          <Divider sx={{ my: 3 }} />

          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Argument
          </Typography>
          <CodeBlock value={execution.argument} />

          <Divider sx={{ my: 3 }} />

          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Result
          </Typography>
          <CodeBlock value={execution.result} />

          {execution.error && (
            <>
              <Divider sx={{ my: 3 }} />
              <Typography variant="subtitle1" color="error" sx={{ mb: 1 }}>
                Error
              </Typography>
              <Typography variant="body2" sx={{ mb: 1 }}>
                {execution.error.context || '—'}
              </Typography>
              <CodeBlock value={execution.error.payload} />
            </>
          )}
        </>
      )}
    </Box>
  )
}
