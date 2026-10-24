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
  Link,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import { deleteWorkflow, getWorkflow, listExecutions } from '../../api/gcp/workflows'
import { useAccount } from '../../context/AccountContext'
import { RunExecutionDialog } from './RunExecutionDialog'
import { WorkflowDialog } from './WorkflowDialog'
import {
  callLogLevelOptions,
  executionStateColor,
  executionSummary,
  shortDate,
  workflowStateColor,
} from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** A small label/value row for the workflow overview. */
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

/** A read-only, scrollable code block. */
function CodeBlock({ value }: { value?: string }) {
  if (!value) return <Typography variant="body2">—</Typography>
  return (
    <Box
      component="pre"
      sx={{
        m: 0,
        p: 2,
        bgcolor: 'action.hover',
        borderRadius: 1,
        overflow: 'auto',
        fontSize: 13,
        fontFamily: 'monospace',
        maxHeight: 320,
      }}
    >
      {value}
    </Box>
  )
}

function callLogLevelLabel(value?: string): string {
  return callLogLevelOptions.find((option) => option.value === (value ?? ''))?.label ?? value ?? '—'
}

/** A single workflow: overview, source, executions and lifecycle actions. */
export function WorkflowDetailPage() {
  const { location = '', workflow: workflowID = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [editOpen, setEditOpen] = useState(false)
  const [runOpen, setRunOpen] = useState(false)

  const key = ['gcp', 'workflows', 'workflow', location, workflowID, accountId]

  const detail = useQuery({
    queryKey: key,
    queryFn: () => getWorkflow(location, workflowID),
    enabled: Boolean(location && workflowID),
  })

  const executions = useQuery({
    queryKey: ['gcp', 'workflows', 'workflow', location, workflowID, 'executions', accountId],
    queryFn: () => listExecutions(location, workflowID),
    enabled: Boolean(location && workflowID),
  })

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'workflows'] })

  const remove = useMutation({
    mutationFn: () => deleteWorkflow(location, workflowID),
    onSuccess: () => {
      invalidate()
      navigate('/gcp/workflows')
    },
  })

  const workflow = detail.data
  const actionError = remove.error

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}>
        <IconButton component={RouterLink} to="/gcp/workflows" aria-label="Back to workflows">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="workflows">{workflowID}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Workflows · {location || '—'}
          </Typography>
        </Box>
        {workflow && (
          <>
            <Button startIcon={<PlayArrowIcon />} onClick={() => setRunOpen(true)}>
              Run
            </Button>
            <Button startIcon={<EditOutlinedIcon />} onClick={() => setEditOpen(true)}>
              Edit
            </Button>
            <Button
              color="error"
              startIcon={<DeleteOutlineIcon />}
              disabled={remove.isPending}
              onClick={() => remove.mutate()}
            >
              Delete
            </Button>
          </>
        )}
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the workflow.</Alert>}
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

      {workflow && (
        <>
          <Box sx={{ mb: 2 }}>
            <Chip
              size="small"
              label={workflow.state || '—'}
              color={workflowStateColor(workflow.state)}
            />
          </Box>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(3, 1fr)' },
              gap: 2,
            }}
          >
            <Detail label="Revision">
              <Box component="code">{workflow.revisionId || '—'}</Box>
            </Detail>
            <Detail label="Service account">{workflow.serviceAccount}</Detail>
            <Detail label="Call log level">{callLogLevelLabel(workflow.callLogLevel)}</Detail>
            <Detail label="Created">{shortDate(workflow.createTime)}</Detail>
            <Detail label="Updated">{shortDate(workflow.updateTime)}</Detail>
            <Detail label="Description">{workflow.description}</Detail>
          </Box>

          <Divider sx={{ my: 3 }} />

          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Source
          </Typography>
          <CodeBlock value={workflow.sourceContents} />

          <Divider sx={{ my: 3 }} />

          <Stack direction="row" sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
            <Typography variant="subtitle1">Executions</Typography>
            <Button size="small" startIcon={<PlayArrowIcon />} onClick={() => setRunOpen(true)}>
              Run workflow
            </Button>
          </Stack>
          {executions.isError && <Alert severity="error">Failed to load executions.</Alert>}
          <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Execution</TableCell>
                  <TableCell>State</TableCell>
                  <TableCell>Started</TableCell>
                  <TableCell>Duration</TableCell>
                  <TableCell>Summary</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {executions.isLoading && (
                  <TableRow>
                    <TableCell colSpan={5} align="center" sx={{ py: 4 }}>
                      <CircularProgress size={24} />
                    </TableCell>
                  </TableRow>
                )}
                {!executions.isLoading && (executions.data?.executions.length ?? 0) === 0 && (
                  <TableRow>
                    <TableCell colSpan={5} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                      No executions yet. Run the workflow to create one.
                    </TableCell>
                  </TableRow>
                )}
                {executions.data?.executions.map((execution) => (
                  <TableRow key={execution.id} hover>
                    <TableCell>
                      <Link
                        component={RouterLink}
                        to={`/gcp/workflows/${encodeURIComponent(location)}/${encodeURIComponent(workflowID)}/executions/${encodeURIComponent(execution.id)}`}
                      >
                        {execution.id}
                      </Link>
                    </TableCell>
                    <TableCell>
                      <Chip
                        size="small"
                        label={execution.state || '—'}
                        color={executionStateColor(execution.state)}
                      />
                    </TableCell>
                    <TableCell>{shortDate(execution.startTime)}</TableCell>
                    <TableCell>{execution.duration || '—'}</TableCell>
                    <TableCell sx={{ maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {executionSummary(execution)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>

          <WorkflowDialog open={editOpen} onClose={() => setEditOpen(false)} workflow={workflow} />
          <RunExecutionDialog
            open={runOpen}
            onClose={() => setRunOpen(false)}
            location={location}
            workflow={workflowID}
          />
        </>
      )}
    </Box>
  )
}
