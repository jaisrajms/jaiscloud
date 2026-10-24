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
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import CleaningServicesOutlinedIcon from '@mui/icons-material/CleaningServicesOutlined'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import PauseIcon from '@mui/icons-material/Pause'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import PlayCircleOutlineIcon from '@mui/icons-material/PlayCircleOutlineOutlined'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import {
  deleteQueue,
  deleteTask,
  getQueue,
  getQueueIam,
  listTasks,
  pauseQueue,
  purgeQueue,
  putQueueIam,
  resumeQueue,
  runTask,
} from '../../api/gcp/tasks'
import { useAccount } from '../../context/AccountContext'
import { IamPolicyPanel } from '../common/IamPolicyPanel'
import { QueueDialog } from './QueueDialog'
import { TaskDialog } from './TaskDialog'
import { queueStateColor, rateLabel, shortDate, taskTargetLabel } from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** A small label/value row for the queue overview. */
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

/** A single Cloud Tasks queue: overview, tasks and IAM policy. */
export function QueueDetailPage() {
  const { location = '', queue: queueName = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [editOpen, setEditOpen] = useState(false)
  const [taskOpen, setTaskOpen] = useState(false)

  const key = ['gcp', 'tasks', 'queue', location, queueName, accountId]

  const detail = useQuery({
    queryKey: key,
    queryFn: () => getQueue(location, queueName),
    enabled: Boolean(location && queueName),
  })

  const tasks = useQuery({
    queryKey: ['gcp', 'tasks', 'queue', location, queueName, 'tasks', accountId],
    queryFn: () => listTasks(location, queueName),
    enabled: Boolean(location && queueName),
  })

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'tasks'] })

  const remove = useMutation({
    mutationFn: () => deleteQueue(location, queueName),
    onSuccess: () => {
      invalidate()
      navigate('/gcp/tasks/queues')
    },
  })
  const pause = useMutation({ mutationFn: () => pauseQueue(location, queueName), onSuccess: invalidate })
  const resume = useMutation({ mutationFn: () => resumeQueue(location, queueName), onSuccess: invalidate })
  const purge = useMutation({ mutationFn: () => purgeQueue(location, queueName), onSuccess: invalidate })
  const removeTask = useMutation({
    mutationFn: (task: string) => deleteTask(location, queueName, task),
    onSuccess: invalidate,
  })
  const runTaskMutation = useMutation({
    mutationFn: (task: string) => runTask(location, queueName, task),
    onSuccess: invalidate,
  })

  const queue = detail.data
  const busy = remove.isPending || pause.isPending || resume.isPending || purge.isPending
  const actionError = remove.error ?? pause.error ?? resume.error ?? purge.error
  const taskError = removeTask.error ?? runTaskMutation.error
  const taskBusy = removeTask.isPending || runTaskMutation.isPending

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}>
        <IconButton component={RouterLink} to="/gcp/tasks/queues" aria-label="Back to queues">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="tasks">{queueName}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Cloud Tasks · {location || '—'}
          </Typography>
        </Box>
        {queue && (
          <>
            {queue.state === 'PAUSED' ? (
              <Button startIcon={<PlayArrowIcon />} disabled={busy} onClick={() => resume.mutate()}>
                Resume
              </Button>
            ) : (
              <Button
                startIcon={<PauseIcon />}
                disabled={busy || queue.state !== 'RUNNING'}
                onClick={() => pause.mutate()}
              >
                Pause
              </Button>
            )}
            <Button startIcon={<CleaningServicesOutlinedIcon />} disabled={busy} onClick={() => purge.mutate()}>
              Purge tasks
            </Button>
            <Button startIcon={<EditOutlinedIcon />} onClick={() => setEditOpen(true)}>
              Edit
            </Button>
            <Button color="error" startIcon={<DeleteOutlineIcon />} disabled={busy} onClick={() => remove.mutate()}>
              Delete
            </Button>
          </>
        )}
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the queue.</Alert>}
      {actionError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(actionError as Error).message}
        </Alert>
      )}
      {taskError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(taskError as Error).message}
        </Alert>
      )}
      {detail.isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      )}

      {queue && (
        <>
          <Box sx={{ mb: 2 }}>
            <Chip size="small" label={queue.state || '—'} color={queueStateColor(queue.state)} />
          </Box>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(3, 1fr)' },
              gap: 2,
            }}
          >
            <Detail label="Dispatch rate">{rateLabel(queue)}</Detail>
            <Detail label="Max burst size">{queue.maxBurstSize}</Detail>
            <Detail label="Max attempts">{queue.maxAttempts}</Detail>
            <Detail label="Min backoff">{queue.minBackoff}</Detail>
            <Detail label="Max backoff">{queue.maxBackoff}</Detail>
            <Detail label="Max retry duration">{queue.maxRetryDuration || 'unbounded'}</Detail>
            <Detail label="Max doublings">{queue.maxDoublings}</Detail>
            <Detail label="Last purged">{shortDate(queue.purgeTime)}</Detail>
          </Box>

          <Divider sx={{ my: 3 }} />

          <Stack direction="row" sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
            <Typography variant="subtitle1">Tasks</Typography>
            <Button size="small" startIcon={<AddIcon />} onClick={() => setTaskOpen(true)}>
              Create task
            </Button>
          </Stack>
          {tasks.isError && <Alert severity="error">Failed to load tasks.</Alert>}
          <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Name</TableCell>
                  <TableCell>Target</TableCell>
                  <TableCell>Schedule time</TableCell>
                  <TableCell>Dispatches</TableCell>
                  <TableCell>Last status</TableCell>
                  <TableCell align="right">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {tasks.isLoading && (
                  <TableRow>
                    <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                      <CircularProgress size={24} />
                    </TableCell>
                  </TableRow>
                )}
                {!tasks.isLoading && (tasks.data?.tasks.length ?? 0) === 0 && (
                  <TableRow>
                    <TableCell colSpan={6} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                      No tasks in this queue.
                    </TableCell>
                  </TableRow>
                )}
                {tasks.data?.tasks.map((task) => (
                  <TableRow key={task.name} hover>
                    <TableCell sx={{ overflowWrap: 'anywhere' }}>{task.name}</TableCell>
                    <TableCell sx={{ maxWidth: 260, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {taskTargetLabel(task)}
                    </TableCell>
                    <TableCell>{shortDate(task.scheduleTime)}</TableCell>
                    <TableCell>{task.dispatchCount}</TableCell>
                    <TableCell>
                      {task.lastAttemptStatus
                        ? `${task.lastAttemptStatus.code}${task.lastAttemptStatus.message ? ` · ${task.lastAttemptStatus.message}` : ''}`
                        : '—'}
                    </TableCell>
                    <TableCell align="right">
                      <Tooltip title="Run now">
                        <span>
                          <IconButton
                            size="small"
                            disabled={taskBusy}
                            onClick={() => runTaskMutation.mutate(task.name)}
                            aria-label={`Run ${task.name}`}
                          >
                            <PlayCircleOutlineIcon fontSize="small" />
                          </IconButton>
                        </span>
                      </Tooltip>
                      <Tooltip title="Delete task">
                        <span>
                          <IconButton
                            size="small"
                            disabled={taskBusy}
                            onClick={() => removeTask.mutate(task.name)}
                            aria-label={`Delete ${task.name}`}
                          >
                            <DeleteOutlineIcon fontSize="small" />
                          </IconButton>
                        </span>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>

          <Divider sx={{ my: 3 }} />

          <IamPolicyPanel
            title="Queue IAM policy"
            queryKey={['gcp', 'tasks', 'queue', location, queueName, 'iam', accountId]}
            load={() => getQueueIam(location, queueName)}
            save={(policy) => putQueueIam(location, queueName, policy)}
            defaultRole="roles/cloudtasks.enqueuer"
          />

          <QueueDialog open={editOpen} onClose={() => setEditOpen(false)} queue={queue} />
          <TaskDialog
            open={taskOpen}
            onClose={() => setTaskOpen(false)}
            location={location}
            queue={queueName}
          />
        </>
      )}
    </Box>
  )
}
