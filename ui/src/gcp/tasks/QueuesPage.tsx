import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, Chip, IconButton, Link, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import PauseIcon from '@mui/icons-material/Pause'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import CleaningServicesOutlinedIcon from '@mui/icons-material/CleaningServicesOutlined'
import { Link as RouterLink } from 'react-router-dom'
import {
  deleteQueue,
  listQueues,
  pauseQueue,
  purgeQueue,
  resumeQueue,
  type TaskQueue,
} from '../../api/gcp/tasks'
import { useAccount } from '../../context/AccountContext'
import { QueueDialog } from './QueueDialog'
import { queueStateColor, rateLabel, shortDate } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function target(queue: TaskQueue) {
  return { location: queue.location, queue: queue.name }
}

/** Cloud Tasks queues across every location, with pause/resume/purge/delete. */
export function QueuesPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'tasks'] })

  const queues = useQuery({
    queryKey: ['gcp', 'tasks', 'queues', accountId],
    queryFn: listQueues,
  })

  const remove = useMutation({
    mutationFn: ({ location, queue }: { location: string; queue: string }) =>
      deleteQueue(location, queue),
    onSuccess: invalidate,
  })
  const pause = useMutation({
    mutationFn: ({ location, queue }: { location: string; queue: string }) =>
      pauseQueue(location, queue),
    onSuccess: invalidate,
  })
  const resume = useMutation({
    mutationFn: ({ location, queue }: { location: string; queue: string }) =>
      resumeQueue(location, queue),
    onSuccess: invalidate,
  })
  const purge = useMutation({
    mutationFn: ({ location, queue }: { location: string; queue: string }) =>
      purgeQueue(location, queue),
    onSuccess: invalidate,
  })

  const busy = remove.isPending || pause.isPending || resume.isPending || purge.isPending
  const actionError = remove.error ?? pause.error ?? resume.error ?? purge.error

  const rows = filterRows(queues.data?.queues ?? [], filter, (queue) =>
    `${queue.name} ${queue.location} ${queue.state}`,
  )

  const columns: GcpColumn<TaskQueue>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (queue) => queue.name,
      render: (queue) => (
        <Link
          component={RouterLink}
          to={`/gcp/tasks/queues/${encodeURIComponent(queue.location)}/${encodeURIComponent(queue.name)}`}
        >
          {queue.name}
        </Link>
      ),
    },
    {
      key: 'location',
      header: 'Location',
      sortable: true,
      sortValue: (queue) => queue.location ?? null,
      render: (queue) => queue.location || '—',
    },
    {
      key: 'state',
      header: 'State',
      sortable: true,
      sortValue: (queue) => queue.state ?? null,
      render: (queue) => (
        <Chip size="small" label={queue.state || '—'} color={queueStateColor(queue.state)} />
      ),
    },
    {
      key: 'dispatchRate',
      header: 'Dispatch rate',
      sortable: true,
      sortValue: (queue) => rateLabel(queue),
      render: (queue) => rateLabel(queue),
    },
    {
      key: 'maxAttempts',
      header: 'Retry attempts',
      sortable: true,
      sortValue: (queue) => queue.maxAttempts ?? null,
      render: (queue) => queue.maxAttempts ?? '—',
    },
    {
      key: 'purgeTime',
      header: 'Last purged',
      sortable: true,
      sortValue: (queue) => queue.purgeTime ?? null,
      render: (queue) => shortDate(queue.purgeTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (queue) => (
        <>
          {queue.state === 'PAUSED' ? (
            <Tooltip title="Resume">
              <span>
                <IconButton
                  size="small"
                  disabled={busy}
                  onClick={() => resume.mutate(target(queue))}
                  aria-label={`Resume ${queue.name}`}
                >
                  <PlayArrowIcon fontSize="small" />
                </IconButton>
              </span>
            </Tooltip>
          ) : (
            <Tooltip title="Pause">
              <span>
                <IconButton
                  size="small"
                  disabled={busy || queue.state !== 'RUNNING'}
                  onClick={() => pause.mutate(target(queue))}
                  aria-label={`Pause ${queue.name}`}
                >
                  <PauseIcon fontSize="small" />
                </IconButton>
              </span>
            </Tooltip>
          )}
          <Tooltip title="Purge tasks">
            <span>
              <IconButton
                size="small"
                disabled={busy}
                onClick={() => purge.mutate(target(queue))}
                aria-label={`Purge ${queue.name}`}
              >
                <CleaningServicesOutlinedIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
          <Tooltip title="Delete queue">
            <span>
              <IconButton
                size="small"
                disabled={busy}
                onClick={() => remove.mutate(target(queue))}
                aria-label={`Delete ${queue.name}`}
              >
                <DeleteOutlineIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
        </>
      ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="tasks"
        title="Cloud Tasks"
        subtitle={`Queues · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create queue
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter queues"
        onRefresh={() => void queues.refetch()}
        refreshing={queues.isFetching}
      />

      {actionError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(actionError as Error).message}
        </Alert>
      )}

      <GcpDataTable
        aria-label="Cloud Tasks queues"
        columns={columns}
        rows={rows}
        getRowKey={(queue) => `${queue.location}/${queue.name}`}
        loading={queues.isLoading}
        error={queues.isError ? 'Failed to load queues.' : null}
        emptyMessage={
          filter ? 'No Cloud Tasks queues match the filter.' : 'No Cloud Tasks queues in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(queue) => <GcpRowDetail row={queue} />}
        detailTitle={(queue) => queue.name}
      />

      <QueueDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Stack>
  )
}
