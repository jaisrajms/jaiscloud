import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Button, Chip, IconButton, Link, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import PauseIcon from '@mui/icons-material/Pause'
import PlayCircleOutlineIcon from '@mui/icons-material/PlayCircleOutlineOutlined'
import { Link as RouterLink } from 'react-router-dom'
import {
  deleteJob,
  listJobs,
  pauseJob,
  resumeJob,
  runJob,
  type SchedulerJob,
} from '../../api/gcp/scheduler'
import { useAccount } from '../../context/AccountContext'
import { JobDialog } from './JobDialog'
import { shortDate, stateColor, targetLabel } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function target(job: SchedulerJob) {
  return { location: job.location, job: job.name }
}

/** Cloud Scheduler jobs across every location, with pause/resume/run/delete. */
export function JobsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'scheduler'] })

  const jobs = useQuery({
    queryKey: ['gcp', 'scheduler', 'jobs', accountId],
    queryFn: listJobs,
  })

  const remove = useMutation({
    mutationFn: ({ location, job }: { location: string; job: string }) => deleteJob(location, job),
    onSuccess: invalidate,
  })
  const pause = useMutation({
    mutationFn: ({ location, job }: { location: string; job: string }) => pauseJob(location, job),
    onSuccess: invalidate,
  })
  const resume = useMutation({
    mutationFn: ({ location, job }: { location: string; job: string }) => resumeJob(location, job),
    onSuccess: invalidate,
  })
  const run = useMutation({
    mutationFn: ({ location, job }: { location: string; job: string }) => runJob(location, job),
    onSuccess: invalidate,
  })

  const busy = remove.isPending || pause.isPending || resume.isPending || run.isPending
  const actionError = remove.error ?? pause.error ?? resume.error ?? run.error

  const rows = filterRows(jobs.data?.jobs ?? [], filter, (job) =>
    `${job.name} ${job.location} ${job.schedule ?? ''} ${targetLabel(job)} ${job.state}`,
  )

  const columns: GcpColumn<SchedulerJob>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (job) => job.name,
      render: (job) => (
        <Link
          component={RouterLink}
          to={`/gcp/scheduler/jobs/${encodeURIComponent(job.location)}/${encodeURIComponent(job.name)}`}
        >
          {job.name}
        </Link>
      ),
    },
    {
      key: 'location',
      header: 'Location',
      sortable: true,
      sortValue: (job) => job.location ?? null,
      render: (job) => job.location || '—',
    },
    {
      key: 'schedule',
      header: 'Schedule',
      sortable: true,
      sortValue: (job) => job.schedule ?? null,
      render: (job) => job.schedule || '—',
    },
    {
      key: 'target',
      header: 'Target',
      sortable: true,
      sortValue: (job) => targetLabel(job),
      render: (job) => (
        <Box sx={{ maxWidth: 260, overflow: 'hidden', textOverflow: 'ellipsis' }}>
          {targetLabel(job)}
        </Box>
      ),
    },
    {
      key: 'state',
      header: 'State',
      sortable: true,
      sortValue: (job) => job.state ?? null,
      render: (job) => (
        <Chip size="small" label={job.state || '—'} color={stateColor(job.state)} />
      ),
    },
    {
      key: 'lastAttempt',
      header: 'Last attempt',
      sortable: true,
      sortValue: (job) => job.lastAttemptTime ?? null,
      render: (job) => shortDate(job.lastAttemptTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (job) => (
        <>
          <Tooltip title="Run now">
            <span>
              <IconButton
                size="small"
                disabled={busy}
                onClick={() => run.mutate(target(job))}
                aria-label={`Run ${job.name}`}
              >
                <PlayCircleOutlineIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
          {job.state === 'PAUSED' ? (
            <Tooltip title="Resume">
              <span>
                <IconButton
                  size="small"
                  disabled={busy}
                  onClick={() => resume.mutate(target(job))}
                  aria-label={`Resume ${job.name}`}
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
                  disabled={busy || job.state !== 'ENABLED'}
                  onClick={() => pause.mutate(target(job))}
                  aria-label={`Pause ${job.name}`}
                >
                  <PauseIcon fontSize="small" />
                </IconButton>
              </span>
            </Tooltip>
          )}
          <Tooltip title="Delete job">
            <span>
              <IconButton
                size="small"
                disabled={busy}
                onClick={() => remove.mutate(target(job))}
                aria-label={`Delete ${job.name}`}
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
        id="scheduler"
        title="Cloud Scheduler"
        subtitle={`Jobs · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create job
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter jobs"
        onRefresh={() => void jobs.refetch()}
        refreshing={jobs.isFetching}
      />

      {actionError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(actionError as Error).message}
        </Alert>
      )}

      <GcpDataTable
        aria-label="Cloud Scheduler jobs"
        columns={columns}
        rows={rows}
        getRowKey={(job) => `${job.location}/${job.name}`}
        loading={jobs.isLoading}
        error={jobs.isError ? 'Failed to load jobs.' : null}
        emptyMessage={
          filter ? 'No Cloud Scheduler jobs match the filter.' : 'No Cloud Scheduler jobs in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(job) => <GcpRowDetail row={job} />}
        detailTitle={(job) => job.name}
      />

      <JobDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Stack>
  )
}
