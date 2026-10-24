import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Chip, IconButton, Link, Stack, Tooltip } from '@mui/material'
import CancelOutlinedIcon from '@mui/icons-material/CancelOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { cancelJob, listJobs, type DataprocJob } from '../../api/gcp/dataproc'
import { useAccount } from '../../context/AccountContext'
import { jobStateColor, jobTypeLabel, shortDate } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** True for a job that has reached a terminal state and cannot be cancelled. */
function isTerminal(state: string): boolean {
  return ['DONE', 'ERROR', 'CANCELLED', 'ATTEMPT_FAILURE'].includes(state)
}

/** Dataproc jobs across every region, with cancel for active jobs. */
export function JobsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })

  const jobs = useQuery({
    queryKey: ['gcp', 'dataproc', 'jobs', accountId],
    queryFn: listJobs,
  })

  const cancel = useMutation({
    mutationFn: ({ region, job }: { region: string; job: string }) => cancelJob(region, job),
    onSuccess: invalidate,
  })

  const rows = filterRows(jobs.data?.jobs ?? [], filter, (job) =>
    [job.id, job.region, job.clusterName ?? '', jobTypeLabel(job.type), job.status].join(' '),
  )

  const columns: GcpColumn<DataprocJob>[] = [
    {
      key: 'id',
      header: 'Job ID',
      sortable: true,
      sortValue: (job) => job.id,
      render: (job) => (
        <Link
          component={RouterLink}
          to={`/gcp/dataproc/jobs/${encodeURIComponent(job.region)}/${encodeURIComponent(job.id)}`}
        >
          {job.id}
        </Link>
      ),
    },
    {
      key: 'region',
      header: 'Region',
      sortable: true,
      sortValue: (job) => job.region,
      render: (job) => job.region || '—',
    },
    {
      key: 'clusterName',
      header: 'Cluster',
      sortable: true,
      sortValue: (job) => job.clusterName ?? '',
      render: (job) => job.clusterName || '—',
    },
    {
      key: 'type',
      header: 'Type',
      sortable: true,
      sortValue: (job) => jobTypeLabel(job.type),
      render: (job) => jobTypeLabel(job.type),
    },
    {
      key: 'status',
      header: 'Status',
      sortable: true,
      sortValue: (job) => job.status,
      render: (job) => (
        <Chip size="small" label={job.status || '—'} color={jobStateColor(job.status)} />
      ),
    },
    {
      key: 'created',
      header: 'Created',
      sortable: true,
      sortValue: (job) => job.createTime ?? '',
      render: (job) => shortDate(job.createTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (job) => (
        <Tooltip title="Cancel job">
          <span>
            <IconButton
              size="small"
              disabled={cancel.isPending || isTerminal(job.status)}
              onClick={() => cancel.mutate({ region: job.region, job: job.id })}
              aria-label={`Cancel ${job.id}`}
            >
              <CancelOutlinedIcon fontSize="small" />
            </IconButton>
          </span>
        </Tooltip>
      ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="dataproc"
        title="Dataproc"
        subtitle={`Jobs · project ${accountId || '—'}`}
      />

      {cancel.error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(cancel.error as Error).message}
        </Alert>
      )}

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter jobs"
        onRefresh={() => void jobs.refetch()}
        refreshing={jobs.isFetching}
      />

      <GcpDataTable
        aria-label="Dataproc jobs"
        columns={columns}
        rows={rows}
        getRowKey={(job) => `${job.region}/${job.id}`}
        loading={jobs.isLoading}
        error={jobs.isError ? 'Failed to load jobs.' : null}
        emptyMessage={filter ? 'No jobs match the filter.' : 'No Dataproc jobs in this project.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(job) => <GcpRowDetail row={job} />}
        detailTitle={(job) => job.id}
      />
    </Stack>
  )
}
