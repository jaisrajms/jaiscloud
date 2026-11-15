import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, Chip, IconButton, Link, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import CancelOutlinedIcon from '@mui/icons-material/CancelOutlined'
import { Link as RouterLink, useSearchParams } from 'react-router-dom'
import { cancelJob, listJobs, type DataprocJob } from '../../api/gcp/dataproc'
import { useAccount } from '../../context/AccountContext'
import { JobDialog } from './JobDialog'
import { ConfirmDialog } from './common'
import { jobStateColor, jobTypeLabel, shortDate, sortableDate } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** True for a job that has reached a terminal state (dataproc.v1 Job.done). */
function isTerminal(state: string): boolean {
  return ['DONE', 'ERROR', 'CANCELLED'].includes(state)
}

/** Dataproc jobs across every region, with cancel for active jobs. */
export function JobsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])
  const [submitting, setSubmitting] = useState(false)
  const [confirmCancel, setConfirmCancel] = useState<DataprocJob | null>(null)
  const [params, setParams] = useSearchParams()
  const clusterFilter = params.get('cluster') ?? ''

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })

  const jobs = useQuery({
    queryKey: ['gcp', 'dataproc', 'jobs', accountId],
    queryFn: listJobs,
  })

  const cancel = useMutation({
    mutationFn: ({ region, job }: { region: string; job: string }) => cancelJob(region, job),
    onSuccess: () => {
      invalidate()
      setConfirmCancel(null)
    },
  })

  const allJobs = jobs.data?.jobs ?? []
  const scoped = clusterFilter
    ? allJobs.filter((job) => job.clusterName === clusterFilter)
    : allJobs
  const clusterJob = clusterFilter
    ? allJobs.find((job) => job.clusterName === clusterFilter)
    : undefined
  const rows = filterRows(scoped, filter, (job) =>
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
      render: (job) =>
        job.clusterName ? (
          <Link
            component={RouterLink}
            to={`/gcp/dataproc/clusters/${encodeURIComponent(job.region)}/${encodeURIComponent(job.clusterName)}`}
          >
            {job.clusterName}
          </Link>
        ) : (
          '—'
        ),
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
      sortValue: (job) => sortableDate(job.createTime),
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
              onClick={() => {
                cancel.reset()
                setConfirmCancel(job)
              }}
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
      >
        {clusterFilter && (
          <Chip
            size="small"
            label={`Cluster: ${clusterFilter}`}
            onDelete={() => {
              const next = new URLSearchParams(params)
              next.delete('cluster')
              setParams(next, { replace: true })
            }}
          />
        )}
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setSubmitting(true)}>
          Submit job
        </Button>
      </GcpToolbar>

      <GcpDataTable
        aria-label="Dataproc jobs"
        columns={columns}
        rows={rows}
        getRowKey={(job) => `${job.region}/${job.id}`}
        loading={jobs.isLoading}
        error={jobs.isError ? 'Failed to load jobs.' : null}
        emptyMessage={
          filter
            ? 'No jobs match the filter.'
            : clusterFilter
              ? `No jobs submitted to ${clusterFilter}.`
              : 'No Dataproc jobs in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(job) => <GcpRowDetail row={job} />}
        detailTitle={(job) => job.id}
      />

      <JobDialog
        open={submitting}
        onClose={() => setSubmitting(false)}
        defaultRegion={clusterJob?.region}
        defaultCluster={clusterFilter || undefined}
      />
      <ConfirmDialog
        open={confirmCancel !== null}
        title="Cancel job"
        confirmLabel="Cancel job"
        body={
          <>
            Cancel job <strong>{confirmCancel?.id}</strong>? The driver is stopped and the job
            becomes CANCELLED.
          </>
        }
        busy={cancel.isPending}
        error={cancel.error?.message}
        onConfirm={() => {
          if (confirmCancel) cancel.mutate({ region: confirmCancel.region, job: confirmCancel.id })
        }}
        onClose={() => {
          cancel.reset()
          setConfirmCancel(null)
        }}
      />
    </Stack>
  )
}
