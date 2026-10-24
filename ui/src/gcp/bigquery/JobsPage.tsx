import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Chip, IconButton, Link, Stack, Tooltip } from '@mui/material'
import CancelOutlinedIcon from '@mui/icons-material/CancelOutlined'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { cancelJob, deleteJob, listJobs, type BigQueryJob } from '../../api/gcp/bigquery'
import { useAccount } from '../../context/AccountContext'
import { BigQueryTabs } from './BigQueryTabs'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function querySummary(job: BigQueryJob): string {
  const q = (job.query ?? '').replace(/\s+/g, ' ').trim()
  if (!q) return '—'
  return q.length > 80 ? `${q.slice(0, 80)}…` : q
}

/** BigQuery jobs in the project, with cancel and delete. */
export function JobsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'bigquery'] })

  const jobs = useQuery({
    queryKey: ['gcp', 'bigquery', 'jobs', accountId],
    queryFn: () => listJobs(),
  })

  const cancel = useMutation({ mutationFn: (job: string) => cancelJob(job), onSuccess: invalidate })
  const remove = useMutation({ mutationFn: (job: string) => deleteJob(job), onSuccess: invalidate })

  const rows = filterRows(jobs.data?.jobs ?? [], filter, (job) =>
    `${job.jobId} ${job.statementType ?? ''} ${job.query ?? ''} ${job.state ?? ''}`,
  )

  const columns: GcpColumn<BigQueryJob>[] = [
    {
      key: 'jobId',
      header: 'Job ID',
      sortable: true,
      sortValue: (job) => job.jobId,
      render: (job) => (
        <Link component={RouterLink} to={`/gcp/bigquery/jobs/${encodeURIComponent(job.jobId)}`}>
          {job.jobId}
        </Link>
      ),
    },
    {
      key: 'statementType',
      header: 'Statement',
      sortable: true,
      sortValue: (job) => job.statementType ?? null,
      render: (job) => job.statementType || '—',
    },
    {
      key: 'query',
      header: 'Query',
      sortable: true,
      sortValue: (job) => job.query ?? null,
      render: (job) => (
        <Box sx={{ fontFamily: 'monospace', maxWidth: 360, overflowWrap: 'anywhere' }}>
          {querySummary(job)}
        </Box>
      ),
    },
    {
      key: 'state',
      header: 'State',
      sortable: true,
      sortValue: (job) => job.state ?? null,
      render: (job) => (
        <Chip
          size="small"
          label={job.state || 'UNKNOWN'}
          color={job.state === 'DONE' ? 'success' : 'warning'}
        />
      ),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (job) => (
        <>
          <Tooltip title="Cancel job">
            <span>
              <IconButton
                size="small"
                disabled={cancel.isPending}
                onClick={() => cancel.mutate(job.jobId)}
                aria-label={`Cancel ${job.jobId}`}
              >
                <CancelOutlinedIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
          <Tooltip title="Delete job">
            <span>
              <IconButton
                size="small"
                disabled={remove.isPending}
                onClick={() => remove.mutate(job.jobId)}
                aria-label={`Delete ${job.jobId}`}
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
        id="bigquery"
        title="BigQuery"
        subtitle={`Jobs · project ${accountId || '—'}`}
      >
        <BigQueryTabs />
      </GcpPageHeader>

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter jobs"
        onRefresh={() => void jobs.refetch()}
        refreshing={jobs.isFetching}
      />

      {(cancel.isError || remove.isError) && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Job action failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="BigQuery jobs"
        columns={columns}
        rows={rows}
        getRowKey={(job) => job.jobId}
        loading={jobs.isLoading}
        error={jobs.isError ? 'Failed to load jobs.' : null}
        emptyMessage={filter ? 'No jobs match the filter.' : 'No jobs in this project.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(job) => <GcpRowDetail row={job} />}
        detailTitle={(job) => job.jobId}
      />
    </Stack>
  )
}
