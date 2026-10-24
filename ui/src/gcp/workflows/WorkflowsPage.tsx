import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, Chip, IconButton, Link, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { deleteWorkflow, listWorkflows, type Workflow } from '../../api/gcp/workflows'
import { useAccount } from '../../context/AccountContext'
import { WorkflowDialog } from './WorkflowDialog'
import { shortDate, workflowStateColor } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function target(workflow: Workflow) {
  return { location: workflow.location, workflow: workflow.id }
}

/** Workflows across every location, with create and delete. */
export function WorkflowsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'workflows'] })

  const workflows = useQuery({
    queryKey: ['gcp', 'workflows', 'list', accountId],
    queryFn: listWorkflows,
  })

  const remove = useMutation({
    mutationFn: ({ location, workflow }: { location: string; workflow: string }) =>
      deleteWorkflow(location, workflow),
    onSuccess: invalidate,
  })

  const rows = filterRows(workflows.data?.workflows ?? [], filter, (workflow) =>
    `${workflow.id} ${workflow.location} ${workflow.state ?? ''}`,
  )

  const columns: GcpColumn<Workflow>[] = [
    {
      key: 'id',
      header: 'Name',
      sortable: true,
      sortValue: (workflow) => workflow.id,
      render: (workflow) => (
        <Link
          component={RouterLink}
          to={`/gcp/workflows/${encodeURIComponent(workflow.location)}/${encodeURIComponent(workflow.id)}`}
        >
          {workflow.id}
        </Link>
      ),
    },
    {
      key: 'location',
      header: 'Location',
      sortable: true,
      sortValue: (workflow) => workflow.location ?? null,
      render: (workflow) => workflow.location || '—',
    },
    {
      key: 'state',
      header: 'State',
      sortable: true,
      sortValue: (workflow) => workflow.state ?? null,
      render: (workflow) => (
        <Chip
          size="small"
          label={workflow.state || '—'}
          color={workflowStateColor(workflow.state)}
        />
      ),
    },
    {
      key: 'revisionId',
      header: 'Revision',
      sortable: true,
      sortValue: (workflow) => workflow.revisionId ?? null,
      render: (workflow) => workflow.revisionId || '—',
    },
    {
      key: 'updateTime',
      header: 'Last updated',
      sortable: true,
      sortValue: (workflow) => workflow.updateTime ?? null,
      render: (workflow) => shortDate(workflow.updateTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (workflow) => (
        <Tooltip title="Delete workflow">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(target(workflow))}
              aria-label={`Delete ${workflow.id}`}
            >
              <DeleteOutlineIcon fontSize="small" />
            </IconButton>
          </span>
        </Tooltip>
      ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="workflows"
        title="Workflows"
        subtitle={`Workflows · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create workflow
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter workflows"
        onRefresh={() => void workflows.refetch()}
        refreshing={workflows.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(remove.error as Error).message}
        </Alert>
      )}

      <GcpDataTable
        aria-label="Workflows"
        columns={columns}
        rows={rows}
        getRowKey={(workflow) => `${workflow.location}/${workflow.id}`}
        loading={workflows.isLoading}
        error={workflows.isError ? 'Failed to load workflows.' : null}
        emptyMessage={filter ? 'No workflows match the filter.' : 'No workflows in this project.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(workflow) => <GcpRowDetail row={workflow} />}
        detailTitle={(workflow) => workflow.id}
      />

      <WorkflowDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Stack>
  )
}
