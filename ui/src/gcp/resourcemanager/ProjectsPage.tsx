import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, Chip, IconButton, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import RestoreFromTrashIcon from '@mui/icons-material/RestoreFromTrashOutlined'
import {
  listProjects,
  undeleteProject,
  type Project,
} from '../../api/gcp/resourcemanager'
import { CreateProjectDialog } from './CreateProjectDialog'
import { DeleteProjectDialog } from './DeleteProjectDialog'
import { invalidateProjects } from './queries'
import { shortDate, stateColor, stateLabel } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail, type GcpRowDetailField } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

const detailFields: GcpRowDetailField[] = [
  { key: 'displayName', label: 'Name' },
  { key: 'projectNumber', label: 'Project number' },
  { key: 'state', label: 'State' },
  { key: 'createTime', label: 'Created', render: (v) => shortDate(typeof v === 'string' ? v : undefined) },
  { key: 'updateTime', label: 'Updated', render: (v) => shortDate(typeof v === 'string' ? v : undefined) },
  { key: 'deleteTime', label: 'Deleted', render: (v) => shortDate(typeof v === 'string' ? v : undefined) },
  { key: 'parent', label: 'Parent' },
]

/**
 * Resource Manager project manager: every project the emulator knows, with
 * create/delete/undelete and lifecycle state. Deleting marks a project
 * DELETE_REQUESTED (restorable), matching real GCP's recovery window.
 */
export function ProjectsPage() {
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [deleting, setDeleting] = useState<Project | null>(null)
  const [filter, setFilter] = useState('')

  const projects = useQuery({
    queryKey: ['gcp', 'resourcemanager', 'projects'],
    queryFn: listProjects,
  })

  const undelete = useMutation({
    mutationFn: (id: string) => undeleteProject(id),
    onSuccess: () => invalidateProjects(queryClient),
  })

  const rows = filterRows(projects.data?.projects ?? [], filter, (p) =>
    `${p.projectId} ${p.displayName ?? ''} ${p.projectNumber ?? ''} ${p.state}`,
  )

  const columns: GcpColumn<Project>[] = [
    {
      key: 'projectId',
      header: 'Project ID',
      sortable: true,
      sortValue: (p) => p.projectId,
      render: (p) => p.projectId,
    },
    {
      key: 'displayName',
      header: 'Name',
      sortable: true,
      sortValue: (p) => p.displayName ?? null,
      render: (p) => p.displayName || '—',
    },
    {
      key: 'projectNumber',
      header: 'Project number',
      sortable: true,
      sortValue: (p) => (p.projectNumber ? Number(p.projectNumber) : null),
      render: (p) => p.projectNumber || '—',
    },
    {
      key: 'state',
      header: 'State',
      sortable: true,
      sortValue: (p) => p.state ?? null,
      render: (p) => <Chip size="small" label={stateLabel(p)} color={stateColor(p.state)} />,
    },
    {
      key: 'createTime',
      header: 'Created',
      sortable: true,
      sortValue: (p) => p.createTime ?? null,
      render: (p) => shortDate(p.createTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (p) =>
        p.state === 'DELETE_REQUESTED' ? (
          <Tooltip title="Undelete project">
            <span>
              <IconButton
                size="small"
                disabled={undelete.isPending}
                onClick={() => undelete.mutate(p.projectId)}
                aria-label={`Undelete ${p.projectId}`}
              >
                <RestoreFromTrashIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
        ) : (
          <Tooltip title="Delete project">
            <span>
              <IconButton
                size="small"
                disabled={undelete.isPending}
                onClick={() => setDeleting(p)}
                aria-label={`Delete ${p.projectId}`}
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
        id="resourcemanager"
        title="Resource Manager"
        subtitle="Projects"
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create project
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter projects"
        onRefresh={() => void projects.refetch()}
        refreshing={projects.isFetching}
      />

      {undelete.error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(undelete.error as Error).message}
        </Alert>
      )}

      <GcpDataTable
        aria-label="Resource Manager projects"
        columns={columns}
        rows={rows}
        getRowKey={(p) => p.projectId}
        loading={projects.isLoading}
        error={projects.isError ? 'Failed to load projects.' : null}
        emptyMessage={filter ? 'No projects match the filter.' : 'No projects found.'}
        renderDetail={(p) => <GcpRowDetail row={p} fields={detailFields} />}
        detailTitle={(p) => p.projectId}
      />

      {createOpen && <CreateProjectDialog onClose={() => setCreateOpen(false)} />}
      {deleting && <DeleteProjectDialog project={deleting} onClose={() => setDeleting(null)} />}
    </Stack>
  )
}
