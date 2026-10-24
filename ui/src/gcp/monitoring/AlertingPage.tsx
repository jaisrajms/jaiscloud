import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Button, Chip, IconButton, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import {
  deleteAlertPolicy,
  listAlertPolicies,
  updateAlertPolicy,
  type AlertPolicy,
} from '../../api/gcp/monitoring'
import { useAccount } from '../../context/AccountContext'
import { AlertPolicyDialog } from './AlertPolicyDialog'
import { resourceID } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function toggleBody(policy: AlertPolicy) {
  return {
    displayName: policy.displayName ?? '',
    documentation: policy.documentation,
    conditions: policy.conditions,
    combiner: policy.combiner,
    enabled: !(policy.enabled ?? true),
    notificationChannels: policy.notificationChannels,
    userLabels: policy.userLabels,
  }
}

/** Cloud Monitoring alerting policies: list, create, edit, toggle and delete. */
export function AlertingPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<AlertPolicy | undefined>(undefined)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const policies = useQuery({
    queryKey: ['gcp', 'monitoring', 'policies', accountId],
    queryFn: () => listAlertPolicies(),
  })

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'monitoring', 'policies'] })

  const remove = useMutation({
    mutationFn: (id: string) => deleteAlertPolicy(id),
    onSuccess: invalidate,
  })
  const toggle = useMutation({
    mutationFn: (policy: AlertPolicy) => updateAlertPolicy(resourceID(policy.name), toggleBody(policy)),
    onSuccess: invalidate,
  })

  const rows = filterRows(
    policies.data?.alertPolicies ?? [],
    filter,
    (policy) => policy.displayName || resourceID(policy.name),
  )

  const columns: GcpColumn<AlertPolicy>[] = [
    {
      key: 'displayName',
      header: 'Display name',
      sortable: true,
      sortValue: (policy) => policy.displayName || resourceID(policy.name),
      render: (policy) => policy.displayName || resourceID(policy.name),
    },
    {
      key: 'combiner',
      header: 'Combiner',
      sortable: true,
      sortValue: (policy) => policy.combiner ?? '',
      render: (policy) => policy.combiner || '—',
    },
    {
      key: 'conditions',
      header: 'Conditions',
      sortable: true,
      sortValue: (policy) => policy.conditions?.length ?? 0,
      render: (policy) => policy.conditions?.length ?? 0,
    },
    {
      key: 'channels',
      header: 'Channels',
      sortable: true,
      sortValue: (policy) => policy.notificationChannels?.length ?? 0,
      render: (policy) => policy.notificationChannels?.length ?? 0,
    },
    {
      key: 'status',
      header: 'Status',
      sortable: true,
      sortValue: (policy) => ((policy.enabled ?? true) ? 'ENABLED' : 'DISABLED'),
      render: (policy) => {
        const on = policy.enabled ?? true
        return (
          <Chip
            size="small"
            label={on ? 'ENABLED' : 'DISABLED'}
            color={on ? 'success' : 'default'}
            onClick={() => toggle.mutate(policy)}
            clickable
          />
        )
      },
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (policy) => (
        <>
          <Tooltip title="Edit policy">
            <IconButton
              size="small"
              onClick={() => {
                setEditing(policy)
                setDialogOpen(true)
              }}
            >
              <EditOutlinedIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete policy">
            <IconButton
              size="small"
              onClick={() => remove.mutate(resourceID(policy.name))}
              disabled={remove.isPending}
            >
              <DeleteOutlineIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </>
      ),
    },
  ]

  return (
    <Box>
      <GcpPageHeader
        id="monitoring"
        title="Alerting"
        subtitle={`Alerting policies · project ${accountId || '—'}`}
        actions={
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => {
              setEditing(undefined)
              setDialogOpen(true)
            }}
          >
            Create policy
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter policies"
        onRefresh={() => void policies.refetch()}
        refreshing={policies.isFetching}
      />

      {(remove.isError || toggle.isError) && (
        <Alert severity="error" sx={{ mb: 2 }}>
          The action failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Alerting policies"
        columns={columns}
        rows={rows}
        getRowKey={(policy) => policy.name}
        loading={policies.isLoading}
        error={policies.isError ? 'Failed to load alert policies.' : null}
        emptyMessage={filter ? 'No alerting policies match the filter.' : 'No alerting policies in this project.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(policy) => <GcpRowDetail row={policy} />}
        detailTitle={(policy) => policy.displayName || resourceID(policy.name)}
      />

      <AlertPolicyDialog open={dialogOpen} onClose={() => setDialogOpen(false)} initial={editing} />
    </Box>
  )
}
