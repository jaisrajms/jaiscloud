import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, Chip, IconButton, Link, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import {
  deleteServiceAccount,
  listServiceAccounts,
  type ServiceAccount,
} from '../../api/gcp/iam'
import { useAccount } from '../../context/AccountContext'
import { CreateServiceAccountDialog } from './CreateServiceAccountDialog'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** IAM service accounts in the project, with create and delete. */
export function ServiceAccountsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<string[]>([])
  const [filter, setFilter] = useState('')
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'iam'] })

  const accounts = useQuery({
    queryKey: ['gcp', 'iam', 'serviceAccounts', accountId],
    queryFn: () => listServiceAccounts(),
  })

  const remove = useMutation({
    mutationFn: (account: ServiceAccount) => deleteServiceAccount(account.email),
    onSuccess: invalidate,
  })

  const rows = filterRows(accounts.data?.accounts ?? [], filter, (account) =>
    `${account.email} ${account.displayName ?? ''}`,
  )

  const columns: GcpColumn<ServiceAccount>[] = [
    {
      key: 'email',
      header: 'Email',
      sortable: true,
      sortValue: (account) => account.email,
      render: (account) => (
        <Link
          component={RouterLink}
          to={`/gcp/iam/service-accounts/${encodeURIComponent(account.email)}`}
        >
          {account.email}
        </Link>
      ),
    },
    {
      key: 'displayName',
      header: 'Display name',
      sortable: true,
      sortValue: (account) => account.displayName ?? null,
      render: (account) => account.displayName || '—',
    },
    {
      key: 'status',
      header: 'Status',
      sortable: true,
      sortValue: (account) => (account.disabled ? 'Disabled' : 'Enabled'),
      render: (account) => (
        <Chip
          size="small"
          label={account.disabled ? 'Disabled' : 'Enabled'}
          color={account.disabled ? 'default' : 'success'}
        />
      ),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (account) => (
        <Tooltip title="Delete service account">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(account)}
              aria-label={`Delete ${account.email}`}
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
        id="iam"
        title="IAM"
        subtitle={`Service accounts · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create service account
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter service accounts"
        onRefresh={() => void accounts.refetch()}
        refreshing={accounts.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Service accounts"
        columns={columns}
        rows={rows}
        getRowKey={(account) => account.email}
        loading={accounts.isLoading}
        error={accounts.isError ? 'Failed to load service accounts.' : null}
        emptyMessage={
          filter ? 'No service accounts match the filter.' : 'No service accounts in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(account) => <GcpRowDetail row={account} />}
        detailTitle={(account) => account.email}
      />

      <CreateServiceAccountDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Stack>
  )
}
