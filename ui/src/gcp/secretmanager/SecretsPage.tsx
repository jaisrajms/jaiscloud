import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, IconButton, Link, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { deleteSecret, listSecrets, type Secret } from '../../api/gcp/secretmanager'
import { useAccount } from '../../context/AccountContext'
import { CreateSecretDialog } from './CreateSecretDialog'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function labelSummary(labels?: Record<string, string>): string {
  if (!labels) return '—'
  const entries = Object.entries(labels)
  if (entries.length === 0) return '—'
  return entries.map(([k, v]) => `${k}=${v}`).join(', ')
}

/** Secret Manager secrets in the project, with create and delete. */
export function SecretsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<string[]>([])
  const [filter, setFilter] = useState('')
  const invalidate = () =>
    void queryClient.invalidateQueries({ queryKey: ['gcp', 'secretmanager'] })

  const secrets = useQuery({
    queryKey: ['gcp', 'secretmanager', 'secrets', accountId],
    queryFn: () => listSecrets(),
  })

  const remove = useMutation({
    mutationFn: (secret: Secret) => deleteSecret(secret.secretId || ''),
    onSuccess: invalidate,
  })

  const rows = filterRows(secrets.data?.secrets ?? [], filter, (secret) =>
    `${secret.secretId ?? ''} ${labelSummary(secret.labels)}`,
  )

  const columns: GcpColumn<Secret>[] = [
    {
      key: 'secretId',
      header: 'Secret ID',
      sortable: true,
      sortValue: (secret) => secret.secretId ?? '',
      render: (secret) => (
        <Link
          component={RouterLink}
          to={`/gcp/secretmanager/secrets/${encodeURIComponent(secret.secretId || '')}`}
        >
          {secret.secretId}
        </Link>
      ),
    },
    {
      key: 'labels',
      header: 'Labels',
      sortable: true,
      sortValue: (secret) => labelSummary(secret.labels),
      render: (secret) => labelSummary(secret.labels),
    },
    {
      key: 'rotation',
      header: 'Rotation',
      sortable: true,
      sortValue: (secret) => secret.rotationPeriod ?? null,
      render: (secret) => secret.rotationPeriod || '—',
    },
    {
      key: 'created',
      header: 'Created',
      sortable: true,
      sortValue: (secret) => secret.createTime ?? null,
      render: (secret) => secret.createTime || '—',
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (secret) => (
        <Tooltip title="Delete secret">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(secret)}
              aria-label={`Delete ${secret.secretId}`}
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
        id="secretmanager"
        title="Secret Manager"
        subtitle={`Secrets · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create secret
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter secrets"
        onRefresh={() => void secrets.refetch()}
        refreshing={secrets.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Secrets"
        columns={columns}
        rows={rows}
        getRowKey={(secret) => secret.name}
        loading={secrets.isLoading}
        error={secrets.isError ? 'Failed to load secrets.' : null}
        emptyMessage={filter ? 'No secrets match the filter.' : 'No secrets in this project.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(secret) => <GcpRowDetail row={secret} />}
        detailTitle={(secret) => secret.secretId || secret.name}
      />

      <CreateSecretDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Stack>
  )
}
