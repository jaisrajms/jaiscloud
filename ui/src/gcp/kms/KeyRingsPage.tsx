import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Button, Link, Stack, TextField } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import { Link as RouterLink } from 'react-router-dom'
import { listKeyRings, type KeyRing } from '../../api/gcp/kms'
import { useAccount } from '../../context/AccountContext'
import { CreateKeyRingDialog } from './CreateKeyRingDialog'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** KMS key rings in one location, with create. */
export function KeyRingsPage() {
  const { accountId } = useAccount()
  const [location, setLocation] = useState('global')
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<string[]>([])
  const [filter, setFilter] = useState('')

  const keyRings = useQuery({
    queryKey: ['gcp', 'kms', 'keyRings', location, accountId],
    queryFn: () => listKeyRings(location),
    enabled: Boolean(location),
  })

  const rows = filterRows(keyRings.data?.keyRings ?? [], filter, (keyRing) =>
    `${keyRing.keyRingId ?? ''} ${keyRing.location ?? ''}`,
  )

  const columns: GcpColumn<KeyRing>[] = [
    {
      key: 'keyRingId',
      header: 'Key ring',
      sortable: true,
      sortValue: (keyRing) => keyRing.keyRingId ?? keyRing.name,
      render: (keyRing) => (
        <Link
          component={RouterLink}
          to={`/gcp/kms/keyrings/${encodeURIComponent(keyRing.location || location)}/${encodeURIComponent(keyRing.keyRingId || '')}`}
        >
          {keyRing.keyRingId}
        </Link>
      ),
    },
    {
      key: 'location',
      header: 'Location',
      sortable: true,
      sortValue: (keyRing) => keyRing.location ?? null,
      render: (keyRing) => keyRing.location || '—',
    },
    {
      key: 'createTime',
      header: 'Created',
      sortable: true,
      sortValue: (keyRing) => keyRing.createTime ?? null,
      render: (keyRing) => keyRing.createTime || '—',
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="kms"
        title="Cloud KMS"
        subtitle={`Key rings · project ${accountId || '—'}`}
        actions={
          <>
            <TextField
              label="Location"
              value={location}
              onChange={(e) => setLocation(e.target.value)}
              size="small"
            />
            <Button
              variant="contained"
              startIcon={<AddIcon />}
              disabled={!location}
              onClick={() => setCreateOpen(true)}
            >
              Create key ring
            </Button>
          </>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter key rings"
        onRefresh={() => void keyRings.refetch()}
        refreshing={keyRings.isFetching}
      />

      <GcpDataTable
        aria-label="Key rings"
        columns={columns}
        rows={rows}
        getRowKey={(keyRing) => keyRing.name}
        loading={keyRings.isLoading}
        error={keyRings.isError ? 'Failed to load key rings.' : null}
        emptyMessage={
          filter
            ? 'No key rings match the filter.'
            : `No key rings in ${location || 'this location'}.`
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(keyRing) => <GcpRowDetail row={keyRing} />}
        detailTitle={(keyRing) => keyRing.keyRingId || keyRing.name}
      />

      <CreateKeyRingDialog
        open={createOpen}
        location={location}
        onClose={() => setCreateOpen(false)}
      />
    </Stack>
  )
}
