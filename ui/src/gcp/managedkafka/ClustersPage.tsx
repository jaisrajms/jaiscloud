import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Button, Chip, IconButton, Link, Stack, Tooltip, Typography } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { getCluster, listClusters, type ManagedKafkaCluster } from '../../api/gcp/managedkafka'
import { formatDate } from '../../lib/date'
import { useAccount } from '../../context/AccountContext'
import { ClusterDialog } from './ClusterDialog'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** Managed Kafka clusters across every location, with create/edit. */
export function ClustersPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<ManagedKafkaCluster | undefined>(undefined)

  const openCreate = () => {
    setEditing(undefined)
    setDialogOpen(true)
  }
  // The list row omits `config`, so fetch the full cluster before editing;
  // otherwise the dialog would start from its default config and clobber the
  // stored one on save.
  const openEdit = async (row: ManagedKafkaCluster) => {
    const full = await queryClient.fetchQuery({
      queryKey: ['gcp', 'managedkafka', 'cluster', row.location, row.id, accountId],
      queryFn: () => getCluster(row.location, row.id),
    })
    setEditing(full)
    setDialogOpen(true)
  }

  const clusters = useQuery({
    queryKey: ['gcp', 'managedkafka', 'clusters', accountId],
    queryFn: listClusters,
  })

  const rows = filterRows(clusters.data?.clusters ?? [], filter, (cluster) =>
    [cluster.id, cluster.location, cluster.bootstrapAddress ?? ''].join(' '),
  )

  const columns: GcpColumn<ManagedKafkaCluster>[] = [
    {
      key: 'id',
      header: 'Name',
      sortable: true,
      sortValue: (cluster) => cluster.id,
      render: (cluster) => (
        <Link
          component={RouterLink}
          to={`/gcp/managedkafka/clusters/${encodeURIComponent(cluster.location)}/${encodeURIComponent(cluster.id)}`}
        >
          {cluster.id}
        </Link>
      ),
    },
    {
      key: 'location',
      header: 'Location',
      sortable: true,
      sortValue: (cluster) => cluster.location,
      render: (cluster) => cluster.location || '—',
    },
    {
      key: 'state',
      header: 'Status',
      sortable: true,
      sortValue: (cluster) => cluster.state ?? '',
      render: (cluster) => (
        <Chip
          size="small"
          label={cluster.state || '—'}
          color={cluster.state === 'ACTIVE' ? 'success' : 'default'}
        />
      ),
    },
    {
      key: 'bootstrapAddress',
      header: 'Bootstrap address',
      sortable: true,
      sortValue: (cluster) => cluster.bootstrapAddress ?? '',
      render: (cluster) =>
        cluster.bootstrapAddress ? (
          <Typography variant="body2" sx={{ fontFamily: 'monospace', overflowWrap: 'anywhere' }}>
            {cluster.bootstrapAddress}
          </Typography>
        ) : (
          '—'
        ),
    },
    {
      key: 'labels',
      header: 'Labels',
      render: (cluster) =>
        cluster.labels && Object.keys(cluster.labels).length > 0 ? (
          <Stack direction="row" spacing={0.5} sx={{ flexWrap: 'wrap', rowGap: 0.5 }}>
            {Object.entries(cluster.labels).map(([k, v]) => (
              <Chip key={k} size="small" variant="outlined" label={`${k}=${v}`} />
            ))}
          </Stack>
        ) : (
          '—'
        ),
    },
    {
      key: 'created',
      header: 'Created',
      sortable: true,
      sortValue: (cluster) => cluster.createTime ?? '',
      render: (cluster) => formatDate(cluster.createTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (cluster) => (
        <Tooltip title="Edit cluster">
          <span>
            <IconButton size="small" onClick={() => openEdit(cluster)} aria-label={`Edit ${cluster.id}`}>
              <EditOutlinedIcon fontSize="small" />
            </IconButton>
          </span>
        </Tooltip>
      ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="managedkafka"
        title="Managed Kafka"
        subtitle={`Clusters · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={openCreate}>
            Create cluster
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter clusters"
        onRefresh={() => void clusters.refetch()}
        refreshing={clusters.isFetching}
      />

      <GcpDataTable
        aria-label="Managed Kafka clusters"
        columns={columns}
        rows={rows}
        getRowKey={(cluster) => `${cluster.location}/${cluster.id}`}
        loading={clusters.isLoading}
        error={clusters.isError ? 'Failed to load clusters.' : null}
        emptyMessage={
          filter ? 'No clusters match the filter.' : 'No Managed Kafka clusters in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(cluster) => <GcpRowDetail row={cluster} />}
        detailTitle={(cluster) => cluster.id}
      />

      <ClusterDialog
        open={dialogOpen}
        onClose={() => setDialogOpen(false)}
        cluster={editing}
      />
    </Stack>
  )
}
