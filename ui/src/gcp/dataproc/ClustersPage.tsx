import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, Chip, IconButton, Link, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import StopIcon from '@mui/icons-material/Stop'
import { Link as RouterLink } from 'react-router-dom'
import {
  deleteCluster,
  listClusters,
  startCluster,
  stopCluster,
  type DataprocCluster,
} from '../../api/gcp/dataproc'
import { useAccount } from '../../context/AccountContext'
import { ClusterDialog } from './ClusterDialog'
import { clusterStateColor, shortDate } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function target(cluster: DataprocCluster) {
  return { region: cluster.region, cluster: cluster.id }
}

/** Dataproc clusters across every region, with start/stop/delete. */
export function ClustersPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })

  const clusters = useQuery({
    queryKey: ['gcp', 'dataproc', 'clusters', accountId],
    queryFn: listClusters,
  })

  const remove = useMutation({
    mutationFn: ({ region, cluster }: { region: string; cluster: string }) =>
      deleteCluster(region, cluster),
    onSuccess: invalidate,
  })
  const start = useMutation({
    mutationFn: ({ region, cluster }: { region: string; cluster: string }) =>
      startCluster(region, cluster),
    onSuccess: invalidate,
  })
  const stop = useMutation({
    mutationFn: ({ region, cluster }: { region: string; cluster: string }) =>
      stopCluster(region, cluster),
    onSuccess: invalidate,
  })

  const busy = remove.isPending || start.isPending || stop.isPending
  const actionError = remove.error ?? start.error ?? stop.error

  const rows = filterRows(clusters.data?.clusters ?? [], filter, (cluster) =>
    [cluster.id, cluster.region, cluster.status, cluster.clusterUuid ?? ''].join(' '),
  )

  const columns: GcpColumn<DataprocCluster>[] = [
    {
      key: 'id',
      header: 'Name',
      sortable: true,
      sortValue: (cluster) => cluster.id,
      render: (cluster) => (
        <Link
          component={RouterLink}
          to={`/gcp/dataproc/clusters/${encodeURIComponent(cluster.region)}/${encodeURIComponent(cluster.id)}`}
        >
          {cluster.id}
        </Link>
      ),
    },
    {
      key: 'region',
      header: 'Region',
      sortable: true,
      sortValue: (cluster) => cluster.region,
      render: (cluster) => cluster.region || '—',
    },
    {
      key: 'status',
      header: 'Status',
      sortable: true,
      sortValue: (cluster) => cluster.status,
      render: (cluster) => (
        <Chip size="small" label={cluster.status || '—'} color={clusterStateColor(cluster.status)} />
      ),
    },
    {
      key: 'clusterUuid',
      header: 'Cluster UUID',
      sortable: true,
      sortValue: (cluster) => cluster.clusterUuid ?? '',
      render: (cluster) => cluster.clusterUuid || '—',
    },
    {
      key: 'created',
      header: 'Created',
      sortable: true,
      sortValue: (cluster) => cluster.createTime ?? '',
      render: (cluster) => shortDate(cluster.createTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (cluster) => (
        <>
          {cluster.status === 'STOPPED' ? (
            <Tooltip title="Start">
              <span>
                <IconButton
                  size="small"
                  disabled={busy || cluster.status !== 'STOPPED'}
                  onClick={() => start.mutate(target(cluster))}
                  aria-label={`Start ${cluster.id}`}
                >
                  <PlayArrowIcon fontSize="small" />
                </IconButton>
              </span>
            </Tooltip>
          ) : (
            <Tooltip title="Stop">
              <span>
                <IconButton
                  size="small"
                  disabled={busy || cluster.status !== 'RUNNING'}
                  onClick={() => stop.mutate(target(cluster))}
                  aria-label={`Stop ${cluster.id}`}
                >
                  <StopIcon fontSize="small" />
                </IconButton>
              </span>
            </Tooltip>
          )}
          <Tooltip title="Delete cluster">
            <span>
              <IconButton
                size="small"
                disabled={busy}
                onClick={() => remove.mutate(target(cluster))}
                aria-label={`Delete ${cluster.id}`}
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
        id="dataproc"
        title="Dataproc"
        subtitle={`Clusters · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create cluster
          </Button>
        }
      />

      {actionError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(actionError as Error).message}
        </Alert>
      )}

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter clusters"
        onRefresh={() => void clusters.refetch()}
        refreshing={clusters.isFetching}
      />

      <GcpDataTable
        aria-label="Dataproc clusters"
        columns={columns}
        rows={rows}
        getRowKey={(cluster) => `${cluster.region}/${cluster.id}`}
        loading={clusters.isLoading}
        error={clusters.isError ? 'Failed to load clusters.' : null}
        emptyMessage={
          filter ? 'No clusters match the filter.' : 'No Dataproc clusters in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(cluster) => <GcpRowDetail row={cluster} />}
        detailTitle={(cluster) => cluster.id}
      />

      <ClusterDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Stack>
  )
}
