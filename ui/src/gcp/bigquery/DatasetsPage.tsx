import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, IconButton, Link, Stack, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { deleteDataset, listDatasets, type BigQueryDataset } from '../../api/gcp/bigquery'
import { useAccount } from '../../context/AccountContext'
import { BigQueryTabs } from './BigQueryTabs'
import { CreateDatasetDialog } from './CreateDatasetDialog'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** BigQuery datasets in the project, with create and delete. */
export function DatasetsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'bigquery'] })

  const datasets = useQuery({
    queryKey: ['gcp', 'bigquery', 'datasets', accountId],
    queryFn: () => listDatasets(),
  })

  const remove = useMutation({
    mutationFn: (dataset: BigQueryDataset) => deleteDataset(dataset.datasetId),
    onSuccess: invalidate,
  })

  const rows = filterRows(datasets.data?.datasets ?? [], filter, (dataset) =>
    `${dataset.datasetId} ${dataset.location ?? ''} ${dataset.friendlyName ?? ''}`,
  )

  const columns: GcpColumn<BigQueryDataset>[] = [
    {
      key: 'datasetId',
      header: 'Dataset ID',
      sortable: true,
      sortValue: (dataset) => dataset.datasetId,
      render: (dataset) => (
        <Link
          component={RouterLink}
          to={`/gcp/bigquery/datasets/${encodeURIComponent(dataset.datasetId)}`}
        >
          {dataset.datasetId}
        </Link>
      ),
    },
    {
      key: 'location',
      header: 'Location',
      sortable: true,
      sortValue: (dataset) => dataset.location ?? null,
      render: (dataset) => dataset.location || '—',
    },
    {
      key: 'friendlyName',
      header: 'Friendly name',
      sortable: true,
      sortValue: (dataset) => dataset.friendlyName ?? null,
      render: (dataset) => dataset.friendlyName || '—',
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (dataset) => (
        <Tooltip title="Delete dataset">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(dataset)}
              aria-label={`Delete ${dataset.datasetId}`}
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
        id="bigquery"
        title="BigQuery"
        subtitle={`Datasets · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create dataset
          </Button>
        }
      >
        <BigQueryTabs />
      </GcpPageHeader>

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter datasets"
        onRefresh={() => void datasets.refetch()}
        refreshing={datasets.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="BigQuery datasets"
        columns={columns}
        rows={rows}
        getRowKey={(dataset) => dataset.datasetId}
        loading={datasets.isLoading}
        error={datasets.isError ? 'Failed to load datasets.' : null}
        emptyMessage={filter ? 'No datasets match the filter.' : 'No datasets in this project.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(dataset) => <GcpRowDetail row={dataset} />}
        detailTitle={(dataset) => dataset.datasetId}
      />

      <CreateDatasetDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Stack>
  )
}
