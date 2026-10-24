import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Box, Button, Link, Stack, Typography } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { listEntities, type DatastoreEntity } from '../../api/gcp/datastore'
import { useAccount } from '../../context/AccountContext'
import { CreateEntityDialog } from './CreateEntityDialog'
import { entityHref } from './entityRoute'
import { keyPathToString, propertiesSummary, shortKey } from './key'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** Entities of one Datastore kind. */
export function EntitiesPage() {
  const { kind = '' } = useParams()
  const { accountId } = useAccount()
  const [filter, setFilter] = useState('')
  const [createOpen, setCreateOpen] = useState(false)

  const entities = useQuery({
    queryKey: ['gcp', 'datastore', 'entities', kind, accountId],
    queryFn: () => listEntities(kind),
    enabled: Boolean(kind),
  })

  const rows = filterRows(entities.data?.entities ?? [], filter, (entity) =>
    `${keyPathToString(entity.key)} ${propertiesSummary(entity.properties)}`,
  )

  const columns: GcpColumn<DatastoreEntity>[] = [
    {
      key: 'key',
      header: 'Key',
      sortable: true,
      sortValue: (entity) => shortKey(entity.key),
      render: (entity) => (
        <Link component={RouterLink} to={entityHref(entity.key)} sx={{ overflowWrap: 'anywhere' }}>
          {keyPathToString(entity.key)}
        </Link>
      ),
    },
    {
      key: 'properties',
      header: 'Properties',
      render: (entity) => (
        <Typography
          variant="body2"
          sx={{ fontFamily: 'monospace', fontSize: 12, overflowWrap: 'anywhere' }}
        >
          {propertiesSummary(entity.properties)}
        </Typography>
      ),
    },
    {
      key: 'updated',
      header: 'Updated',
      sortable: true,
      sortValue: (entity) => entity.updateTime ?? '',
      render: (entity) => shortDate(entity.updateTime),
    },
    {
      key: 'version',
      header: 'Version',
      align: 'right',
      sortable: true,
      sortValue: (entity) => Number(entity.version ?? 0),
      render: (entity) => entity.version ?? '—',
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="datastore"
        title={kind}
        subtitle={`Entities · project ${accountId || '—'}`}
        backTo="/gcp/datastore/kinds"
        backAriaLabel="Back to kinds"
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create entity
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter entities"
        onRefresh={() => void entities.refetch()}
        refreshing={entities.isFetching}
      />

      <GcpDataTable
        aria-label={`Entities of ${kind}`}
        columns={columns}
        rows={rows}
        getRowKey={(entity) => JSON.stringify(entity.key)}
        loading={entities.isLoading}
        error={entities.isError ? 'Failed to load entities.' : null}
        emptyMessage={
          filter
            ? 'No entities match the filter.'
            : 'No entities of this kind. Create one to get started.'
        }
        renderDetail={(entity) => (
          <Stack spacing={0.5}>
            <Typography variant="caption" color="text.secondary">
              Key
            </Typography>
            <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
              {keyPathToString(entity.key)}
            </Typography>
            <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
              Properties
            </Typography>
            <Box
              component="pre"
              sx={{
                m: 0,
                p: 1,
                bgcolor: 'action.hover',
                borderRadius: 1,
                fontSize: 12,
                overflow: 'auto',
              }}
            >
              {JSON.stringify(entity.properties, null, 2)}
            </Box>
          </Stack>
        )}
        detailTitle={(entity) => shortKey(entity.key)}
      />

      <CreateEntityDialog
        open={createOpen}
        kind={kind}
        onClose={() => setCreateOpen(false)}
        onCreated={() => {
          setCreateOpen(false)
          void entities.refetch()
        }}
      />
    </Stack>
  )
}
