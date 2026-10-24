import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Chip,
  IconButton,
  Link,
  Stack,
  Tooltip,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { deleteTrigger, listTriggers, type EventarcTrigger } from '../../api/gcp/eventarc'
import { useAccount } from '../../context/AccountContext'
import { TriggerDialog } from './TriggerDialog'
import { destinationLabel, filterSummary, shortDate } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** Eventarc triggers across every location, with create/delete. */
export function TriggersPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'eventarc'] })

  const triggers = useQuery({
    queryKey: ['gcp', 'eventarc', 'triggers', accountId],
    queryFn: listTriggers,
  })

  const remove = useMutation({
    mutationFn: (trigger: EventarcTrigger) => deleteTrigger(trigger.location, trigger.name),
    onSuccess: invalidate,
  })

  const rows = filterRows(triggers.data?.triggers ?? [], filter, (trigger) =>
    [
      trigger.name,
      trigger.location,
      destinationLabel(trigger.destinationType),
      trigger.destination ?? '',
      filterSummary(trigger),
    ].join(' '),
  )

  const columns: GcpColumn<EventarcTrigger>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (trigger) => trigger.name,
      render: (trigger) => (
        <Link
          component={RouterLink}
          to={`/gcp/eventarc/triggers/${encodeURIComponent(trigger.location)}/${encodeURIComponent(trigger.name)}`}
        >
          {trigger.name}
        </Link>
      ),
    },
    {
      key: 'location',
      header: 'Location',
      sortable: true,
      sortValue: (trigger) => trigger.location,
      render: (trigger) => trigger.location || '—',
    },
    {
      key: 'destinationType',
      header: 'Destination type',
      sortable: true,
      sortValue: (trigger) => destinationLabel(trigger.destinationType),
      render: (trigger) => (
        <Chip size="small" label={destinationLabel(trigger.destinationType)} />
      ),
    },
    {
      key: 'destination',
      header: 'Destination',
      sortable: true,
      sortValue: (trigger) => trigger.destination ?? '',
      render: (trigger) => trigger.destination || '—',
    },
    {
      key: 'filters',
      header: 'Filters',
      sortable: true,
      sortValue: (trigger) => filterSummary(trigger),
      render: (trigger) => filterSummary(trigger),
    },
    {
      key: 'created',
      header: 'Created',
      sortable: true,
      sortValue: (trigger) => trigger.createTime ?? '',
      render: (trigger) => shortDate(trigger.createTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (trigger) => (
        <Tooltip title="Delete trigger">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(trigger)}
              aria-label={`Delete ${trigger.name}`}
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
        id="eventarc"
        title="Eventarc"
        subtitle={`Triggers · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create trigger
          </Button>
        }
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(remove.error as Error).message}
        </Alert>
      )}

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter triggers"
        onRefresh={() => void triggers.refetch()}
        refreshing={triggers.isFetching}
      />

      <GcpDataTable
        aria-label="Eventarc triggers"
        columns={columns}
        rows={rows}
        getRowKey={(trigger) => `${trigger.location}/${trigger.name}`}
        loading={triggers.isLoading}
        error={triggers.isError ? 'Failed to load triggers.' : null}
        emptyMessage={
          filter ? 'No triggers match the filter.' : 'No Eventarc triggers in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(trigger) => <GcpRowDetail row={trigger} />}
        detailTitle={(trigger) => trigger.name}
      />

      <TriggerDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Stack>
  )
}
