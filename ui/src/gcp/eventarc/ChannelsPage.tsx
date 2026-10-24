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
import { deleteChannel, listChannels, type EventarcChannel } from '../../api/gcp/eventarc'
import { useAccount } from '../../context/AccountContext'
import { ChannelDialog } from './ChannelDialog'
import { channelProviderLabel, channelStateColor, shortDate } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** Eventarc channels across every location, with create/delete. */
export function ChannelsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'eventarc'] })

  const channels = useQuery({
    queryKey: ['gcp', 'eventarc', 'channels', accountId],
    queryFn: listChannels,
  })

  const remove = useMutation({
    mutationFn: (channel: EventarcChannel) => deleteChannel(channel.location, channel.name),
    onSuccess: invalidate,
  })

  const rows = filterRows(channels.data?.channels ?? [], filter, (channel) =>
    [
      channel.name,
      channel.location,
      channel.state ?? '',
      channelProviderLabel(channel),
      channel.pubsubTopic ?? '',
    ].join(' '),
  )

  const columns: GcpColumn<EventarcChannel>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (channel) => channel.name,
      render: (channel) => (
        <Link
          component={RouterLink}
          to={`/gcp/eventarc/channels/${encodeURIComponent(channel.location)}/${encodeURIComponent(channel.name)}`}
        >
          {channel.name}
        </Link>
      ),
    },
    {
      key: 'location',
      header: 'Location',
      sortable: true,
      sortValue: (channel) => channel.location,
      render: (channel) => channel.location || '—',
    },
    {
      key: 'state',
      header: 'State',
      sortable: true,
      sortValue: (channel) => channel.state ?? '',
      render: (channel) => (
        <Chip size="small" label={channel.state || '—'} color={channelStateColor(channel.state)} />
      ),
    },
    {
      key: 'provider',
      header: 'Provider',
      sortable: true,
      sortValue: (channel) => channelProviderLabel(channel),
      render: (channel) => channelProviderLabel(channel),
    },
    {
      key: 'pubsubTopic',
      header: 'Pub/Sub topic',
      sortable: true,
      sortValue: (channel) => channel.pubsubTopic ?? '',
      render: (channel) => channel.pubsubTopic || '—',
    },
    {
      key: 'created',
      header: 'Created',
      sortable: true,
      sortValue: (channel) => channel.createTime ?? '',
      render: (channel) => shortDate(channel.createTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (channel) => (
        <Tooltip title="Delete channel">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(channel)}
              aria-label={`Delete ${channel.name}`}
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
        subtitle={`Channels · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create channel
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
        filterPlaceholder="Filter channels"
        onRefresh={() => void channels.refetch()}
        refreshing={channels.isFetching}
      />

      <GcpDataTable
        aria-label="Eventarc channels"
        columns={columns}
        rows={rows}
        getRowKey={(channel) => `${channel.location}/${channel.name}`}
        loading={channels.isLoading}
        error={channels.isError ? 'Failed to load channels.' : null}
        emptyMessage={
          filter ? 'No channels match the filter.' : 'No Eventarc channels in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(channel) => <GcpRowDetail row={channel} />}
        detailTitle={(channel) => channel.name}
      />

      <ChannelDialog open={createOpen} onClose={() => setCreateOpen(false)} />
    </Stack>
  )
}
