import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Button, Chip, IconButton, Tooltip } from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import {
  deleteNotificationChannel,
  listNotificationChannels,
  type NotificationChannel,
} from '../../api/gcp/monitoring'
import { useAccount } from '../../context/AccountContext'
import { ChannelDialog } from './ChannelDialog'
import { resourceID, verificationColor } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** Notification channels: list, create, edit and delete. */
export function ChannelsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<NotificationChannel | undefined>(undefined)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const channels = useQuery({
    queryKey: ['gcp', 'monitoring', 'channels', accountId],
    queryFn: () => listNotificationChannels(),
  })

  const remove = useMutation({
    mutationFn: (id: string) => deleteNotificationChannel(id),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'monitoring', 'channels'] }),
  })

  const rows = filterRows(
    channels.data?.notificationChannels ?? [],
    filter,
    (channel) => channel.displayName || resourceID(channel.name),
  )

  const columns: GcpColumn<NotificationChannel>[] = [
    {
      key: 'displayName',
      header: 'Display name',
      sortable: true,
      sortValue: (channel) => channel.displayName || resourceID(channel.name),
      render: (channel) => channel.displayName || resourceID(channel.name),
    },
    {
      key: 'type',
      header: 'Type',
      sortable: true,
      sortValue: (channel) => channel.type ?? '',
      render: (channel) => channel.type || '—',
    },
    {
      key: 'verification',
      header: 'Verification',
      sortable: true,
      sortValue: (channel) => channel.verificationStatus || 'UNVERIFIED',
      render: (channel) => (
        <Chip
          size="small"
          label={channel.verificationStatus || 'UNVERIFIED'}
          color={verificationColor(channel.verificationStatus)}
        />
      ),
    },
    {
      key: 'status',
      header: 'Status',
      sortable: true,
      sortValue: (channel) => ((channel.enabled ?? true) ? 'ENABLED' : 'DISABLED'),
      render: (channel) => (
        <Chip
          size="small"
          label={(channel.enabled ?? true) ? 'ENABLED' : 'DISABLED'}
          color={(channel.enabled ?? true) ? 'success' : 'default'}
        />
      ),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (channel) => (
        <>
          <Tooltip title="Edit channel">
            <IconButton
              size="small"
              onClick={() => {
                setEditing(channel)
                setDialogOpen(true)
              }}
            >
              <EditOutlinedIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete channel">
            <IconButton
              size="small"
              onClick={() => remove.mutate(resourceID(channel.name))}
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
        title="Notification channels"
        subtitle={`Destinations for alert notifications · project ${accountId || '—'}`}
        actions={
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => {
              setEditing(undefined)
              setDialogOpen(true)
            }}
          >
            Create channel
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter channels"
        onRefresh={() => void channels.refetch()}
        refreshing={channels.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Notification channels"
        columns={columns}
        rows={rows}
        getRowKey={(channel) => channel.name}
        loading={channels.isLoading}
        error={channels.isError ? 'Failed to load channels.' : null}
        emptyMessage={
          filter ? 'No notification channels match the filter.' : 'No notification channels in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(channel) => <GcpRowDetail row={channel} />}
        detailTitle={(channel) => channel.displayName || resourceID(channel.name)}
      />

      <ChannelDialog open={dialogOpen} onClose={() => setDialogOpen(false)} initial={editing} />
    </Box>
  )
}
