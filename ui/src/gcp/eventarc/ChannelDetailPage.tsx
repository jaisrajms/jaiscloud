import { useState } from 'react'
import type { ReactNode } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  IconButton,
  Stack,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import {
  deleteChannel,
  getChannel,
  getChannelIam,
  putChannelIam,
  type EventarcChannel,
} from '../../api/gcp/eventarc'
import { useAccount } from '../../context/AccountContext'
import { IamPolicyPanel } from '../common/IamPolicyPanel'
import { ChannelDialog } from './ChannelDialog'
import { channelStateColor, shortDate } from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** A small label/value row for the channel overview. */
function Detail({ label, children }: { label: string; children: ReactNode }) {
  return (
    <Stack spacing={0.5}>
      <Typography variant="caption" color="text.secondary">
        {label}
      </Typography>
      <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
        {children || '—'}
      </Typography>
    </Stack>
  )
}

/** A single Eventarc channel: overview, IAM and raw config. */
export function ChannelDetailPage() {
  const { location = '', channel: channelName = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [editOpen, setEditOpen] = useState(false)

  const key = ['gcp', 'eventarc', 'channel', location, channelName, accountId]

  const detail = useQuery({
    queryKey: key,
    queryFn: () => getChannel(location, channelName),
    enabled: Boolean(location && channelName),
  })

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'eventarc'] })

  const remove = useMutation({
    mutationFn: () => deleteChannel(location, channelName),
    onSuccess: () => {
      invalidate()
      navigate('/gcp/eventarc/channels')
    },
  })

  const channel: EventarcChannel | undefined = detail.data

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}>
        <IconButton component={RouterLink} to="/gcp/eventarc/channels" aria-label="Back to channels">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="eventarc">{channelName}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Eventarc channel · {location || '—'}
          </Typography>
        </Box>
        {channel && (
          <>
            <Button startIcon={<EditOutlinedIcon />} onClick={() => setEditOpen(true)}>
              Edit
            </Button>
            <Button
              color="error"
              startIcon={<DeleteOutlineIcon />}
              disabled={remove.isPending}
              onClick={() => remove.mutate()}
            >
              Delete
            </Button>
          </>
        )}
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the channel.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(remove.error as Error).message}
        </Alert>
      )}
      {detail.isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      )}

      {channel && (
        <>
          <Box sx={{ mb: 2 }}>
            <Chip size="small" label={channel.state || '—'} color={channelStateColor(channel.state)} />
          </Box>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(3, 1fr)' },
              gap: 2,
            }}
          >
            <Detail label="Provider">{channel.provider}</Detail>
            <Detail label="Crypto key">{channel.cryptoKeyName}</Detail>
            <Detail label="Pub/Sub topic">{channel.pubsubTopic}</Detail>
            <Detail label="Activation token">{channel.activationToken}</Detail>
            <Detail label="Created">{shortDate(channel.createTime)}</Detail>
            <Detail label="Updated">{shortDate(channel.updateTime)}</Detail>
          </Box>

          <Divider sx={{ my: 3 }} />

          <IamPolicyPanel
            title="Channel IAM policy"
            queryKey={['gcp', 'eventarc', 'channel', location, channelName, 'iam', accountId]}
            load={() => getChannelIam(location, channelName)}
            save={(policy) => putChannelIam(location, channelName, policy)}
            defaultRole="roles/eventarc.admin"
          />

          <ChannelDialog open={editOpen} onClose={() => setEditOpen(false)} channel={channel} />
        </>
      )}
    </Box>
  )
}
