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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import {
  deleteTrigger,
  getTrigger,
  getTriggerIam,
  putTriggerIam,
  type EventarcTrigger,
} from '../../api/gcp/eventarc'
import { useAccount } from '../../context/AccountContext'
import { IamPolicyPanel } from '../common/IamPolicyPanel'
import { TriggerDialog } from './TriggerDialog'
import { destinationLabel, shortDate } from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** A small label/value row for the trigger overview. */
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

/** A single Eventarc trigger: overview, filters, IAM and raw config. */
export function TriggerDetailPage() {
  const { location = '', trigger: triggerName = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [editOpen, setEditOpen] = useState(false)

  const key = ['gcp', 'eventarc', 'trigger', location, triggerName, accountId]

  const detail = useQuery({
    queryKey: key,
    queryFn: () => getTrigger(location, triggerName),
    enabled: Boolean(location && triggerName),
  })

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'eventarc'] })

  const remove = useMutation({
    mutationFn: () => deleteTrigger(location, triggerName),
    onSuccess: () => {
      invalidate()
      navigate('/gcp/eventarc/triggers')
    },
  })

  const trigger: EventarcTrigger | undefined = detail.data

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}>
        <IconButton component={RouterLink} to="/gcp/eventarc/triggers" aria-label="Back to triggers">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="eventarc">{triggerName}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Eventarc trigger · {location || '—'}
          </Typography>
        </Box>
        {trigger && (
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

      {detail.isError && <Alert severity="error">Failed to load the trigger.</Alert>}
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

      {trigger && (
        <>
          <Box sx={{ mb: 2 }}>
            <Chip size="small" label={destinationLabel(trigger.destinationType)} />
          </Box>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(3, 1fr)' },
              gap: 2,
            }}
          >
            <Detail label="Destination">{trigger.destination}</Detail>
            {trigger.destinationType === 'cloudRun' && (
              <Detail label="Destination region">{trigger.destinationRegion}</Detail>
            )}
            <Detail label="Service account">{trigger.serviceAccount}</Detail>
            <Detail label="Channel">{trigger.channel}</Detail>
            <Detail label="Transport Pub/Sub topic">{trigger.transportPubsubTopic}</Detail>
            <Detail label="Transport subscription">{trigger.transportPubsubSubscription}</Detail>
            <Detail label="Event data content type">{trigger.eventDataContentType}</Detail>
            <Detail label="Created">{shortDate(trigger.createTime)}</Detail>
            <Detail label="Updated">{shortDate(trigger.updateTime)}</Detail>
          </Box>

          <Divider sx={{ my: 3 }} />

          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Event filters
          </Typography>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Attribute</TableCell>
                <TableCell>Operator</TableCell>
                <TableCell>Value</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {(trigger.eventFilters ?? []).map((filter, index) => (
                <TableRow key={index}>
                  <TableCell>{filter.attribute}</TableCell>
                  <TableCell>{filter.operator || '='}</TableCell>
                  <TableCell sx={{ overflowWrap: 'anywhere' }}>{filter.value}</TableCell>
                </TableRow>
              ))}
              {(trigger.eventFilters ?? []).length === 0 && (
                <TableRow>
                  <TableCell colSpan={3} sx={{ color: 'text.secondary' }}>
                    No filters.
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>

          <Divider sx={{ my: 3 }} />

          <IamPolicyPanel
            title="Trigger IAM policy"
            queryKey={['gcp', 'eventarc', 'trigger', location, triggerName, 'iam', accountId]}
            load={() => getTriggerIam(location, triggerName)}
            save={(policy) => putTriggerIam(location, triggerName, policy)}
            defaultRole="roles/eventarc.admin"
          />

          <TriggerDialog open={editOpen} onClose={() => setEditOpen(false)} trigger={trigger} />
        </>
      )}
    </Box>
  )
}
