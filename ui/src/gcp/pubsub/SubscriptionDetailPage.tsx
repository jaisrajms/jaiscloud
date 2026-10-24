import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Checkbox,
  CircularProgress,
  Divider,
  FormControlLabel,
  IconButton,
  Stack,
  Tab,
  Tabs,
  TextField,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { getSubscription, updateSubscription } from '../../api/gcp/pubsub'
import { useAccount } from '../../context/AccountContext'
import { IamPanel } from './IamPanel'
import { GcpPageTitle } from '../common/PageTitle'

/** Subscription detail: configuration edit + IAM. */
export function SubscriptionDetailPage() {
  const { subscription = '' } = useParams()
  const [tab, setTab] = useState(0)

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 1 }}>
        <IconButton
          component={RouterLink}
          to="/gcp/pubsub/subscriptions"
          aria-label="Back to subscriptions"
        >
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ minWidth: 0 }}>
          <GcpPageTitle id="pubsub">{subscription}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Pub/Sub subscription
          </Typography>
        </Box>
      </Stack>

      <Tabs
        value={tab}
        onChange={(_, v: number) => setTab(v)}
        sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}
      >
        <Tab label="Details" />
        <Tab label="Permissions" />
      </Tabs>

      {tab === 0 && <DetailsTab name={subscription} />}
      {tab === 1 && <IamPanel kind="subscription" name={subscription} />}
    </Box>
  )
}

function DetailsTab({ name }: { name: string }) {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const query = useQuery({
    queryKey: ['gcp', 'pubsub', 'subscription', name, accountId],
    queryFn: () => getSubscription(name),
  })

  const [ackDeadline, setAckDeadline] = useState('')
  const [exactlyOnce, setExactlyOnce] = useState(false)
  const [ordering, setOrdering] = useState(false)
  const [deadLetterTopic, setDeadLetterTopic] = useState('')
  const [maxAttempts, setMaxAttempts] = useState('')
  const [minBackoff, setMinBackoff] = useState('')
  const [maxBackoff, setMaxBackoff] = useState('')

  useEffect(() => {
    const d = query.data
    if (!d) return
    setAckDeadline(d.ackDeadlineSeconds ? String(d.ackDeadlineSeconds) : '')
    setExactlyOnce(Boolean(d.enableExactlyOnceDelivery))
    setOrdering(Boolean(d.enableMessageOrdering))
    setDeadLetterTopic(d.deadLetterTopic ?? '')
    setMaxAttempts(d.maxDeliveryAttempts ? String(d.maxDeliveryAttempts) : '')
    const rp = d.retryPolicy as { minimumBackoff?: string; maximumBackoff?: string } | undefined
    setMinBackoff(rp?.minimumBackoff ?? '')
    setMaxBackoff(rp?.maximumBackoff ?? '')
  }, [query.data])

  const save = useMutation({
    mutationFn: (body: { updateMask: string; subscription: Record<string, unknown> }) =>
      updateSubscription(name, body),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'pubsub', 'subscription', name] })
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'pubsub', 'subscriptions'] })
    },
  })

  const onSave = () => {
    const mask = ['ackDeadlineSeconds', 'enableExactlyOnceDelivery', 'enableMessageOrdering']
    const subscription: Record<string, unknown> = {
      ackDeadlineSeconds: Number(ackDeadline) || 0,
      enableExactlyOnceDelivery: exactlyOnce,
      enableMessageOrdering: ordering,
    }
    if (deadLetterTopic) {
      subscription.deadLetterPolicy = {
        deadLetterTopic,
        ...(maxAttempts ? { maxDeliveryAttempts: Number(maxAttempts) } : {}),
      }
      mask.push('deadLetterPolicy')
    }
    if (minBackoff || maxBackoff) {
      subscription.retryPolicy = {
        ...(minBackoff ? { minimumBackoff: minBackoff } : {}),
        ...(maxBackoff ? { maximumBackoff: maxBackoff } : {}),
      }
      mask.push('retryPolicy')
    }
    save.mutate({ updateMask: mask.join(','), subscription })
  }

  const d = query.data
  const isPush = Boolean(d?.pushEndpoint)

  return (
    <Box sx={{ maxWidth: 640 }}>
      {query.isError && <Alert severity="error">Failed to load the subscription.</Alert>}
      {query.isLoading && <CircularProgress size={24} />}
      {save.isError && <Alert severity="error">Failed to save changes.</Alert>}
      {save.isSuccess && <Alert severity="success">Subscription updated.</Alert>}

      {d && (
        <Stack spacing={1} sx={{ mb: 3 }}>
          <DetailRow label="Full resource name" value={d.fullName ?? name} />
          <DetailRow label="Topic" value={d.topic || '—'} />
          <DetailRow label="Delivery" value={isPush ? `Push · ${d.pushEndpoint}` : 'Pull'} />
          <DetailRow label="Message retention" value={d.messageRetentionDuration || '7 days'} />
          <DetailRow label="Expiration" value={d.expirationTtl || '31 days'} />
          <DetailRow label="Filter" value={d.filter || '—'} />
          <DetailRow label="State" value={d.detached ? 'Detached' : d.state || 'ACTIVE'} />
        </Stack>
      )}

      <Divider sx={{ mb: 2 }} />
      <Typography variant="h6" sx={{ mb: 1 }}>
        Edit configuration
      </Typography>
      <Stack spacing={2}>
        <TextField
          label="Ack deadline (10-600s)"
          value={ackDeadline}
          onChange={(e) => setAckDeadline(e.target.value)}
          size="small"
          disabled={!d}
        />
        <FormControlLabel
          control={
            <Checkbox
              checked={exactlyOnce}
              disabled={!d || isPush}
              onChange={(e) => setExactlyOnce(e.target.checked)}
            />
          }
          label="Exactly-once delivery (pull only)"
        />
        <FormControlLabel
          control={
            <Checkbox
              checked={ordering}
              disabled={!d}
              onChange={(e) => setOrdering(e.target.checked)}
            />
          }
          label="Message ordering"
        />
        <TextField
          label="Dead-letter topic (ID)"
          value={deadLetterTopic}
          onChange={(e) => setDeadLetterTopic(e.target.value)}
          size="small"
          disabled={!d}
        />
        <TextField
          label="Max delivery attempts (5-100)"
          value={maxAttempts}
          onChange={(e) => setMaxAttempts(e.target.value)}
          size="small"
          disabled={!d || !deadLetterTopic}
        />
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}>
          <TextField
            label="Min retry backoff (e.g. 10s)"
            value={minBackoff}
            onChange={(e) => setMinBackoff(e.target.value)}
            size="small"
            fullWidth
            disabled={!d}
          />
          <TextField
            label="Max retry backoff (e.g. 600s)"
            value={maxBackoff}
            onChange={(e) => setMaxBackoff(e.target.value)}
            size="small"
            fullWidth
            disabled={!d}
          />
        </Stack>
        <Box>
          <Button variant="contained" disabled={!d || save.isPending} onClick={onSave}>
            Save changes
          </Button>
        </Box>
        <Typography variant="caption" color="text.secondary">
          Filter, push endpoint and message retention are immutable after creation.
        </Typography>
      </Stack>
    </Box>
  )
}

function DetailRow({ label, value }: { label: string; value: string }) {
  return (
    <Box>
      <Typography variant="caption" color="text.secondary">
        {label}
      </Typography>
      <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
        {value}
      </Typography>
    </Box>
  )
}
