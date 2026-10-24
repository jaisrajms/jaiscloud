import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Checkbox,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  IconButton,
  Link,
  MenuItem,
  Stack,
  TextField,
  Tooltip,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import {
  createSubscription,
  deleteSubscription,
  listSubscriptions,
  listTopics,
  type Subscription,
} from '../../api/gcp/pubsub'
import { useAccount } from '../../context/AccountContext'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** Pub/Sub subscription list with create / delete. */
export function SubscriptionsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)

  const [name, setName] = useState('')
  const [topic, setTopic] = useState('')
  const [delivery, setDelivery] = useState<'pull' | 'push'>('pull')
  const [pushEndpoint, setPushEndpoint] = useState('')
  const [ackDeadline, setAckDeadline] = useState('')
  const [retention, setRetention] = useState('')
  const [subscriptionFilter, setSubscriptionFilter] = useState('')
  const [exactlyOnce, setExactlyOnce] = useState(false)
  const [ordering, setOrdering] = useState(false)
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const subscriptions = useQuery({
    queryKey: ['gcp', 'pubsub', 'subscriptions', accountId],
    queryFn: () => listSubscriptions(),
  })
  const topics = useQuery({
    queryKey: ['gcp', 'pubsub', 'topics', accountId],
    queryFn: () => listTopics(),
  })

  const create = useMutation({
    mutationFn: createSubscription,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'pubsub', 'subscriptions'] })
      setCreateOpen(false)
      setName('')
      setTopic('')
      setDelivery('pull')
      setPushEndpoint('')
      setAckDeadline('')
      setRetention('')
      setSubscriptionFilter('')
      setExactlyOnce(false)
      setOrdering(false)
    },
  })

  const remove = useMutation({
    mutationFn: deleteSubscription,
    onSuccess: () =>
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'pubsub', 'subscriptions'] }),
  })

  const submit = () =>
    create.mutate({
      name,
      topic,
      ...(delivery === 'push' ? { pushEndpoint } : {}),
      ...(ackDeadline ? { ackDeadlineSeconds: Number(ackDeadline) } : {}),
      ...(retention ? { messageRetentionDuration: retention } : {}),
      ...(subscriptionFilter ? { filter: subscriptionFilter } : {}),
      ...(exactlyOnce ? { enableExactlyOnceDelivery: true } : {}),
      ...(ordering ? { enableMessageOrdering: true } : {}),
    })

  const rows = filterRows(subscriptions.data?.subscriptions ?? [], filter, (sub) =>
    `${sub.name} ${sub.topic ?? ''} ${sub.pushEndpoint ? 'Push' : 'Pull'} ${
      sub.ackDeadlineSeconds ?? ''
    } ${sub.detached ? 'Detached' : sub.state || 'ACTIVE'}`,
  )

  const columns: GcpColumn<Subscription>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (sub) => sub.name,
      render: (sub) => (
        <Link
          component={RouterLink}
          to={`/gcp/pubsub/subscriptions/${encodeURIComponent(sub.name)}`}
        >
          {sub.name}
        </Link>
      ),
    },
    {
      key: 'topic',
      header: 'Topic',
      sortable: true,
      sortValue: (sub) => sub.topic ?? '',
      render: (sub) => sub.topic || '—',
    },
    {
      key: 'delivery',
      header: 'Delivery',
      sortable: true,
      sortValue: (sub) => (sub.pushEndpoint ? 'Push' : 'Pull'),
      render: (sub) => (sub.pushEndpoint ? 'Push' : 'Pull'),
    },
    {
      key: 'ackDeadline',
      header: 'Ack deadline',
      sortable: true,
      sortValue: (sub) => sub.ackDeadlineSeconds ?? null,
      render: (sub) => (sub.ackDeadlineSeconds ? `${sub.ackDeadlineSeconds}s` : '—'),
    },
    {
      key: 'state',
      header: 'State',
      sortable: true,
      sortValue: (sub) => (sub.detached ? 'Detached' : sub.state || 'ACTIVE'),
      render: (sub) => (sub.detached ? 'Detached' : sub.state || 'ACTIVE'),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (sub) => (
        <Tooltip title="Delete subscription">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(sub.name)}
              aria-label={`Delete ${sub.name}`}
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
        id="pubsub"
        title="Subscriptions"
        subtitle={`Pub/Sub · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create subscription
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter subscriptions"
        onRefresh={() => void subscriptions.refetch()}
        refreshing={subscriptions.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Subscriptions"
        columns={columns}
        rows={rows}
        getRowKey={(sub) => sub.name}
        loading={subscriptions.isLoading}
        error={subscriptions.isError ? 'Failed to load subscriptions.' : null}
        emptyMessage={
          filter ? 'No subscriptions match the filter.' : 'No subscriptions in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(sub) => <GcpRowDetail row={sub} />}
        detailTitle={(sub) => sub.name}
      />

      <Dialog open={createOpen} onClose={() => setCreateOpen(false)} fullWidth maxWidth="sm">
        <DialogTitle>Create subscription</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            {create.isError && <Alert severity="error">Could not create the subscription.</Alert>}
            <TextField
              autoFocus
              label="Subscription ID"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my-subscription"
              fullWidth
            />
            <TextField
              select
              label="Topic"
              value={topic}
              onChange={(e) => setTopic(e.target.value)}
              fullWidth
            >
              {(topics.data?.topics ?? []).map((t) => (
                <MenuItem key={t.name} value={t.name}>
                  {t.name}
                </MenuItem>
              ))}
            </TextField>
            <TextField
              select
              label="Delivery type"
              value={delivery}
              onChange={(e) => setDelivery(e.target.value as 'pull' | 'push')}
              fullWidth
            >
              <MenuItem value="pull">Pull</MenuItem>
              <MenuItem value="push">Push</MenuItem>
            </TextField>
            {delivery === 'push' && (
              <TextField
                label="Push endpoint"
                value={pushEndpoint}
                onChange={(e) => setPushEndpoint(e.target.value)}
                placeholder="https://example.com/push"
                fullWidth
              />
            )}
            <TextField
              label="Ack deadline (10-600s)"
              value={ackDeadline}
              onChange={(e) => setAckDeadline(e.target.value)}
              fullWidth
            />
            <TextField
              label="Message retention (e.g. 604800s)"
              value={retention}
              onChange={(e) => setRetention(e.target.value)}
              fullWidth
            />
            <TextField
              label="Filter (immutable after create)"
              value={subscriptionFilter}
              onChange={(e) => setSubscriptionFilter(e.target.value)}
              fullWidth
            />
            <FormControlLabel
              control={
                <Checkbox
                  checked={exactlyOnce}
                  disabled={delivery === 'push'}
                  onChange={(e) => setExactlyOnce(e.target.checked)}
                />
              }
              label="Exactly-once delivery (pull only)"
            />
            <FormControlLabel
              control={
                <Checkbox checked={ordering} onChange={(e) => setOrdering(e.target.checked)} />
              }
              label="Message ordering"
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={!name || !topic || (delivery === 'push' && !pushEndpoint) || create.isPending}
            onClick={submit}
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>
    </Stack>
  )
}
