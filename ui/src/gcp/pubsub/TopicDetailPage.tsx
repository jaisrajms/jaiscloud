import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Divider,
  IconButton,
  Link,
  Stack,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Tabs,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import SendOutlinedIcon from '@mui/icons-material/SendOutlined'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { getTopic, listSubscriptions } from '../../api/gcp/pubsub'
import { useAccount } from '../../context/AccountContext'
import { IamPanel } from './IamPanel'
import { PublishDialog } from './PublishDialog'
import { GcpPageTitle } from '../common/PageTitle'

/** Topic detail: configuration, dependent subscriptions, IAM. */
export function TopicDetailPage() {
  const { topic = '' } = useParams()
  const { accountId } = useAccount()
  const [tab, setTab] = useState(0)
  const [publishOpen, setPublishOpen] = useState(false)

  const query = useQuery({
    queryKey: ['gcp', 'pubsub', 'topic', topic, accountId],
    queryFn: () => getTopic(topic),
  })

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 1, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton component={RouterLink} to="/gcp/pubsub/topics" aria-label="Back to topics">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="pubsub">{topic}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Pub/Sub topic · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<SendOutlinedIcon />} onClick={() => setPublishOpen(true)}>
          Publish message
        </Button>
      </Stack>

      {query.isError && <Alert severity="error">Failed to load the topic.</Alert>}
      {query.isLoading && <CircularProgress size={24} />}

      <Tabs
        value={tab}
        onChange={(_, v: number) => setTab(v)}
        sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}
      >
        <Tab label="Details" />
        <Tab label="Subscriptions" />
        <Tab label="Permissions" />
      </Tabs>

      {tab === 0 && query.data && (
        <Stack spacing={1} sx={{ maxWidth: 640 }}>
          <DetailRow label="Full resource name" value={query.data.fullName ?? topic} />
          <DetailRow label="Message retention" value={query.data.messageRetentionDuration || '7 days'} />
          <DetailRow
            label="Encryption"
            value={query.data.kmsKeyName ? `CMEK · ${query.data.kmsKeyName}` : 'Google-managed'}
          />
          <DetailRow
            label="Labels"
            value={
              query.data.labels
                ? Object.entries(query.data.labels)
                    .map(([k, v]) => `${k}=${v}`)
                    .join(', ')
                : '—'
            }
          />
        </Stack>
      )}

      {tab === 1 && <TopicSubscriptions topic={topic} />}
      {tab === 2 && <IamPanel kind="topic" name={topic} />}

      <PublishDialog topic={topic} open={publishOpen} onClose={() => setPublishOpen(false)} />
    </Box>
  )
}

function TopicSubscriptions({ topic }: { topic: string }) {
  const { accountId } = useAccount()
  const query = useQuery({
    queryKey: ['gcp', 'pubsub', 'subscriptions', accountId],
    queryFn: () => listSubscriptions(),
  })
  const subs = (query.data?.subscriptions ?? []).filter((s) => s.topic === topic)

  return (
    <Box sx={{ maxWidth: 720 }}>
      {query.isError && <Alert severity="error">Failed to load subscriptions.</Alert>}
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>Subscription</TableCell>
            <TableCell>Delivery</TableCell>
            <TableCell>State</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {subs.map((sub) => (
            <TableRow key={sub.name}>
              <TableCell>
                <Link
                  component={RouterLink}
                  to={`/gcp/pubsub/subscriptions/${encodeURIComponent(sub.name)}`}
                >
                  {sub.name}
                </Link>
              </TableCell>
              <TableCell>{sub.pushEndpoint ? 'Push' : 'Pull'}</TableCell>
              <TableCell>{sub.state || 'ACTIVE'}</TableCell>
            </TableRow>
          ))}
          {subs.length === 0 && (
            <TableRow>
              <TableCell colSpan={3} sx={{ color: 'text.secondary' }}>
                No subscriptions attached to this topic.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </Box>
  )
}

function DetailRow({ label, value }: { label: string; value: string }) {
  return (
    <Box>
      <Typography variant="caption" color="text.secondary">
        {label}
      </Typography>
      <Typography variant="body1" sx={{ overflowWrap: 'anywhere' }}>
        {value}
      </Typography>
      <Divider sx={{ mt: 1 }} />
    </Box>
  )
}
