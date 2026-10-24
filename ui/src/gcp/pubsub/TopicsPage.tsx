import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  Link,
  Stack,
  TextField,
  Tooltip,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import SendOutlinedIcon from '@mui/icons-material/SendOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { createTopic, deleteTopic, listTopics, type Topic } from '../../api/gcp/pubsub'
import { useAccount } from '../../context/AccountContext'
import { PublishDialog } from './PublishDialog'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function topicLabels(topic: Topic): string {
  return topic.labels
    ? Object.entries(topic.labels)
        .map(([k, v]) => `${k}=${v}`)
        .join(', ')
    : '—'
}

/** Pub/Sub topic list with create / delete / publish. */
export function TopicsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [name, setName] = useState('')
  const [retention, setRetention] = useState('')
  const [publishTopic, setPublishTopic] = useState('')
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const topics = useQuery({
    queryKey: ['gcp', 'pubsub', 'topics', accountId],
    queryFn: () => listTopics(),
  })

  const create = useMutation({
    mutationFn: createTopic,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'pubsub', 'topics'] })
      setCreateOpen(false)
      setName('')
      setRetention('')
    },
  })

  const remove = useMutation({
    mutationFn: deleteTopic,
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'pubsub', 'topics'] }),
  })

  const rows = filterRows(topics.data?.topics ?? [], filter, (topic) =>
    `${topic.name} ${topic.messageRetentionDuration ?? ''} ${topic.kmsKeyName ?? ''} ${topicLabels(topic)}`,
  )

  const columns: GcpColumn<Topic>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (topic) => topic.name,
      render: (topic) => (
        <Link component={RouterLink} to={`/gcp/pubsub/topics/${encodeURIComponent(topic.name)}`}>
          {topic.name}
        </Link>
      ),
    },
    {
      key: 'retention',
      header: 'Message retention',
      sortable: true,
      sortValue: (topic) => topic.messageRetentionDuration ?? '7 days',
      render: (topic) => topic.messageRetentionDuration || '7 days',
    },
    {
      key: 'encryption',
      header: 'Encryption',
      sortable: true,
      sortValue: (topic) => (topic.kmsKeyName ? 'CMEK' : 'Google-managed'),
      render: (topic) => (topic.kmsKeyName ? 'CMEK' : 'Google-managed'),
    },
    {
      key: 'labels',
      header: 'Labels',
      sortable: true,
      sortValue: (topic) => topicLabels(topic),
      render: (topic) => topicLabels(topic),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (topic) => (
        <>
          <Tooltip title="Publish message">
            <IconButton size="small" onClick={() => setPublishTopic(topic.name)}>
              <SendOutlinedIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete topic">
            <span>
              <IconButton
                size="small"
                disabled={remove.isPending}
                onClick={() => remove.mutate(topic.name)}
                aria-label={`Delete ${topic.name}`}
              >
                <DeleteOutlineIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
        </>
      ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="pubsub"
        title="Topics"
        subtitle={`Pub/Sub · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create topic
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter topics"
        onRefresh={() => void topics.refetch()}
        refreshing={topics.isFetching}
      />

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Topics"
        columns={columns}
        rows={rows}
        getRowKey={(topic) => topic.name}
        loading={topics.isLoading}
        error={topics.isError ? 'Failed to load topics.' : null}
        emptyMessage={filter ? 'No topics match the filter.' : 'No topics in this project.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(topic) => <GcpRowDetail row={topic} />}
        detailTitle={(topic) => topic.name}
      />

      <Dialog open={createOpen} onClose={() => setCreateOpen(false)} fullWidth maxWidth="xs">
        <DialogTitle>Create topic</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            {create.isError && <Alert severity="error">Could not create the topic.</Alert>}
            <TextField
              autoFocus
              label="Topic ID"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my-topic"
              fullWidth
            />
            <TextField
              label="Message retention (e.g. 604800s)"
              value={retention}
              onChange={(e) => setRetention(e.target.value)}
              fullWidth
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={!name || create.isPending}
            onClick={() =>
              create.mutate({
                name,
                ...(retention ? { messageRetentionDuration: retention } : {}),
              })
            }
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>

      {publishTopic && (
        <PublishDialog
          topic={publishTopic}
          open={Boolean(publishTopic)}
          onClose={() => setPublishTopic('')}
        />
      )}
    </Stack>
  )
}
