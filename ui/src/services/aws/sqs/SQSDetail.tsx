import { useEffect, useRef, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getQueue, purgeQueue, listDLQSources, getTags, tagQueue, untagQueue, peekMessages, type PeekedMessage } from '../../../api/sqs'
import {
  AttributeEditor,
  Badge,
  Box,
  Button,
  Container,
  ContentLayout,
  Header,
  Input,
  KeyValuePairs,
  Link,
  Modal,
  Pagination,
  SpaceBetween,
  Table,
  Tabs,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import type { TableProps } from '@cloudscape-design/components'
import { formatDate } from '../../../lib/date'
import { useNotifications } from '../../../components/notifications'
import { SQSMessageSend } from './SQSMessageSend'

type Tab = 'overview' | 'messages' | 'dlq' | 'tags'

const TAB_LABELS: Record<Tab, string> = {
  overview: 'Overview',
  messages: 'Messages',
  dlq: 'Dead-letter queue',
  tags: 'Tags',
}

export function SQSDetail() {
  const { queueUrl: rawParam } = useParams<{ queueUrl: string }>()
  const queueUrl = rawParam ? decodeURIComponent(rawParam) : ''
  const [tab, setTab] = useState<Tab>('overview')
  const [msgPage, setMsgPage] = useState(0)
  const [viewMessage, setViewMessage] = useState<PeekedMessage | null>(null)
  const [showSend, setShowSend] = useState(false)
  const [purgeConfirm, setPurgeConfirm] = useState(false)
  const PAGE_SIZE = 50
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { data: queue, isLoading, error } = useQuery({
    queryKey: ['sqs', 'queue', queueUrl],
    queryFn: () => getQueue(queueUrl),
    enabled: !!queueUrl,
  })

  const { data: dlqSources } = useQuery({
    queryKey: ['sqs', 'queue', queueUrl, 'dlq-sources'],
    queryFn: () => listDLQSources(queueUrl),
    enabled: tab === 'dlq' && !!queueUrl,
  })

  const { data: tags } = useQuery({
    queryKey: ['sqs', 'queue', queueUrl, 'tags'],
    queryFn: () => getTags(queueUrl),
    enabled: tab === 'tags' && !!queueUrl,
  })

  const [tagItems, setTagItems] = useState<{ key: string; value: string }[]>([])
  const tagBaseline = useRef<Record<string, string>>({})
  const { notify } = useNotifications()

  useEffect(() => {
    if (tags) {
      tagBaseline.current = tags
      setTagItems(Object.entries(tags).map(([key, value]) => ({ key, value })))
    }
  }, [tags])

  const saveTags = useMutation({
    mutationFn: async () => {
      const next: Record<string, string> = {}
      for (const { key, value } of tagItems) {
        if (key.trim()) next[key.trim()] = value
      }
      const added: Record<string, string> = {}
      for (const [k, v] of Object.entries(next)) {
        if (tagBaseline.current[k] !== v) added[k] = v
      }
      const removed = Object.keys(tagBaseline.current).filter((k) => !(k in next))
      if (Object.keys(added).length > 0) await tagQueue(queueUrl, added)
      if (removed.length > 0) await untagQueue(queueUrl, removed)
    },
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl, 'tags'] })
      notify({ type: 'success', header: 'Tags saved' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Failed to save tags', content: (err as Error).message }),
  })

  const { data: peekData, isFetching: peekFetching, refetch: refetchPeek } = useQuery({
    queryKey: ['sqs', 'queue', queueUrl, 'peek', msgPage],
    queryFn: () => peekMessages(queueUrl, { offset: msgPage * PAGE_SIZE, limit: PAGE_SIZE }),
    enabled: tab === 'messages' && !!queueUrl,
  })

  const purgeMut = useMutation({
    mutationFn: () => purgeQueue(queueUrl),
    onSuccess: () => {
      setPurgeConfirm(false)
      void qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] })
    },
  })

  if (isLoading) {
    return (
      <ContentLayout header={<Header variant="h1">Queue</Header>}>
        <Box padding="l">Loading…</Box>
      </ContentLayout>
    )
  }

  if (error || !queue) {
    return (
      <ContentLayout header={<Header variant="h1">Queue</Header>}>
        <ErrorState header="Queue not found" message={error ? (error as Error).message : 'Queue not found.'} />
      </ContentLayout>
    )
  }

  const overviewRows: [string, string][] = [
    ['URL', queue.url],
    ['ARN', queue.arn],
    ['Type', queue.type],
    ['Messages available', queue.messagesAvailable.toLocaleString()],
    ['Messages in flight', queue.messagesInFlight.toLocaleString()],
    ['Visibility timeout', `${queue.visibilityTimeout}s`],
    ['Message retention', `${queue.retentionPeriod}s`],
    ['Max message size', `${Math.round(queue.maxMessageSize / 1024)} KB`],
    ['Dead-letter queue', queue.dlqArn || '—'],
    ['Max receive count', queue.dlqMaxReceive ? String(queue.dlqMaxReceive) : '—'],
    ['Created', formatDate(queue.createdAt)],
  ]

  const messageColumns: TableProps.ColumnDefinition<PeekedMessage>[] = [
    {
      id: 'status',
      header: 'Status',
      cell: (m) => (
        <Badge
          color={m.status === 'visible' ? 'green' : m.status === 'in-flight' ? 'blue' : 'grey'}
        >
          {m.status}
        </Badge>
      ),
    },
    { id: 'id', header: 'Message ID', cell: (m) => <Box variant="code">{m.messageId}</Box> },
    {
      id: 'body',
      header: 'Body',
      cell: (m) => (
        <Box variant="code">{m.body.length > 80 ? `${m.body.slice(0, 80)}…` : m.body}</Box>
      ),
    },
    { id: 'rcv', header: 'Receive count', cell: (m) => m.receiveCount },
    { id: 'sent', header: 'Sent at', cell: (m) => formatDate(m.sentAt) },
  ]
  if (queue.type === 'FIFO') {
    messageColumns.push({ id: 'group', header: 'Group', cell: (m) => m.groupId ?? '—' })
  }
  messageColumns.push({
    id: 'actions',
    header: '',
    cell: (m) => (
      <Button variant="inline-link" onClick={() => setViewMessage(m)}>
        View
      </Button>
    ),
  })

  return (
    <ContentLayout
      header={
        <Header
          variant="h1"
          description={<Box variant="code">{queue.arn}</Box>}
          actions={<Button onClick={() => setPurgeConfirm(true)}>Purge queue</Button>}
        >
          {queue.name}
        </Header>
      }
    >
      <Tabs
        tabs={(Object.keys(TAB_LABELS) as Tab[]).map((t) => ({ id: t, label: TAB_LABELS[t] }))}
        activeTabId={tab}
        onChange={({ detail }) => setTab(detail.activeTabId as Tab)}
      />

      {/* Tab content */}
      {tab === 'overview' && (
        <KeyValuePairs
          columns={2}
          items={overviewRows.map(([label, value]) => ({ label, value }))}
        />
      )}

      {tab === 'messages' && (
        <SpaceBetween size="m">
          <SpaceBetween direction="horizontal" size="xs">
            <Button
              iconName="refresh"
              loading={peekFetching}
              onClick={() => {
                setMsgPage(0)
                void refetchPeek()
              }}
            >
              Refresh
            </Button>
            <Button onClick={() => setShowSend((s) => !s)}>
              {showSend ? 'Hide send form' : 'Send message'}
            </Button>
            <Box color="text-body-secondary" variant="span">
              {peekData
                ? `${peekData.total.toLocaleString()} message${peekData.total !== 1 ? 's' : ''}`
                : '—'}
            </Box>
          </SpaceBetween>

          {showSend && (
            <Container header={<Header variant="h2">Send message</Header>}>
              <SQSMessageSend
                queueUrl={queueUrl}
                isFifo={queue.type === 'FIFO'}
                onSent={() => {
                  void refetchPeek()
                }}
              />
            </Container>
          )}

          <Table
            columnDefinitions={messageColumns}
            items={peekData?.messages ?? []}
            loading={peekFetching}
            loadingText="Loading messages"
            trackBy="messageId"
            header={<Header counter={`(${peekData?.total ?? 0})`}>Messages</Header>}
            empty={
              <Box textAlign="center" color="inherit">
                <b>No messages</b>
              </Box>
            }
            pagination={
              peekData && peekData.total > PAGE_SIZE ? (
                <Pagination
                  currentPageIndex={msgPage + 1}
                  pagesCount={Math.ceil(peekData.total / PAGE_SIZE)}
                  onChange={({ detail }) => {
                    setMsgPage(detail.currentPageIndex - 1)
                    setViewMessage(null)
                  }}
                />
              ) : undefined
            }
          />
        </SpaceBetween>
      )}

      {tab === 'dlq' && (
        <SpaceBetween size="l">
          {queue.dlqArn ? (
            <Container header={<Header variant="h2">Dead-letter queue</Header>}>
              <KeyValuePairs
                columns={1}
                items={[
                  { label: 'ARN', value: <Box variant="code">{queue.dlqArn}</Box> },
                  {
                    label: 'Max receive count',
                    value: queue.dlqMaxReceive ? String(queue.dlqMaxReceive) : '—',
                  },
                ]}
              />
            </Container>
          ) : (
            <Box color="text-body-secondary">No dead-letter queue configured.</Box>
          )}

          <Container
            header={
              <Header variant="h2" counter={`(${dlqSources?.items.length ?? 0})`}>
                Source queues using this queue as DLQ
              </Header>
            }
          >
            <Table
              columnDefinitions={[
                {
                  id: 'name',
                  header: 'Name',
                  cell: (q) => (
                    <Link
                      href={`/ui/aws/sqs/${encodeURIComponent(q.url)}`}
                      onFollow={(event) => {
                        event.preventDefault()
                        navigate(`/aws/sqs/${encodeURIComponent(q.url)}`)
                      }}
                    >
                      {q.name}
                    </Link>
                  ),
                },
                { id: 'type', header: 'Type', cell: (q) => q.type },
                {
                  id: 'maxReceive',
                  header: 'Max receive count',
                  cell: (q) => q.dlqMaxReceive ?? '—',
                },
              ]}
              items={dlqSources?.items ?? []}
              trackBy="url"
              empty={
                <Box textAlign="center" color="inherit">
                  <b>No source queues</b>
                  <Box variant="p" color="inherit">
                    No queues are using this queue as their dead-letter queue.
                  </Box>
                </Box>
              }
            />
          </Container>
        </SpaceBetween>
      )}

      {tab === 'tags' && (
        <SpaceBetween size="m">
          <AttributeEditor
            items={tagItems}
            onAddButtonClick={() => setTagItems((prev) => [...prev, { key: '', value: '' }])}
            onRemoveButtonClick={({ detail: { itemIndex } }) =>
              setTagItems((prev) => prev.filter((_, i) => i !== itemIndex))
            }
            addButtonText="Add tag"
            removeButtonText="Remove"
            definition={[
              {
                label: 'Key',
                control: (item: { key: string; value: string }, i: number) => (
                  <Input
                    value={item.key}
                    placeholder="Key"
                    onChange={({ detail }) =>
                      setTagItems((prev) =>
                        prev.map((it, idx) => (idx === i ? { ...it, key: detail.value } : it)),
                      )
                    }
                  />
                ),
              },
              {
                label: 'Value',
                control: (item: { key: string; value: string }, i: number) => (
                  <Input
                    value={item.value}
                    placeholder="Value"
                    onChange={({ detail }) =>
                      setTagItems((prev) =>
                        prev.map((it, idx) => (idx === i ? { ...it, value: detail.value } : it)),
                      )
                    }
                  />
                ),
              },
            ]}
          />
          <Button variant="primary" loading={saveTags.isPending} onClick={() => saveTags.mutate()}>
            Save tags
          </Button>
        </SpaceBetween>
      )}

      <Modal
        visible={purgeConfirm}
        onDismiss={() => setPurgeConfirm(false)}
        header="Purge queue"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setPurgeConfirm(false)}>
                Cancel
              </Button>
              <Button variant="primary" loading={purgeMut.isPending} onClick={() => purgeMut.mutate()}>
                Purge
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        {purgeMut.error && (
          <Box color="text-status-error" margin={{ bottom: 's' }}>
            {(purgeMut.error as Error).message}
          </Box>
        )}
        All messages in <b>{queue.name}</b> will be permanently deleted. This action cannot be
        undone.
      </Modal>

      <Modal
        visible={viewMessage != null}
        onDismiss={() => setViewMessage(null)}
        header="Message body"
        size="large"
      >
        <Box variant="pre">{prettyBody(viewMessage?.body ?? '')}</Box>
      </Modal>
    </ContentLayout>
  )
}

function prettyBody(body: string): string {
  try {
    return JSON.stringify(JSON.parse(body), null, 2)
  } catch {
    return body
  }
}
