import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  Badge,
  Box,
  Button,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Link,
  Modal,
  SpaceBetween,
  Textarea,
  Toggle,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import { listTopics, createTopic, deleteTopic, publish, type Topic } from '../../../api/sns'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

export function SNSList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newFIFO, setNewFIFO] = useState(false)
  const [publishTopic, setPublishTopic] = useState<Topic | null>(null)
  const [publishMsg, setPublishMsg] = useState('')
  const [publishSubject, setPublishSubject] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<Topic | null>(null)
  const qc = useQueryClient()
  const navigate = useNavigate()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['sns', 'topics'],
    queryFn: () => listTopics(),
  })

  const createMut = useMutation({
    mutationFn: () => createTopic({ name: newName, fifo: newFIFO }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sns', 'topics'] })
      notify({ type: 'success', header: 'Topic created', content: newName })
      setCreateOpen(false)
      setNewName('')
      setNewFIFO(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create topic', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (arn: string) => deleteTopic(arn),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sns', 'topics'] })
      notify({ type: 'success', header: 'Topic deleted' })
      setConfirmDelete(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const publishMut = useMutation({
    mutationFn: ({ arn, msg, subj }: { arn: string; msg: string; subj: string }) =>
      publish(arn, { message: msg, subject: subj || undefined }),
    onSuccess: () => {
      notify({ type: 'success', header: 'Message published' })
      setPublishTopic(null)
      setPublishMsg('')
      setPublishSubject('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Publish failed', content: (err as Error).message }),
  })

  const topics = data?.items ?? []

  const columns: ResourceColumn<Topic>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (t) => t.name,
      cell: (t) => (
        <Link
          href={`/ui/aws/sns/${encodeURIComponent(t.arn)}`}
          onFollow={(event) => {
            event.preventDefault()
            navigate(`/aws/sns/${encodeURIComponent(t.arn)}`)
          }}
        >
          {t.name}
        </Link>
      ),
    },
    {
      id: 'type',
      header: 'Type',
      filterLabel: 'Type',
      filterValue: (t) => t.type,
      cell: (t) => <Badge color={t.type === 'FIFO' ? 'blue' : 'grey'}>{t.type}</Badge>,
    },
    {
      id: 'subscriptions',
      header: 'Subscriptions',
      cell: (t) => t.subscriptionCount,
    },
    {
      id: 'actions',
      header: 'Actions',
      cell: (t) => (
        <div onClick={(event) => event.stopPropagation()}>
          <SpaceBetween direction="horizontal" size="xs">
            <Button
              variant="link"
              onClick={() => {
                setPublishTopic(t)
                setPublishMsg('')
                setPublishSubject('')
              }}
            >
              Publish
            </Button>
            <Button variant="link" onClick={() => setConfirmDelete(t)}>
              Delete
            </Button>
          </SpaceBetween>
        </div>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">SNS topics</Header>}>
      {error ? (
        <ErrorState header="Failed to load topics" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="sns"
          favorite={(t) => ({ id: t.arn, label: t.name, href: '/aws/sns/' + encodeURIComponent(t.arn), type: 'topic' })}
          items={topics}
          columns={columns}
          trackBy={(t) => t.arn}
          title="Topics"
          loading={isLoading}
          onRowClick={(t) => navigate(`/aws/sns/${encodeURIComponent(t.arn)}`)}
          actions={
            <Button variant="primary" onClick={() => setCreateOpen(true)}>
              Create topic
            </Button>
          }
          emptyTitle="No topics"
          emptyBody="SNS topics enable fan-out messaging to multiple subscribers."
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create topic"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!newName}
                onClick={() => createMut.mutate()}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="l">
            <FormField label="Topic name">
              <Input
                autoFocus
                value={newName}
                onChange={({ detail }) => setNewName(detail.value)}
                placeholder="my-topic"
              />
            </FormField>
            <FormField label="Delivery type">
              <Toggle
                checked={newFIFO}
                onChange={({ detail }) => setNewFIFO(detail.checked)}
              >
                FIFO topic
              </Toggle>
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={publishTopic !== null}
        onDismiss={() => setPublishTopic(null)}
        header="Publish to topic"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setPublishTopic(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={publishMut.isPending}
                disabled={!publishMsg}
                onClick={() =>
                  publishMut.mutate({
                    arn: publishTopic?.arn ?? '',
                    msg: publishMsg,
                    subj: publishSubject,
                  })
                }
              >
                Publish
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="l">
            <FormField label="Subject" description="Optional">
              <Input
                value={publishSubject}
                onChange={({ detail }) => setPublishSubject(detail.value)}
                placeholder="My subject"
              />
            </FormField>
            <FormField label="Message">
              <Textarea
                autoFocus
                value={publishMsg}
                onChange={({ detail }) => setPublishMsg(detail.value)}
                placeholder="Message body…"
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete !== null}
        onDismiss={() => setConfirmDelete(null)}
        header="Delete topic"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => deleteMut.mutate(confirmDelete?.arn ?? '')}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Permanently delete <b>{confirmDelete?.name}</b> and all its subscriptions?
      </Modal>
    </ContentLayout>
  )
}
