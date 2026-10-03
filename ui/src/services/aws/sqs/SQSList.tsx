import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  Badge,
  Box,
  Button,
  ButtonDropdown,
  ContentLayout,
  Header,
  Link,
  Modal,
  SpaceBetween,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import { listQueues, deleteQueue, type Queue } from '../../../api/sqs'
import { formatDate } from '../../../lib/date'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'
import { SQSCreate } from './SQSCreate'

export function SQSList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<Queue[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const qc = useQueryClient()
  const navigate = useNavigate()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['sqs', 'queues'],
    queryFn: () => listQueues(),
  })

  const deleteMut = useMutation({
    mutationFn: async (queues: Queue[]) => {
      for (const queue of queues) await deleteQueue(queue.url)
    },
    onSuccess: (_result, queues) => {
      void qc.invalidateQueries({ queryKey: ['sqs', 'queues'] })
      notify({
        type: 'success',
        header: `Deleted ${queues.length} queue${queues.length !== 1 ? 's' : ''}`,
      })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const queues = data?.items ?? []

  const columns: ResourceColumn<Queue>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (q) => q.name,
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
    {
      id: 'type',
      header: 'Type',
      filterLabel: 'Type',
      filterValue: (q) => q.type,
      cell: (q) => <Badge color={q.type === 'FIFO' ? 'blue' : 'grey'}>{q.type}</Badge>,
    },
    {
      id: 'available',
      header: 'Messages available',
      cell: (q) => q.messagesAvailable.toLocaleString(),
    },
    {
      id: 'inflight',
      header: 'Messages in flight',
      cell: (q) => q.messagesInFlight.toLocaleString(),
    },
    { id: 'created', header: 'Created', cell: (q) => formatDate(q.createdAt) },
  ]

  return (
    <ContentLayout header={<Header variant="h1">SQS queues</Header>}>
      {error ? (
        <ErrorState header="Failed to load queues" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="sqs"
          favorite={(q) => ({ id: q.url, label: q.name, href: '/aws/sqs/' + encodeURIComponent(q.url), type: 'queue' })}
          items={queues}
          columns={columns}
          trackBy={(q) => q.url}
          title="Queues"
          loading={isLoading}
          onRowClick={(q) => navigate(`/aws/sqs/${encodeURIComponent(q.url)}`)}
          selectionType="multi"
          selectedItems={selected}
          onSelectionChange={setSelected}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <ButtonDropdown
                items={[{ id: 'delete', text: 'Delete', disabled: selected.length === 0 }]}
                onItemClick={() => setConfirmDelete(true)}
                disabled={selected.length === 0}
              >
                Actions
              </ButtonDropdown>
              <Button variant="primary" onClick={() => setCreateOpen(true)}>
                Create queue
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No queues"
          emptyBody="SQS queues let your applications communicate asynchronously."
        />
      )}

      {createOpen && (
        <SQSCreate
          onClose={() => setCreateOpen(false)}
          onCreated={() => {
            setCreateOpen(false)
            void qc.invalidateQueries({ queryKey: ['sqs', 'queues'] })
            notify({ type: 'success', header: 'Queue created' })
          }}
        />
      )}

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete queues"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => deleteMut.mutate(selected)}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Permanently delete {selected.length} queue{selected.length !== 1 ? 's' : ''}? All messages
        will be lost and cannot be recovered.
      </Modal>
    </ContentLayout>
  )
}
