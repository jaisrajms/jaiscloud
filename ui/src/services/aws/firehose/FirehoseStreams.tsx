import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Box,
  Button,
  ButtonDropdown,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Modal,
  Select,
  SpaceBetween,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import {
  listDeliveryStreams,
  createDeliveryStream,
  deleteDeliveryStream,
  type DeliveryStream,
} from '../../../api/firehose'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { useNotifications } from '../../../components/notifications'

const TYPE_OPTIONS = [
  { label: 'DirectPut', value: 'DirectPut' },
  { label: 'KinesisStreamAsSource', value: 'KinesisStreamAsSource' },
]

export function FirehoseStreams() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<DeliveryStream[]>([])
  const [details, setDetails] = useState<DeliveryStream | null>(null)
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [form, setForm] = useState({ name: '', type: 'DirectPut' })
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['firehose', 'streams'],
    queryFn: listDeliveryStreams,
  })

  const create = useMutation({
    mutationFn: () => createDeliveryStream(form),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['firehose', 'streams'] })
      notify({ type: 'success', header: 'Delivery stream creating', content: form.name })
      setCreateOpen(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const del = useMutation({
    mutationFn: async (streams: DeliveryStream[]) => {
      for (const stream of streams) await deleteDeliveryStream(stream.name)
    },
    onSuccess: (_r, streams) => {
      void qc.invalidateQueries({ queryKey: ['firehose', 'streams'] })
      notify({ type: 'success', header: `Deleted ${streams.length} stream(s)` })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const items = data?.items ?? []

  const columns: ResourceColumn<DeliveryStream>[] = [
    {
      id: 'name',
      header: 'Stream name',
      filterLabel: 'Stream name',
      filterValue: (s) => s.name,
      cell: (s) => s.name,
    },
    {
      id: 'actions',
      header: '',
      cell: (s) => (
        <Button variant="inline-link" onClick={() => setDetails(s)}>
          View details
        </Button>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">Firehose delivery streams</Header>}>
      {error ? (
        <ErrorState header="Failed to load delivery streams" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="firehose"
          favorite={(s) => ({ id: s.name, label: s.name, href: '/aws/firehose/streams', type: 'delivery stream' })}
          items={items}
          columns={columns}
          trackBy={(s) => s.name}
          title="Delivery streams"
          loading={isLoading}
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
                Create stream
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No delivery streams"
          emptyBody="Create a Firehose delivery stream to load data into a destination."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={details?.name ?? 'Delivery stream'}
        items={details ? [{ label: 'Stream name', value: details.name }] : []}
      />

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create delivery stream"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={create.isPending}
                disabled={!form.name.trim()}
                onClick={() => create.mutate()}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="Stream name">
              <Input
                autoFocus
                value={form.name}
                onChange={({ detail }) => setForm({ ...form, name: detail.value })}
                placeholder="my-delivery-stream"
              />
            </FormField>
            <FormField label="Type">
              <Select
                selectedOption={TYPE_OPTIONS.find((o) => o.value === form.type) ?? TYPE_OPTIONS[0]!}
                onChange={({ detail }) =>
                  setForm({ ...form, type: detail.selectedOption.value ?? 'DirectPut' })
                }
                options={TYPE_OPTIONS}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete delivery streams"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(false)}>
                Cancel
              </Button>
              <Button variant="primary" loading={del.isPending} onClick={() => del.mutate(selected)}>
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Permanently delete {selected.length} delivery stream{selected.length !== 1 ? 's' : ''}?
      </Modal>
    </ContentLayout>
  )
}
