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
  SpaceBetween,
  StatusIndicator,
} from '@cloudscape-design/components'
import { CopyText } from '../../../components/CopyText'
import { ErrorState } from '../../../components/ErrorState'
import { listStreams, createStream, deleteStream, type Stream } from '../../../api/kinesis'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { useNotifications } from '../../../components/notifications'

export function KinesisStreams() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<Stream[]>([])
  const [details, setDetails] = useState<Stream | null>(null)
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [form, setForm] = useState({ name: '', shardCount: 1 })
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['kinesis', 'streams'],
    queryFn: listStreams,
  })

  const create = useMutation({
    mutationFn: () => createStream(form),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['kinesis', 'streams'] })
      notify({ type: 'success', header: 'Stream creating', content: form.name })
      setCreateOpen(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const del = useMutation({
    mutationFn: async (streams: Stream[]) => {
      for (const stream of streams) await deleteStream(stream.name)
    },
    onSuccess: (_r, streams) => {
      void qc.invalidateQueries({ queryKey: ['kinesis', 'streams'] })
      notify({ type: 'success', header: `Deleted ${streams.length} stream(s)` })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const items = data?.items ?? []

  const columns: ResourceColumn<Stream>[] = [
    { id: 'name', header: 'Stream name', filterLabel: 'Stream name', filterValue: (s) => s.name, cell: (s) => s.name },
    {
      id: 'status',
      header: 'Status',
      filterLabel: 'Status',
      filterValue: (s) => s.status,
      cell: (s) => (
        <StatusIndicator type={resourceStatus(s.status)}>{s.status || '—'}</StatusIndicator>
      ),
    },
    { id: 'mode', header: 'Mode', filterLabel: 'Mode', filterValue: (s) => s.mode, cell: (s) => s.mode || '—' },
    { id: 'arn', header: 'ARN', cell: (s) => <CopyText value={s.arn} label="stream ARN" /> },
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
    <ContentLayout header={<Header variant="h1">Kinesis streams</Header>}>
      {error ? (
        <ErrorState header="Failed to load streams" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="kinesis"
          favorite={(s) => ({ id: s.name, label: s.name, href: '/aws/kinesis/streams', type: 'stream' })}
          items={items}
          columns={columns}
          trackBy={(s) => s.name}
          title="Streams"
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
          emptyTitle="No streams"
          emptyBody="Create a Kinesis data stream to start ingesting records."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={details?.name ?? 'Stream'}
        items={
          details
            ? [
                { label: 'Stream name', value: details.name },
                {
                  label: 'Status',
                  value: (
                    <StatusIndicator type={resourceStatus(details.status)}>
                      {details.status || '—'}
                    </StatusIndicator>
                  ),
                },
                { label: 'Mode', value: details.mode || '—' },
                { label: 'ARN', value: <Box variant="code">{details.arn || '—'}</Box> },
              ]
            : []
        }
      />

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create Kinesis stream"
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
                placeholder="my-stream"
              />
            </FormField>
            <FormField label="Shard count">
              <Input
                type="number"
                value={String(form.shardCount)}
                onChange={({ detail }) => setForm({ ...form, shardCount: Number(detail.value) })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete streams"
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
        Permanently delete {selected.length} stream{selected.length !== 1 ? 's' : ''}?
      </Modal>
    </ContentLayout>
  )
}
