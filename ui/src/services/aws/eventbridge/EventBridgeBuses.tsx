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
  Textarea,
} from '@cloudscape-design/components'
import { CopyText } from '../../../components/CopyText'
import { ErrorState } from '../../../components/ErrorState'
import {
  listEventBuses,
  createEventBus,
  deleteEventBus,
  putEvents,
  type EventBus,
} from '../../../api/eventbridge'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { useNotifications } from '../../../components/notifications'

const EMPTY_EVENT_FORM = { source: '', detailType: '', detail: '{}', bus: '' }

export function EventBridgeBuses() {
  const qc = useQueryClient()
  const { notify } = useNotifications()
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<EventBus[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [sendEventsOpen, setSendEventsOpen] = useState(false)
  const [busName, setBusName] = useState('')
  const [details, setDetails] = useState<EventBus | null>(null)
  const [eventForm, setEventForm] = useState(EMPTY_EVENT_FORM)

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['eventbridge', 'buses'],
    queryFn: () => listEventBuses(),
  })

  const createMut = useMutation({
    mutationFn: () => createEventBus(busName),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['eventbridge', 'buses'] })
      notify({ type: 'success', header: 'Event bus created', content: busName })
      setCreateOpen(false)
      setBusName('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: async (buses: EventBus[]) => {
      for (const bus of buses) await deleteEventBus(bus.name)
    },
    onSuccess: (_result, buses) => {
      void qc.invalidateQueries({ queryKey: ['eventbridge', 'buses'] })
      notify({
        type: 'success',
        header: `Deleted ${buses.length} event bus${buses.length !== 1 ? 'es' : ''}`,
      })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const sendEventsMut = useMutation({
    mutationFn: () =>
      putEvents([
        {
          source: eventForm.source,
          detailType: eventForm.detailType,
          detail: eventForm.detail,
          bus: eventForm.bus || undefined,
        },
      ]),
    onSuccess: () => {
      notify({ type: 'success', header: 'Event sent' })
      setSendEventsOpen(false)
      setEventForm(EMPTY_EVENT_FORM)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Failed to send event', content: (err as Error).message }),
  })

  const buses = data?.items ?? []
  const includesDefault = selected.some((b) => b.name === 'default')

  const columns: ResourceColumn<EventBus>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (b) => b.name,
      cell: (b) => <Box fontWeight="bold">{b.name}</Box>,
    },
    {
      id: 'arn',
      header: 'ARN',
      cell: (b) => (b.arn ? <CopyText value={b.arn} label="event bus ARN" /> : '—'),
    },
    {
      id: 'actions',
      header: '',
      cell: (b) => (
        <Button variant="inline-link" onClick={() => setDetails(b)}>
          View details
        </Button>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">Event buses</Header>}>
      {error ? (
        <ErrorState header="Failed to load event buses" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="eventbridge"
          favorite={(b) => ({ id: b.name, label: b.name, href: '/aws/eventbridge/buses', type: 'event bus' })}
          items={buses}
          columns={columns}
          trackBy={(b) => b.name}
          title="Event buses"
          loading={isLoading}
          selectionType="multi"
          selectedItems={selected}
          onSelectionChange={setSelected}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <ButtonDropdown
                items={[
                  {
                    id: 'delete',
                    text: 'Delete',
                    disabled: selected.length === 0 || includesDefault,
                  },
                ]}
                onItemClick={() => setConfirmDelete(true)}
                disabled={selected.length === 0 || includesDefault}
              >
                Actions
              </ButtonDropdown>
              <Button onClick={() => setSendEventsOpen(true)}>Send events</Button>
              <Button variant="primary" onClick={() => setCreateOpen(true)}>
                Create bus
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No custom event buses"
          emptyBody="The default bus is always available."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={details?.name ?? 'Event bus'}
        items={
          details
            ? [
                { label: 'Name', value: details.name },
                { label: 'ARN', value: <Box variant="code">{details.arn || '—'}</Box> },
              ]
            : []
        }
      />

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create event bus"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!busName.trim()}
                onClick={() => createMut.mutate()}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <FormField label="Name" constraintText="Required">
            <Input
              value={busName}
              placeholder="my-custom-bus"
              onChange={({ detail }) => setBusName(detail.value)}
            />
          </FormField>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete event buses"
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
        Delete {selected.length} event bus{selected.length !== 1 ? 'es' : ''}? This action cannot be
        undone.
      </Modal>

      <Modal
        visible={sendEventsOpen}
        onDismiss={() => setSendEventsOpen(false)}
        header="Send event"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setSendEventsOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={sendEventsMut.isPending}
                disabled={!eventForm.source.trim() || !eventForm.detailType.trim()}
                onClick={() => sendEventsMut.mutate()}
              >
                Send
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="Source" constraintText="Required">
              <Input
                value={eventForm.source}
                placeholder="my.app"
                onChange={({ detail }) => setEventForm({ ...eventForm, source: detail.value })}
              />
            </FormField>
            <FormField label="Detail type" constraintText="Required">
              <Input
                value={eventForm.detailType}
                placeholder="StateChange"
                onChange={({ detail }) => setEventForm({ ...eventForm, detailType: detail.value })}
              />
            </FormField>
            <FormField label="Detail (JSON)" constraintText="Required">
              <Textarea
                value={eventForm.detail}
                placeholder="{}"
                onChange={({ detail }) => setEventForm({ ...eventForm, detail: detail.value })}
              />
            </FormField>
            <FormField label="Event bus" description="Optional. Defaults to the default bus.">
              <Input
                value={eventForm.bus}
                placeholder="default"
                onChange={({ detail }) => setEventForm({ ...eventForm, bus: detail.value })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>
    </ContentLayout>
  )
}
