import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  Box,
  Button,
  ButtonDropdown,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Link,
  Modal,
  Select,
  SpaceBetween,
  StatusIndicator,
  Textarea,
} from '@cloudscape-design/components'
import { CopyText } from '../../../components/CopyText'
import { ErrorState } from '../../../components/ErrorState'
import {
  listStateMachines,
  createStateMachine,
  deleteStateMachine,
  type StateMachine,
} from '../../../api/sfn'
import { resourceStatus } from '../../../lib/status'
import { formatDate } from '../../../lib/date'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { useNotifications } from '../../../components/notifications'

const TYPE_OPTIONS = [
  { label: 'STANDARD', value: 'STANDARD' },
  { label: 'EXPRESS', value: 'EXPRESS' },
]

export function SFNStateMachines() {
  const qc = useQueryClient()
  const navigate = useNavigate()
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<StateMachine[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [details, setDetails] = useState<StateMachine | null>(null)
  const [form, setForm] = useState({ name: '', definition: '', roleArn: '', type: 'STANDARD' })
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['sfn', 'state-machines'],
    queryFn: () => listStateMachines(),
  })

  const createMut = useMutation({
    mutationFn: () =>
      createStateMachine({
        name: form.name,
        definition: form.definition || undefined,
        roleArn: form.roleArn || undefined,
        type: form.type || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sfn', 'state-machines'] })
      notify({ type: 'success', header: 'State machine creating', content: form.name })
      setCreateOpen(false)
      setForm({ name: '', definition: '', roleArn: '', type: 'STANDARD' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: async (machines: StateMachine[]) => {
      for (const machine of machines) await deleteStateMachine(machine.arn)
    },
    onSuccess: (_r, machines) => {
      void qc.invalidateQueries({ queryKey: ['sfn', 'state-machines'] })
      notify({ type: 'success', header: `Deleted ${machines.length} state machine(s)` })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const machines = data?.items ?? []

  const goToExecutions = (arn: string) => navigate(`executions?arn=${encodeURIComponent(arn)}`)

  const columns: ResourceColumn<StateMachine>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (sm) => sm.name,
      cell: (sm) => (
        <Link
          href={`/ui/aws/sfn/executions?arn=${encodeURIComponent(sm.arn)}`}
          onFollow={(event) => {
            event.preventDefault()
            goToExecutions(sm.arn)
          }}
        >
          {sm.name}
        </Link>
      ),
    },
    { id: 'type', header: 'Type', filterLabel: 'Type', filterValue: (sm) => sm.type ?? '', cell: (sm) => sm.type || '—' },
    {
      id: 'status',
      header: 'Status',
      filterLabel: 'Status',
      filterValue: (sm) => sm.status ?? '',
      cell: (sm) => (
        <StatusIndicator type={resourceStatus(sm.status)}>{sm.status || '—'}</StatusIndicator>
      ),
    },
    { id: 'arn', header: 'ARN', cell: (sm) => <CopyText value={sm.arn} label="state machine ARN" /> },
    {
      id: 'actions',
      header: '',
      cell: (sm) => (
        <div onClick={(event) => event.stopPropagation()}>
          <Button variant="inline-link" onClick={() => setDetails(sm)}>
            View details
          </Button>
        </div>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">Step Functions state machines</Header>}>
      {error ? (
        <ErrorState header="Failed to load state machines" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="sfn"
          favorite={(sm) => ({ id: sm.arn, label: sm.name, href: '/aws/sfn/state-machines', type: 'state machine' })}
          items={machines}
          columns={columns}
          trackBy={(sm) => sm.arn}
          title="State machines"
          loading={isLoading}
          onRowClick={(sm) => goToExecutions(sm.arn)}
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
                Create state machine
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No state machines"
          emptyBody="Create a state machine to orchestrate workflows."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={details?.name ?? 'State machine'}
        items={
          details
            ? [
                { label: 'Name', value: details.name },
                { label: 'ARN', value: <Box variant="code">{details.arn}</Box> },
                { label: 'Type', value: details.type || '—' },
                {
                  label: 'Status',
                  value: (
                    <StatusIndicator type={resourceStatus(details.status)}>
                      {details.status || '—'}
                    </StatusIndicator>
                  ),
                },
                {
                  label: 'Role ARN',
                  value: details.roleArn ? <Box variant="code">{details.roleArn}</Box> : '—',
                },
                { label: 'Created', value: formatDate(details.createdAt) },
                {
                  label: 'Definition',
                  value: details.definition ? (
                    <Textarea
                      readOnly
                      rows={8}
                      value={details.definition}
                      ariaLabel="State machine definition"
                    />
                  ) : (
                    '—'
                  ),
                },
              ]
            : []
        }
      />

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create state machine"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!form.name.trim()}
                onClick={() => createMut.mutate()}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="Name">
              <Input
                autoFocus
                value={form.name}
                onChange={({ detail }) => setForm({ ...form, name: detail.value })}
                placeholder="my-workflow"
              />
            </FormField>
            <FormField label="Role ARN">
              <Input
                value={form.roleArn}
                onChange={({ detail }) => setForm({ ...form, roleArn: detail.value })}
                placeholder="arn:aws:iam::123456789012:role/step-functions-role"
              />
            </FormField>
            <FormField label="Definition (JSON, optional)">
              <Textarea
                rows={6}
                value={form.definition}
                onChange={({ detail }) => setForm({ ...form, definition: detail.value })}
              />
            </FormField>
            <FormField label="Type">
              <Select
                selectedOption={TYPE_OPTIONS.find((o) => o.value === form.type) ?? TYPE_OPTIONS[0]!}
                onChange={({ detail }) =>
                  setForm({ ...form, type: detail.selectedOption.value ?? 'STANDARD' })
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
        header="Delete state machines"
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
        Permanently delete {selected.length} state machine{selected.length !== 1 ? 's' : ''}?
      </Modal>
    </ContentLayout>
  )
}
