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
  Textarea,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import { listStacks, createStack, deleteStack, type Stack } from '../../../api/cfn'
import { formatDate } from '../../../lib/date'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { useNotifications } from '../../../components/notifications'

const DEFAULT_TEMPLATE = JSON.stringify(
  { AWSTemplateFormatVersion: '2010-09-09', Resources: {} },
  null,
  2,
)

export function CFNStacks() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<Stack[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [details, setDetails] = useState<Stack | null>(null)
  const [form, setForm] = useState({ name: '', templateBody: DEFAULT_TEMPLATE })
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['cfn', 'stacks'],
    queryFn: listStacks,
  })

  const create = useMutation({
    mutationFn: () => createStack({ name: form.name, templateBody: form.templateBody }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['cfn', 'stacks'] })
      notify({ type: 'success', header: 'Stack creating', content: form.name })
      setCreateOpen(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const del = useMutation({
    mutationFn: async (stacks: Stack[]) => {
      for (const stack of stacks) await deleteStack(stack.name)
    },
    onSuccess: (_r, stacks) => {
      void qc.invalidateQueries({ queryKey: ['cfn', 'stacks'] })
      notify({ type: 'success', header: `Deleted ${stacks.length} stack(s)` })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const items = data?.items ?? []

  const columns: ResourceColumn<Stack>[] = [
    { id: 'name', header: 'Name', filterLabel: 'Name', filterValue: (s) => s.name, cell: (s) => s.name },
    {
      id: 'status',
      header: 'Status',
      filterLabel: 'Status',
      filterValue: (s) => s.status,
      cell: (s) => (
        <StatusIndicator type={resourceStatus(s.status)}>{s.status}</StatusIndicator>
      ),
    },
    { id: 'description', header: 'Description', cell: (s) => s.description || '—' },
    { id: 'created', header: 'Created', cell: (s) => formatDate(s.createdAt) },
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
    <ContentLayout header={<Header variant="h1">CloudFormation stacks</Header>}>
      {error ? (
        <ErrorState header="Failed to load stacks" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="cloudformation"
          favorite={(s) => ({ id: s.name, label: s.name, href: '/aws/cloudformation/stacks', type: 'stack' })}
          items={items}
          columns={columns}
          trackBy={(s) => s.name}
          title="Stacks"
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
                Create stack
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No stacks"
          emptyBody="Create a CloudFormation stack to provision resources."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={details?.name ?? 'Stack'}
        items={
          details
            ? [
                { label: 'Name', value: details.name },
                { label: 'Stack ID', value: <Box variant="code">{details.stackId || '—'}</Box> },
                {
                  label: 'Status',
                  value: (
                    <StatusIndicator type={resourceStatus(details.status)}>
                      {details.status || '—'}
                    </StatusIndicator>
                  ),
                },
                { label: 'Description', value: details.description || '—' },
                { label: 'Created', value: formatDate(details.createdAt) },
              ]
            : []
        }
      />

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create stack"
        size="large"
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
            <FormField label="Stack name">
              <Input
                autoFocus
                value={form.name}
                onChange={({ detail }) => setForm({ ...form, name: detail.value })}
                placeholder="my-stack"
              />
            </FormField>
            <FormField label="Template body (JSON/YAML)">
              <Textarea
                rows={8}
                value={form.templateBody}
                onChange={({ detail }) => setForm({ ...form, templateBody: detail.value })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete stacks"
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
        Permanently delete {selected.length} stack{selected.length !== 1 ? 's' : ''}?
      </Modal>
    </ContentLayout>
  )
}
