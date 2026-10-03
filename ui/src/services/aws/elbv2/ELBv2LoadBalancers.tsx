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
  StatusIndicator,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import {
  listLoadBalancers,
  createLoadBalancer,
  deleteLoadBalancer,
  type LoadBalancer,
} from '../../../api/elbv2'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { useNotifications } from '../../../components/notifications'

const TYPE_OPTIONS = [
  { label: 'Application', value: 'application' },
  { label: 'Network', value: 'network' },
  { label: 'Gateway', value: 'gateway' },
]

const SCHEME_OPTIONS = [
  { label: 'Internet-facing', value: 'internet-facing' },
  { label: 'Internal', value: 'internal' },
]

export function ELBv2LoadBalancers() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<LoadBalancer[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [details, setDetails] = useState<LoadBalancer | null>(null)
  const [form, setForm] = useState({ name: '', type: 'application', scheme: 'internet-facing' })
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['elbv2', 'load-balancers'],
    queryFn: listLoadBalancers,
  })

  const create = useMutation({
    mutationFn: () => createLoadBalancer(form),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['elbv2', 'load-balancers'] })
      notify({ type: 'success', header: 'Load balancer creating', content: form.name })
      setCreateOpen(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const del = useMutation({
    mutationFn: async (loadBalancers: LoadBalancer[]) => {
      for (const lb of loadBalancers) await deleteLoadBalancer(lb.arn)
    },
    onSuccess: (_r, loadBalancers) => {
      void qc.invalidateQueries({ queryKey: ['elbv2', 'load-balancers'] })
      notify({ type: 'success', header: `Deleted ${loadBalancers.length} load balancer(s)` })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const items = data?.items ?? []

  const columns: ResourceColumn<LoadBalancer>[] = [
    { id: 'name', header: 'Name', filterLabel: 'Name', filterValue: (lb) => lb.name, cell: (lb) => lb.name },
    {
      id: 'state',
      header: 'State',
      filterLabel: 'State',
      filterValue: (lb) => lb.state,
      cell: (lb) => (
        <StatusIndicator type={resourceStatus(lb.state)}>{lb.state || '—'}</StatusIndicator>
      ),
    },
    { id: 'type', header: 'Type', filterLabel: 'Type', filterValue: (lb) => lb.type, cell: (lb) => lb.type || '—' },
    {
      id: 'scheme',
      header: 'Scheme',
      filterLabel: 'Scheme',
      filterValue: (lb) => lb.scheme,
      cell: (lb) => lb.scheme || '—',
    },
    { id: 'dnsName', header: 'DNS name', cell: (lb) => <Box variant="code">{lb.dnsName || '—'}</Box> },
    {
      id: 'actions',
      header: '',
      cell: (lb) => (
        <Button variant="inline-link" onClick={() => setDetails(lb)}>
          View details
        </Button>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">Load balancers</Header>}>
      {error ? (
        <ErrorState header="Failed to load load balancers" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="elbv2"
          favorite={(lb) => ({ id: lb.arn, label: lb.name, href: '/aws/elbv2/load-balancers', type: 'load balancer' })}
          items={items}
          columns={columns}
          trackBy={(lb) => lb.arn}
          title="Load balancers"
          description="metadata only"
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
                Create load balancer
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No load balancers"
          emptyBody="Create an Elastic Load Balancer to get started."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={details?.name ?? 'Load balancer'}
        items={
          details
            ? [
                { label: 'Name', value: details.name },
                { label: 'ARN', value: <Box variant="code">{details.arn || '—'}</Box> },
                {
                  label: 'State',
                  value: (
                    <StatusIndicator type={resourceStatus(details.state)}>
                      {details.state || '—'}
                    </StatusIndicator>
                  ),
                },
                { label: 'Type', value: details.type || '—' },
                { label: 'Scheme', value: details.scheme || '—' },
                { label: 'DNS name', value: <Box variant="code">{details.dnsName || '—'}</Box> },
              ]
            : []
        }
      />

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create load balancer"
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
            <FormField label="Name">
              <Input
                autoFocus
                value={form.name}
                onChange={({ detail }) => setForm({ ...form, name: detail.value })}
                placeholder="my-load-balancer"
              />
            </FormField>
            <FormField label="Type">
              <Select
                selectedOption={TYPE_OPTIONS.find((o) => o.value === form.type) ?? TYPE_OPTIONS[0]!}
                onChange={({ detail }) =>
                  setForm({ ...form, type: detail.selectedOption.value ?? 'application' })
                }
                options={TYPE_OPTIONS}
              />
            </FormField>
            <FormField label="Scheme">
              <Select
                selectedOption={
                  SCHEME_OPTIONS.find((o) => o.value === form.scheme) ?? SCHEME_OPTIONS[0]!
                }
                onChange={({ detail }) =>
                  setForm({ ...form, scheme: detail.selectedOption.value ?? 'internet-facing' })
                }
                options={SCHEME_OPTIONS}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete load balancers"
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
        Permanently delete {selected.length} load balancer{selected.length !== 1 ? 's' : ''}?
      </Modal>
    </ContentLayout>
  )
}
