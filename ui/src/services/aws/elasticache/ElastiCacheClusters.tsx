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
import { CopyText } from '../../../components/CopyText'
import { ErrorState } from '../../../components/ErrorState'
import {
  listClusters,
  createCluster,
  deleteCluster,
  type CacheCluster,
} from '../../../api/elasticache'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { useNotifications } from '../../../components/notifications'

const ENGINE_OPTIONS = [
  { label: 'Redis', value: 'redis' },
  { label: 'Memcached', value: 'memcached' },
]

export function ElastiCacheClusters() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<CacheCluster[]>([])
  const [details, setDetails] = useState<CacheCluster | null>(null)
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [form, setForm] = useState({ id: '', engine: 'redis', nodeType: 'cache.t3.micro', numNodes: 1 })
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['elasticache', 'clusters'],
    queryFn: listClusters,
  })

  const create = useMutation({
    mutationFn: () => createCluster(form),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['elasticache', 'clusters'] })
      notify({ type: 'success', header: 'Cluster creating', content: form.id })
      setCreateOpen(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const del = useMutation({
    mutationFn: async (clusters: CacheCluster[]) => {
      for (const cluster of clusters) await deleteCluster(cluster.id)
    },
    onSuccess: (_r, clusters) => {
      void qc.invalidateQueries({ queryKey: ['elasticache', 'clusters'] })
      notify({ type: 'success', header: `Deleted ${clusters.length} cluster(s)` })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const items = data?.items ?? []

  const columns: ResourceColumn<CacheCluster>[] = [
    {
      id: 'id',
      header: 'Cluster ID',
      filterLabel: 'Cluster ID',
      filterValue: (c) => c.id,
      cell: (c) => c.id,
    },
    {
      id: 'status',
      header: 'Status',
      filterLabel: 'Status',
      filterValue: (c) => c.status,
      cell: (c) => (
        <StatusIndicator type={resourceStatus(c.status)}>{c.status || '—'}</StatusIndicator>
      ),
    },
    {
      id: 'engine',
      header: 'Engine',
      filterLabel: 'Engine',
      filterValue: (c) => c.engine,
      cell: (c) => c.engine,
    },
    { id: 'nodeType', header: 'Node type', cell: (c) => c.nodeType },
    { id: 'nodes', header: 'Nodes', cell: (c) => c.numNodes },
    { id: 'endpoint', header: 'Endpoint', cell: (c) => <CopyText value={c.endpoint} label="endpoint" /> },
    {
      id: 'actions',
      header: '',
      cell: (c) => (
        <Button variant="inline-link" onClick={() => setDetails(c)}>
          View details
        </Button>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">ElastiCache clusters</Header>}>
      {error ? (
        <ErrorState header="Failed to load clusters" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="elasticache"
          favorite={(c) => ({ id: c.id, label: c.id, href: '/aws/elasticache/clusters', type: 'cluster' })}
          items={items}
          columns={columns}
          trackBy={(c) => c.id}
          title="Clusters"
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
                Create cluster
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No clusters"
          emptyBody="Create an ElastiCache cluster to get started."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={details?.id ?? 'Cluster'}
        items={
          details
            ? [
                { label: 'Identifier', value: details.id },
                {
                  label: 'Status',
                  value: (
                    <StatusIndicator type={resourceStatus(details.status)}>
                      {details.status || '—'}
                    </StatusIndicator>
                  ),
                },
                { label: 'Engine', value: details.engine },
                { label: 'Node type', value: details.nodeType },
                { label: 'Nodes', value: String(details.numNodes) },
                { label: 'Endpoint', value: <Box variant="code">{details.endpoint || '—'}</Box> },
              ]
            : []
        }
      />

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create ElastiCache cluster"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={create.isPending}
                disabled={!form.id.trim()}
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
            <FormField label="Cluster ID">
              <Input
                autoFocus
                value={form.id}
                onChange={({ detail }) => setForm({ ...form, id: detail.value })}
                placeholder="my-cluster"
              />
            </FormField>
            <FormField label="Engine">
              <Select
                selectedOption={
                  ENGINE_OPTIONS.find((o) => o.value === form.engine) ?? ENGINE_OPTIONS[0]!
                }
                onChange={({ detail }) =>
                  setForm({ ...form, engine: detail.selectedOption.value ?? 'redis' })
                }
                options={ENGINE_OPTIONS}
              />
            </FormField>
            <FormField label="Node type">
              <Input
                value={form.nodeType}
                onChange={({ detail }) => setForm({ ...form, nodeType: detail.value })}
                placeholder="cache.t3.micro"
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete clusters"
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
        Permanently delete {selected.length} cluster{selected.length !== 1 ? 's' : ''}?
      </Modal>
    </ContentLayout>
  )
}
