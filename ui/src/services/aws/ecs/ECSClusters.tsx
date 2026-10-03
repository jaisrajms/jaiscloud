import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Box,
  Button,
  ButtonDropdown,
  Container,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Modal,
  SpaceBetween,
  StatusIndicator,
  Table,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import type { TableProps } from '@cloudscape-design/components'
import {
  listClusters,
  createCluster,
  deleteCluster,
  listTasks,
  listServices,
  type ECSCluster,
  type ECSTask,
  type ECSService,
} from '../../../api/ecs'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

const taskColumns: TableProps.ColumnDefinition<ECSTask>[] = [
  { id: 'arn', header: 'Task ARN', cell: (t) => <Box variant="code">{t.arn}</Box> },
  {
    id: 'status',
    header: 'Status',
    cell: (t) => <StatusIndicator type={resourceStatus(t.status)}>{t.status || '—'}</StatusIndicator>,
  },
]

const serviceColumns: TableProps.ColumnDefinition<ECSService>[] = [
  { id: 'arn', header: 'Service ARN', cell: (s) => <Box variant="code">{s.arn}</Box> },
  {
    id: 'status',
    header: 'Status',
    cell: (s) => <StatusIndicator type={resourceStatus(s.status)}>{s.status || '—'}</StatusIndicator>,
  },
]

export function ECSClusters() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [clusterName, setClusterName] = useState('')
  const [selected, setSelected] = useState<ECSCluster[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [expanded, setExpanded] = useState<string | null>(null)
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['ecs', 'clusters'],
    queryFn: listClusters,
  })

  const tasksQuery = useQuery({
    queryKey: ['ecs', 'tasks', expanded],
    queryFn: () => listTasks(expanded!),
    enabled: !!expanded,
  })

  const servicesQuery = useQuery({
    queryKey: ['ecs', 'services', expanded],
    queryFn: () => listServices(expanded!),
    enabled: !!expanded,
  })

  const create = useMutation({
    mutationFn: (name: string) => createCluster(name),
    onSuccess: (_r, name) => {
      void qc.invalidateQueries({ queryKey: ['ecs', 'clusters'] })
      notify({ type: 'success', header: 'Cluster creating', content: name })
      setCreateOpen(false)
      setClusterName('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const del = useMutation({
    mutationFn: async (names: string[]) => {
      for (const name of names) await deleteCluster(name)
    },
    onSuccess: (_r, names) => {
      void qc.invalidateQueries({ queryKey: ['ecs', 'clusters'] })
      notify({ type: 'success', header: `Deleted ${names.length} cluster(s)` })
      setExpanded(null)
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const items = data?.items ?? []
  const expandedCluster = items.find((c) => c.name === expanded)

  const columns: ResourceColumn<ECSCluster>[] = [
    { id: 'name', header: 'Name', filterLabel: 'Name', filterValue: (c) => c.name, cell: (c) => c.name },
    {
      id: 'status',
      header: 'Status',
      filterLabel: 'Status',
      filterValue: (c) => c.status,
      cell: (c) => (
        <StatusIndicator type={resourceStatus(c.status)}>{c.status || '—'}</StatusIndicator>
      ),
    },
    { id: 'arn', header: 'ARN', cell: (c) => <Box variant="code">{c.arn}</Box> },
  ]

  return (
    <ContentLayout header={<Header variant="h1">ECS clusters</Header>}>
      {error ? (
        <ErrorState header="Failed to load clusters" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <SpaceBetween size="l">
          <ResourceTable
            favoriteService="ecs"
            favorite={(c) => ({ id: c.name, label: c.name, href: '/aws/ecs/clusters', type: 'cluster' })}
            items={items}
            columns={columns}
            trackBy={(c) => c.name}
            title="Clusters"
            description="metadata only"
            loading={isLoading}
            onRowClick={(c) => setExpanded(expanded === c.name ? null : c.name)}
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
            emptyBody="Create an ECS cluster to get started."
          />

          {expandedCluster && (
            <Container
              header={
                <Header
                  variant="h2"
                  actions={
                    <Button variant="link" onClick={() => setExpanded(null)}>
                      Close
                    </Button>
                  }
                >
                  {expandedCluster.name}
                </Header>
              }
            >
              <SpaceBetween size="l">
                <Table
                  items={tasksQuery.data?.items ?? []}
                  columnDefinitions={taskColumns}
                  loading={tasksQuery.isLoading}
                  loadingText="Loading tasks"
                  trackBy={(t) => t.arn}
                  header={<Header variant="h3">Tasks ({tasksQuery.data?.total ?? 0})</Header>}
                  empty={<Box textAlign="center">No tasks</Box>}
                />
                <Table
                  items={servicesQuery.data?.items ?? []}
                  columnDefinitions={serviceColumns}
                  loading={servicesQuery.isLoading}
                  loadingText="Loading services"
                  trackBy={(s) => s.arn}
                  header={<Header variant="h3">Services ({servicesQuery.data?.total ?? 0})</Header>}
                  empty={<Box textAlign="center">No services</Box>}
                />
              </SpaceBetween>
            </Container>
          )}
        </SpaceBetween>
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create cluster"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={create.isPending}
                disabled={!clusterName.trim()}
                onClick={() => create.mutate(clusterName)}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <FormField label="Cluster name">
            <Input
              autoFocus
              value={clusterName}
              onChange={({ detail }) => setClusterName(detail.value)}
              placeholder="my-cluster"
            />
          </FormField>
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
              <Button
                variant="primary"
                loading={del.isPending}
                onClick={() => del.mutate(selected.map((c) => c.name))}
              >
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
