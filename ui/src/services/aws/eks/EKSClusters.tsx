import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Box,
  Button,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Modal,
  SpaceBetween,
  StatusIndicator,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import { listClusters, createCluster, deleteCluster, type EKSCluster } from '../../../api/eks'
import { formatDate } from '../../../lib/date'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { useNotifications } from '../../../components/notifications'

export function EKSClusters() {
  const qc = useQueryClient()
  const [clusterName, setClusterName] = useState('')
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<EKSCluster[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [details, setDetails] = useState<EKSCluster | null>(null)
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['eks', 'clusters'],
    queryFn: listClusters,
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['eks', 'clusters'] })

  const create = useMutation({
    mutationFn: (name: string) => createCluster(name),
    onSuccess: (_r, name) => {
      void invalidate()
      notify({ type: 'success', header: 'Cluster creating', content: name })
      setCreateOpen(false)
      setClusterName('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const del = useMutation({
    mutationFn: async (clusters: EKSCluster[]) => {
      for (const cluster of clusters) await deleteCluster(cluster.name)
    },
    onSuccess: (_r, clusters) => {
      void invalidate()
      notify({ type: 'success', header: `Deleted ${clusters.length} cluster(s)` })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) => notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const items = data?.items ?? []

  const columns: ResourceColumn<EKSCluster>[] = [
    { id: 'name', header: 'Name', filterLabel: 'Name', filterValue: (c) => c.name, cell: (c) => c.name },
    {
      id: 'status',
      header: 'Status',
      filterLabel: 'Status',
      filterValue: (c) => c.status,
      cell: (c) => <StatusIndicator type={resourceStatus(c.status)}>{c.status || '—'}</StatusIndicator>,
    },
    { id: 'version', header: 'Version', cell: (c) => c.version || '—' },
    { id: 'created', header: 'Created', cell: (c) => formatDate(c.createdAt) },
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
    <ContentLayout header={<Header variant="h1">EKS clusters</Header>}>
      {error ? (
        <ErrorState header="Failed to load clusters" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="eks"
          favorite={(c) => ({ id: c.name, label: c.name, href: '/aws/eks/clusters', type: 'cluster' })}
          items={items}
          columns={columns}
          trackBy={(c) => c.name}
          title="Clusters"
          description="metadata only"
          loading={isLoading}
          selectionType="multi"
          selectedItems={selected}
          onSelectionChange={setSelected}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <Button
                disabled={selected.length === 0}
                onClick={() => setConfirmDelete(true)}
              >
                Delete
              </Button>
              <Button variant="primary" onClick={() => setCreateOpen(true)}>
                Create cluster
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No clusters"
          emptyBody="Create an EKS cluster to get started."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={details?.name ?? 'Cluster'}
        items={
          details
            ? [
                { label: 'Name', value: details.name },
                {
                  label: 'Status',
                  value: (
                    <StatusIndicator type={resourceStatus(details.status)}>
                      {details.status || '—'}
                    </StatusIndicator>
                  ),
                },
                { label: 'Version', value: details.version || '—' },
                { label: 'ARN', value: <Box variant="code">{details.arn}</Box> },
                { label: 'Created', value: formatDate(details.createdAt) },
              ]
            : []
        }
      />

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
                Create cluster
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
