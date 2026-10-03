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
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import {
  listVirtualClusters,
  createVirtualCluster,
  deleteVirtualCluster,
  type VirtualCluster,
} from '../../../api/emroneks'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

const STATES = ['RUNNING', 'ARRESTED', 'TERMINATING', 'TERMINATED']

const EMPTY_FORM = { name: '', eksClusterId: '', namespace: '' }

export function EMRContainersList() {
  const [stateFilter, setStateFilter] = useState('')
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<VirtualCluster[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [form, setForm] = useState(EMPTY_FORM)
  const qc = useQueryClient()
  const navigate = useNavigate()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['emrc', 'virtual-clusters', stateFilter],
    queryFn: () => listVirtualClusters(stateFilter ? { state: stateFilter } : undefined),
  })

  const createMut = useMutation({
    mutationFn: () =>
      createVirtualCluster({
        name: form.name,
        eksClusterId: form.eksClusterId || undefined,
        namespace: form.namespace || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emrc', 'virtual-clusters'] })
      notify({ type: 'success', header: 'Virtual cluster created', content: form.name })
      setCreateOpen(false)
      setForm(EMPTY_FORM)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: async (clusters: VirtualCluster[]) => {
      for (const cluster of clusters) await deleteVirtualCluster(cluster.id)
    },
    onSuccess: (_result, clusters) => {
      void qc.invalidateQueries({ queryKey: ['emrc', 'virtual-clusters'] })
      notify({
        type: 'success',
        header: `Deleted ${clusters.length} virtual cluster${clusters.length !== 1 ? 's' : ''}`,
      })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const virtualClusters = data?.items ?? []

  const columns: ResourceColumn<VirtualCluster>[] = [
    {
      id: 'id',
      header: 'Virtual cluster ID',
      filterLabel: 'Virtual cluster ID',
      filterValue: (vc) => vc.id,
      cell: (vc) => <Box variant="code">{vc.id}</Box>,
    },
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (vc) => vc.name,
      cell: (vc) => (
        <Link
          href={`/ui/aws/emr-containers/${encodeURIComponent(vc.id)}`}
          onFollow={(event) => {
            event.preventDefault()
            navigate(`/aws/emr-containers/${encodeURIComponent(vc.id)}`)
          }}
        >
          {vc.name}
        </Link>
      ),
    },
    {
      id: 'state',
      header: 'State',
      filterLabel: 'State',
      filterValue: (vc) => vc.state,
      cell: (vc) => <StatusIndicator type={resourceStatus(vc.state)}>{vc.state}</StatusIndicator>,
    },
    { id: 'eks', header: 'EKS cluster', cell: (vc) => vc.eksCluster || '—' },
    { id: 'namespace', header: 'Namespace', cell: (vc) => vc.namespace || '—' },
  ]

  return (
    <ContentLayout header={<Header variant="h1">EMR on EKS virtual clusters</Header>}>
      {error ? (
        <ErrorState header="Failed to load virtual clusters" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="emr-containers"
          favorite={(vc) => ({ id: vc.id, label: vc.name, href: '/aws/emr-containers/' + encodeURIComponent(vc.id), type: 'virtual cluster' })}
          items={virtualClusters}
          columns={columns}
          trackBy={(vc) => vc.id}
          title="Virtual clusters"
          loading={isLoading}
          onRowClick={(vc) => navigate(`/aws/emr-containers/${encodeURIComponent(vc.id)}`)}
          selectionType="multi"
          selectedItems={selected}
          onSelectionChange={setSelected}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <Select
                ariaLabel="Filter virtual clusters by state"
                placeholder="All states"
                selectedOption={stateFilter ? { value: stateFilter, label: stateFilter } : null}
                onChange={({ detail }) => setStateFilter(detail.selectedOption.value ?? '')}
                options={[
                  { value: '', label: 'All states' },
                  ...STATES.map((s) => ({ value: s, label: s })),
                ]}
              />
              <ButtonDropdown
                items={[{ id: 'delete', text: 'Delete', disabled: selected.length === 0 }]}
                onItemClick={() => setConfirmDelete(true)}
                disabled={selected.length === 0}
              >
                Actions
              </ButtonDropdown>
              <Button variant="primary" onClick={() => setCreateOpen(true)}>
                Create virtual cluster
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No virtual clusters"
          emptyBody="Create a virtual cluster to run jobs on EKS."
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create virtual cluster"
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
            <FormField label="Name" constraintText="Required">
              <Input
                value={form.name}
                placeholder="my-vc"
                onChange={({ detail }) => setForm({ ...form, name: detail.value })}
              />
            </FormField>
            <FormField label="EKS cluster ID">
              <Input
                value={form.eksClusterId}
                placeholder="my-eks-cluster"
                onChange={({ detail }) => setForm({ ...form, eksClusterId: detail.value })}
              />
            </FormField>
            <FormField label="Namespace">
              <Input
                value={form.namespace}
                placeholder="default"
                onChange={({ detail }) => setForm({ ...form, namespace: detail.value })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete virtual clusters"
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
        Delete {selected.length} virtual cluster{selected.length !== 1 ? 's' : ''}? This action
        cannot be undone.
      </Modal>
    </ContentLayout>
  )
}
