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
import { CopyText } from '../../../components/CopyText'
import { ErrorState } from '../../../components/ErrorState'
import {
  listClusters,
  runJobFlow,
  terminateCluster,
  type ClusterSummary,
} from '../../../api/emr'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

const STATES = [
  'RUNNING',
  'WAITING',
  'STARTING',
  'BOOTSTRAPPING',
  'TERMINATING',
  'TERMINATED',
  'TERMINATED_WITH_ERRORS',
]

const EMPTY_FORM = { name: '', releaseLabel: '', logUri: '', serviceRole: '', jobFlowRole: '' }

export function EMRList() {
  const [stateFilter, setStateFilter] = useState('')
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<ClusterSummary[]>([])
  const [confirmTerminate, setConfirmTerminate] = useState(false)
  const [form, setForm] = useState(EMPTY_FORM)
  const qc = useQueryClient()
  const navigate = useNavigate()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['emr', 'clusters', stateFilter],
    queryFn: () => listClusters(stateFilter ? { state: stateFilter } : undefined),
  })

  const createMut = useMutation({
    mutationFn: () =>
      runJobFlow({
        name: form.name,
        releaseLabel: form.releaseLabel || undefined,
        logUri: form.logUri || undefined,
        serviceRole: form.serviceRole || undefined,
        jobFlowRole: form.jobFlowRole || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emr', 'clusters'] })
      notify({ type: 'success', header: 'Cluster created', content: form.name })
      setCreateOpen(false)
      setForm(EMPTY_FORM)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const terminateMut = useMutation({
    mutationFn: async (clusters: ClusterSummary[]) => {
      for (const cluster of clusters) await terminateCluster(cluster.id)
    },
    onSuccess: (_result, clusters) => {
      void qc.invalidateQueries({ queryKey: ['emr', 'clusters'] })
      notify({
        type: 'success',
        header: `Terminated ${clusters.length} cluster${clusters.length !== 1 ? 's' : ''}`,
      })
      setSelected([])
      setConfirmTerminate(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Terminate failed', content: (err as Error).message }),
  })

  const clusters = data?.items ?? []

  const columns: ResourceColumn<ClusterSummary>[] = [
    {
      id: 'id',
      header: 'Cluster ID',
      filterLabel: 'Cluster ID',
      filterValue: (c) => c.id,
      cell: (c) => <CopyText value={c.id} label="cluster ID" />,
    },
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (c) => c.name,
      cell: (c) => (
        <Link
          href={`/ui/aws/emr/${encodeURIComponent(c.id)}`}
          onFollow={(event) => {
            event.preventDefault()
            navigate(`/aws/emr/${encodeURIComponent(c.id)}`)
          }}
        >
          {c.name}
        </Link>
      ),
    },
    {
      id: 'state',
      header: 'State',
      filterLabel: 'State',
      filterValue: (c) => c.state,
      cell: (c) => <StatusIndicator type={resourceStatus(c.state)}>{c.state}</StatusIndicator>,
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">EMR clusters</Header>}>
      {error ? (
        <ErrorState header="Failed to load clusters" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="emr"
          favorite={(c) => ({ id: c.id, label: c.name, href: '/aws/emr/' + encodeURIComponent(c.id), type: 'cluster' })}
          items={clusters}
          columns={columns}
          trackBy={(c) => c.id}
          title="Clusters"
          loading={isLoading}
          onRowClick={(c) => navigate(`/aws/emr/${encodeURIComponent(c.id)}`)}
          selectionType="multi"
          selectedItems={selected}
          onSelectionChange={setSelected}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <Select
                ariaLabel="Filter clusters by state"
                placeholder="All states"
                selectedOption={stateFilter ? { value: stateFilter, label: stateFilter } : null}
                onChange={({ detail }) => setStateFilter(detail.selectedOption.value ?? '')}
                options={[
                  { value: '', label: 'All states' },
                  ...STATES.map((s) => ({ value: s, label: s })),
                ]}
              />
              <ButtonDropdown
                items={[{ id: 'terminate', text: 'Terminate', disabled: selected.length === 0 }]}
                onItemClick={() => setConfirmTerminate(true)}
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
          emptyBody="Create an EMR cluster to get started."
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create EMR cluster"
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
                placeholder="my-cluster"
                onChange={({ detail }) => setForm({ ...form, name: detail.value })}
              />
            </FormField>
            <FormField label="Release label">
              <Input
                value={form.releaseLabel}
                placeholder="emr-6.10.0"
                onChange={({ detail }) => setForm({ ...form, releaseLabel: detail.value })}
              />
            </FormField>
            <FormField label="Log URI">
              <Input
                value={form.logUri}
                placeholder="s3://my-bucket/logs"
                onChange={({ detail }) => setForm({ ...form, logUri: detail.value })}
              />
            </FormField>
            <FormField label="Service role">
              <Input
                value={form.serviceRole}
                placeholder="EMR_DefaultRole"
                onChange={({ detail }) => setForm({ ...form, serviceRole: detail.value })}
              />
            </FormField>
            <FormField label="Job flow role">
              <Input
                value={form.jobFlowRole}
                placeholder="EMR_EC2_DefaultRole"
                onChange={({ detail }) => setForm({ ...form, jobFlowRole: detail.value })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmTerminate}
        onDismiss={() => setConfirmTerminate(false)}
        header="Terminate clusters"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmTerminate(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={terminateMut.isPending}
                onClick={() => terminateMut.mutate(selected)}
              >
                Terminate
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Terminate {selected.length} cluster{selected.length !== 1 ? 's' : ''}? This action cannot be
        undone.
      </Modal>
    </ContentLayout>
  )
}
