import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Box,
  Button,
  ButtonDropdown,
  ContentLayout,
  CopyToClipboard,
  Header,
  Modal,
  SpaceBetween,
  StatusIndicator,
} from '@cloudscape-design/components'
import { CopyText } from '../../../components/CopyText'
import { ErrorState } from '../../../components/ErrorState'
import {
  listInstances,
  terminateInstance,
  startInstance,
  stopInstance,
  type Instance,
} from '../../../api/ec2'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { useNotifications } from '../../../components/notifications'

export function EC2Instances() {
  const qc = useQueryClient()
  const [selected, setSelected] = useState<Instance[]>([])
  const [confirmTerminate, setConfirmTerminate] = useState(false)
  const [details, setDetails] = useState<Instance | null>(null)
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['ec2', 'instances'],
    queryFn: () => listInstances(),
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['ec2', 'instances'] })

  const start = useMutation({
    mutationFn: async (instances: Instance[]) => {
      for (const instance of instances) await startInstance(instance.id)
    },
    onSuccess: (_r, instances) => {
      void invalidate()
      notify({ type: 'success', header: `Starting ${instances.length} instance(s)` })
      setSelected([])
    },
    onError: (err) => notify({ type: 'error', header: 'Start failed', content: (err as Error).message }),
  })
  const stop = useMutation({
    mutationFn: async (instances: Instance[]) => {
      for (const instance of instances) await stopInstance(instance.id)
    },
    onSuccess: (_r, instances) => {
      void invalidate()
      notify({ type: 'success', header: `Stopping ${instances.length} instance(s)` })
      setSelected([])
    },
    onError: (err) => notify({ type: 'error', header: 'Stop failed', content: (err as Error).message }),
  })
  const terminate = useMutation({
    mutationFn: async (instances: Instance[]) => {
      for (const instance of instances) await terminateInstance(instance.id)
    },
    onSuccess: (_r, instances) => {
      void invalidate()
      notify({ type: 'success', header: `Terminated ${instances.length} instance(s)` })
      setSelected([])
      setConfirmTerminate(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Terminate failed', content: (err as Error).message }),
  })

  const items = data?.items ?? []

  const columns: ResourceColumn<Instance>[] = [
    {
      id: 'id',
      header: 'Instance ID',
      filterLabel: 'Instance ID',
      filterValue: (i) => i.id,
      cell: (i) => (
        <SpaceBetween direction="horizontal" size="xs">
          <Box variant="code" display="inline">
            {i.id}
          </Box>
          <CopyToClipboard
            variant="icon"
            textToCopy={i.id}
            copyButtonAriaLabel={`Copy instance ID ${i.id}`}
            copySuccessText="Instance ID copied"
            copyErrorText="Failed to copy instance ID"
          />
        </SpaceBetween>
      ),
    },
    {
      id: 'state',
      header: 'State',
      filterLabel: 'State',
      filterValue: (i) => i.state,
      cell: (i) => <StatusIndicator type={resourceStatus(i.state)}>{i.state}</StatusIndicator>,
    },
    {
      id: 'type',
      header: 'Instance type',
      filterLabel: 'Instance type',
      filterValue: (i) => i.instanceType,
      cell: (i) => i.instanceType,
    },
    { id: 'image', header: 'AMI ID', cell: (i) => <CopyText value={i.imageId} label="AMI ID" /> },
    { id: 'private', header: 'Private IP', cell: (i) => <Box variant="code">{i.privateIp || '—'}</Box> },
    { id: 'public', header: 'Public IP', cell: (i) => <Box variant="code">{i.publicIp || '—'}</Box> },
    {
      id: 'actions',
      header: '',
      cell: (i) => (
        <Button variant="inline-link" onClick={() => setDetails(i)}>
          View details
        </Button>
      ),
    },
  ]

  const hasStopped = selected.some((i) => i.state === 'stopped')
  const hasRunning = selected.some((i) => i.state === 'running')

  return (
    <ContentLayout header={<Header variant="h1">Instances</Header>}>
      {error ? (
        <ErrorState header="Failed to load instances" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="ec2"
          favorite={(i) => ({ id: i.id, label: i.id, href: '/aws/ec2/instances', type: 'instance' })}
          items={items}
          columns={columns}
          trackBy={(i) => i.id}
          title="Instances"
          description="EC2 instances (metadata only)"
          loading={isLoading}
          selectionType="multi"
          selectedItems={selected}
          onSelectionChange={setSelected}
          actions={
            <ButtonDropdown
              items={[
                { id: 'start', text: 'Start', disabled: !hasStopped },
                { id: 'stop', text: 'Stop', disabled: !hasRunning },
                { id: 'terminate', text: 'Terminate', disabled: selected.length === 0 },
              ]}
              onItemClick={({ detail }) => {
                if (detail.id === 'start') start.mutate(selected)
                else if (detail.id === 'stop') stop.mutate(selected)
                else setConfirmTerminate(true)
              }}
              disabled={selected.length === 0}
            >
              Actions
            </ButtonDropdown>
          }
          emptyTitle="No instances"
          emptyBody="No EC2 instances found in this region."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={`Instance ${details?.id ?? ''}`}
        items={
          details
            ? [
                { label: 'Instance ID', value: <Box variant="code">{details.id}</Box> },
                {
                  label: 'State',
                  value: (
                    <StatusIndicator type={resourceStatus(details.state)}>
                      {details.state}
                    </StatusIndicator>
                  ),
                },
                { label: 'Instance type', value: details.instanceType },
                { label: 'AMI ID', value: <Box variant="code">{details.imageId}</Box> },
                { label: 'Private IP', value: <Box variant="code">{details.privateIp || '—'}</Box> },
                { label: 'Public IP', value: <Box variant="code">{details.publicIp || '—'}</Box> },
              ]
            : []
        }
      />

      <Modal
        visible={confirmTerminate}
        onDismiss={() => setConfirmTerminate(false)}
        header="Terminate instances"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmTerminate(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={terminate.isPending}
                onClick={() => terminate.mutate(selected)}
              >
                Terminate
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Terminate {selected.length} instance{selected.length !== 1 ? 's' : ''}? This cannot be undone.
      </Modal>
    </ContentLayout>
  )
}
