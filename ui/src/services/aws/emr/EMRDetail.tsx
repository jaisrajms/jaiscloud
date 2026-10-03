import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Badge,
  Box,
  Button,
  Container,
  ContentLayout,
  Form,
  FormField,
  Header,
  Icon,
  Input,
  KeyValuePairs,
  Link,
  Modal,
  SpaceBetween,
  StatusIndicator,
  Table,
  Tabs,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import type { TableProps } from '@cloudscape-design/components'
import { describeCluster, listSteps, addSteps, cancelStep, type Step } from '../../../api/emr'
import { resourceStatus } from '../../../lib/status'
import { useNotifications } from '../../../components/notifications'

type Tab = 'overview' | 'steps'

export function EMRDetail() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const qc = useQueryClient()
  const { notify } = useNotifications()
  const [tab, setTab] = useState<Tab>('overview')
  const [addStepOpen, setAddStepOpen] = useState(false)
  const [stepName, setStepName] = useState('')
  const [cancelTarget, setCancelTarget] = useState<Step | null>(null)

  const { data: cluster, isLoading, error } = useQuery({
    queryKey: ['emr', 'cluster', id],
    queryFn: () => describeCluster(id!),
    enabled: !!id,
  })

  const { data: stepsData } = useQuery({
    queryKey: ['emr', 'steps', id],
    queryFn: () => listSteps(id!),
    enabled: !!id,
  })

  const addMut = useMutation({
    mutationFn: () =>
      addSteps(id!, [
        {
          Name: stepName,
          ActionOnFailure: 'CONTINUE',
          HadoopJarStep: { Jar: 'command-runner.jar', Args: ['echo', stepName] },
        },
      ]),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emr', 'steps', id] })
      notify({ type: 'success', header: 'Step added', content: stepName })
      setAddStepOpen(false)
      setStepName('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Failed to add step', content: (err as Error).message }),
  })

  const cancelMut = useMutation({
    mutationFn: (stepId: string) => cancelStep(id!, stepId),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emr', 'steps', id] })
      notify({ type: 'success', header: 'Step cancelled' })
      setCancelTarget(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Failed to cancel step', content: (err as Error).message }),
  })

  if (isLoading) {
    return (
      <ContentLayout header={<Header variant="h1">Cluster</Header>}>
        <Box color="text-body-secondary">Loading cluster…</Box>
      </ContentLayout>
    )
  }

  if (error || !cluster) {
    return (
      <ContentLayout header={<Header variant="h1">Cluster</Header>}>
        <ErrorState header="Failed to load cluster" message={error ? (error as Error).message : 'Cluster not found.'} />
      </ContentLayout>
    )
  }

  const steps = stepsData?.items ?? []

  const stepColumns: TableProps.ColumnDefinition<Step>[] = [
    { id: 'id', header: 'Step ID', cell: (s) => <Box variant="code">{s.id}</Box> },
    { id: 'name', header: 'Name', cell: (s) => s.name },
    {
      id: 'state',
      header: 'State',
      cell: (s) => <StatusIndicator type={resourceStatus(s.state)}>{s.state}</StatusIndicator>,
    },
    {
      id: 'actions',
      header: '',
      cell: (s) =>
        ['RUNNING', 'PENDING'].includes(s.state) ? (
          <Button variant="link" onClick={() => setCancelTarget(s)}>
            Cancel
          </Button>
        ) : null,
    },
  ]

  return (
    <ContentLayout
      header={
        <Header
          variant="h1"
          description={
            <SpaceBetween size="xs">
              <Link
                href="/ui/aws/emr/clusters"
                onFollow={(event) => {
                  event.preventDefault()
                  navigate('/aws/emr/clusters')
                }}
              >
                <Icon name="angle-left" /> Clusters
              </Link>
              <SpaceBetween direction="horizontal" size="xs">
                <StatusIndicator type={resourceStatus(cluster.state)}>{cluster.state}</StatusIndicator>
                {cluster.stateChangeReason && (
                  <Box color="text-body-secondary">{cluster.stateChangeReason}</Box>
                )}
              </SpaceBetween>
            </SpaceBetween>
          }
        >
          {cluster.name}
        </Header>
      }
    >
      <Tabs
        tabs={[
          { id: 'overview', label: 'Overview' },
          { id: 'steps', label: `Steps (${steps.length})` },
        ]}
        activeTabId={tab}
        onChange={({ detail }) => setTab(detail.activeTabId as Tab)}
      />

      {tab === 'overview' && (
        <Container header={<Header variant="h2">Details</Header>}>
          <KeyValuePairs
            columns={2}
            items={[
              { label: 'ID', value: <Box variant="code">{cluster.id}</Box> },
              { label: 'ARN', value: cluster.arn || '—' },
              { label: 'Release label', value: cluster.releaseLabel || '—' },
              { label: 'Log URI', value: cluster.logUri || '—' },
              { label: 'Auto terminate', value: String(cluster.autoTerminate) },
              { label: 'Termination protected', value: String(cluster.terminationProtected) },
              {
                label: 'Applications',
                value:
                  cluster.applications.length > 0 ? (
                    <SpaceBetween direction="horizontal" size="xs">
                      {cluster.applications.map((a) => (
                        <Badge key={a}>{a}</Badge>
                      ))}
                    </SpaceBetween>
                  ) : (
                    '—'
                  ),
              },
            ]}
          />
        </Container>
      )}

      {tab === 'steps' && (
        <Container
          header={
            <Header
              variant="h2"
              counter={`(${steps.length})`}
              actions={
                <Button variant="primary" onClick={() => setAddStepOpen(true)}>
                  Add step
                </Button>
              }
            >
              Steps
            </Header>
          }
        >
          <Table
            variant="embedded"
            items={steps}
            trackBy={(s) => s.id}
            columnDefinitions={stepColumns}
            empty={
              <Box textAlign="center" color="inherit">
                <b>No steps</b>
                <Box variant="p" color="inherit">
                  Add a step to run work on this cluster.
                </Box>
              </Box>
            }
          />
        </Container>
      )}

      <Modal
        visible={addStepOpen}
        onDismiss={() => setAddStepOpen(false)}
        header="Add step"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setAddStepOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={addMut.isPending}
                disabled={!stepName.trim()}
                onClick={() => addMut.mutate()}
              >
                Add step
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <FormField label="Step name" constraintText="Required">
            <Input
              value={stepName}
              placeholder="my-step"
              onChange={({ detail }) => setStepName(detail.value)}
            />
          </FormField>
        </Form>
      </Modal>

      <Modal
        visible={!!cancelTarget}
        onDismiss={() => setCancelTarget(null)}
        header="Cancel step"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCancelTarget(null)}>
                Back
              </Button>
              <Button
                variant="primary"
                loading={cancelMut.isPending}
                onClick={() => cancelTarget && cancelMut.mutate(cancelTarget.id)}
              >
                Cancel step
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Cancel step <b>{cancelTarget?.name}</b>?
      </Modal>
    </ContentLayout>
  )
}
