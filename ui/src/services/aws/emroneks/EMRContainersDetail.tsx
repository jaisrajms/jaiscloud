import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
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
import {
  describeVirtualCluster,
  listJobRuns,
  startJobRun,
  cancelJobRun,
  type JobRun,
} from '../../../api/emroneks'
import { resourceStatus } from '../../../lib/status'
import { useNotifications } from '../../../components/notifications'

type Tab = 'overview' | 'jobs'

export function EMRContainersDetail() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const qc = useQueryClient()
  const { notify } = useNotifications()
  const [tab, setTab] = useState<Tab>('overview')
  const [submitOpen, setSubmitOpen] = useState(false)
  const [cancelTarget, setCancelTarget] = useState<JobRun | null>(null)
  const [form, setForm] = useState({ name: '', releaseLabel: '', executionRoleArn: '' })

  const { data: vc, isLoading, error } = useQuery({
    queryKey: ['emrc', 'vc', id],
    queryFn: () => describeVirtualCluster(id!),
    enabled: !!id,
  })

  const { data: jobsData } = useQuery({
    queryKey: ['emrc', 'jobs', id],
    queryFn: () => listJobRuns(id!),
    enabled: !!id,
  })

  const submitMut = useMutation({
    mutationFn: () =>
      startJobRun(id!, {
        name: form.name,
        releaseLabel: form.releaseLabel || undefined,
        executionRoleArn: form.executionRoleArn || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emrc', 'jobs', id] })
      notify({ type: 'success', header: 'Job run submitted', content: form.name })
      setSubmitOpen(false)
      setForm({ name: '', releaseLabel: '', executionRoleArn: '' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Failed to submit job run', content: (err as Error).message }),
  })

  const cancelMut = useMutation({
    mutationFn: (jobId: string) => cancelJobRun(id!, jobId),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['emrc', 'jobs', id] })
      notify({ type: 'success', header: 'Job run cancelled' })
      setCancelTarget(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Failed to cancel job run', content: (err as Error).message }),
  })

  if (isLoading) {
    return (
      <ContentLayout header={<Header variant="h1">Virtual cluster</Header>}>
        <Box color="text-body-secondary">Loading virtual cluster…</Box>
      </ContentLayout>
    )
  }

  if (error || !vc) {
    return (
      <ContentLayout header={<Header variant="h1">Virtual cluster</Header>}>
        <ErrorState header="Failed to load virtual cluster" message={error ? (error as Error).message : 'Virtual cluster not found.'} />
      </ContentLayout>
    )
  }

  const jobs = jobsData?.items ?? []

  const jobColumns: TableProps.ColumnDefinition<JobRun>[] = [
    { id: 'id', header: 'Job run ID', cell: (j) => <Box variant="code">{j.id}</Box> },
    { id: 'name', header: 'Name', cell: (j) => j.name },
    {
      id: 'state',
      header: 'State',
      cell: (j) => <StatusIndicator type={resourceStatus(j.state)}>{j.state}</StatusIndicator>,
    },
    { id: 'release', header: 'Release label', cell: (j) => j.releaseLabel || '—' },
    {
      id: 'actions',
      header: '',
      cell: (j) =>
        ['RUNNING', 'SUBMITTED', 'PENDING'].includes(j.state) ? (
          <Button variant="link" onClick={() => setCancelTarget(j)}>
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
                href="/ui/aws/emr-containers/clusters"
                onFollow={(event) => {
                  event.preventDefault()
                  navigate('/aws/emr-containers/clusters')
                }}
              >
                <Icon name="angle-left" /> Virtual clusters
              </Link>
              <SpaceBetween direction="horizontal" size="xs">
                <StatusIndicator type={resourceStatus(vc.state)}>{vc.state}</StatusIndicator>
                {vc.eksCluster && (
                  <Box color="text-body-secondary">
                    EKS: {vc.eksCluster}
                    {vc.namespace ? ` / ${vc.namespace}` : ''}
                  </Box>
                )}
              </SpaceBetween>
            </SpaceBetween>
          }
        >
          {vc.name}
        </Header>
      }
    >
      <Tabs
        tabs={[
          { id: 'overview', label: 'Overview' },
          { id: 'jobs', label: `Job runs (${jobs.length})` },
        ]}
        activeTabId={tab}
        onChange={({ detail }) => setTab(detail.activeTabId as Tab)}
      />

      {tab === 'overview' && (
        <Container header={<Header variant="h2">Details</Header>}>
          <KeyValuePairs
            columns={2}
            items={[
              { label: 'ID', value: <Box variant="code">{vc.id}</Box> },
              { label: 'ARN', value: vc.arn || '—' },
              { label: 'EKS cluster', value: vc.eksCluster || '—' },
              { label: 'Namespace', value: vc.namespace || '—' },
              { label: 'State', value: <StatusIndicator type={resourceStatus(vc.state)}>{vc.state}</StatusIndicator> },
            ]}
          />
        </Container>
      )}

      {tab === 'jobs' && (
        <Container
          header={
            <Header
              variant="h2"
              counter={`(${jobs.length})`}
              actions={
                <Button variant="primary" onClick={() => setSubmitOpen(true)}>
                  Submit job run
                </Button>
              }
            >
              Job runs
            </Header>
          }
        >
          <Table
            variant="embedded"
            items={jobs}
            trackBy={(j) => j.id}
            columnDefinitions={jobColumns}
            empty={
              <Box textAlign="center" color="inherit">
                <b>No job runs</b>
                <Box variant="p" color="inherit">
                  Submit a job to get started.
                </Box>
              </Box>
            }
          />
        </Container>
      )}

      <Modal
        visible={submitOpen}
        onDismiss={() => setSubmitOpen(false)}
        header="Submit job run"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setSubmitOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={submitMut.isPending}
                disabled={!form.name.trim()}
                onClick={() => submitMut.mutate()}
              >
                Submit
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="Job name" constraintText="Required">
              <Input
                value={form.name}
                placeholder="my-spark-job"
                onChange={({ detail }) => setForm({ ...form, name: detail.value })}
              />
            </FormField>
            <FormField label="Release label">
              <Input
                value={form.releaseLabel}
                placeholder="emr-6.10.0-latest"
                onChange={({ detail }) => setForm({ ...form, releaseLabel: detail.value })}
              />
            </FormField>
            <FormField label="Execution role ARN">
              <Input
                value={form.executionRoleArn}
                placeholder="arn:aws:iam::…:role/EMRRole"
                onChange={({ detail }) => setForm({ ...form, executionRoleArn: detail.value })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={!!cancelTarget}
        onDismiss={() => setCancelTarget(null)}
        header="Cancel job run"
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
                Cancel job
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Cancel job <b>{cancelTarget?.name}</b>?
      </Modal>
    </ContentLayout>
  )
}
