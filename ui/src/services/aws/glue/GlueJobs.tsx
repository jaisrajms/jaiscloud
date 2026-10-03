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
  listJobs,
  createJob,
  deleteJob,
  startJobRun,
  listJobRuns,
  type Job,
  type JobRun,
} from '../../../api/glue'
import { formatDate } from '../../../lib/date'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

const EMPTY_FORM = { name: '', role: '', command: '' }

export function GlueJobs() {
  const qc = useQueryClient()
  const { notify } = useNotifications()
  const [selectedJob, setSelectedJob] = useState<Job | null>(null)
  const [createOpen, setCreateOpen] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<Job | null>(null)
  const [form, setForm] = useState(EMPTY_FORM)

  const { data: jobsData, isLoading, error } = useQuery({
    queryKey: ['glue', 'jobs'],
    queryFn: () => listJobs(),
  })

  const { data: runsData } = useQuery({
    queryKey: ['glue', 'runs', selectedJob?.name],
    queryFn: () => listJobRuns(selectedJob!.name),
    enabled: !!selectedJob,
  })

  const createMut = useMutation({
    mutationFn: () =>
      createJob({
        name: form.name,
        role: form.role || undefined,
        command: form.command || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'jobs'] })
      notify({ type: 'success', header: 'Job created', content: form.name })
      setCreateOpen(false)
      setForm(EMPTY_FORM)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteJob(name),
    onSuccess: (_result, name) => {
      void qc.invalidateQueries({ queryKey: ['glue', 'jobs'] })
      notify({ type: 'success', header: 'Job deleted', content: name })
      if (deleteTarget?.name === selectedJob?.name) setSelectedJob(null)
      setDeleteTarget(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const runMut = useMutation({
    mutationFn: (name: string) => startJobRun(name),
    onSuccess: (_result, name) => {
      void qc.invalidateQueries({ queryKey: ['glue', 'runs', selectedJob?.name] })
      notify({ type: 'success', header: 'Job run started', content: name })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Failed to start job run', content: (err as Error).message }),
  })

  const jobs = jobsData?.items ?? []
  const runs = runsData?.items ?? []

  const columns: ResourceColumn<Job>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (j) => j.name,
      cell: (j) => <Box fontWeight="bold">{j.name}</Box>,
    },
    {
      id: 'role',
      header: 'Role',
      filterLabel: 'Role',
      filterValue: (j) => j.role ?? '',
      cell: (j) => j.role || '—',
    },
    { id: 'command', header: 'Command', cell: (j) => (j.command ? <Box variant="code">{j.command}</Box> : '—') },
  ]

  const runColumns: TableProps.ColumnDefinition<JobRun>[] = [
    { id: 'id', header: 'Run ID', cell: (r) => <Box variant="code">{r.id}</Box> },
    {
      id: 'state',
      header: 'State',
      cell: (r) => <StatusIndicator type={resourceStatus(r.state)}>{r.state}</StatusIndicator>,
    },
    { id: 'started', header: 'Started', cell: (r) => formatDate(r.startedOn) },
    { id: 'completed', header: 'Completed', cell: (r) => formatDate(r.completedOn) },
  ]

  return (
    <ContentLayout header={<Header variant="h1">Glue jobs</Header>}>
      <SpaceBetween size="l">
        {error ? (
          <ErrorState header="Failed to load jobs" message={(error as Error).message} />
        ) : (
          <ResourceTable
            favoriteService="glue"
            favorite={(j) => ({ id: j.name, label: j.name, href: '/aws/glue/jobs', type: 'job' })}
            items={jobs}
            columns={columns}
            trackBy={(j) => j.name}
            title="Jobs"
            loading={isLoading}
            onRowClick={(j) => setSelectedJob(j)}
            selectionType="single"
            selectedItems={selectedJob ? [selectedJob] : []}
            onSelectionChange={(items) => setSelectedJob(items[0] ?? null)}
            actions={
              <SpaceBetween direction="horizontal" size="xs">
                <ButtonDropdown
                  items={[
                    { id: 'run', text: 'Run', disabled: !selectedJob },
                    { id: 'delete', text: 'Delete', disabled: !selectedJob },
                  ]}
                  onItemClick={({ detail }) => {
                    if (!selectedJob) return
                    if (detail.id === 'run') runMut.mutate(selectedJob.name)
                    else if (detail.id === 'delete') setDeleteTarget(selectedJob)
                  }}
                  disabled={!selectedJob}
                >
                  Actions
                </ButtonDropdown>
                <Button variant="primary" onClick={() => setCreateOpen(true)}>
                  Create job
                </Button>
              </SpaceBetween>
            }
            emptyTitle="No jobs"
            emptyBody="Create a Glue ETL job to get started."
          />
        )}

        {selectedJob && (
          <Container
            header={
              <Header
                variant="h2"
                counter={`(${runs.length})`}
                actions={
                  <Button
                    iconName="close"
                    variant="icon"
                    ariaLabel="Close runs"
                    onClick={() => setSelectedJob(null)}
                  />
                }
              >
                Runs for {selectedJob.name}
              </Header>
            }
          >
            <Table
              variant="embedded"
              items={runs}
              trackBy={(r) => r.id}
              columnDefinitions={runColumns}
              empty={
                <Box textAlign="center" color="inherit">
                  <b>No runs</b>
                  <Box variant="p" color="inherit">
                    Use the Actions menu to run this job.
                  </Box>
                </Box>
              }
            />
          </Container>
        )}
      </SpaceBetween>

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create Glue job"
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
                placeholder="my-etl-job"
                onChange={({ detail }) => setForm({ ...form, name: detail.value })}
              />
            </FormField>
            <FormField label="IAM role">
              <Input
                value={form.role}
                placeholder="AWSGlueServiceRole"
                onChange={({ detail }) => setForm({ ...form, role: detail.value })}
              />
            </FormField>
            <FormField label="Command script">
              <Input
                value={form.command}
                placeholder="glueetl"
                onChange={({ detail }) => setForm({ ...form, command: detail.value })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={!!deleteTarget}
        onDismiss={() => setDeleteTarget(null)}
        header="Delete job"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setDeleteTarget(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => deleteTarget && deleteMut.mutate(deleteTarget.name)}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Delete job <b>{deleteTarget?.name}</b>? This action cannot be undone.
      </Modal>
    </ContentLayout>
  )
}
