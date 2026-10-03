import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useSearchParams, useNavigate } from 'react-router-dom'
import {
  Alert,
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
  Textarea,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import type { TableProps } from '@cloudscape-design/components'
import {
  listExecutions,
  startExecution,
  stopExecution,
  getExecutionHistory,
  type Execution,
  type HistoryEvent,
} from '../../../api/sfn'
import { formatDate } from '../../../lib/date'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

const historyColumns: TableProps.ColumnDefinition<HistoryEvent>[] = [
  { id: 'id', header: 'ID', cell: (ev) => ev.id },
  { id: 'type', header: 'Type', cell: (ev) => ev.type },
  { id: 'timestamp', header: 'Timestamp', cell: (ev) => formatDate(ev.timestamp) },
]

export function SFNExecutions() {
  const qc = useQueryClient()
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const smArn = searchParams.get('arn') ?? ''

  const [startOpen, setStartOpen] = useState(false)
  const [selected, setSelected] = useState<Execution[]>([])
  const [historyExec, setHistoryExec] = useState<Execution | null>(null)
  const [form, setForm] = useState({ name: '', input: '{}' })
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['sfn', 'executions', smArn],
    queryFn: () => listExecutions(smArn),
    enabled: !!smArn,
  })

  const { data: historyData, isLoading: historyLoading } = useQuery({
    queryKey: ['sfn', 'history', historyExec?.arn],
    queryFn: () => getExecutionHistory(historyExec!.arn),
    enabled: !!historyExec,
  })

  const startMut = useMutation({
    mutationFn: () => startExecution(smArn, { name: form.name || undefined, input: form.input }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['sfn', 'executions', smArn] })
      notify({ type: 'success', header: 'Execution started', content: form.name || undefined })
      setStartOpen(false)
      setForm({ name: '', input: '{}' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Start failed', content: (err as Error).message }),
  })

  const stopMut = useMutation({
    mutationFn: async (executions: Execution[]) => {
      for (const execution of executions) await stopExecution(execution.arn)
    },
    onSuccess: (_r, executions) => {
      void qc.invalidateQueries({ queryKey: ['sfn', 'executions', smArn] })
      notify({ type: 'success', header: `Stopping ${executions.length} execution(s)` })
      setSelected([])
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Stop failed', content: (err as Error).message }),
  })

  if (!smArn) {
    return (
      <ContentLayout header={<Header variant="h1">Executions</Header>}>
        <SpaceBetween size="l">
          <Button variant="link" iconName="angle-left" onClick={() => navigate('../state-machines')}>
            State machines
          </Button>
          <Alert type="info" header="No state machine selected">
            Open a state machine to view and start its executions.
          </Alert>
        </SpaceBetween>
      </ContentLayout>
    )
  }

  const executions = data?.items ?? []
  const events = historyData?.events ?? []
  const hasRunning = selected.some((e) => e.status === 'RUNNING')

  const columns: ResourceColumn<Execution>[] = [
    { id: 'name', header: 'Name', filterLabel: 'Name', filterValue: (e) => e.name, cell: (e) => e.name },
    {
      id: 'status',
      header: 'Status',
      filterLabel: 'Status',
      filterValue: (e) => e.status,
      cell: (e) => (
        <StatusIndicator type={resourceStatus(e.status)}>{e.status}</StatusIndicator>
      ),
    },
    { id: 'started', header: 'Started', filterLabel: 'Started', filterValue: (e) => formatDate(e.startDate), cell: (e) => formatDate(e.startDate) },
  ]

  return (
    <ContentLayout
      header={
        <Header
          variant="h1"
          description={<Box variant="code">{smArn}</Box>}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <Button iconName="angle-left" onClick={() => navigate('../state-machines')}>State machines</Button>
              <Button variant="primary" onClick={() => setStartOpen(true)}>
                Start execution
              </Button>
            </SpaceBetween>
          }
        >
          Executions
        </Header>
      }
    >
      {error ? (
        <ErrorState header="Failed to load executions" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <SpaceBetween size="l">
          <ResourceTable
            favoriteService="sfn"
            favorite={(e) => ({ id: e.arn, label: e.name, href: '/aws/sfn/executions', type: 'execution' })}
            items={executions}
            columns={columns}
            trackBy={(e) => e.arn}
            title="Executions"
            loading={isLoading}
            onRowClick={(e) => setHistoryExec(e)}
            selectionType="multi"
            selectedItems={selected}
            onSelectionChange={setSelected}
            actions={
              <ButtonDropdown
                items={[{ id: 'stop', text: 'Stop', disabled: !hasRunning }]}
                onItemClick={() => stopMut.mutate(selected.filter((e) => e.status === 'RUNNING'))}
                disabled={selected.length === 0}
              >
                Actions
              </ButtonDropdown>
            }
            emptyTitle="No executions"
            emptyBody="Start an execution to run this state machine."
          />

          {historyExec && (
            <Container
              header={
                <Header
                  variant="h2"
                  actions={
                    <Button variant="link" onClick={() => setHistoryExec(null)}>
                      Close
                    </Button>
                  }
                >
                  History ({events.length})
                </Header>
              }
            >
              <Table
                items={events}
                columnDefinitions={historyColumns}
                loading={historyLoading}
                loadingText="Loading history"
                trackBy={(ev) => String(ev.id)}
                empty={<Box textAlign="center">No events</Box>}
              />
            </Container>
          )}
        </SpaceBetween>
      )}

      <Modal
        visible={startOpen}
        onDismiss={() => setStartOpen(false)}
        header="Start execution"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setStartOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={startMut.isPending}
                onClick={() => startMut.mutate()}
              >
                Start
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="Execution name (optional)">
              <Input
                autoFocus
                value={form.name}
                onChange={({ detail }) => setForm({ ...form, name: detail.value })}
                placeholder="my-execution"
              />
            </FormField>
            <FormField label="Input (JSON)">
              <Textarea
                rows={6}
                value={form.input}
                onChange={({ detail }) => setForm({ ...form, input: detail.value })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>
    </ContentLayout>
  )
}
