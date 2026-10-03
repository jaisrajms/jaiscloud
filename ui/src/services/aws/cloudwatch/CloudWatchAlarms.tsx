import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Badge,
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
import { ErrorState } from '../../../components/ErrorState'
import type { StatusIndicatorProps } from '@cloudscape-design/components'
import {
  listAlarms,
  putAlarm,
  deleteAlarm,
  setAlarmState,
  enableAlarmActions,
  disableAlarmActions,
  type CWAlarm,
} from '../../../api/cloudwatch'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { formatDate } from '../../../lib/date'
import { useNotifications } from '../../../components/notifications'

const STATE_OPTIONS = [
  { value: '', label: 'All states' },
  { value: 'OK', label: 'OK' },
  { value: 'ALARM', label: 'ALARM' },
  { value: 'INSUFFICIENT_DATA', label: 'INSUFFICIENT_DATA' },
]

const STATISTIC_OPTIONS = ['Average', 'Sum', 'Minimum', 'Maximum', 'SampleCount'].map((s) => ({
  value: s,
  label: s,
}))

const COMPARISON_OPTIONS = [
  'GreaterThanThreshold',
  'GreaterThanOrEqualToThreshold',
  'LessThanThreshold',
  'LessThanOrEqualToThreshold',
].map((o) => ({ value: o, label: o }))

function alarmStatus(state?: string): StatusIndicatorProps.Type {
  switch (state) {
    case 'OK':
      return 'success'
    case 'ALARM':
      return 'error'
    case 'INSUFFICIENT_DATA':
      return 'warning'
    default:
      return 'info'
  }
}

export function CloudWatchAlarms() {
  const [createOpen, setCreateOpen] = useState(false)
  const [stateFilter, setStateFilter] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<CWAlarm | null>(null)
  const [stateTarget, setStateTarget] = useState<CWAlarm | null>(null)
  const [details, setDetails] = useState<CWAlarm | null>(null)
  const [newStateValue, setNewStateValue] = useState('OK')
  const [newStateReason, setNewStateReason] = useState('')

  // Create alarm form
  const [form, setForm] = useState({
    alarmName: '',
    namespace: '',
    metricName: '',
    statistic: 'Average',
    period: 60,
    threshold: 0,
    comparisonOperator: 'GreaterThanThreshold',
    evaluationPeriods: 1,
  })

  const qc = useQueryClient()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['cloudwatch', 'alarms', stateFilter],
    queryFn: () => listAlarms(stateFilter ? { stateValue: stateFilter } : undefined),
  })

  const createMut = useMutation({
    mutationFn: () => putAlarm({ ...form, actionsEnabled: true }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] })
      notify({ type: 'success', header: 'Alarm created', content: form.alarmName })
      setCreateOpen(false)
      setForm({ alarmName: '', namespace: '', metricName: '', statistic: 'Average', period: 60, threshold: 0, comparisonOperator: 'GreaterThanThreshold', evaluationPeriods: 1 })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create alarm', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteAlarm(name),
    onSuccess: (_result, name) => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] })
      notify({ type: 'success', header: 'Alarm deleted', content: name })
      setDeleteTarget(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not delete alarm', content: (err as Error).message }),
  })

  const stateMut = useMutation({
    mutationFn: () => setAlarmState({ alarmName: stateTarget!.alarmName, stateValue: newStateValue, stateReason: newStateReason }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] })
      notify({ type: 'success', header: 'Alarm state updated' })
      setStateTarget(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not update alarm state', content: (err as Error).message }),
  })

  const enableMut = useMutation({
    mutationFn: (name: string) => enableAlarmActions([name]),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] })
      notify({ type: 'success', header: 'Alarm actions enabled' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not enable actions', content: (err as Error).message }),
  })

  const disableMut = useMutation({
    mutationFn: (name: string) => disableAlarmActions([name]),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] })
      notify({ type: 'success', header: 'Alarm actions disabled' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not disable actions', content: (err as Error).message }),
  })

  const alarms = data?.items ?? []

  const columns: ResourceColumn<CWAlarm>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (a) => a.alarmName,
      cell: (a) => a.alarmName,
    },
    {
      id: 'metric',
      header: 'Metric',
      cell: (a) =>
        a.namespace || a.metricName ? `${a.namespace ?? '—'}/${a.metricName ?? '—'}` : '—',
    },
    {
      id: 'state',
      header: 'State',
      cell: (a) => (
        <StatusIndicator type={alarmStatus(a.stateValue)}>
          {a.stateValue ?? '—'}
        </StatusIndicator>
      ),
    },
    {
      id: 'actionsEnabled',
      header: 'Actions',
      cell: (a) => (
        <Badge color={a.actionsEnabled ? 'green' : 'grey'}>
          {a.actionsEnabled ? 'enabled' : 'disabled'}
        </Badge>
      ),
    },
    {
      id: 'rowActions',
      header: '',
      cell: (a) => (
        <ButtonDropdown
          variant="icon"
          ariaLabel={`Actions for ${a.alarmName}`}
          items={[
            { id: 'details', text: 'View details' },
            { id: 'state', text: 'Set state' },
            { id: 'toggle', text: a.actionsEnabled ? 'Disable actions' : 'Enable actions' },
            { id: 'delete', text: 'Delete' },
          ]}
          onItemClick={({ detail }) => {
            if (detail.id === 'details') {
              setDetails(a)
            } else if (detail.id === 'state') {
              setStateTarget(a)
              setNewStateValue('OK')
              setNewStateReason('')
            } else if (detail.id === 'toggle') {
              if (a.actionsEnabled) disableMut.mutate(a.alarmName)
              else enableMut.mutate(a.alarmName)
            } else if (detail.id === 'delete') {
              setDeleteTarget(a)
            }
          }}
        />
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">CloudWatch alarms</Header>}>
      {error ? (
        <ErrorState header="Failed to load alarms" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="cloudwatch"
          favorite={(a) => ({ id: a.alarmName, label: a.alarmName, href: '/aws/cloudwatch/alarms', type: 'alarm' })}
          items={alarms}
          columns={columns}
          trackBy={(a) => a.alarmName}
          title="Alarms"
          loading={isLoading}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <Select
                selectedOption={STATE_OPTIONS.find((o) => o.value === stateFilter) ?? STATE_OPTIONS[0]!}
                onChange={({ detail }) => setStateFilter(detail.selectedOption.value ?? '')}
                options={STATE_OPTIONS}
                ariaLabel="Filter alarms by state"
              />
              <Button variant="primary" onClick={() => setCreateOpen(true)}>
                Create alarm
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No alarms"
          emptyBody="Create a CloudWatch alarm to watch a metric and notify you when it crosses a threshold."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={details?.alarmName ?? 'Alarm'}
        items={
          details
            ? [
                { label: 'Name', value: details.alarmName },
                { label: 'ARN', value: <Box variant="code">{details.alarmArn || '—'}</Box> },
                { label: 'Description', value: details.alarmDescription || '—' },
                {
                  label: 'State',
                  value: (
                    <StatusIndicator type={alarmStatus(details.stateValue)}>
                      {details.stateValue ?? '—'}
                    </StatusIndicator>
                  ),
                },
                {
                  label: 'Metric',
                  value:
                    details.namespace || details.metricName
                      ? `${details.namespace ?? '—'}/${details.metricName ?? '—'}`
                      : '—',
                },
                { label: 'Statistic', value: details.statistic || '—' },
                { label: 'Period', value: details.period != null ? `${details.period}s` : '—' },
                { label: 'Threshold', value: details.threshold != null ? String(details.threshold) : '—' },
                { label: 'Comparison operator', value: details.comparisonOperator || '—' },
                { label: 'Evaluation periods', value: details.evaluationPeriods != null ? String(details.evaluationPeriods) : '—' },
                {
                  label: 'Actions',
                  value: (
                    <Badge color={details.actionsEnabled ? 'green' : 'grey'}>
                      {details.actionsEnabled ? 'enabled' : 'disabled'}
                    </Badge>
                  ),
                },
                { label: 'State reason', value: details.stateReason || '—' },
                { label: 'Updated', value: formatDate(details.updatedAt) },
              ]
            : []
        }
      />

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create alarm"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!form.alarmName}
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
            <FormField label="Alarm name">
              <Input
                autoFocus
                value={form.alarmName}
                onChange={({ detail }) => setForm((f) => ({ ...f, alarmName: detail.value }))}
                placeholder="my-alarm"
              />
            </FormField>
            <FormField label="Namespace">
              <Input
                value={form.namespace}
                onChange={({ detail }) => setForm((f) => ({ ...f, namespace: detail.value }))}
                placeholder="AWS/EC2"
              />
            </FormField>
            <FormField label="Metric name">
              <Input
                value={form.metricName}
                onChange={({ detail }) => setForm((f) => ({ ...f, metricName: detail.value }))}
                placeholder="CPUUtilization"
              />
            </FormField>
            <FormField label="Statistic">
              <Select
                selectedOption={STATISTIC_OPTIONS.find((o) => o.value === form.statistic) ?? STATISTIC_OPTIONS[0]!}
                onChange={({ detail }) =>
                  setForm((f) => ({ ...f, statistic: detail.selectedOption.value ?? 'Average' }))
                }
                options={STATISTIC_OPTIONS}
              />
            </FormField>
            <FormField label="Period (seconds)">
              <Input
                type="number"
                value={String(form.period)}
                onChange={({ detail }) => setForm((f) => ({ ...f, period: Number(detail.value) || 0 }))}
              />
            </FormField>
            <FormField label="Threshold">
              <Input
                type="number"
                value={String(form.threshold)}
                onChange={({ detail }) => setForm((f) => ({ ...f, threshold: Number(detail.value) || 0 }))}
              />
            </FormField>
            <FormField label="Comparison operator">
              <Select
                selectedOption={
                  COMPARISON_OPTIONS.find((o) => o.value === form.comparisonOperator) ?? COMPARISON_OPTIONS[0]!
                }
                onChange={({ detail }) =>
                  setForm((f) => ({ ...f, comparisonOperator: detail.selectedOption.value ?? 'GreaterThanThreshold' }))
                }
                options={COMPARISON_OPTIONS}
              />
            </FormField>
            <FormField label="Evaluation periods">
              <Input
                type="number"
                value={String(form.evaluationPeriods)}
                onChange={({ detail }) =>
                  setForm((f) => ({ ...f, evaluationPeriods: Number(detail.value) || 0 }))
                }
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={stateTarget != null}
        onDismiss={() => setStateTarget(null)}
        header="Set alarm state"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setStateTarget(null)}>
                Cancel
              </Button>
              <Button variant="primary" loading={stateMut.isPending} onClick={() => stateMut.mutate()}>
                Apply
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <Box variant="p" color="text-body-secondary">
              {stateTarget?.alarmName}
            </Box>
            <FormField label="State">
              <Select
                selectedOption={
                  STATE_OPTIONS.find((o) => o.value === newStateValue) ?? STATE_OPTIONS[1]!
                }
                onChange={({ detail }) => setNewStateValue(detail.selectedOption.value ?? 'OK')}
                options={STATE_OPTIONS.slice(1)}
              />
            </FormField>
            <FormField label="Reason">
              <Input
                value={newStateReason}
                onChange={({ detail }) => setNewStateReason(detail.value)}
                placeholder="Manual state change"
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={deleteTarget != null}
        onDismiss={() => setDeleteTarget(null)}
        header="Delete alarm"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setDeleteTarget(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => deleteTarget && deleteMut.mutate(deleteTarget.alarmName)}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Delete <b>{deleteTarget?.alarmName}</b>? This action cannot be undone.
      </Modal>
    </ContentLayout>
  )
}
