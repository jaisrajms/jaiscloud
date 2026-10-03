import { useMemo, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Box,
  Button,
  ButtonDropdown,
  ContentLayout,
  Container,
  Form,
  FormField,
  Header,
  Input,
  LineChart,
  Modal,
  SpaceBetween,
  Spinner,
  Tabs,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import {
  getMetricStatistics,
  listDashboards,
  getDashboard,
  putDashboard,
  deleteDashboard,
  type CWDashboard,
} from '../../../api/cloudwatch'
import { formatDate } from '../../../lib/date'
import { rangeToWindow } from '../../../lib/timeRange'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { JsonEditor } from '../../../components/JsonEditor'
import { useNotifications } from '../../../components/notifications'

const DEFAULT_BODY = JSON.stringify({ widgets: [] }, null, 2)

export function CloudWatchDashboards() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newBody, setNewBody] = useState(DEFAULT_BODY)
  const [viewDash, setViewDash] = useState<CWDashboard | null>(null)
  const [viewBody, setViewBody] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<CWDashboard | null>(null)
  const [loadingView, setLoadingView] = useState(false)

  const qc = useQueryClient()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['cloudwatch', 'dashboards'],
    queryFn: listDashboards,
  })

  const createMut = useMutation({
    mutationFn: () => putDashboard({ dashboardName: newName, dashboardBody: newBody }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'dashboards'] })
      notify({ type: 'success', header: 'Dashboard created', content: newName })
      setCreateOpen(false)
      setNewName('')
      setNewBody(DEFAULT_BODY)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create dashboard', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteDashboard(name),
    onSuccess: (_result, name) => {
      void qc.invalidateQueries({ queryKey: ['cloudwatch', 'dashboards'] })
      notify({ type: 'success', header: 'Dashboard deleted', content: name })
      setDeleteTarget(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not delete dashboard', content: (err as Error).message }),
  })

  async function viewDashboard(d: CWDashboard) {
    setViewDash(d)
    setLoadingView(true)
    setViewBody('')
    try {
      const full = await getDashboard(d.dashboardName)
      try {
        setViewBody(JSON.stringify(JSON.parse(full.dashboardBody ?? '{}'), null, 2))
      } catch {
        setViewBody(full.dashboardBody ?? '')
      }
    } catch {
      setViewBody('Failed to load dashboard body')
    } finally {
      setLoadingView(false)
    }
  }

  const dashboards = data?.items ?? []

  const columns: ResourceColumn<CWDashboard>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (d) => d.dashboardName,
      cell: (d) => d.dashboardName,
    },
    {
      id: 'lastModified',
      header: 'Last modified',
      cell: (d) => formatDate(d.lastModified),
    },
    {
      id: 'rowActions',
      header: '',
      cell: (d) => (
        <ButtonDropdown
          variant="icon"
          ariaLabel={`Actions for ${d.dashboardName}`}
          items={[
            { id: 'view', text: 'View' },
            { id: 'delete', text: 'Delete' },
          ]}
          onItemClick={({ detail }) => {
            if (detail.id === 'view') void viewDashboard(d)
            else if (detail.id === 'delete') setDeleteTarget(d)
          }}
        />
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">CloudWatch dashboards</Header>}>
      {error ? (
        <ErrorState header="Failed to load dashboards" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="cloudwatch"
          favorite={(d) => ({ id: d.dashboardName, label: d.dashboardName, href: '/aws/cloudwatch/dashboards', type: 'dashboard' })}
          items={dashboards}
          columns={columns}
          trackBy={(d) => d.dashboardName}
          title="Dashboards"
          loading={isLoading}
          actions={
            <Button variant="primary" onClick={() => setCreateOpen(true)}>
              Create dashboard
            </Button>
          }
          emptyTitle="No dashboards"
          emptyBody="Create a dashboard to visualize your CloudWatch metrics."
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create dashboard"
        size="large"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!newName}
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
            <FormField label="Dashboard name">
              <Input
                autoFocus
                value={newName}
                onChange={({ detail }) => setNewName(detail.value)}
                placeholder="my-dashboard"
              />
            </FormField>
            <FormField label="Dashboard body" description="CloudWatch dashboard definition as JSON.">
              <JsonEditor value={newBody} onChange={setNewBody} height={280} ariaLabel="Dashboard body" />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={viewDash != null}
        onDismiss={() => setViewDash(null)}
        header={viewDash?.dashboardName ?? 'Dashboard'}
        size="large"
        footer={
          <Box float="right">
            <Button variant="link" onClick={() => setViewDash(null)}>
              Close
            </Button>
          </Box>
        }
      >
        {loadingView ? (
          <Spinner size="large" />
        ) : (
          <Tabs
            tabs={[
              {
                id: 'charts',
                label: 'Charts',
                content: <DashboardCharts body={viewBody} />,
              },
              {
                id: 'json',
                label: 'JSON',
                content: (
                  <JsonEditor value={viewBody} onChange={() => {}} height={360} ariaLabel="Dashboard body" />
                ),
              },
            ]}
          />
        )}
      </Modal>

      <Modal
        visible={deleteTarget != null}
        onDismiss={() => setDeleteTarget(null)}
        header="Delete dashboard"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setDeleteTarget(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => deleteTarget && deleteMut.mutate(deleteTarget.dashboardName)}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Delete <b>{deleteTarget?.dashboardName}</b>? This action cannot be undone.
      </Modal>
    </ContentLayout>
  )
}

interface DashboardWidgetDef {
  type?: string
  properties?: Record<string, unknown>
}

function DashboardCharts({ body }: { body: string }) {
  let widgets: DashboardWidgetDef[] = []
  try {
    const parsed = JSON.parse(body) as { widgets?: DashboardWidgetDef[] }
    widgets = parsed.widgets ?? []
  } catch {
    widgets = []
  }

  if (widgets.length === 0) {
    return <Box color="text-body-secondary">No widgets defined in this dashboard.</Box>
  }

  return (
    <SpaceBetween size="l">
      {widgets.map((widget, index) => (
        <DashboardWidget key={index} widget={widget} />
      ))}
    </SpaceBetween>
  )
}

function DashboardWidget({ widget }: { widget: DashboardWidgetDef }) {
  const properties = widget.properties ?? {}
  const title = typeof properties.title === 'string' ? properties.title : undefined

  if (widget.type === 'text') {
    return (
      <Container header={<Header variant="h2">{title ?? 'Text'}</Header>}>
        <Box variant="p">
          {typeof properties.markdown === 'string' ? properties.markdown : ''}
        </Box>
      </Container>
    )
  }

  const rawMetrics = Array.isArray(properties.metrics) ? properties.metrics : []
  const metrics = rawMetrics.filter(
    (m): m is unknown[] => Array.isArray(m) && m.length >= 2,
  )

  return (
    <Container header={<Header variant="h2">{title ?? 'Metric'}</Header>}>
      {metrics.length === 0 ? (
        <Box color="text-body-secondary">No metrics defined for this widget.</Box>
      ) : (
        <SpaceBetween size="l">
          {metrics.map((metric, index) => (
            <MetricSeries
              key={index}
              namespace={String(metric[0])}
              metricName={String(metric[1])}
            />
          ))}
        </SpaceBetween>
      )}
    </Container>
  )
}

function MetricSeries({ namespace, metricName }: { namespace: string; metricName: string }) {
  const { start, end } = useMemo(
    () => rangeToWindow({ type: 'relative', amount: 3, unit: 'hour' }),
    [],
  )
  const { data, isLoading } = useQuery({
    queryKey: ['cloudwatch', 'stats', namespace, metricName, start.toISOString(), end.toISOString()],
    queryFn: () =>
      getMetricStatistics({
        namespace,
        metricName,
        startTime: start.toISOString(),
        endTime: end.toISOString(),
        period: 300,
        statistics: ['Average', 'Maximum'],
      }),
  })

  const datapoints = data?.datapoints ?? []
  const series = [
    {
      title: 'Average',
      type: 'line' as const,
      data: datapoints
        .filter((d) => d.average != null)
        .map((d) => ({ x: new Date(d.timestamp), y: d.average as number })),
    },
    {
      title: 'Maximum',
      type: 'line' as const,
      data: datapoints
        .filter((d) => d.maximum != null)
        .map((d) => ({ x: new Date(d.timestamp), y: d.maximum as number })),
    },
  ].filter((s) => s.data.length > 0)

  return (
    <div>
      <Box variant="h4" margin={{ bottom: 'xs' }}>
        {namespace} · {metricName}
      </Box>
      <LineChart
        series={series}
        xScaleType="time"
        yScaleType="linear"
        height={240}
        statusType={isLoading ? 'loading' : 'finished'}
        empty={<span>No datapoints in the last 3 hours.</span>}
        ariaLabel={`${metricName} chart`}
        i18nStrings={{
          xTickFormatter: (value) =>
            new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
          yTickFormatter: (value) => value.toLocaleString(),
          filterLabel: 'Filter series',
          filterPlaceholder: 'Filter series',
        }}
      />
    </div>
  )
}
