import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Container,
  ContentLayout,
  DateRangePicker,
  Header,
  LineChart,
  SpaceBetween,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import type { DateRangePickerProps } from '@cloudscape-design/components'
import { listMetrics, getMetricStatistics, type CWMetric } from '../../../api/cloudwatch'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { RELATIVE_OPTIONS, rangeToWindow } from '../../../lib/timeRange'

export function CloudWatchMetrics() {
  const [selected, setSelected] = useState<CWMetric | null>(null)
  const [range, setRange] = useState<DateRangePickerProps.Value | null>({
    type: 'relative',
    amount: 3,
    unit: 'hour',
  })

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['cloudwatch', 'metrics'],
    queryFn: () => listMetrics(),
  })

  const { start, end } = useMemo(() => rangeToWindow(range), [range])

  const stats = useQuery({
    queryKey: ['cloudwatch', 'stats', selected?.namespace, selected?.metricName, start.toISOString(), end.toISOString()],
    enabled: selected != null,
    queryFn: () =>
      getMetricStatistics({
        namespace: selected!.namespace,
        metricName: selected!.metricName,
        startTime: start.toISOString(),
        endTime: end.toISOString(),
        period: 300,
        statistics: ['Sum', 'Average', 'Maximum', 'SampleCount'],
      }),
  })

  const metrics = data?.items ?? []

  const columns: ResourceColumn<CWMetric>[] = [
    { id: 'namespace', header: 'Namespace', filterLabel: 'Namespace', filterValue: (m) => m.namespace, cell: (m) => m.namespace },
    {
      id: 'metricName',
      header: 'Metric name',
      filterLabel: 'Metric name',
      filterValue: (m) => m.metricName,
      cell: (m) => m.metricName,
    },
  ]

  const datapoints = stats.data?.datapoints ?? []
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
    {
      title: 'Sum',
      type: 'line' as const,
      data: datapoints
        .filter((d) => d.sum != null)
        .map((d) => ({ x: new Date(d.timestamp), y: d.sum as number })),
    },
  ].filter((s) => s.data.length > 0)

  return (
    <ContentLayout header={<Header variant="h1">CloudWatch metrics</Header>}>
      {error ? (
        <ErrorState header="Failed to load metrics" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <SpaceBetween size="l">
          <ResourceTable
            favoriteService="cloudwatch"
            favorite={(m) => ({ id: m.namespace + '/' + m.metricName, label: m.namespace + '/' + m.metricName, href: '/aws/cloudwatch/metrics', type: 'metric' })}
            items={metrics}
            columns={columns}
            trackBy={(m) => `${m.namespace}/${m.metricName}`}
            title="Metrics"
            loading={isLoading}
            selectionType="single"
            selectedItems={selected ? [selected] : []}
            onSelectionChange={(items) => setSelected(items[0] ?? null)}
            onRowClick={(m) => setSelected(m)}
            emptyTitle="No metrics"
            emptyBody="Publish metric data via PutMetricData to see metrics here."
          />

          {selected && (
            <Container
              header={
                <Header variant="h2" description={`${selected.namespace} · 5-minute periods`}>
                  {selected.metricName}
                </Header>
              }
            >
              <SpaceBetween size="m">
                <DateRangePicker
                  value={range}
                  onChange={({ detail }) => setRange(detail.value)}
                  relativeOptions={RELATIVE_OPTIONS}
                  isValidRange={() => ({ valid: true })}
                  placeholder="Filter by a date and time range"
                  ariaLabel="Time range"
                />
                <LineChart
                  series={series}
                  xScaleType="time"
                  yScaleType="linear"
                  xTitle="Time"
                  yTitle="Value"
                  height={320}
                  statusType={stats.isLoading ? 'loading' : 'finished'}
                  loadingText="Loading statistics"
                  errorText="Failed to load statistics"
                  empty={<span>No datapoints in the selected range.</span>}
                  ariaLabel={`Statistics for ${selected.metricName}`}
                  i18nStrings={{
                    xTickFormatter: (value) =>
                      new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
                    yTickFormatter: (value) => value.toLocaleString(),
                    filterLabel: 'Filter series',
                    filterPlaceholder: 'Filter series',
                  }}
                />
              </SpaceBetween>
            </Container>
          )}
        </SpaceBetween>
      )}
    </ContentLayout>
  )
}
