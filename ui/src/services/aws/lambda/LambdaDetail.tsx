import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
  Box,
  Button,
  Container,
  ContentLayout,
  Header,
  Icon,
  KeyValuePairs,
  Link,
  Select,
  SpaceBetween,
  StatusIndicator,
  Table,
  Tabs,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import type { TableProps } from '@cloudscape-design/components'
import { getFunction, type LambdaFunction } from '../../../api/lambda'
import { listLogStreams, getLogEvents } from '../../../api/logs'
import { formatDate } from '../../../lib/date'
import { resourceStatus } from '../../../lib/status'
import { LambdaTest } from './LambdaTest'

type Tab = 'test' | 'logs' | 'configuration'

const TAB_LABELS: Record<Tab, string> = {
  test: 'Test',
  logs: 'Logs',
  configuration: 'Configuration',
}

export function LambdaDetail() {
  const { name: encodedName } = useParams<{ name: string }>()
  const name = encodedName ? decodeURIComponent(encodedName) : ''
  const navigate = useNavigate()
  const [tab, setTab] = useState<Tab>('test')

  const { data: fn, isLoading, error } = useQuery({
    queryKey: ['lambda', 'function', name],
    queryFn: () => getFunction(name),
    enabled: !!name,
  })

  if (isLoading) {
    return (
      <ContentLayout header={<Header variant="h1">Lambda function</Header>}>
        <Box color="text-body-secondary">Loading…</Box>
      </ContentLayout>
    )
  }

  if (error || !fn) {
    return (
      <ContentLayout header={<Header variant="h1">Lambda function</Header>}>
        <ErrorState header="Failed to load function" message={error ? (error as Error).message : 'Function not found.'} />
      </ContentLayout>
    )
  }

  return (
    <ContentLayout
      header={
        <Header
          variant="h1"
          description={
            <Link
              href="/ui/aws/lambda"
              onFollow={(event) => {
                event.preventDefault()
                navigate('/aws/lambda')
              }}
            >
              <Icon name="angle-left" /> Lambda functions
            </Link>
          }
          actions={<StatusIndicator type={resourceStatus(fn.state)}>{fn.state}</StatusIndicator>}
        >
          {fn.name}
        </Header>
      }
    >
      <Tabs
        tabs={(Object.keys(TAB_LABELS) as Tab[]).map((id) => ({ id, label: TAB_LABELS[id] }))}
        activeTabId={tab}
        onChange={({ detail }) => setTab(detail.activeTabId as Tab)}
      />

      <Box margin={{ top: 'l' }}>
        {tab === 'test' && <LambdaTest name={name} />}
        {tab === 'logs' && <LambdaLogs name={name} />}
        {tab === 'configuration' && <LambdaConfig fn={fn} />}
      </Box>
    </ContentLayout>
  )
}

function LambdaConfig({ fn }: { fn: LambdaFunction }) {
  const configItems = [
    { label: 'ARN', value: fn.arn },
    { label: 'Runtime', value: fn.runtime },
    { label: 'Handler', value: fn.handler },
    { label: 'Role ARN', value: fn.roleArn },
    { label: 'Timeout', value: `${fn.timeout}s` },
    { label: 'Memory', value: `${fn.memorySize} MB` },
    { label: 'State', value: fn.state },
    { label: 'Description', value: fn.description || '—' },
    { label: 'Last modified', value: formatDate(fn.lastModified) },
  ]

  const envVars = fn.envVars ?? {}
  const envEntries = Object.entries(envVars)
  const envColumns: TableProps.ColumnDefinition<{ key: string; value: string }>[] = [
    { id: 'key', header: 'Key', cell: (item) => <Box variant="code">{item.key}</Box> },
    { id: 'value', header: 'Value', cell: (item) => <Box variant="code">{item.value}</Box> },
  ]

  return (
    <SpaceBetween size="l">
      <Container header={<Header variant="h2">Configuration</Header>}>
        <KeyValuePairs columns={2} items={configItems} />
      </Container>

      {envEntries.length > 0 && (
        <Table
          items={envEntries.map(([key, value]) => ({ key, value }))}
          columnDefinitions={envColumns}
          trackBy={(item) => item.key}
          header={
            <Header variant="h2" counter={`(${envEntries.length})`}>
              Environment variables
            </Header>
          }
          empty={<Box textAlign="center">No environment variables.</Box>}
        />
      )}
    </SpaceBetween>
  )
}

function LambdaLogs({ name }: { name: string }) {
  const logGroupName = `/aws/lambda/${name}`
  const [selectedStream, setSelectedStream] = useState<string | null>(null)

  const { data: streamsData, isLoading: streamsLoading, refetch } = useQuery({
    queryKey: ['logs', 'lambda', name, 'streams'],
    queryFn: () => listLogStreams(logGroupName, { pageSize: 20 }),
    retry: false,
  })

  const streams = streamsData?.items ?? []
  const activeStream = selectedStream ?? streams[0]?.name ?? null

  const { data: eventsData, isLoading: eventsLoading } = useQuery({
    queryKey: ['logs', 'lambda', name, 'events', activeStream],
    queryFn: () =>
      getLogEvents(logGroupName, activeStream!, {
        limit: 200,
        startTime: Date.now() - 24 * 60 * 60 * 1000,
        endTime: Date.now(),
      }),
    enabled: !!activeStream,
  })

  if (streamsLoading) return <Box color="text-body-secondary">Loading log streams…</Box>

  if (streams.length === 0) {
    return (
      <SpaceBetween size="s">
        <Box color="text-body-secondary">
          No log streams found for <Box variant="code">/aws/lambda/{name}</Box>.
        </Box>
        <Box color="text-body-secondary">Invoke the function to generate logs.</Box>
        <Button onClick={() => refetch()}>Refresh</Button>
      </SpaceBetween>
    )
  }

  return (
    <Container
      header={
        <Header
          variant="h2"
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <Select
                selectedOption={{ value: activeStream ?? '', label: activeStream ?? '' }}
                onChange={({ detail }) => setSelectedStream(detail.selectedOption.value ?? null)}
                options={streams.map((stream) => ({ value: stream.name, label: stream.name }))}
                ariaLabel="Log stream"
              />
              <Button onClick={() => refetch()}>Refresh</Button>
            </SpaceBetween>
          }
        >
          Log stream
        </Header>
      }
    >
      <pre style={logPreStyle}>
        {eventsLoading ? (
          <span style={{ color: '#9da8b5' }}>Loading…</span>
        ) : !eventsData || eventsData.events.length === 0 ? (
          <span style={{ color: '#9da8b5' }}>No events in this stream.</span>
        ) : (
          eventsData.events.map((event, index) => (
            <div key={index} style={colorLogLine(event.message)}>
              {event.message}
            </div>
          ))
        )}
      </pre>
    </Container>
  )
}

function logColor(line: string): string {
  if (/^START /.test(line)) return '#4caf78'
  if (/^END /.test(line)) return '#79b8ff'
  if (/^REPORT /.test(line)) return '#9da8b5'
  if (/^ERROR/.test(line)) return '#ff6b6b'
  if (/^WARN/.test(line)) return '#ffa94d'
  return '#e0e0e0'
}

function colorLogLine(line: string): React.CSSProperties {
  return { color: logColor(line) }
}

const logPreStyle: React.CSSProperties = {
  margin: 0,
  padding: '0.75rem 1rem',
  background: '#1e1e1e',
  borderRadius: 6,
  overflow: 'auto',
  maxHeight: 500,
  fontSize: '0.8em',
  fontFamily: 'monospace',
  lineHeight: 1.6,
}
