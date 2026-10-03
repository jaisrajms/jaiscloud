import { useEffect, useRef, useState } from 'react'
import type { CSSProperties } from 'react'
import { useParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Container,
  ContentLayout,
  Header,
  Input,
  Select,
  SpaceBetween,
  StatusIndicator,
  Toggle,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import { getLogEvents, filterLogEvents, type LogEvent } from '../../../api/logs'

type Preset = '5m' | '15m' | '1h' | '24h'

const PRESETS: { label: string; key: Preset; ms: number }[] = [
  { label: 'Last 5 minutes', key: '5m', ms: 5 * 60 * 1000 },
  { label: 'Last 15 minutes', key: '15m', ms: 15 * 60 * 1000 },
  { label: 'Last 1 hour', key: '1h', ms: 60 * 60 * 1000 },
  { label: 'Last 24 hours', key: '24h', ms: 24 * 60 * 60 * 1000 },
]

function logLineColor(msg: string): string {
  if (/^START /.test(msg)) return '#4caf78'
  if (/^END /.test(msg)) return '#79b8ff'
  if (/^REPORT /.test(msg)) return '#9da8b5'
  if (/^ERROR/.test(msg)) return '#ff6b6b'
  if (/^WARN/.test(msg)) return '#ffa94d'
  if (/^INFO/.test(msg)) return '#74c7f0'
  if (/^DEBUG/.test(msg)) return '#b197fc'
  return '#e0e0e0'
}

function fmtTs(ms: number): string {
  return new Date(ms).toISOString().replace('T', ' ').replace('Z', '')
}

export function LogStreamView() {
  const { name: encodedGroup, stream: encodedStream } = useParams<{ name: string; stream: string }>()
  const groupName = encodedGroup ? decodeURIComponent(encodedGroup) : ''
  const streamName = encodedStream ? decodeURIComponent(encodedStream) : ''

  const [preset, setPreset] = useState<Preset>('15m')
  const [filterPattern, setFilterPattern] = useState('')
  const [liveTail, setLiveTail] = useState(false)
  const [events, setEvents] = useState<LogEvent[]>([])
  const bottomRef = useRef<HTMLDivElement>(null)

  // queryKey uses stable state values — NOT Date.now() which changes every ms and causes blink
  const queryKey = ['logs', 'events', groupName, streamName, preset, filterPattern]

  const { refetch, isFetching, error } = useQuery({
    queryKey,
    queryFn: async () => {
      const now = Date.now()
      const presetMs = PRESETS.find((p) => p.key === preset)?.ms ?? 15 * 60 * 1000
      const startTime = now - presetMs
      let result
      if (filterPattern) {
        result = await filterLogEvents(groupName, { filterPattern, startTime, endTime: now })
      } else {
        result = await getLogEvents(groupName, streamName, { startTime, endTime: now })
      }
      setEvents((prev) => {
        const combined = [...prev, ...(result.events ?? [])]
        const unique = Array.from(new Map(combined.map((e) => [e.timestamp + e.message, e])).values())
        unique.sort((a, b) => a.timestamp - b.timestamp)
        return unique.slice(-10000)
      })
      return result
    },
    refetchInterval: liveTail ? 3000 : false,
    staleTime: 0,
  })

  useEffect(() => {
    setEvents([])
    void refetch()
  }, [groupName, streamName, preset, filterPattern])

  useEffect(() => {
    if (liveTail && bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [events, liveTail])

  return (
    <ContentLayout
      header={
        <Header variant="h1" description={groupName}>
          {streamName}
        </Header>
      }
    >
      <SpaceBetween size="l">
        <SpaceBetween direction="horizontal" size="s" alignItems="end">
          <Select
            selectedOption={
              PRESETS.map((p) => ({ value: p.key, label: p.label })).find((o) => o.value === preset) ?? null
            }
            onChange={({ detail }) => {
              setPreset(detail.selectedOption.value as Preset)
              setEvents([])
            }}
            options={PRESETS.map((p) => ({ value: p.key, label: p.label }))}
            ariaLabel="Time range"
          />
          <Input
            value={filterPattern}
            onChange={({ detail }) => {
              setFilterPattern(detail.value)
              setEvents([])
            }}
            placeholder="Filter pattern"
            ariaLabel="Filter pattern"
          />
          <Button
            onClick={() => {
              setEvents([])
              void refetch()
            }}
            loading={isFetching}
          >
            Refresh
          </Button>
          <Toggle checked={liveTail} onChange={({ detail }) => setLiveTail(detail.checked)}>
            Live tail
          </Toggle>
          {liveTail && (
            <StatusIndicator type="success">active</StatusIndicator>
          )}
        </SpaceBetween>

        {error && (
          <ErrorState header="Failed to load log events" message={(error as Error).message} />
        )}

        <Container header={<Header variant="h2">Log events</Header>}>
          <div style={logViewerStyle}>
            {events.length === 0 && !isFetching && (
              <Box color="text-body-secondary">No log events in the selected time range.</Box>
            )}
            {events.map((ev, i) => (
              <div key={`${ev.timestamp}-${i}`} style={logRowStyle}>
                <span style={logTimestampStyle}>{fmtTs(ev.timestamp)}</span>
                <span style={{ color: logLineColor(ev.message) }}>{ev.message}</span>
              </div>
            ))}
            <div ref={bottomRef} />
          </div>
        </Container>

        {events.length >= 10000 && (
          <Alert type="warning">
            Display capped at 10,000 events. Narrow the time range to see earlier events.
          </Alert>
        )}
      </SpaceBetween>
    </ContentLayout>
  )
}

const logViewerStyle: CSSProperties = {
  background: '#0f141a',
  color: '#e0e0e0',
  fontFamily: 'monospace',
  fontSize: '0.8em',
  padding: '0.75rem',
  borderRadius: 8,
  minHeight: 400,
  maxHeight: 640,
  overflow: 'auto',
}

const logRowStyle: CSSProperties = {
  display: 'flex',
  gap: '1rem',
  padding: '0.1rem 0',
  whiteSpace: 'pre-wrap',
  wordBreak: 'break-all',
}

const logTimestampStyle: CSSProperties = {
  color: '#4ec9b0',
  flexShrink: 0,
  userSelect: 'none',
}
