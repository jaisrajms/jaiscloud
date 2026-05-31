import { useEffect, useRef, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { getLogEvents, filterLogEvents, type LogEvent } from '../../../api/logs'

type Preset = '5m' | '15m' | '1h' | '24h' | 'custom'

const PRESETS: { label: string; key: Preset; ms: number }[] = [
  { label: '5m', key: '5m', ms: 5 * 60 * 1000 },
  { label: '15m', key: '15m', ms: 15 * 60 * 1000 },
  { label: '1h', key: '1h', ms: 60 * 60 * 1000 },
  { label: '24h', key: '24h', ms: 24 * 60 * 60 * 1000 },
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
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Breadcrumb */}
      <div style={{ marginBottom: '1rem', fontSize: '0.85em' }}>
        <Link to="/aws/logs/groups" style={{ color: '#0972d3', textDecoration: 'none' }}>Log Groups</Link>
        {' / '}
        <Link to={`/aws/logs/groups/${encodeURIComponent(groupName)}`} style={{ color: '#0972d3', textDecoration: 'none' }}>{groupName}</Link>
        {' / '}
        <span style={{ color: '#5f6b7a', fontFamily: 'monospace' }}>{streamName}</span>
      </div>

      {/* Controls */}
      <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', marginBottom: '0.75rem', flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', border: '1px solid #c9cdd4', borderRadius: 4, overflow: 'hidden' }}>
          {PRESETS.map((p) => (
            <button
              key={p.key}
              onClick={() => { setPreset(p.key); setEvents([]) }}
              style={{
                background: preset === p.key ? '#e77600' : '#fff',
                color: preset === p.key ? '#fff' : '#5f6b7a',
                border: 'none', padding: '0.35rem 0.7rem', cursor: 'pointer', fontSize: '0.82em', fontWeight: preset === p.key ? 600 : 400,
              }}
            >
              {p.label}
            </button>
          ))}
        </div>

        <input
          value={filterPattern}
          onChange={(e) => { setFilterPattern(e.target.value); setEvents([]) }}
          placeholder="Filter pattern…"
          style={{ border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.35rem 0.6rem', fontSize: '0.85em', width: 220 }}
        />

        <button
          onClick={() => { setEvents([]); void refetch() }}
          disabled={isFetching}
          style={{ background: '#0972d3', color: '#fff', border: 'none', borderRadius: 4, padding: '0.35rem 0.75rem', cursor: 'pointer', fontSize: '0.85em', opacity: isFetching ? 0.6 : 1 }}
        >
          {isFetching ? 'Loading…' : 'Refresh'}
        </button>

        <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', fontSize: '0.85em', cursor: 'pointer', marginLeft: 'auto' }}>
          <input type="checkbox" checked={liveTail} onChange={(e) => setLiveTail(e.target.checked)} />
          Live tail
          {liveTail && <span style={{ color: '#1d8102', fontSize: '0.85em' }}>● active</span>}
        </label>
      </div>

      {error && <p style={{ color: '#d13212', fontSize: '0.85em', margin: '0 0 0.5rem' }}>{(error as Error).message}</p>}

      {/* Log viewer */}
      <div style={{
        flex: 1, minHeight: 400, border: '1px solid #e7e9ec', borderRadius: 6,
        background: '#1e1e1e', overflow: 'auto', fontFamily: 'monospace', fontSize: '0.8em',
        padding: '0.5rem',
      }}>
        {events.length === 0 && !isFetching && (
          <div style={{ color: '#5f6b7a', padding: '0.5rem', fontStyle: 'italic' }}>No log events in the selected time range.</div>
        )}
        {events.map((ev, i) => (
          <div key={`${ev.timestamp}-${i}`} style={{ display: 'flex', gap: '1rem', padding: '0.15rem 0', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
            <span style={{ color: '#4ec9b0', flexShrink: 0, userSelect: 'none' }}>{fmtTs(ev.timestamp)}</span>
            <span style={{ color: logLineColor(ev.message) }}>{ev.message}</span>
          </div>
        ))}
        <div ref={bottomRef} />
      </div>

      {events.length >= 10000 && (
        <p style={{ color: '#e77600', fontSize: '0.8em', margin: '0.5rem 0 0', fontStyle: 'italic' }}>
          Display capped at 10,000 events. Narrow the time range to see earlier events.
        </p>
      )}
    </div>
  )
}
