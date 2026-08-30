import { useEffect, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'

const SSE_PATH = '/api/ui/v1/events/stream'

/** Reconnect backoff steps in ms: 2s, 4s, 8s, 16s, 30s (cap). */
const BACKOFF = [2_000, 4_000, 8_000, 16_000, 30_000]

/**
 * useEventStream opens GET /api/ui/v1/events/stream and wires events to
 * QueryClient cache invalidation. On disconnect it switches affected queries
 * to fallback polling and reconnects with exponential backoff.
 */
export function useEventStream(): { connected: boolean } {
  const qc = useQueryClient()
  const [connected, setConnected] = useState(false)
  const attempt = useRef(0)
  const esRef = useRef<EventSource | null>(null)

  useEffect(() => {
    let cancelled = false

    function connect() {
      if (cancelled) return

      // Dev mode: append ?token= if available from cookie
      let url = SSE_PATH
      const match = document.cookie.split('; ').find((r) => r.startsWith('session='))
      const token = match?.split('=')[1]
      if (token && window.location.hostname === 'localhost') {
        url += `?token=${encodeURIComponent(token)}`
      }

      const es = new EventSource(url, { withCredentials: true })
      esRef.current = es

      es.onopen = () => {
        setConnected(true)
        attempt.current = 0
        // Re-invalidate all stale queries on reconnect.
        qc.invalidateQueries()
      }

      es.onmessage = (evt) => {
        try {
          const event = JSON.parse(evt.data) as {
            type: string
            resource?: string
            id?: string
            state?: string
          }
          if (event.type === 'reset') {
            qc.invalidateQueries()
            return
          }
          if (event.type === 'close') {
            es.close()
            return
          }
          if (event.resource) {
            qc.invalidateQueries({ queryKey: [event.resource] })
          }
        } catch {
          // malformed event — ignore
        }
      }

      es.onerror = () => {
        setConnected(false)
        es.close()
        if (!cancelled) {
          const delay = BACKOFF[Math.min(attempt.current, BACKOFF.length - 1)] ?? 30_000
          attempt.current++
          setTimeout(connect, delay)
        }
      }
    }

    connect()

    return () => {
      cancelled = true
      esRef.current?.close()
      esRef.current = null
    }
  }, [qc])

  return { connected }
}
