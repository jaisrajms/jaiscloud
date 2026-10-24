import { useEffect, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { refreshSession } from '../api/client'
import { FALLBACK_POLL_MS } from './useAutoRefresh'

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

  // Keep the "Polling" state honest: when the stream is down, actually poll via
  // a global default refetch interval; when live, stop polling. Queries with an
  // explicit refetchInterval keep their own cadence.
  useEffect(() => {
    const prev = qc.getDefaultOptions()
    qc.setDefaultOptions({
      ...prev,
      queries: { ...prev.queries, refetchInterval: connected ? false : FALLBACK_POLL_MS },
    })
  }, [connected, qc])

  useEffect(() => {
    let cancelled = false

    function connect() {
      if (cancelled) return

      // The HttpOnly session cookie is sent automatically with withCredentials;
      // no token query param (which would leak the token into URLs/logs).
      const es = new EventSource(SSE_PATH, { withCredentials: true })
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
            keys?: string[]
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
          // GCP status events carry the exact React Query key prefix to
          // invalidate; AWS events fall back to the resource name.
          const key = event.keys?.length
            ? event.keys
            : event.resource
              ? [event.resource]
              : null
          if (key) {
            qc.invalidateQueries({ queryKey: key })
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
          // EventSource hides the HTTP status, so a 401 after a restart is
          // indistinguishable from a transient drop. Re-acquire the session
          // cookie before retrying; it is a no-op when the token is still valid.
          void refreshSession().finally(() => {
            if (!cancelled) setTimeout(connect, delay)
          })
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
