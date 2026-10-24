import { useEffect, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { refreshSession } from '../api/client'

/**
 * Recover the console after the emulator restarts or the page is restored from
 * the back/forward cache.
 *
 * - A changed meta `bootId` means a new process is serving us: re-acquire the
 *   session cookie and drop every cached query. (The persisted instanceId does
 *   not change across restarts, so bootId is the reliable marker.)
 * - A bfcache `pageshow` (event.persisted) restores a frozen page whose SSE
 *   connection and cached data are stale.
 */
export function useRestartRecovery(bootId: string | undefined) {
  const qc = useQueryClient()
  const lastBootId = useRef<string | undefined>(undefined)

  useEffect(() => {
    if (!bootId) return
    if (lastBootId.current === undefined) {
      lastBootId.current = bootId
      return
    }
    if (lastBootId.current !== bootId) {
      lastBootId.current = bootId
      void refreshSession()
      void qc.invalidateQueries()
    }
  }, [bootId, qc])

  useEffect(() => {
    const onPageShow = (event: PageTransitionEvent) => {
      if (!event.persisted) return
      void refreshSession()
      void qc.invalidateQueries()
    }
    window.addEventListener('pageshow', onPageShow)
    return () => window.removeEventListener('pageshow', onPageShow)
  }, [qc])
}
