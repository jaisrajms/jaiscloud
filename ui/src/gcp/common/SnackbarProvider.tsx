import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react'
import { Alert, Snackbar, type AlertColor } from '@mui/material'

/** Severity levels accepted by the console Snackbar. */
export type GcpNoticeSeverity = AlertColor

export interface GcpNotifyOptions {
  severity?: GcpNoticeSeverity
  /** Auto-hide delay in milliseconds. */
  duration?: number
}

/** Imperative notification API exposed to page components. */
export interface GcpSnackbarApi {
  notify: (message: string, options?: GcpNotifyOptions) => void
}

const GcpSnackbarContext = createContext<GcpSnackbarApi | null>(null)

interface Notice {
  message: string
  severity: GcpNoticeSeverity
  duration: number
}

/**
 * Single app-wide Snackbar so pages report create/delete/update outcomes
 * without each mounting their own. Mount once near the console root and call
 * `useGcpSnackbar().notify(...)` from any page.
 */
export function GcpSnackbarProvider({ children }: { children: ReactNode }) {
  const [notice, setNotice] = useState<Notice | null>(null)

  const notify = useCallback((message: string, options?: GcpNotifyOptions) => {
    setNotice({
      message,
      severity: options?.severity ?? 'success',
      duration: options?.duration ?? 6000,
    })
  }, [])

  const api = useMemo<GcpSnackbarApi>(() => ({ notify }), [notify])

  return (
    <GcpSnackbarContext.Provider value={api}>
      {children}
      <Snackbar
        open={notice != null}
        autoHideDuration={notice?.duration ?? 6000}
        onClose={() => setNotice(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert severity={notice?.severity ?? 'success'} onClose={() => setNotice(null)}>
          {notice?.message}
        </Alert>
      </Snackbar>
    </GcpSnackbarContext.Provider>
  )
}

/** Access the console Snackbar; throws outside `GcpSnackbarProvider`. */
export function useGcpSnackbar(): GcpSnackbarApi {
  const context = useContext(GcpSnackbarContext)
  if (!context) {
    throw new Error('useGcpSnackbar must be used within GcpSnackbarProvider')
  }
  return context
}
