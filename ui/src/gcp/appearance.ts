import { useCallback, useState } from 'react'
import { useMediaQuery } from '@mui/material'
import type { GcpDensity, GcpThemeMode } from './theme'

/** Shared with the AWS/Cloudscape shell, so appearance stays consistent. */
export const GCP_MODE_KEY = 'jaiscloud-mode'
export const GCP_DENSITY_KEY = 'jaiscloud-density'

/** `system` follows `prefers-color-scheme`; the rest pin the theme. */
export type GcpModePreference = 'system' | GcpThemeMode

export interface GcpAppearance {
  mode: GcpThemeMode
  preference: GcpModePreference
  setPreference: (preference: GcpModePreference) => void
  density: GcpDensity
  setDensity: (density: GcpDensity) => void
}

/** Pure resolution of the effective light/dark mode. */
export function resolveThemeMode(
  preference: GcpModePreference,
  prefersDark: boolean,
): GcpThemeMode {
  if (preference === 'system') return prefersDark ? 'dark' : 'light'
  return preference
}

function readPreference(): GcpModePreference {
  if (typeof window === 'undefined') return 'system'
  try {
    const raw = window.localStorage.getItem(GCP_MODE_KEY)
    return raw === 'light' || raw === 'dark' ? raw : 'system'
  } catch {
    return 'system'
  }
}

function readDensity(): GcpDensity {
  if (typeof window === 'undefined') return 'comfortable'
  try {
    return window.localStorage.getItem(GCP_DENSITY_KEY) === 'compact' ? 'compact' : 'comfortable'
  } catch {
    return 'comfortable'
  }
}

function persist(key: string, value: string) {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(key, value)
  } catch {
    /* ignore storage errors */
  }
}

/**
 * GCP console appearance: light/dark (system default, user override) and table
 * density. Persisted under the same keys the AWS shell uses.
 */
export function useGcpAppearance(): GcpAppearance {
  const prefersDark = useMediaQuery('(prefers-color-scheme: dark)')
  const [preference, setPreferenceState] = useState<GcpModePreference>(readPreference)
  const [density, setDensityState] = useState<GcpDensity>(readDensity)

  const setPreference = useCallback((next: GcpModePreference) => {
    setPreferenceState(next)
    persist(GCP_MODE_KEY, next)
  }, [])

  const setDensity = useCallback((next: GcpDensity) => {
    setDensityState(next)
    persist(GCP_DENSITY_KEY, next)
  }, [])

  return {
    mode: resolveThemeMode(preference, prefersDark),
    preference,
    setPreference,
    density,
    setDensity,
  }
}
