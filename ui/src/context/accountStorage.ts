/**
 * Persisted console account/project selection.
 *
 * The selected account (an AWS account id or a GCP project id) is a user
 * preference like the theme or the nav state: it must survive a browser
 * refresh, otherwise every reload snaps back to the server's configured
 * default project.
 */

/** Shared with the AWS/Azure shell; every shell uses the same AccountContext. */
export const SELECTED_ACCOUNT_KEY = 'jaiscloud-account'

/** Minimal Storage surface so the helpers are testable without a browser. */
export interface StorageLike {
  getItem(key: string): string | null
  setItem(key: string, value: string): void
}

function defaultStorage(): StorageLike | undefined {
  if (typeof window === 'undefined') return undefined
  try {
    return window.localStorage
  } catch {
    return undefined
  }
}

/** Read the persisted account id, tolerating absent storage or a read error. */
export function readSelectedAccount(storage?: StorageLike): string {
  const store = storage ?? defaultStorage()
  if (!store) return ''
  try {
    return (store.getItem(SELECTED_ACCOUNT_KEY) ?? '').trim()
  } catch {
    return ''
  }
}

/** Persist an explicit selection; an empty id is never written. */
export function persistSelectedAccount(id: string, storage?: StorageLike): void {
  const value = id.trim()
  if (!value) return
  const store = storage ?? defaultStorage()
  if (!store) return
  try {
    store.setItem(SELECTED_ACCOUNT_KEY, value)
  } catch {
    /* ignore storage errors */
  }
}
