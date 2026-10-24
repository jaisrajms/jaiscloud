import type { KeyRef } from '../../api/gcp/datastore'

/** Route for the entity detail page, carrying the full key as a query param so
 * ancestor paths and partitions survive the trip. */
export function entityHref(key: KeyRef): string {
  return `/gcp/datastore/entity?key=${encodeURIComponent(JSON.stringify(key))}`
}

/** Parse the ?key= query parameter back into a KeyRef, or null when absent or
 * malformed. */
export function parseKeyParam(raw: string | null): KeyRef | null {
  if (!raw) return null
  try {
    const parsed = JSON.parse(raw) as KeyRef
    if (!parsed || !Array.isArray(parsed.path) || parsed.path.length === 0) return null
    return parsed
  } catch {
    return null
  }
}
