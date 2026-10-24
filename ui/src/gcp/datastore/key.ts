import type { DatastoreValue, KeyElement, KeyRef } from '../../api/gcp/datastore'

/** The identifier of a key element: its numeric id or string name, else ''. */
export function keyElementId(element: KeyElement): string {
  return element.id ?? element.name ?? ''
}

/** Display form of a full key path, e.g. `Parent:1/Child:alice`. */
export function keyPathToString(key: KeyRef): string {
  return key.path.map((element) => `${element.kind}:${keyElementId(element)}`).join('/')
}

/** The last path element's identifier (the entity's own id/name). */
export function shortKey(key: KeyRef): string {
  const last = key.path[key.path.length - 1]
  return last ? keyElementId(last) : ''
}

/** A compact human-readable rendering of a single Datastore value. */
export function valueToText(value: DatastoreValue | undefined): string {
  if (!value || typeof value !== 'object') return JSON.stringify(value) ?? '—'
  if ('stringValue' in value) return String(value.stringValue)
  if ('integerValue' in value) return String(value.integerValue)
  if ('doubleValue' in value) return String(value.doubleValue)
  if ('booleanValue' in value) return String(value.booleanValue)
  if ('nullValue' in value) return 'null'
  if ('timestampValue' in value) return String(value.timestampValue)
  if ('keyValue' in value) {
    const key = value.keyValue as KeyRef | undefined
    return key ? keyPathToString(key) : 'key'
  }
  if ('blobValue' in value) return '(blob)'
  if ('geoPointValue' in value) {
    const point = value.geoPointValue as { latitude?: number; longitude?: number } | undefined
    return point ? `${point.latitude},${point.longitude}` : 'geo point'
  }
  if ('entityValue' in value) return '(entity)'
  if ('arrayValue' in value) {
    const array = value.arrayValue as { values?: unknown[] } | undefined
    return `[${array?.values?.length ?? 0}]`
  }
  return JSON.stringify(value)
}

/** One-line summary of an entity's properties for a table cell. */
export function propertiesSummary(properties: Record<string, DatastoreValue>): string {
  const entries = Object.entries(properties)
  if (entries.length === 0) return '(no properties)'
  return entries
    .slice(0, 4)
    .map(([name, value]) => `${name}: ${valueToText(value)}`)
    .join(' · ')
}
