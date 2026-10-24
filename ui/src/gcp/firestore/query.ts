/**
 * Firestore StructuredQuery builder helpers.
 *
 * The console offers two ways to build a query: a structured form (collection
 * + filters + order + limit) and a raw JSON editor. Both produce the same
 * `structuredQuery` object, which the UI API forwards verbatim to the core
 * `RunQuery`. Values typed into the form are JSON-parsed when possible and
 * wrapped in Firestore's typed value encoding (`{ stringValue }`,
 * `{ integerValue }`, …) so the query matches the document encoding the rest of
 * the console uses.
 */

import { pathSegments } from './util'

/** Field-filter operators as they appear in a Firestore StructuredQuery. */
export type FilterOperator =
  | 'EQUAL'
  | 'NOT_EQUAL'
  | 'LESS_THAN'
  | 'LESS_THAN_OR_EQUAL'
  | 'GREATER_THAN'
  | 'GREATER_THAN_OR_EQUAL'
  | 'IN'
  | 'NOT_IN'
  | 'ARRAY_CONTAINS'
  | 'ARRAY_CONTAINS_ANY'

/** Operators that take a list value. */
export const LIST_OPERATORS: FilterOperator[] = ['IN', 'NOT_IN', 'ARRAY_CONTAINS_ANY']

export const FILTER_OPERATORS: { value: FilterOperator; label: string }[] = [
  { value: 'EQUAL', label: '==' },
  { value: 'NOT_EQUAL', label: '!=' },
  { value: 'LESS_THAN', label: '<' },
  { value: 'LESS_THAN_OR_EQUAL', label: '<=' },
  { value: 'GREATER_THAN', label: '>' },
  { value: 'GREATER_THAN_OR_EQUAL', label: '>=' },
  { value: 'IN', label: 'in' },
  { value: 'NOT_IN', label: 'not-in' },
  { value: 'ARRAY_CONTAINS', label: 'array-contains' },
  { value: 'ARRAY_CONTAINS_ANY', label: 'array-contains-any' },
]

export type SortDirection = 'ASCENDING' | 'DESCENDING'

export interface QueryFilter {
  field: string
  operator: FilterOperator
  /** Raw editor text, parsed as JSON with a plain-string fallback. */
  value: string
}

export interface QueryOrder {
  field: string
  direction: SortDirection
}

/** The structured builder's state. */
export interface QuerySpec {
  collectionId: string
  /** Collection-group scope (`allDescendants`). */
  allDescendants: boolean
  filters: QueryFilter[]
  orderBy: QueryOrder[]
  limit: string
  offset: string
}

/**
 * toFirestoreValue wraps a JavaScript value in Firestore's typed value encoding
 * — the same encoding used by the document editor. Integers become the exact
 * decimal string an `integerValue` requires; non-integers become `doubleValue`.
 * Timestamps, geo points and bytes have no JavaScript equivalent in the form
 * and must be supplied through the raw JSON editor.
 */
export function toFirestoreValue(value: unknown): Record<string, unknown> {
  if (value === null) return { nullValue: null }
  if (Array.isArray(value)) {
    return { arrayValue: { values: value.map(toFirestoreValue) } }
  }
  switch (typeof value) {
    case 'boolean':
      return { booleanValue: value }
    case 'number':
      if (!Number.isFinite(value)) return { doubleValue: String(value) }
      return Number.isInteger(value) ? { integerValue: String(value) } : { doubleValue: value }
    case 'string':
      return { stringValue: value }
    case 'object':
      return {
        mapValue: {
          fields: Object.fromEntries(
            Object.entries(value as Record<string, unknown>).map(([key, v]) => [
              key,
              toFirestoreValue(v),
            ]),
          ),
        },
      }
    default:
      return { stringValue: String(value) }
  }
}

/**
 * parseQueryValue parses a filter value from editor text. Valid JSON is used as
 * written (so `42` is a number and `"Ada"` a string); a bare word like `Ada` is
 * taken as the string `"Ada"`.
 */
export function parseQueryValue(text: string): unknown {
  const trimmed = text.trim()
  try {
    return JSON.parse(trimmed)
  } catch {
    return text
  }
}

/**
 * collectionQueryTarget turns a collection path into the builder's collection
 * id + parent scope, so deep-linking from a collection pre-fills the query. A
 * root collection (`users`) yields scope `''`; a nested collection
 * (`users/alice/orders`) yields scope `users/alice` and id `orders`. A document
 * path (even segment count) or an empty path yields null.
 */
export function collectionQueryTarget(
  path: string,
): { collectionId: string; scope: string } | null {
  const segments = pathSegments(path)
  if (segments.length === 0 || segments.length % 2 === 0) return null
  return {
    collectionId: segments[segments.length - 1] as string,
    scope: segments.slice(0, -1).join('/'),
  }
}

/**
 * firestoreValueToText decodes one field's Firestore typed value into a
 * human-readable string for the results view. `integerValue` stays the exact
 * decimal string (no JS number coercion, which would lose int64 precision);
 * unknown shapes fall back to raw JSON.
 */
export function firestoreValueToText(value: unknown): string {
  if (value === null || typeof value !== 'object') return String(value)
  const v = value as Record<string, unknown>
  if ('stringValue' in v) return String(v.stringValue)
  if ('integerValue' in v) return String(v.integerValue)
  if ('doubleValue' in v) return String(v.doubleValue)
  if ('booleanValue' in v) return String(v.booleanValue)
  if ('nullValue' in v) return 'null'
  if ('timestampValue' in v) return String(v.timestampValue)
  if ('bytesValue' in v) return `${String(v.bytesValue)} (bytes)`
  if ('referenceValue' in v) return String(v.referenceValue)
  if ('geoPointValue' in v) {
    const g = v.geoPointValue as Record<string, unknown> | undefined
    return g ? `${g.latitude}, ${g.longitude}` : ''
  }
  if ('arrayValue' in v) {
    const a = v.arrayValue as { values?: unknown[] } | undefined
    return `[${(a?.values ?? []).map(firestoreValueToText).join(', ')}]`
  }
  if ('mapValue' in v) {
    const m = v.mapValue as { fields?: Record<string, unknown> } | undefined
    const fields = m?.fields ?? {}
    return `{${Object.entries(fields)
      .map(([key, val]) => `${key}: ${firestoreValueToText(val)}`)
      .join(', ')}}`
  }
  return JSON.stringify(value)
}

/**
 * scopeDocumentTarget splits a parent scope (`cities/SF`) into the collection
 * path + document id that `listSubcollections` needs, so the query builder can
 * suggest the subcollection ids available under the scope. Returns null for an
 * empty scope, a collection path (odd segments) or a path too short to be a
 * document.
 */
export function scopeDocumentTarget(
  scope: string,
): { collection: string; document: string } | null {
  const segments = pathSegments(scope)
  if (segments.length < 2 || segments.length % 2 !== 0) return null
  return {
    collection: segments.slice(0, -1).join('/'),
    document: segments[segments.length - 1] as string,
  }
}

/** buildStructuredQuery converts the builder state into a Firestore
 * StructuredQuery, or returns a human-readable error. */
export function buildStructuredQuery(spec: QuerySpec): {
  structuredQuery?: Record<string, unknown>
  error?: string
} {
  const collectionId = spec.collectionId.trim()
  if (!collectionId) return { error: 'Collection ID is required' }
  if (collectionId.includes('/')) {
    return { error: 'Collection ID must be a single segment; set the parent scope for subcollections' }
  }

  const filters: Record<string, unknown>[] = []
  for (const filter of spec.filters) {
    const field = filter.field.trim()
    if (!field) continue
    if (!filter.value.trim()) {
      return { error: `Filter on "${field}" needs a value` }
    }
    filters.push({
      fieldFilter: {
        field: { fieldPath: field },
        op: filter.operator,
        value: toFirestoreValue(parseQueryValue(filter.value)),
      },
    })
  }

  const orderBy: Record<string, unknown>[] = []
  for (const order of spec.orderBy) {
    const field = order.field.trim()
    if (!field) continue
    orderBy.push({ field: { fieldPath: field }, direction: order.direction })
  }

  const limit = positiveInt(spec.limit, 'Limit')
  if (limit.error) return { error: limit.error }
  const offset = positiveInt(spec.offset, 'Offset')
  if (offset.error) return { error: offset.error }

  const from: Record<string, unknown> = { collectionId }
  if (spec.allDescendants) from.allDescendants = true

  const structuredQuery: Record<string, unknown> = { from: [from] }
  if (filters.length > 0) {
    structuredQuery.where = { compositeFilter: { op: 'AND', filters } }
  }
  if (orderBy.length > 0) structuredQuery.orderBy = orderBy
  if (limit.value !== undefined) structuredQuery.limit = limit.value
  if (offset.value !== undefined) structuredQuery.offset = offset.value
  return { structuredQuery }
}

function positiveInt(
  text: string,
  label: string,
): { value?: number; error?: string } {
  const trimmed = text.trim()
  if (!trimmed) return {}
  const value = Number(trimmed)
  if (!Number.isInteger(value) || value < 0) {
    return { error: `${label} must be a non-negative integer` }
  }
  return { value }
}
