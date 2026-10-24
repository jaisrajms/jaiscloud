import { api } from '../client'
import { fetchAllPages } from '../paging'

const BASE = '/api/ui/v1/gcp/datastore'

/** One element of a Datastore key path: a kind plus at most one of a numeric id
 * (a decimal string, preserving int64 range) or a string name. An element with
 * neither is the incomplete final element of an auto-ID key. */
export interface KeyElement {
  kind: string
  id?: string
  name?: string
}

/** A full Datastore key: ancestor path (root first) plus the partition. */
export interface KeyRef {
  path: KeyElement[]
  namespace?: string
  database?: string
}

/** Datastore's typed value oneof, e.g. `{ stringValue: 'Ada' }`,
 * `{ integerValue: '42' }`, `{ arrayValue: { values: [...] } }`. The console
 * edits this object verbatim so every value type round-trips. */
export type DatastoreValue = Record<string, unknown>

export interface DatastoreEntity {
  key: KeyRef
  kind: string
  properties: Record<string, DatastoreValue>
  version?: string
  updateTime?: string
}

export interface Kind {
  name: string
}

export interface ListKindsResponse {
  kinds: Kind[]
  total: number
}

export interface ListEntitiesResponse {
  entities: DatastoreEntity[]
  total: number
  nextPageToken?: string
}

export interface Property {
  name: string
  representations: string[]
}

export interface ListPropertiesResponse {
  properties: Property[]
  total: number
}

export interface UpsertEntityRequest {
  key: KeyRef
  properties: Record<string, DatastoreValue>
}

export interface GQLQueryRequest {
  queryString: string
  namespace?: string
  database?: string
  allowLiterals?: boolean
}

export interface QueryResponse {
  entities: DatastoreEntity[]
  total: number
  skippedResults?: number
  moreResults?: boolean
}

export function listKinds() {
  return api.get<ListKindsResponse>(`${BASE}/kinds`)
}

/** Drains every entity page; pass `pageToken` for a single raw page. The
 * console's entities endpoint pages at 25 by default, so the drain asks for the
 * handler's maximum (1000) to keep the number of round trips down. */
export async function listEntities(
  kind: string,
  params?: { pageToken?: string },
): Promise<ListEntitiesResponse> {
  const path = `${BASE}/kinds/${encodeURIComponent(kind)}/entities`
  if (params?.pageToken) {
    return api.get<ListEntitiesResponse>(path, { pageToken: params.pageToken })
  }
  const entities = await fetchAllPages(
    (pageToken) =>
      api.get<ListEntitiesResponse>(path, {
        pageSize: 1000,
        ...(pageToken ? { pageToken } : {}),
      }),
    (page) => page.entities,
  )
  return { entities, total: entities.length }
}

export function listProperties(kind: string) {
  return api.get<ListPropertiesResponse>(`${BASE}/kinds/${encodeURIComponent(kind)}/properties`)
}

/** Read an entity by its full key. */
export function getEntity(key: KeyRef) {
  return api.get<DatastoreEntity>(`${BASE}/entity`, { key: JSON.stringify(key) })
}

/** Upsert an entity. An incomplete final key element makes the server allocate
 * a numeric ID. */
export function upsertEntity(req: UpsertEntityRequest) {
  return api.put<DatastoreEntity>(`${BASE}/entity`, req)
}

export function deleteEntity(key: KeyRef) {
  return api.delete<void>(`${BASE}/entity`, { key: JSON.stringify(key) })
}

/** Run a GQL query (e.g. `SELECT * FROM Task`). */
export function runQuery(req: GQLQueryRequest) {
  return api.post<QueryResponse>(`${BASE}/query`, req)
}
