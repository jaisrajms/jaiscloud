import { api } from './client'

export interface TableSummary {
  name: string
  status: string
  itemCount: number
  sizeBytes: number
  createdAt?: string
  billingMode: string
  arn?: string
}

export interface ListTablesResponse {
  items: TableSummary[]
  lastTableName?: string
  total: number
}

export interface KeySchemaElement {
  attributeName: string
  keyType: 'HASH' | 'RANGE'
}

export interface AttributeDefinition {
  attributeName: string
  attributeType: 'S' | 'N' | 'B'
}

export interface CreateTableRequest {
  tableName: string
  keySchema: KeySchemaElement[]
  attributeDefinitions: AttributeDefinition[]
  billingMode: 'PAY_PER_REQUEST' | 'PROVISIONED'
  readCapacity?: number
  writeCapacity?: number
}

export interface ScanResponse {
  items: Record<string, unknown>[]
  count: number
  scannedCount: number
  lastEvaluatedKey?: Record<string, unknown>
}

const BASE = '/api/ui/v1/dynamodb'

export function listTables(params?: { limit?: number; nextToken?: string }): Promise<ListTablesResponse> {
  return api.get<ListTablesResponse>(`${BASE}/tables`, params as Record<string, string | number>)
}

export function createTable(req: CreateTableRequest): Promise<unknown> {
  return api.post<unknown>(`${BASE}/tables`, req)
}

export function describeTable(name: string): Promise<unknown> {
  return api.get<unknown>(`${BASE}/tables/${encodeURIComponent(name)}`)
}

export function deleteTable(name: string): Promise<void> {
  return api.delete<void>(`${BASE}/tables/${encodeURIComponent(name)}`)
}

export function scanTable(
  name: string,
  params?: { limit?: number; nextToken?: string },
): Promise<ScanResponse> {
  const qp: Record<string, string | number> = {}
  if (params?.limit) qp.limit = params.limit
  if (params?.nextToken) qp.nextToken = params.nextToken
  return api.get<ScanResponse>(`${BASE}/tables/${encodeURIComponent(name)}/scan`, qp)
}

export function putItem(table: string, item: Record<string, unknown>): Promise<void> {
  return api.post<void>(`${BASE}/tables/${encodeURIComponent(table)}/items`, { item })
}

export function deleteItem(table: string, key: Record<string, unknown>): Promise<void> {
  return fetch(`${BASE}/tables/${encodeURIComponent(table)}/items`, {
    method: 'DELETE',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include',
    body: JSON.stringify({ key }),
  }).then((r) => {
    if (!r.ok && r.status !== 204) throw new Error('delete item failed')
  })
}
