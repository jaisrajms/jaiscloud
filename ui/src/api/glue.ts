import { api } from './client'

const BASE = '/api/ui/v1/glue'

export interface Database {
  name: string
  description?: string
  locationUri?: string
}

export interface Table {
  name: string
  databaseName: string
  description?: string
  storageType?: string
  location?: string
}

export interface Job {
  name: string
  role?: string
  command?: string
}

export interface JobRun {
  id: string
  jobName: string
  state: string
  startedOn?: string
  completedOn?: string
}

export interface Crawler {
  name: string
  role?: string
  state?: string
}

export interface ListDatabasesResponse {
  items: Database[]
  total: number
  nextToken?: string
}

export interface ListTablesResponse {
  items: Table[]
  total: number
  nextToken?: string
}

export interface ListJobsResponse {
  items: Job[]
  total: number
  nextToken?: string
}

export interface ListJobRunsResponse {
  items: JobRun[]
  total: number
  nextToken?: string
}

export interface ListCrawlersResponse {
  items: Crawler[]
  total: number
  nextToken?: string
}

export function listDatabases(params?: { nextToken?: string }): Promise<ListDatabasesResponse> {
  const p: Record<string, string> = {}
  if (params?.nextToken) p.nextToken = params.nextToken
  return api.get(`${BASE}/databases`, p)
}

export function createDatabase(req: { name: string; description?: string; locationUri?: string }): Promise<unknown> {
  return api.post(`${BASE}/databases`, req)
}

export function deleteDatabase(name: string): Promise<void> {
  return api.delete(`${BASE}/databases/${encodeURIComponent(name)}`)
}

export function listTables(db: string, params?: { nextToken?: string }): Promise<ListTablesResponse> {
  const p: Record<string, string> = {}
  if (params?.nextToken) p.nextToken = params.nextToken
  return api.get(`${BASE}/databases/${encodeURIComponent(db)}/tables`, p)
}

export function createTable(
  db: string,
  req: { name: string; description?: string; location?: string; storageType?: string },
): Promise<unknown> {
  return api.post(`${BASE}/databases/${encodeURIComponent(db)}/tables`, req)
}

export function deleteTable(db: string, name: string): Promise<void> {
  return api.delete(`${BASE}/databases/${encodeURIComponent(db)}/tables/${encodeURIComponent(name)}`)
}

export function listJobs(params?: { nextToken?: string }): Promise<ListJobsResponse> {
  const p: Record<string, string> = {}
  if (params?.nextToken) p.nextToken = params.nextToken
  return api.get(`${BASE}/jobs`, p)
}

export function createJob(req: { name: string; role?: string; command?: string }): Promise<unknown> {
  return api.post(`${BASE}/jobs`, req)
}

export function deleteJob(name: string): Promise<void> {
  return api.delete(`${BASE}/jobs/${encodeURIComponent(name)}`)
}

export function startJobRun(jobName: string): Promise<unknown> {
  return api.post(`${BASE}/jobs/${encodeURIComponent(jobName)}/runs`)
}

export function listJobRuns(jobName: string, params?: { nextToken?: string }): Promise<ListJobRunsResponse> {
  const p: Record<string, string> = {}
  if (params?.nextToken) p.nextToken = params.nextToken
  return api.get(`${BASE}/jobs/${encodeURIComponent(jobName)}/runs`, p)
}

export function listCrawlers(params?: { nextToken?: string }): Promise<ListCrawlersResponse> {
  const p: Record<string, string> = {}
  if (params?.nextToken) p.nextToken = params.nextToken
  return api.get(`${BASE}/crawlers`, p)
}

export function createCrawler(req: {
  name: string
  role?: string
  databaseName?: string
  s3Targets?: string[]
}): Promise<unknown> {
  return api.post(`${BASE}/crawlers`, req)
}

export function deleteCrawler(name: string): Promise<void> {
  return api.delete(`${BASE}/crawlers/${encodeURIComponent(name)}`)
}

export function startCrawler(name: string): Promise<void> {
  return api.post(`${BASE}/crawlers/${encodeURIComponent(name)}/start`)
}
