import { api } from '../client'
import { fetchAllPages } from '../paging'

const BASE = '/api/ui/v1/gcp/logging'

/** A Cloud Logging log entry. */
export interface LogEntry {
  logName?: string
  severity?: string
  timestamp?: string
  insertId?: string
  resource?: { type?: string; labels?: Record<string, string> }
  labels?: Record<string, string>
  textPayload?: string
  jsonPayload?: Record<string, unknown>
}

export interface ListEntriesResponse {
  entries?: LogEntry[]
  nextPageToken?: string
}

export interface ListLogsResponse {
  logNames?: string[]
  nextPageToken?: string
}

export interface ListEntriesParams {
  filter?: string
  orderBy?: string
  pageSize?: number
  pageToken?: string
}

/** A logs-based metric. */
export interface LogMetric {
  name: string
  description?: string
  filter: string
  disabled?: boolean
  valueExtractor?: string
  labelExtractors?: Record<string, string>
  metricDescriptor?: {
    metricKind?: string
    valueType?: string
    unit?: string
    displayName?: string
    labels?: { key: string; valueType?: string; description?: string }[]
  }
}

export interface ListMetricsResponse {
  metrics?: LogMetric[]
  nextPageToken?: string
}

export interface MetricRequest {
  name: string
  description?: string
  filter: string
  disabled?: boolean
  valueExtractor?: string
  labelExtractors?: Record<string, string>
  metricDescriptor?: LogMetric['metricDescriptor']
}

/** A log router sink. */
export interface LogSink {
  name: string
  resourceName?: string
  destination: string
  filter?: string
  description?: string
  disabled?: boolean
  includeChildren?: boolean
  writerIdentity?: string
  createTime?: string
  updateTime?: string
}

export interface ListSinksResponse {
  sinks?: LogSink[]
  nextPageToken?: string
}

export interface SinkRequest {
  name: string
  destination: string
  filter?: string
  description?: string
  disabled?: boolean
  includeChildren?: boolean
}

/** A resource-level log exclusion. */
export interface LogExclusion {
  name: string
  description?: string
  filter: string
  disabled?: boolean
  createTime?: string
  updateTime?: string
}

export interface ListExclusionsResponse {
  exclusions?: LogExclusion[]
  nextPageToken?: string
}

export interface ExclusionRequest {
  name: string
  description?: string
  filter: string
  disabled?: boolean
}

// ─── entries + logs ───────────────────────────────────────────────────────────

export const listEntries = (params: ListEntriesParams = {}) =>
  api.get<ListEntriesResponse>(`${BASE}/entries`, {
    filter: params.filter ?? '',
    orderBy: params.orderBy ?? '',
    // Never send an empty page size: the GCP query binding rejects
    // `?pageSize=` (an explicitly empty int32), so an unset size must be
    // omitted, not sent as ''.
    ...(params.pageSize !== undefined ? { pageSize: params.pageSize } : {}),
    pageToken: params.pageToken ?? '',
  })

/** List every log name; pass `pageToken` for a single raw page. */
export async function listLogs(params?: { pageToken?: string }): Promise<ListLogsResponse> {
  if (params?.pageToken) {
    return api.get<ListLogsResponse>(`${BASE}/logs`, { pageToken: params.pageToken })
  }
  const logNames = await fetchAllPages(
    (pageToken) =>
      api.get<ListLogsResponse>(`${BASE}/logs`, pageToken ? { pageToken } : undefined),
    (page) => page.logNames ?? [],
  )
  return { logNames }
}

export const deleteLog = (log: string) =>
  api.delete<void>(`${BASE}/logs/${encodeURIComponent(log)}`)

// ─── logs-based metrics ───────────────────────────────────────────────────────

const metricPath = (metric: string) => `${BASE}/metrics/${encodeURIComponent(metric)}`

/** List every logs-based metric; pass `pageToken` for a single raw page. */
export async function listMetrics(params?: { pageToken?: string }): Promise<ListMetricsResponse> {
  if (params?.pageToken) {
    return api.get<ListMetricsResponse>(`${BASE}/metrics`, { pageToken: params.pageToken })
  }
  const metrics = await fetchAllPages(
    (pageToken) =>
      api.get<ListMetricsResponse>(`${BASE}/metrics`, pageToken ? { pageToken } : undefined),
    (page) => page.metrics ?? [],
  )
  return { metrics }
}

export const createMetric = (body: MetricRequest) =>
  api.post<LogMetric>(`${BASE}/metrics`, body)

export const updateMetric = (metric: string, body: MetricRequest) =>
  api.put<LogMetric>(metricPath(metric), body)

export const deleteMetric = (metric: string) => api.delete<void>(metricPath(metric))

// ─── sinks ────────────────────────────────────────────────────────────────────

const sinkPath = (sink: string) => `${BASE}/sinks/${encodeURIComponent(sink)}`

/** List every log router sink; pass `pageToken` for a single raw page. */
export async function listSinks(params?: { pageToken?: string }): Promise<ListSinksResponse> {
  if (params?.pageToken) {
    return api.get<ListSinksResponse>(`${BASE}/sinks`, { pageToken: params.pageToken })
  }
  const sinks = await fetchAllPages(
    (pageToken) =>
      api.get<ListSinksResponse>(`${BASE}/sinks`, pageToken ? { pageToken } : undefined),
    (page) => page.sinks ?? [],
  )
  return { sinks }
}

export const createSink = (body: SinkRequest) => api.post<LogSink>(`${BASE}/sinks`, body)

export const updateSink = (sink: string, body: SinkRequest) =>
  api.patch<LogSink>(sinkPath(sink), body)

export const deleteSink = (sink: string) => api.delete<void>(sinkPath(sink))

// ─── exclusions ───────────────────────────────────────────────────────────────

const exclusionPath = (exclusion: string) => `${BASE}/exclusions/${encodeURIComponent(exclusion)}`

/** List every log exclusion; pass `pageToken` for a single raw page. */
export async function listExclusions(params?: {
  pageToken?: string
}): Promise<ListExclusionsResponse> {
  if (params?.pageToken) {
    return api.get<ListExclusionsResponse>(`${BASE}/exclusions`, { pageToken: params.pageToken })
  }
  const exclusions = await fetchAllPages(
    (pageToken) =>
      api.get<ListExclusionsResponse>(`${BASE}/exclusions`, pageToken ? { pageToken } : undefined),
    (page) => page.exclusions ?? [],
  )
  return { exclusions }
}

export const createExclusion = (body: ExclusionRequest) =>
  api.post<LogExclusion>(`${BASE}/exclusions`, body)

export const updateExclusion = (exclusion: string, body: ExclusionRequest) =>
  api.patch<LogExclusion>(exclusionPath(exclusion), body)

export const deleteExclusion = (exclusion: string) =>
  api.delete<void>(exclusionPath(exclusion))
