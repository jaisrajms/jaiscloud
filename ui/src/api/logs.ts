import { api } from './client'

export interface LogGroup {
  name: string
  arn: string
  retentionDays?: number
  storedBytes: number
  createdAt: number
}

export interface LogStream {
  name: string
  arn: string
  lastEventAt?: number
  firstEventAt?: number
  uploadSequenceToken?: string
}

export interface LogEvent {
  timestamp: number
  message: string
  ingestionTime: number
}

export interface LogEventsResponse {
  events: LogEvent[]
  nextForwardToken?: string
  nextBackwardToken?: string
  truncated: boolean
}

export interface FilterLogEventsRequest {
  logGroupName: string
  filterPattern?: string
  startTime?: number
  endTime?: number
  nextToken?: string
  limit?: number
}

const BASE = '/api/ui/v1/logs'

export function listLogGroups(params?: { pageSize?: number; nextToken?: string }) {
  return api.get<{ items: LogGroup[]; nextToken?: string }>(`${BASE}/groups`, params as Record<string, string | number>)
}

export function createLogGroup(logGroupName: string) {
  return api.post<void>(`${BASE}/groups`, { logGroupName })
}

export function deleteLogGroup(name: string) {
  return api.delete<void>(`${BASE}/groups/${encodeURIComponent(name)}`)
}

export function setRetention(name: string, retentionDays: number) {
  return api.put<void>(`${BASE}/groups/${encodeURIComponent(name)}/retention`, { retentionDays })
}

export function listLogStreams(groupName: string, params?: { pageSize?: number; nextToken?: string }) {
  return api.get<{ items: LogStream[]; nextToken?: string }>(
    `${BASE}/groups/${encodeURIComponent(groupName)}/streams`,
    params as Record<string, string | number>,
  )
}

export function getLogEvents(groupName: string, streamName: string, params?: { startTime?: number; endTime?: number; nextToken?: string; limit?: number }) {
  return api.get<LogEventsResponse>(
    `${BASE}/groups/${encodeURIComponent(groupName)}/streams/${encodeURIComponent(streamName)}/events`,
    params as Record<string, string | number>,
  )
}

export function filterLogEvents(groupName: string, req: Omit<FilterLogEventsRequest, 'logGroupName'>) {
  return api.post<LogEventsResponse>(`${BASE}/groups/${encodeURIComponent(groupName)}/filter`, {
    ...req,
    logGroupName: groupName,
  })
}
