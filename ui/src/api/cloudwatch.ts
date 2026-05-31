import { api } from './client'

const BASE = '/api/ui/v1/cloudwatch'

export interface CWMetric {
  namespace: string
  metricName: string
}

export interface ListMetricsResponse {
  items: CWMetric[]
  nextToken?: string
  total: number
}

export interface Datapoint {
  timestamp: string
  sum?: number
  average?: number
  minimum?: number
  maximum?: number
  sampleCount?: number
  unit?: string
}

export interface MetricStatisticsResponse {
  label: string
  datapoints: Datapoint[]
}

export interface CWAlarm {
  alarmName: string
  alarmArn?: string
  alarmDescription?: string
  namespace?: string
  metricName?: string
  statistic?: string
  period?: number
  threshold?: number
  comparisonOperator?: string
  evaluationPeriods?: number
  stateValue?: string
  stateReason?: string
  actionsEnabled: boolean
  updatedAt?: string
}

export interface ListAlarmsResponse {
  items: CWAlarm[]
  nextToken?: string
  total: number
}

export interface CWDashboard {
  dashboardName: string
  dashboardArn?: string
  lastModified?: string
  dashboardBody?: string
}

export interface ListDashboardsResponse {
  items: CWDashboard[]
  total: number
}

// Metrics

export function listMetrics(params?: { namespace?: string; metricName?: string; nextToken?: string }): Promise<ListMetricsResponse> {
  return api.get<ListMetricsResponse>(`${BASE}/metrics`, params as Record<string, string>)
}

export function getMetricStatistics(params: {
  namespace: string
  metricName: string
  startTime?: string
  endTime?: string
  period?: number
  statistics?: string[]
}): Promise<MetricStatisticsResponse> {
  const { statistics, ...rest } = params
  const q: Record<string, string | number> = rest as Record<string, string | number>
  if (statistics) {
    q['statistics'] = statistics.join(',')
  }
  return api.get<MetricStatisticsResponse>(`${BASE}/metrics/statistics`, q)
}

// Alarms

export function listAlarms(params?: { stateValue?: string; alarmNamePrefix?: string; nextToken?: string }): Promise<ListAlarmsResponse> {
  return api.get<ListAlarmsResponse>(`${BASE}/alarms`, params as Record<string, string>)
}

export function putAlarm(req: {
  alarmName: string
  alarmDescription?: string
  namespace?: string
  metricName?: string
  statistic?: string
  period?: number
  threshold?: number
  comparisonOperator?: string
  evaluationPeriods?: number
  actionsEnabled?: boolean
}): Promise<void> {
  return api.post<void>(`${BASE}/alarms`, req)
}

export function deleteAlarm(alarmName: string): Promise<void> {
  return api.delete<void>(`${BASE}/alarms`, { alarmName })
}

export function setAlarmState(req: { alarmName: string; stateValue: string; stateReason?: string }): Promise<void> {
  return api.post<void>(`${BASE}/alarms/state`, req)
}

export function enableAlarmActions(alarmNames: string[]): Promise<void> {
  return api.post<void>(`${BASE}/alarms/enable`, { alarmNames })
}

export function disableAlarmActions(alarmNames: string[]): Promise<void> {
  return api.post<void>(`${BASE}/alarms/disable`, { alarmNames })
}

// Dashboards

export function listDashboards(): Promise<ListDashboardsResponse> {
  return api.get<ListDashboardsResponse>(`${BASE}/dashboards`)
}

export function getDashboard(name: string): Promise<CWDashboard> {
  return api.get<CWDashboard>(`${BASE}/dashboards/detail`, { name })
}

export function putDashboard(req: { dashboardName: string; dashboardBody?: string }): Promise<void> {
  return api.put<void>(`${BASE}/dashboards`, req)
}

export function deleteDashboard(name: string): Promise<void> {
  return api.delete<void>(`${BASE}/dashboards`, { name })
}
