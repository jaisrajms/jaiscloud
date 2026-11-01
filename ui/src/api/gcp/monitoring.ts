import { api } from '../client'
import { fetchAllPages } from '../paging'

const BASE = '/api/ui/v1/gcp/monitoring'

/** A Cloud Monitoring metric descriptor. */
export interface MetricDescriptor {
  name?: string
  type: string
  metricKind?: string
  valueType?: string
  unit?: string
  description?: string
  displayName?: string
  monitoredResourceTypes?: string[]
  labels?: { key: string; valueType?: string; description?: string }[]
}

export interface ListMetricDescriptorsResponse {
  metricDescriptors?: MetricDescriptor[]
  nextPageToken?: string
}

export interface TypedValue {
  boolValue?: boolean
  int64Value?: string
  doubleValue?: number
  stringValue?: string
  distributionValue?: Record<string, unknown>
}

export interface Point {
  interval?: { startTime?: string; endTime?: string }
  value?: TypedValue
}

/** A time series: points identified by metric + monitored resource. */
export interface TimeSeries {
  metric?: { type?: string; labels?: Record<string, string> }
  resource?: { type?: string; labels?: Record<string, string> }
  metricKind?: string
  valueType?: string
  unit?: string
  points?: Point[]
}

export interface ListTimeSeriesResponse {
  timeSeries?: TimeSeries[]
  nextPageToken?: string
}

export interface ListTimeSeriesParams {
  filter?: string
  startTime?: string
  endTime?: string
  pageSize?: number
  pageToken?: string
}

/** An alerting policy. */
export interface AlertPolicy {
  name: string
  displayName?: string
  documentation?: Record<string, unknown>
  conditions?: Record<string, unknown>[]
  combiner?: string
  enabled?: boolean
  notificationChannels?: string[]
  userLabels?: Record<string, string>
}

export interface ListAlertPoliciesResponse {
  alertPolicies?: AlertPolicy[]
  totalSize?: number
  nextPageToken?: string
}

export interface AlertPolicyRequest {
  displayName: string
  documentation?: Record<string, unknown>
  conditions?: Record<string, unknown>[]
  combiner?: string
  enabled?: boolean
  notificationChannels?: string[]
  userLabels?: Record<string, string>
}

/** A notification channel. */
export interface NotificationChannel {
  name: string
  type?: string
  displayName?: string
  description?: string
  labels?: Record<string, string>
  userLabels?: Record<string, string>
  enabled?: boolean
  verificationStatus?: string
}

export interface ListNotificationChannelsResponse {
  notificationChannels?: NotificationChannel[]
  totalSize?: number
  nextPageToken?: string
}

export interface NotificationChannelRequest {
  type: string
  displayName: string
  description?: string
  labels?: Record<string, string>
  userLabels?: Record<string, string>
  enabled?: boolean
}

export interface NotificationChannelDescriptor {
  name?: string
  type: string
  displayName?: string
  description?: string
  labels?: { key: string; valueType?: string; description?: string }[]
}

export interface ListNotificationChannelDescriptorsResponse {
  channelDescriptors?: NotificationChannelDescriptor[]
  nextPageToken?: string
}

// ─── metrics ──────────────────────────────────────────────────────────────────

/** List every metric descriptor; pass `pageToken` for a single raw page. */
export async function listMetricDescriptors(
  filter = '',
  params?: { pageToken?: string },
): Promise<ListMetricDescriptorsResponse> {
  if (params?.pageToken) {
    return api.get<ListMetricDescriptorsResponse>(`${BASE}/metricDescriptors`, {
      filter,
      pageToken: params.pageToken,
    })
  }
  const metricDescriptors = await fetchAllPages(
    (pageToken) =>
      api.get<ListMetricDescriptorsResponse>(`${BASE}/metricDescriptors`, {
        filter,
        ...(pageToken ? { pageToken } : {}),
      }),
    (page) => page.metricDescriptors ?? [],
  )
  return { metricDescriptors }
}

export const listTimeSeries = (params: ListTimeSeriesParams = {}) =>
  api.get<ListTimeSeriesResponse>(`${BASE}/timeSeries`, {
    filter: params.filter ?? '',
    startTime: params.startTime ?? '',
    endTime: params.endTime ?? '',
    // Never send an empty page size: the GCP query binding rejects
    // `?pageSize=` (an explicitly empty int32), so an unset size must be
    // omitted, not sent as ''.
    ...(params.pageSize !== undefined ? { pageSize: params.pageSize } : {}),
    pageToken: params.pageToken ?? '',
  })

// ─── alerting ─────────────────────────────────────────────────────────────────

const policyPath = (id: string) => `${BASE}/alertPolicies/${encodeURIComponent(id)}`

/** List every alerting policy; pass `pageToken` for a single raw page. */
export async function listAlertPolicies(params?: {
  pageToken?: string
}): Promise<ListAlertPoliciesResponse> {
  if (params?.pageToken) {
    return api.get<ListAlertPoliciesResponse>(`${BASE}/alertPolicies`, {
      pageToken: params.pageToken,
    })
  }
  const alertPolicies = await fetchAllPages(
    (pageToken) =>
      api.get<ListAlertPoliciesResponse>(
        `${BASE}/alertPolicies`,
        pageToken ? { pageToken } : undefined,
      ),
    (page) => page.alertPolicies ?? [],
  )
  return { alertPolicies, totalSize: alertPolicies.length }
}

export const createAlertPolicy = (body: AlertPolicyRequest) =>
  api.post<AlertPolicy>(`${BASE}/alertPolicies`, body)

export const updateAlertPolicy = (id: string, body: AlertPolicyRequest) =>
  api.patch<AlertPolicy>(policyPath(id), body)

export const deleteAlertPolicy = (id: string) => api.delete<void>(policyPath(id))

// ─── notification channels ────────────────────────────────────────────────────

const channelPath = (id: string) => `${BASE}/notificationChannels/${encodeURIComponent(id)}`

/** List every notification channel; pass `pageToken` for a single raw page. */
export async function listNotificationChannels(params?: {
  pageToken?: string
}): Promise<ListNotificationChannelsResponse> {
  if (params?.pageToken) {
    return api.get<ListNotificationChannelsResponse>(`${BASE}/notificationChannels`, {
      pageToken: params.pageToken,
    })
  }
  const notificationChannels = await fetchAllPages(
    (pageToken) =>
      api.get<ListNotificationChannelsResponse>(
        `${BASE}/notificationChannels`,
        pageToken ? { pageToken } : undefined,
      ),
    (page) => page.notificationChannels ?? [],
  )
  return { notificationChannels, totalSize: notificationChannels.length }
}

export const createNotificationChannel = (body: NotificationChannelRequest) =>
  api.post<NotificationChannel>(`${BASE}/notificationChannels`, body)

export const updateNotificationChannel = (id: string, body: NotificationChannelRequest) =>
  api.patch<NotificationChannel>(channelPath(id), body)

export const deleteNotificationChannel = (id: string) =>
  api.delete<void>(channelPath(id))

/** List every notification channel descriptor; pass `pageToken` for one page. */
export async function listNotificationChannelDescriptors(params?: {
  pageToken?: string
}): Promise<ListNotificationChannelDescriptorsResponse> {
  const path = `${BASE}/notificationChannelDescriptors`
  if (params?.pageToken) {
    return api.get<ListNotificationChannelDescriptorsResponse>(path, {
      pageToken: params.pageToken,
    })
  }
  const channelDescriptors = await fetchAllPages(
    (pageToken) =>
      api.get<ListNotificationChannelDescriptorsResponse>(
        path,
        pageToken ? { pageToken } : undefined,
      ),
    (page) => page.channelDescriptors ?? [],
  )
  return { channelDescriptors }
}

// ─── service monitoring: services ─────────────────────────────────────────────

/** A Service Monitoring service. Exactly one identifier field is set (or
 * `basicService` identifies a basic service instead). */
export interface Service {
  name: string
  displayName?: string
  userLabels?: Record<string, string>
  custom?: Record<string, unknown>
  appEngine?: Record<string, unknown>
  cloudEndpoints?: Record<string, unknown>
  clusterIstio?: Record<string, unknown>
  meshIstio?: Record<string, unknown>
  istioCanonicalService?: Record<string, unknown>
  cloudRun?: Record<string, unknown>
  gkeNamespace?: Record<string, unknown>
  gkeWorkload?: Record<string, unknown>
  gkeService?: Record<string, unknown>
  basicService?: Record<string, unknown>
  telemetry?: Record<string, unknown>
}

export interface ListServicesResponse {
  services?: Service[]
  totalSize?: number
  nextPageToken?: string
}

/** The create/update body for a Service Monitoring service. */
export interface ServiceWriteInput {
  displayName?: string
  userLabels?: Record<string, string>
  custom?: Record<string, unknown>
  appEngine?: Record<string, unknown>
  cloudEndpoints?: Record<string, unknown>
  clusterIstio?: Record<string, unknown>
  meshIstio?: Record<string, unknown>
  istioCanonicalService?: Record<string, unknown>
  cloudRun?: Record<string, unknown>
  gkeNamespace?: Record<string, unknown>
  gkeWorkload?: Record<string, unknown>
  gkeService?: Record<string, unknown>
  basicService?: Record<string, unknown>
  telemetry?: Record<string, unknown>
}

const servicePath = (service: string) => `${BASE}/services/${encodeURIComponent(service)}`

/** List every Service Monitoring service; pass `pageToken` for one raw page. */
export async function listServices(params?: {
  filter?: string
  pageToken?: string
}): Promise<ListServicesResponse> {
  const query = (pageToken?: string) => ({
    ...(params?.filter ? { filter: params.filter } : {}),
    ...(pageToken ? { pageToken } : {}),
  })
  if (params?.pageToken) {
    return api.get<ListServicesResponse>(`${BASE}/services`, query(params.pageToken))
  }
  const services = await fetchAllPages(
    (pageToken) => api.get<ListServicesResponse>(`${BASE}/services`, query(pageToken)),
    (page) => page.services ?? [],
  )
  return { services, totalSize: services.length }
}

export const getService = (service: string) => api.get<Service>(servicePath(service))

export const createService = (body: ServiceWriteInput, serviceId?: string) =>
  api.post<Service>(`${BASE}/services`, body, serviceId ? { serviceId } : undefined)

export const updateService = (service: string, body: ServiceWriteInput) =>
  api.patch<Service>(servicePath(service), body)

export const deleteService = (service: string) => api.delete<void>(servicePath(service))

// ─── service monitoring: service level objectives ─────────────────────────────

/** A basic SLI: availability or latency, optionally narrowed by method. */
export interface BasicSli {
  method?: string[]
  location?: string[]
  version?: string[]
  availability?: Record<string, never>
  latency?: { threshold?: string }
}

/** The service level indicator of an SLO. The form builds a `basicSli`; an SLO
 * created elsewhere may carry a request- or windows-based indicator instead. */
export interface ServiceLevelIndicator {
  basicSli?: BasicSli
  requestBased?: Record<string, unknown>
  windowsBased?: Record<string, unknown>
}

/** A Service Monitoring service level objective. */
export interface ServiceLevelObjective {
  name: string
  displayName?: string
  goal?: number
  serviceLevelIndicator?: ServiceLevelIndicator
  /** Proto duration string, e.g. "86400s". Mutually exclusive with
   * `calendarPeriod`. */
  rollingPeriod?: string
  /** DAY, WEEK, FORTNIGHT or MONTH. Mutually exclusive with `rollingPeriod`. */
  calendarPeriod?: string
  userLabels?: Record<string, string>
}

export interface ListServiceLevelObjectivesResponse {
  serviceLevelObjectives?: ServiceLevelObjective[]
  totalSize?: number
  nextPageToken?: string
}

/** The create/update body for an SLO. Exactly one of `rollingPeriod` and
 * `calendarPeriod` must be set, matching the Core's validation. */
export interface ServiceLevelObjectiveWriteInput {
  displayName?: string
  goal: number
  serviceLevelIndicator: ServiceLevelIndicator
  rollingPeriod?: string
  calendarPeriod?: string
  userLabels?: Record<string, string>
}

const sloPath = (service: string, slo: string) =>
  `${servicePath(service)}/serviceLevelObjectives/${encodeURIComponent(slo)}`

/** List every SLO of a service; pass `pageToken` for one raw page. */
export async function listServiceLevelObjectives(
  service: string,
  params?: { filter?: string; pageToken?: string },
): Promise<ListServiceLevelObjectivesResponse> {
  const path = `${servicePath(service)}/serviceLevelObjectives`
  const query = (pageToken?: string) => ({
    ...(params?.filter ? { filter: params.filter } : {}),
    ...(pageToken ? { pageToken } : {}),
  })
  if (params?.pageToken) {
    return api.get<ListServiceLevelObjectivesResponse>(path, query(params.pageToken))
  }
  const serviceLevelObjectives = await fetchAllPages(
    (pageToken) => api.get<ListServiceLevelObjectivesResponse>(path, query(pageToken)),
    (page) => page.serviceLevelObjectives ?? [],
  )
  return { serviceLevelObjectives, totalSize: serviceLevelObjectives.length }
}

export const getServiceLevelObjective = (service: string, slo: string) =>
  api.get<ServiceLevelObjective>(sloPath(service, slo))

export const createServiceLevelObjective = (
  service: string,
  body: ServiceLevelObjectiveWriteInput,
  serviceLevelObjectiveId?: string,
) =>
  api.post<ServiceLevelObjective>(
    `${servicePath(service)}/serviceLevelObjectives`,
    body,
    serviceLevelObjectiveId ? { serviceLevelObjectiveId } : undefined,
  )

export const updateServiceLevelObjective = (
  service: string,
  slo: string,
  body: ServiceLevelObjectiveWriteInput,
) => api.patch<ServiceLevelObjective>(sloPath(service, slo), body)

export const deleteServiceLevelObjective = (service: string, slo: string) =>
  api.delete<void>(sloPath(service, slo))
