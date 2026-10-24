import { api } from '../client'

const BASE = '/api/ui/v1/gcp/run'

/** A Cloud Run service summary, flattened across locations for the list. */
export interface RunService {
  id: string
  region: string
  name: string
  uri?: string
  latestReadyRevision?: string
  latestCreatedRevision?: string
  generation?: string
  createTime?: string
  updateTime?: string
  ready: boolean
}

export interface ListServicesResponse {
  services: RunService[]
  total: number
}

/** A full google.cloud.run.v2.Service wire object. */
export type RunServiceDetail = Record<string, unknown>

/** A full google.cloud.run.v2.Revision wire object. */
export type RunRevision = Record<string, unknown>

export interface ListRevisionsResponse {
  revisions: RunRevision[]
  total: number
}

export const listServices = () => api.get<ListServicesResponse>(`${BASE}/services`)

export const getService = (region: string, service: string) =>
  api.get<RunServiceDetail>(
    `${BASE}/services/${encodeURIComponent(region)}/${encodeURIComponent(service)}`,
  )

export const deleteService = (region: string, service: string) =>
  api.delete<void>(
    `${BASE}/services/${encodeURIComponent(region)}/${encodeURIComponent(service)}`,
  )

export const listRevisions = (region: string, service: string) =>
  api.get<ListRevisionsResponse>(
    `${BASE}/services/${encodeURIComponent(region)}/${encodeURIComponent(service)}/revisions`,
  )

export const getRevision = (region: string, service: string, revision: string) =>
  api.get<RunRevision>(
    `${BASE}/services/${encodeURIComponent(region)}/${encodeURIComponent(service)}/revisions/${encodeURIComponent(revision)}`,
  )
