import { api } from './client'

const BASE = '/api/ui/v1/apigateway'

export interface RestAPI {
  id: string
  name: string
  description?: string
  createdDate?: string
}

export interface Resource {
  id: string
  path: string
  pathPart?: string
  parentId?: string
}

export interface Stage {
  name: string
  deploymentId?: string
  description?: string
}

export interface Deployment {
  id: string
  description?: string
  createdDate?: string
}

export interface ListRestAPIsResponse {
  items: RestAPI[]
  total: number
}

export interface ListResourcesResponse {
  items: Resource[]
  total: number
}

export interface ListStagesResponse {
  items: Stage[]
  total: number
}

export interface ListDeploymentsResponse {
  items: Deployment[]
  total: number
}

export function listRestAPIs(): Promise<ListRestAPIsResponse> {
  return api.get(`${BASE}/apis`)
}

export function createRestAPI(req: { name: string; description?: string }): Promise<unknown> {
  return api.post(`${BASE}/apis`, req)
}

export function deleteRestAPI(id: string): Promise<void> {
  return api.delete(`${BASE}/apis/${encodeURIComponent(id)}`)
}

export function listResources(apiId: string): Promise<ListResourcesResponse> {
  return api.get(`${BASE}/apis/${encodeURIComponent(apiId)}/resources`)
}

export function listStages(apiId: string): Promise<ListStagesResponse> {
  return api.get(`${BASE}/apis/${encodeURIComponent(apiId)}/stages`)
}

export function listDeployments(apiId: string): Promise<ListDeploymentsResponse> {
  return api.get(`${BASE}/apis/${encodeURIComponent(apiId)}/deployments`)
}

export function createDeployment(apiId: string, req: { stageName?: string; description?: string }): Promise<unknown> {
  return api.post(`${BASE}/apis/${encodeURIComponent(apiId)}/deployments`, req)
}
