import { api } from './client'

const BASE = '/api/ui/v1/emr'

export interface ClusterSummary {
  id: string
  name: string
  state: string
  arn?: string
}

export interface ClusterDetail {
  id: string
  name: string
  state: string
  stateChangeReason?: string
  arn?: string
  releaseLabel?: string
  logUri?: string
  autoTerminate: boolean
  terminationProtected: boolean
  applications: string[]
  tags: { key: string; value: string }[]
}

export interface ClusterStatus {
  state: string
  stateChangeReason?: string
  steps: { id: string; name: string; state: string }[]
}

export interface Step {
  id: string
  name: string
  state: string
  config?: Record<string, unknown>
}

export interface ListClustersResponse {
  items: ClusterSummary[]
  total: number
  nextToken?: string
}

export interface ListStepsResponse {
  items: Step[]
  total: number
  nextToken?: string
}

export function listClusters(params?: { state?: string; nextToken?: string }): Promise<ListClustersResponse> {
  const p: Record<string, string> = {}
  if (params?.state) p.state = params.state
  if (params?.nextToken) p.nextToken = params.nextToken
  return api.get(`${BASE}/clusters`, p)
}

export function describeCluster(id: string): Promise<ClusterDetail> {
  return api.get(`${BASE}/clusters/${encodeURIComponent(id)}`)
}

export function runJobFlow(req: {
  name: string
  releaseLabel?: string
  logUri?: string
  keepAlive?: boolean
  serviceRole?: string
  jobFlowRole?: string
}): Promise<unknown> {
  return api.post(`${BASE}/clusters`, req)
}

export function terminateCluster(id: string): Promise<void> {
  return api.delete(`${BASE}/clusters/${encodeURIComponent(id)}`)
}

export function getClusterStatus(id: string): Promise<ClusterStatus> {
  return api.get(`${BASE}/clusters/${encodeURIComponent(id)}/status`)
}

export function listSteps(clusterId: string, params?: { nextToken?: string }): Promise<ListStepsResponse> {
  const p: Record<string, string> = {}
  if (params?.nextToken) p.nextToken = params.nextToken
  return api.get(`${BASE}/clusters/${encodeURIComponent(clusterId)}/steps`, p)
}

export function describeStep(clusterId: string, stepId: string): Promise<Step> {
  return api.get(`${BASE}/clusters/${encodeURIComponent(clusterId)}/steps/${encodeURIComponent(stepId)}`)
}

export function addSteps(clusterId: string, steps: unknown[]): Promise<unknown> {
  return api.post(`${BASE}/clusters/${encodeURIComponent(clusterId)}/steps`, { steps })
}

export function cancelStep(clusterId: string, stepId: string): Promise<void> {
  return api.post(`${BASE}/clusters/${encodeURIComponent(clusterId)}/steps/${encodeURIComponent(stepId)}/cancel`)
}
