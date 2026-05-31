import { api } from './client'

const BASE = '/api/ui/v1/emr-containers'

export interface VirtualCluster {
  id: string
  name: string
  state: string
  eksCluster?: string
  namespace?: string
  arn?: string
}

export interface JobRun {
  id: string
  name: string
  virtualClusterId: string
  state: string
  releaseLabel?: string
  arn?: string
}

export interface ListVirtualClustersResponse {
  items: VirtualCluster[]
  total: number
}

export interface ListJobRunsResponse {
  items: JobRun[]
  total: number
}

export function listVirtualClusters(params?: { state?: string }): Promise<ListVirtualClustersResponse> {
  const p: Record<string, string> = {}
  if (params?.state) p.state = params.state
  return api.get(`${BASE}/virtual-clusters`, p)
}

export function describeVirtualCluster(id: string): Promise<VirtualCluster> {
  return api.get(`${BASE}/virtual-clusters/${encodeURIComponent(id)}`)
}

export function createVirtualCluster(req: {
  name: string
  eksClusterId?: string
  namespace?: string
}): Promise<unknown> {
  return api.post(`${BASE}/virtual-clusters`, req)
}

export function deleteVirtualCluster(id: string): Promise<void> {
  return api.delete(`${BASE}/virtual-clusters/${encodeURIComponent(id)}`)
}

export function listJobRuns(vcId: string): Promise<ListJobRunsResponse> {
  return api.get(`${BASE}/virtual-clusters/${encodeURIComponent(vcId)}/jobs`)
}

export function describeJobRun(vcId: string, jobId: string): Promise<JobRun> {
  return api.get(
    `${BASE}/virtual-clusters/${encodeURIComponent(vcId)}/jobs/${encodeURIComponent(jobId)}`,
  )
}

export function startJobRun(
  vcId: string,
  req: { name: string; releaseLabel?: string; executionRoleArn?: string },
): Promise<unknown> {
  return api.post(
    `${BASE}/virtual-clusters/${encodeURIComponent(vcId)}/jobs`,
    req,
  )
}

export function cancelJobRun(vcId: string, jobId: string): Promise<void> {
  return api.delete(
    `${BASE}/virtual-clusters/${encodeURIComponent(vcId)}/jobs/${encodeURIComponent(jobId)}`,
  )
}
