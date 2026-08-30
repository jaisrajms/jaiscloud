import { api } from './client'

const BASE = '/api/ui/v1/ecs'

export interface ECSCluster { name: string; arn: string; status: string }
export interface ListECSClustersResponse { items: ECSCluster[]; total: number; nextToken?: string }
export interface ECSTask { arn: string; clusterArn: string; status?: string }
export interface ListECSTasksResponse { items: ECSTask[]; total: number }
export interface ECSService { arn: string; status?: string }
export interface ListECSServicesResponse { items: ECSService[]; total: number }

export const listClusters = () => api.get<ListECSClustersResponse>(`${BASE}/clusters`)

export const createCluster = (name: string) =>
  api.post<unknown>(`${BASE}/clusters`, { name })

export const deleteCluster = (name: string) =>
  api.delete<void>(`${BASE}/clusters/${encodeURIComponent(name)}`)

export const listTasks = (clusterName: string) =>
  api.get<ListECSTasksResponse>(`${BASE}/clusters/${encodeURIComponent(clusterName)}/tasks`)

export const runTask = (clusterName: string, taskDefinition: string, count?: number) =>
  api.post<unknown>(`${BASE}/clusters/${encodeURIComponent(clusterName)}/tasks`, { taskDefinition, count })

export const stopTask = (taskArn: string) =>
  api.post<void>(`${BASE}/tasks/${encodeURIComponent(taskArn)}/stop`)

export const listServices = (clusterName: string) =>
  api.get<ListECSServicesResponse>(`${BASE}/clusters/${encodeURIComponent(clusterName)}/services`)
