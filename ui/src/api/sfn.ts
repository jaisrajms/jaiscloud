import { api } from './client'

const BASE = '/api/ui/v1/sfn'

export interface StateMachine {
  arn: string
  name: string
  type?: string
  status?: string
  definition?: string
  roleArn?: string
  createdAt?: number
}

export interface Execution {
  arn: string
  name: string
  stateMachineArn: string
  status: string
  startDate?: number
  stopDate?: number
  input?: string
  output?: string
}

export interface HistoryEvent {
  id: number
  type: string
  timestamp: number
}

export interface ListStateMachinesResponse {
  items: StateMachine[]
  total: number
  nextToken?: string
}

export interface ListExecutionsResponse {
  items: Execution[]
  total: number
  nextToken?: string
}

export interface ExecutionHistoryResponse {
  events: HistoryEvent[]
}

export function listStateMachines(params?: { nextToken?: string }): Promise<ListStateMachinesResponse> {
  const p: Record<string, string> = {}
  if (params?.nextToken) p.nextToken = params.nextToken
  return api.get(`${BASE}/state-machines`, p)
}

export function createStateMachine(req: {
  name: string
  definition?: string
  roleArn?: string
  type?: string
}): Promise<unknown> {
  return api.post(`${BASE}/state-machines`, req)
}

export function deleteStateMachine(arn: string): Promise<void> {
  return api.delete(`${BASE}/state-machines/${encodeURIComponent(arn)}`)
}

export function startExecution(arn: string, req: { name?: string; input?: string }): Promise<unknown> {
  return api.post(`${BASE}/state-machines/${encodeURIComponent(arn)}/executions`, req)
}

export function listExecutions(arn: string, params?: { statusFilter?: string }): Promise<ListExecutionsResponse> {
  const p: Record<string, string> = {}
  if (params?.statusFilter) p.statusFilter = params.statusFilter
  return api.get(`${BASE}/state-machines/${encodeURIComponent(arn)}/executions`, p)
}

export function stopExecution(arn: string): Promise<void> {
  return api.post(`${BASE}/executions/${encodeURIComponent(arn)}/stop`)
}

export function getExecutionHistory(arn: string): Promise<ExecutionHistoryResponse> {
  return api.get(`${BASE}/executions/${encodeURIComponent(arn)}/history`)
}
