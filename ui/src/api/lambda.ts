import { api } from './client'

export interface LambdaFunction {
  name: string
  arn: string
  runtime: string
  handler: string
  roleArn: string
  timeout: number
  memorySize: number
  state: string
  lastModified: string
  description?: string
  envVars?: Record<string, string>
  reservedConcurrency?: number
}

export interface ListFunctionsResponse {
  items: LambdaFunction[]
  nextToken?: string
}

export interface InvokeRequest {
  payload: string
  invocationType: 'RequestResponse' | 'Event' | 'DryRun'
}

export interface InvokeResponse {
  statusCode: number
  functionError?: string
  executedVersion: string
  payload: string
  logResult: string
  billedDurationMs?: number
  requestId?: string
}

export interface UpdateConfigRequest {
  timeout?: number
  memorySize?: number
  handler?: string
  description?: string
  envVars?: Record<string, string>
  roleArn?: string
}

const BASE = '/api/ui/v1/lambda'

export function listFunctions(params?: { nextToken?: string }): Promise<ListFunctionsResponse> {
  return api.get<ListFunctionsResponse>(`${BASE}/functions`, params as Record<string, string | number>)
}

export function getFunction(name: string): Promise<LambdaFunction> {
  return api.get<LambdaFunction>(`${BASE}/functions/${encodeURIComponent(name)}`)
}

export function getFunctionStatus(name: string): Promise<{ state: string }> {
  return api.get<{ state: string }>(`${BASE}/functions/${encodeURIComponent(name)}/status`)
}

export function deleteFunction(name: string): Promise<void> {
  return api.delete<void>(`${BASE}/functions/${encodeURIComponent(name)}`)
}

export function invokeFunction(name: string, req: InvokeRequest): Promise<InvokeResponse> {
  return api.post<InvokeResponse>(`${BASE}/functions/${encodeURIComponent(name)}/invoke`, req)
}

export function updateFunctionConfig(name: string, req: UpdateConfigRequest): Promise<LambdaFunction> {
  return api.patch<LambdaFunction>(`${BASE}/functions/${encodeURIComponent(name)}/config`, req)
}
