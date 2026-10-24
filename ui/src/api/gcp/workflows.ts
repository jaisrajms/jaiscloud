import { api } from '../client'
import { fetchAllPages } from '../paging'

const BASE = '/api/ui/v1/gcp/workflows'

/** A Cloud Workflows workflow, flattened across locations for the list. */
export interface Workflow {
  id: string
  location: string
  name: string
  description?: string
  state?: string
  revisionId?: string
  serviceAccount?: string
  sourceContents?: string
  callLogLevel?: string
  labels?: Record<string, string>
  userEnvVars?: Record<string, string>
  createTime?: string
  updateTime?: string
}

export interface ListWorkflowsResponse {
  workflows: Workflow[]
  total: number
}

/** The create form body. Location and id are required. */
export interface WorkflowInput {
  id: string
  location: string
  description: string
  serviceAccount: string
  sourceContents: string
  callLogLevel: string
  labels?: Record<string, string>
  userEnvVars?: Record<string, string>
}

/** The update form body. UpdateMask is optional (the server applies the form fields). */
export interface WorkflowUpdateInput {
  description: string
  serviceAccount: string
  sourceContents: string
  callLogLevel: string
  labels?: Record<string, string>
  userEnvVars?: Record<string, string>
  updateMask?: string
}

export interface ExecutionError {
  payload: string
  context?: string
}

export interface Step {
  routine?: string
  step?: string
}

/** A workflow execution. */
export interface Execution {
  id: string
  workflowId: string
  location: string
  name: string
  state?: string
  argument?: string
  result?: string
  error?: ExecutionError
  startTime?: string
  endTime?: string
  duration?: string
  workflowRevisionId?: string
  callLogLevel?: string
  labels?: Record<string, string>
  currentSteps?: Step[]
}

export interface ListExecutionsResponse {
  executions: Execution[]
  total: number
  nextPageToken?: string
}

export interface RunExecutionInput {
  argument: string
  callLogLevel: string
  labels?: Record<string, string>
}

const workflowPath = (location: string, workflow: string) =>
  `${BASE}/workflows/${encodeURIComponent(location)}/${encodeURIComponent(workflow)}`

const executionPath = (location: string, workflow: string, execution: string) =>
  `${workflowPath(location, workflow)}/executions/${encodeURIComponent(execution)}`

export const listWorkflows = () => api.get<ListWorkflowsResponse>(`${BASE}/workflows`)

export const getWorkflow = (location: string, workflow: string) =>
  api.get<Workflow>(workflowPath(location, workflow))

export const createWorkflow = (input: WorkflowInput) =>
  api.post<Workflow>(`${BASE}/workflows`, input)

export const updateWorkflow = (location: string, workflow: string, input: WorkflowUpdateInput) =>
  api.put<Workflow>(workflowPath(location, workflow), input)

export const deleteWorkflow = (location: string, workflow: string) =>
  api.delete<void>(workflowPath(location, workflow))

/** List every execution of a workflow; pass `pageToken` for a single raw page. */
export async function listExecutions(
  location: string,
  workflow: string,
  params?: { pageToken?: string },
): Promise<ListExecutionsResponse> {
  const path = `${workflowPath(location, workflow)}/executions`
  if (params?.pageToken) {
    return api.get<ListExecutionsResponse>(path, { pageToken: params.pageToken })
  }
  const executions = await fetchAllPages(
    (pageToken) =>
      api.get<ListExecutionsResponse>(path, pageToken ? { pageToken } : undefined),
    (page) => page.executions,
  )
  return { executions, total: executions.length }
}

export const getExecution = (location: string, workflow: string, execution: string) =>
  api.get<Execution>(executionPath(location, workflow, execution))

export const runWorkflow = (location: string, workflow: string, input: RunExecutionInput) =>
  api.post<Execution>(`${workflowPath(location, workflow)}/executions`, input)

export const cancelExecution = (location: string, workflow: string, execution: string) =>
  api.post<Execution>(`${executionPath(location, workflow, execution)}/cancel`)
