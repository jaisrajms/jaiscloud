import { api } from '../client'

const BASE = '/api/ui/v1/gcp/resourcemanager'

/** A Cloud Resource Manager project as rendered by the console. */
export interface Project {
  projectId: string
  projectNumber?: string
  displayName?: string
  state: string
  etag?: string
  parent?: string
  createTime?: string
  updateTime?: string
  deleteTime?: string
  labels?: Record<string, string>
}

export interface ListProjectsResponse {
  projects: Project[]
  total: number
}

/** The create form body; the project number/state/timestamps are derived. */
export interface CreateProjectInput {
  projectId: string
  displayName?: string
  labels?: Record<string, string>
}

const projectPath = (id: string) => `${BASE}/projects/${encodeURIComponent(id)}`

export const listProjects = () => api.get<ListProjectsResponse>(`${BASE}/projects`)

export const createProject = (input: CreateProjectInput) =>
  api.post<Project>(`${BASE}/projects`, input)

export const deleteProject = (id: string) => api.delete<Project>(projectPath(id))

export const undeleteProject = (id: string) => api.post<Project>(`${projectPath(id)}/undelete`)
