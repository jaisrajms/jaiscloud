import { api } from '../client'

const BASE = '/api/ui/v1/gcp/scheduler'

/** A Cloud Scheduler job, flattened across locations for the list. */
export interface SchedulerJob {
  name: string
  location: string
  state: string
  description?: string
  schedule?: string
  timeZone?: string
  target?: string
  httpUri?: string
  httpMethod?: string
  httpBody?: string
  httpHeaders?: Record<string, string>
  pubsubTopic?: string
  pubsubData?: string
  appEngineUri?: string
  appEngineMethod?: string
  retryCount?: number
  attemptDeadline?: string
  scheduleTime?: string
  lastAttemptTime?: string
  userUpdateTime?: string
  lastStatus?: { code: number; message?: string }
}

export interface ListJobsResponse {
  jobs: SchedulerJob[]
  total: number
}

/** The create/update form body. Location is required on create. */
export interface JobInput {
  name: string
  location: string
  schedule: string
  timeZone: string
  description?: string
  target: string
  httpUri?: string
  httpMethod?: string
  httpBody?: string
  httpHeaders?: Record<string, string>
  pubsubTopic?: string
  pubsubData?: string
  appEngineUri?: string
  appEngineMethod?: string
  retryCount?: number
  attemptDeadline?: string
}

const jobPath = (location: string, job: string) =>
  `${BASE}/jobs/${encodeURIComponent(location)}/${encodeURIComponent(job)}`

export const listJobs = () => api.get<ListJobsResponse>(`${BASE}/jobs`)

export const getJob = (location: string, job: string) =>
  api.get<SchedulerJob>(jobPath(location, job))

export const createJob = (input: JobInput) => api.post<SchedulerJob>(`${BASE}/jobs`, input)

export const updateJob = (location: string, job: string, input: JobInput) =>
  api.put<SchedulerJob>(jobPath(location, job), input)

export const deleteJob = (location: string, job: string) =>
  api.delete<void>(jobPath(location, job))

export const pauseJob = (location: string, job: string) =>
  api.post<SchedulerJob>(`${jobPath(location, job)}/pause`)

export const resumeJob = (location: string, job: string) =>
  api.post<SchedulerJob>(`${jobPath(location, job)}/resume`)

export const runJob = (location: string, job: string) =>
  api.post<SchedulerJob>(`${jobPath(location, job)}/run`)
