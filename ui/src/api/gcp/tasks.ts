import { api } from '../client'

const BASE = '/api/ui/v1/gcp/tasks'

/** A Cloud Tasks queue, flattened across locations for the list. */
export interface TaskQueue {
  name: string
  location: string
  state: string
  purgeTime?: string
  maxDispatchesPerSecond?: number
  maxBurstSize?: number
  maxConcurrentDispatches?: number
  maxAttempts?: number
  maxRetryDuration?: string
  minBackoff?: string
  maxBackoff?: string
  maxDoublings?: number
}

export interface ListQueuesResponse {
  queues: TaskQueue[]
  total: number
}

/** The create/update queue form body. Location is required on create. */
export interface QueueInput {
  name: string
  location: string
  maxDispatchesPerSecond: number
  maxBurstSize: number
  maxConcurrentDispatches: number
  maxAttempts: number
  maxRetryDuration: string
  minBackoff: string
  maxBackoff: string
  maxDoublings: number
}

/** A Cloud Tasks task. Target fields are only populated for the kind in use. */
export interface CloudTask {
  name: string
  location: string
  queue: string
  target?: string
  httpUrl?: string
  httpMethod?: string
  httpHeaders?: Record<string, string>
  httpBody?: string
  appEngineUri?: string
  appEngineMethod?: string
  scheduleTime?: string
  createTime?: string
  dispatchDeadline?: string
  dispatchCount: number
  responseCount: number
  lastAttemptStatus?: { code: number; message?: string }
}

export interface ListTasksResponse {
  tasks: CloudTask[]
  total: number
}

/** The create-task form body. */
export interface TaskInput {
  name: string
  target: string
  httpUrl: string
  httpMethod: string
  httpHeaders?: Record<string, string>
  httpBody: string
  appEngineUri: string
  appEngineMethod: string
  scheduleTime: string
  dispatchDeadline: string
}

export interface IamPolicy {
  bindings?: { role: string; members: string[]; condition?: Record<string, unknown> }[]
  etag?: string
  version?: number
}

const queuePath = (location: string, queue: string) =>
  `${BASE}/queues/${encodeURIComponent(location)}/${encodeURIComponent(queue)}`

const taskPath = (location: string, queue: string, task: string) =>
  `${queuePath(location, queue)}/tasks/${encodeURIComponent(task)}`

export const listQueues = () => api.get<ListQueuesResponse>(`${BASE}/queues`)

export const getQueue = (location: string, queue: string) =>
  api.get<TaskQueue>(queuePath(location, queue))

export const createQueue = (input: QueueInput) => api.post<TaskQueue>(`${BASE}/queues`, input)

export const updateQueue = (location: string, queue: string, input: QueueInput) =>
  api.put<TaskQueue>(queuePath(location, queue), input)

export const deleteQueue = (location: string, queue: string) =>
  api.delete<void>(queuePath(location, queue))

export const pauseQueue = (location: string, queue: string) =>
  api.post<TaskQueue>(`${queuePath(location, queue)}/pause`)

export const resumeQueue = (location: string, queue: string) =>
  api.post<TaskQueue>(`${queuePath(location, queue)}/resume`)

export const purgeQueue = (location: string, queue: string) =>
  api.post<TaskQueue>(`${queuePath(location, queue)}/purge`)

export const getQueueIam = (location: string, queue: string) =>
  api.get<IamPolicy>(`${queuePath(location, queue)}/iam`)

export const putQueueIam = (location: string, queue: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${queuePath(location, queue)}/iam`, policy)

export const listTasks = (location: string, queue: string) =>
  api.get<ListTasksResponse>(`${queuePath(location, queue)}/tasks`)

export const getTask = (location: string, queue: string, task: string) =>
  api.get<CloudTask>(taskPath(location, queue, task))

export const createTask = (location: string, queue: string, input: TaskInput) =>
  api.post<CloudTask>(`${queuePath(location, queue)}/tasks`, input)

export const deleteTask = (location: string, queue: string, task: string) =>
  api.delete<void>(taskPath(location, queue, task))

export const runTask = (location: string, queue: string, task: string) =>
  api.post<CloudTask>(`${taskPath(location, queue, task)}/run`)
