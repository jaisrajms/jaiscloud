import { api, putBlob } from '../client'

const BASE = '/api/ui/v1/gcp/functions'

/** A function's event trigger. */
export interface FunctionEventTrigger {
  eventType?: string
  resource?: string
  retry?: boolean
  retryPolicy?: string
  trigger?: string
}

/** A Cloud Functions function, flattened across locations for the list. */
export interface GcpFunction {
  id: string
  location: string
  name: string
  status?: string
  url?: string
  runtime?: string
  entryPoint?: string
  /** "http" | "event" */
  triggerType?: string
  eventTrigger?: FunctionEventTrigger
  availableMemoryMB?: number
  timeout?: string
  timeoutSeconds?: number
  minInstanceCount?: number
  maxInstanceCount?: number
  maxInstanceRequestConcurrency?: number
  availableCpu?: string
  revision?: number
  sourceArchiveUrl?: string
  sourceUploadUrl?: string
  sourceSize?: number
  sourceSha256?: string
  description?: string
  labels?: Record<string, string>
  environmentVariables?: Record<string, string>
  createTime?: string
  updateTime?: string
}

export interface ListFunctionsResponse {
  functions: GcpFunction[]
  total: number
}

/**
 * The create-function form body. Location, id and runtime are required. Source
 * is optional: a gs:// sourceArchiveUrl, an inline file
 * (sourceFilename + sourceInline) the server packages into an archive, or the
 * storageSource (sourceBucket + sourceObject) returned by the generateUploadUrl
 * flow after the archive has been PUT to the upload URL.
 */
export interface CreateFunctionInput {
  id: string
  location: string
  runtime: string
  entryPoint: string
  description: string
  triggerType: 'http' | 'event'
  eventType?: string
  eventResource?: string
  retry?: boolean
  availableMemoryMB?: number
  timeout?: string
  minInstanceCount?: number
  maxInstanceCount?: number
  maxInstanceRequestConcurrency?: number
  availableCpu?: string
  sourceArchiveUrl?: string
  sourceInline?: string
  sourceFilename?: string
  sourceBucket?: string
  sourceObject?: string
  labels?: Record<string, string>
  environmentVariables?: Record<string, string>
}

/** The v2 generateUploadUrl target: PUT the zip to the console upload route with this bucket/object, then send them on create. */
export interface SourceUploadTarget {
  bucket: string
  object: string
}

/** One persisted event-delivery record (the emulator-only Executions tab). */
export interface FunctionDelivery {
  id: string
  functionId: string
  source?: string
  eventType?: string
  resource?: string
  eventId?: string
  status?: string
  attempts?: number
  error?: string
  result?: string
  deadLetterTopic?: string
  attributes?: Record<string, string>
  createTime?: string
  updateTime?: string
}

export interface ListDeliveriesResponse {
  deliveries: FunctionDelivery[]
  total: number
}

export interface CallFunctionResponse {
  executionId: string
  result?: string
  error?: string
}

export interface IamPolicy {
  bindings?: { role: string; members: string[]; condition?: Record<string, unknown> }[]
  etag?: string
  version?: number
}

const functionPath = (location: string, fn: string) =>
  `${BASE}/functions/${encodeURIComponent(location)}/${encodeURIComponent(fn)}`

export const listFunctions = () => api.get<ListFunctionsResponse>(`${BASE}/functions`)

export const getFunction = (location: string, fn: string) =>
  api.get<GcpFunction>(functionPath(location, fn))

export const createFunction = (input: CreateFunctionInput) =>
  api.post<GcpFunction>(`${BASE}/functions`, input)

/**
 * Provision the GCS upload target for a source archive (the real v2
 * generateUploadUrl flow): PUT the zip with `uploadFunctionSource`, then pass
 * `bucket`/`object` back as sourceBucket/sourceObject on create.
 */
export const generateUploadUrl = (location: string) =>
  api.post<SourceUploadTarget>(`${BASE}/functions/upload`, { location })

/**
 * PUT the source zip to the target issued by `generateUploadUrl`. The console
 * transfers the bytes through its own API origin (the UI server is a separate
 * listener from the emulator gateway); the backend stores them in the emitted
 * GCS bucket/object, which the create call then reads as its storageSource.
 */
export const uploadFunctionSource = (location: string, object: string, zip: Blob) =>
  putBlob(`${BASE}/functions/upload`, { location, object }, zip, 'application/zip')

export const deleteFunction = (location: string, fn: string) =>
  api.delete<void>(functionPath(location, fn))

export const callFunction = (location: string, fn: string, data: string) =>
  api.post<CallFunctionResponse>(`${functionPath(location, fn)}/call`, { data })

export const getFunctionIam = (location: string, fn: string) =>
  api.get<IamPolicy>(`${functionPath(location, fn)}/iam`)

export const putFunctionIam = (location: string, fn: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${functionPath(location, fn)}/iam`, policy)

export const listDeliveries = (location: string, fn: string) =>
  api.get<ListDeliveriesResponse>(`${functionPath(location, fn)}/deliveries`)
