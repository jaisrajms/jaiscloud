import { api } from '../client'

const BASE = '/api/ui/v1/gcp/dataproc'

/** One entry of a Dataproc cluster or job status history. */
export interface DataprocStatusEvent {
  state: string
  detail?: string
  stateStartTime?: string
}

/** A Dataproc cluster, flattened across regions for the list. */
export interface DataprocCluster {
  id: string
  name: string
  region: string
  status: string
  statusDetail?: string
  statusHistory?: DataprocStatusEvent[]
  clusterUuid?: string
  labels?: Record<string, string>
  gkeBacked?: boolean
  /** Effective k8s namespace the cluster's jobs run in; detail only. */
  namespace?: string
  /** Whether the emulator created the effective namespace; detail only. */
  namespaceOwned?: boolean
  /** Caller-requested kubernetesNamespace (GKE virtual clusters); detail only. */
  kubernetesNamespace?: string
  /** Full ClusterConfig wire object; detail only. */
  config?: unknown
  /** Full VirtualClusterConfig wire object; detail only. */
  virtualClusterConfig?: unknown
  createTime?: string
  updateTime?: string
}

export interface ListClustersResponse {
  clusters: DataprocCluster[]
  total: number
}

/**
 * Create-cluster body (POST /clusters). Exactly one of `config` (a GCE
 * ClusterConfig) or `virtualClusterConfig` (a Dataproc-on-GKE
 * VirtualClusterConfig carrying `kubernetesNamespace`) is sent.
 */
export interface DataprocClusterInput {
  region: string
  name: string
  labels?: Record<string, string>
  config?: unknown
  virtualClusterConfig?: unknown
}

/** A Dataproc job, flattened across regions for the list. */
export interface DataprocJob {
  id: string
  name: string
  region: string
  clusterName?: string
  /** sparkJob | pysparkJob | sparkSqlJob | sparkRJob | hadoopJob | hiveJob | pigJob */
  type?: string
  /** Type-specific job body; detail only. */
  typeJob?: unknown
  status: string
  statusDetail?: string
  substate?: string
  statusHistory?: DataprocStatusEvent[]
  labels?: Record<string, string>
  driverOutputResourceUri?: string
  driverControlFilesUri?: string
  jobUuid?: string
  createTime?: string
}

export interface ListJobsResponse {
  jobs: DataprocJob[]
  total: number
}

/** A Dataproc workflow template, flattened across regions for the list. */
export interface DataprocWorkflowTemplate {
  id: string
  name: string
  region: string
  version: number
  /** Full WorkflowTemplate wire object; detail only. */
  definition?: unknown
  createTime?: string
  updateTime?: string
}

export interface ListWorkflowTemplatesResponse {
  templates: DataprocWorkflowTemplate[]
  total: number
}

/**
 * Submit-job body (POST /jobs). The console is region-optional, so the region
 * travels in the body next to a dataproc.v1.Job object.
 */
export interface DataprocSubmitJobInput {
  region: string
  job: Record<string, unknown>
}

/**
 * Cluster patch body (PATCH /clusters/{region}/{cluster}). updateMask carries
 * the camelCase paths the user touched; the server passes it through verbatim.
 */
export interface DataprocClusterPatch {
  labels?: Record<string, string>
  config?: Record<string, unknown>
  updateMask?: string[]
}

/**
 * Workflow-template create/update body. definition is the dataproc.v1
 * WorkflowTemplate (placement, jobs, parameters, ...). version is the
 * optimistic-concurrency token on update.
 */
export interface DataprocWorkflowTemplateInput {
  region: string
  id: string
  version?: number
  definition: Record<string, unknown>
}

export interface DataprocInstantiateInput {
  version?: number
  parameters?: Record<string, string>
}

const clusterPath = (region: string, cluster: string) =>
  `${BASE}/clusters/${encodeURIComponent(region)}/${encodeURIComponent(cluster)}`

const jobPath = (region: string, job: string) =>
  `${BASE}/jobs/${encodeURIComponent(region)}/${encodeURIComponent(job)}`

const templatePath = (region: string, template: string) =>
  `${BASE}/workflow-templates/${encodeURIComponent(region)}/${encodeURIComponent(template)}`

export const listClusters = () => api.get<ListClustersResponse>(`${BASE}/clusters`)

export const createCluster = (input: DataprocClusterInput) =>
  api.post<DataprocCluster>(`${BASE}/clusters`, input)

export const getCluster = (region: string, cluster: string) =>
  api.get<DataprocCluster>(clusterPath(region, cluster))

export const startCluster = (region: string, cluster: string) =>
  api.post<DataprocCluster>(`${clusterPath(region, cluster)}/start`)

export const stopCluster = (region: string, cluster: string) =>
  api.post<DataprocCluster>(`${clusterPath(region, cluster)}/stop`)

export const deleteCluster = (region: string, cluster: string) =>
  api.delete<void>(clusterPath(region, cluster))

export const updateCluster = (region: string, cluster: string, patch: DataprocClusterPatch) =>
  api.patch<DataprocCluster>(clusterPath(region, cluster), patch)

export const listJobs = () => api.get<ListJobsResponse>(`${BASE}/jobs`)

export const submitJob = (input: DataprocSubmitJobInput) =>
  api.post<DataprocJob>(`${BASE}/jobs`, input)

export const getJob = (region: string, job: string) => api.get<DataprocJob>(jobPath(region, job))

export const cancelJob = (region: string, job: string) =>
  api.post<DataprocJob>(`${jobPath(region, job)}/cancel`)

export const listWorkflowTemplates = () =>
  api.get<ListWorkflowTemplatesResponse>(`${BASE}/workflow-templates`)

export const getWorkflowTemplate = (region: string, template: string) =>
  api.get<DataprocWorkflowTemplate>(templatePath(region, template))

export const createWorkflowTemplate = (input: DataprocWorkflowTemplateInput) =>
  api.post<DataprocWorkflowTemplate>(`${BASE}/workflow-templates`, input)

export const updateWorkflowTemplate = (
  region: string,
  template: string,
  input: DataprocWorkflowTemplateInput,
) => api.put<DataprocWorkflowTemplate>(templatePath(region, template), input)

export const deleteWorkflowTemplate = (region: string, template: string, version?: number) =>
  api.delete<void>(
    `${templatePath(region, template)}${version ? `?version=${encodeURIComponent(String(version))}` : ''}`,
  )

export const instantiateWorkflowTemplate = (
  region: string,
  template: string,
  input: DataprocInstantiateInput,
) => api.post<unknown>(`${templatePath(region, template)}/instantiate`, input)
