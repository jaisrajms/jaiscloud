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

export const listJobs = () => api.get<ListJobsResponse>(`${BASE}/jobs`)

export const getJob = (region: string, job: string) => api.get<DataprocJob>(jobPath(region, job))

export const cancelJob = (region: string, job: string) =>
  api.post<DataprocJob>(`${jobPath(region, job)}/cancel`)

export const listWorkflowTemplates = () =>
  api.get<ListWorkflowTemplatesResponse>(`${BASE}/workflow-templates`)

export const getWorkflowTemplate = (region: string, template: string) =>
  api.get<DataprocWorkflowTemplate>(templatePath(region, template))
