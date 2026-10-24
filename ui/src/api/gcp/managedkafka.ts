import { api } from '../client'

const BASE = '/api/ui/v1/gcp/managedkafka'

/** A Managed Kafka cluster, flattened across locations for the list. */
export interface ManagedKafkaCluster {
  id: string
  name: string
  location: string
  /** Cluster state; the emulator renders ACTIVE (its LRO settles lazily). */
  state?: string
  bootstrapAddress?: string
  labels?: Record<string, string>
  /** Verbatim cluster wire body (capacityConfig, gcpConfig, ...); detail only. */
  config?: unknown
  createTime?: string
  updateTime?: string
}

export interface ListClustersResponse {
  clusters: ManagedKafkaCluster[]
  total: number
}

/** A Managed Kafka topic. `cluster` + `location` identify its parent cluster. */
export interface ManagedKafkaTopic {
  id: string
  name: string
  location: string
  cluster: string
  partitionCount: number
  replicationFactor: number
  /** Kafka property overrides (wire `configs` map); detail only. */
  configs?: Record<string, string>
  createTime?: string
  updateTime?: string
}

export interface ListTopicsResponse {
  topics: ManagedKafkaTopic[]
  total: number
}

/** One access grant within a Managed Kafka ACL. */
export interface ManagedKafkaAclEntry {
  principal?: string
  permissionType?: string
  operation?: string
  host?: string
}

export interface ManagedKafkaAcl {
  id: string
  name: string
  location: string
  cluster: string
  resourceType?: string
  resourceName?: string
  patternType?: string
  etag?: string
  aclEntries: ManagedKafkaAclEntry[]
}

export interface ListAclsResponse {
  acls: ManagedKafkaAcl[]
  total: number
}

/** One committed partition offset of a consumer group. */
export interface ManagedKafkaConsumerGroupOffset {
  topic: string
  partition: number
  offset: number
  metadata?: string
}

export interface ManagedKafkaConsumerGroup {
  id: string
  name: string
  location: string
  cluster: string
  offsets: ManagedKafkaConsumerGroupOffset[]
}

export interface ListConsumerGroupsResponse {
  groups: ManagedKafkaConsumerGroup[]
  total: number
}

/** The create/update body for a Managed Kafka cluster. */
export interface ClusterWriteInput {
  id?: string
  location?: string
  labels?: Record<string, string>
  /** Verbatim cluster body (capacityConfig, gcpConfig, ...); update merges it. */
  config?: unknown
}

/** The create/update body for a Managed Kafka topic. */
export interface TopicWriteInput {
  id?: string
  partitionCount?: number
  replicationFactor?: number
  configs?: Record<string, string>
}

/**
 * The create/update body for a Managed Kafka ACL. `id` encodes the resource
 * pattern (e.g. "topic/orders", "allTopics", "cluster"); `etag` is required on
 * update (optimistic concurrency) and `aclEntries` replaces the entry list.
 */
export interface AclWriteInput {
  id?: string
  etag?: string
  aclEntries: ManagedKafkaAclEntry[]
}

/** The update body for a consumer group: the committed offsets to set. */
export interface ConsumerGroupWriteInput {
  offsets: ManagedKafkaConsumerGroupOffset[]
}

const clusterPath = (location: string, cluster: string) =>
  `${BASE}/clusters/${encodeURIComponent(location)}/${encodeURIComponent(cluster)}`

const topicPath = (location: string, cluster: string, topic: string) =>
  `${clusterPath(location, cluster)}/topics/${encodeURIComponent(topic)}`

const aclPath = (location: string, cluster: string, acl: string) =>
  `${clusterPath(location, cluster)}/acls/${encodeURIComponent(acl)}`

export const listClusters = () => api.get<ListClustersResponse>(`${BASE}/clusters`)

export const getCluster = (location: string, cluster: string) =>
  api.get<ManagedKafkaCluster>(clusterPath(location, cluster))

export const listTopics = () => api.get<ListTopicsResponse>(`${BASE}/topics`)

export const listClusterTopics = (location: string, cluster: string) =>
  api.get<ListTopicsResponse>(`${clusterPath(location, cluster)}/topics`)

export const getTopic = (location: string, cluster: string, topic: string) =>
  api.get<ManagedKafkaTopic>(topicPath(location, cluster, topic))

export const listAcls = (location: string, cluster: string) =>
  api.get<ListAclsResponse>(`${clusterPath(location, cluster)}/acls`)

export const listConsumerGroups = (location: string, cluster: string) =>
  api.get<ListConsumerGroupsResponse>(`${clusterPath(location, cluster)}/consumer-groups`)

// ─── writes ──────────────────────────────────────────────────────────────────

export const createCluster = (input: ClusterWriteInput) =>
  api.post<ManagedKafkaCluster>(`${BASE}/clusters`, input)

export const updateCluster = (location: string, cluster: string, input: ClusterWriteInput) =>
  api.put<ManagedKafkaCluster>(clusterPath(location, cluster), input)

export const createTopic = (location: string, cluster: string, input: TopicWriteInput) =>
  api.post<ManagedKafkaTopic>(`${clusterPath(location, cluster)}/topics`, input)

export const updateTopic = (location: string, cluster: string, topic: string, input: TopicWriteInput) =>
  api.put<ManagedKafkaTopic>(topicPath(location, cluster, topic), input)

export const deleteTopic = (location: string, cluster: string, topic: string) =>
  api.delete<void>(topicPath(location, cluster, topic))

export const createAcl = (location: string, cluster: string, input: AclWriteInput) =>
  api.post<ManagedKafkaAcl>(`${clusterPath(location, cluster)}/acls`, input)

export const updateAcl = (location: string, cluster: string, acl: string, input: AclWriteInput) =>
  api.put<ManagedKafkaAcl>(aclPath(location, cluster, acl), input)

export const deleteAcl = (location: string, cluster: string, acl: string) =>
  api.delete<void>(aclPath(location, cluster, acl))

export const updateConsumerGroup = (
  location: string,
  cluster: string,
  group: string,
  input: ConsumerGroupWriteInput,
) => api.put<ManagedKafkaConsumerGroup>(`${clusterPath(location, cluster)}/consumer-groups/${encodeURIComponent(group)}`, input)

export const deleteConsumerGroup = (location: string, cluster: string, group: string) =>
  api.delete<void>(`${clusterPath(location, cluster)}/consumer-groups/${encodeURIComponent(group)}`)
