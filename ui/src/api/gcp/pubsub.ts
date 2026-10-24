import { api } from '../client'
import { fetchAllPages } from '../paging'

const BASE = '/api/ui/v1/gcp/pubsub'

export interface Topic {
  name: string
  fullName?: string
  messageRetentionDuration?: string
  kmsKeyName?: string
  labels?: Record<string, string>
}

export interface ListTopicsResponse {
  topics: Topic[]
  total: number
  nextPageToken?: string
}

export interface Subscription {
  name: string
  fullName?: string
  topic?: string
  topicFull?: string
  ackDeadlineSeconds?: number
  messageRetentionDuration?: string
  expirationTtl?: string
  enableExactlyOnceDelivery?: boolean
  enableMessageOrdering?: boolean
  filter?: string
  pushEndpoint?: string
  state?: string
  detached?: boolean
  deadLetterTopic?: string
  maxDeliveryAttempts?: number
  retryPolicy?: Record<string, unknown>
  labels?: Record<string, string>
}

export interface ListSubscriptionsResponse {
  subscriptions: Subscription[]
  total: number
  nextPageToken?: string
}

export interface CreateTopicRequest {
  name: string
  messageRetentionDuration?: string
  kmsKeyName?: string
  labels?: Record<string, string>
}

export interface PublishMessage {
  /** Base64-encoded payload, matching the Pub/Sub wire representation. */
  data: string
  attributes?: Record<string, string>
  orderingKey?: string
}

export interface PublishResponse {
  messageIds?: string[]
}

export interface CreateSubscriptionRequest {
  name: string
  topic: string
  ackDeadlineSeconds?: number
  messageRetentionDuration?: string
  enableExactlyOnceDelivery?: boolean
  enableMessageOrdering?: boolean
  filter?: string
  pushEndpoint?: string
  deadLetterTopic?: string
  maxDeliveryAttempts?: number
}

export interface UpdateSubscriptionRequest {
  updateMask?: string
  subscription: Record<string, unknown>
}

export interface IamBinding {
  role: string
  members: string[]
}

export interface IamPolicy {
  kind?: string
  resourceId?: string
  bindings?: IamBinding[]
  etag?: string
  version?: number
}

// ─── Topics ──────────────────────────────────────────────────────────────────

/** List every topic; pass `pageToken` for a single raw page. */
export async function listTopics(params?: { pageToken?: string }): Promise<ListTopicsResponse> {
  if (params?.pageToken) {
    return api.get<ListTopicsResponse>(`${BASE}/topics`, { pageToken: params.pageToken })
  }
  const topics = await fetchAllPages(
    (pageToken) =>
      api.get<ListTopicsResponse>(`${BASE}/topics`, pageToken ? { pageToken } : undefined),
    (page) => page.topics,
  )
  return { topics, total: topics.length }
}

export const createTopic = (body: CreateTopicRequest) => api.post<Topic>(`${BASE}/topics`, body)

export const getTopic = (name: string) =>
  api.get<Topic>(`${BASE}/topics/${encodeURIComponent(name)}`)

export const deleteTopic = (name: string) =>
  api.delete<void>(`${BASE}/topics/${encodeURIComponent(name)}`)

export const publishToTopic = (name: string, messages: PublishMessage[]) =>
  api.post<PublishResponse>(`${BASE}/topics/${encodeURIComponent(name)}/publish`, { messages })

export const getTopicIam = (name: string) =>
  api.get<IamPolicy>(`${BASE}/topics/${encodeURIComponent(name)}/iam`)

export const putTopicIam = (name: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${BASE}/topics/${encodeURIComponent(name)}/iam`, policy)

// ─── Subscriptions ───────────────────────────────────────────────────────────

/** List every subscription; pass `pageToken` for a single raw page. */
export async function listSubscriptions(params?: {
  pageToken?: string
}): Promise<ListSubscriptionsResponse> {
  if (params?.pageToken) {
    return api.get<ListSubscriptionsResponse>(`${BASE}/subscriptions`, {
      pageToken: params.pageToken,
    })
  }
  const subscriptions = await fetchAllPages(
    (pageToken) =>
      api.get<ListSubscriptionsResponse>(
        `${BASE}/subscriptions`,
        pageToken ? { pageToken } : undefined,
      ),
    (page) => page.subscriptions,
  )
  return { subscriptions, total: subscriptions.length }
}

export const createSubscription = (body: CreateSubscriptionRequest) =>
  api.post<Subscription>(`${BASE}/subscriptions`, body)

export const getSubscription = (name: string) =>
  api.get<Subscription>(`${BASE}/subscriptions/${encodeURIComponent(name)}`)

export const updateSubscription = (name: string, body: UpdateSubscriptionRequest) =>
  api.patch<Subscription>(`${BASE}/subscriptions/${encodeURIComponent(name)}`, body)

export const deleteSubscription = (name: string) =>
  api.delete<void>(`${BASE}/subscriptions/${encodeURIComponent(name)}`)

export const getSubscriptionIam = (name: string) =>
  api.get<IamPolicy>(`${BASE}/subscriptions/${encodeURIComponent(name)}/iam`)

export const putSubscriptionIam = (name: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${BASE}/subscriptions/${encodeURIComponent(name)}/iam`, policy)
