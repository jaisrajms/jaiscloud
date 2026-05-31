import { api } from './client'

export interface Topic {
  arn: string
  name: string
  displayName?: string
  subscriptionCount: number
  type: 'Standard' | 'FIFO'
  attributes?: Record<string, string>
}

export interface ListTopicsResponse {
  items: Topic[]
  nextToken?: string
  total: number
}

export interface Subscription {
  subscriptionArn: string
  topicArn: string
  protocol: string
  endpoint: string
  owner?: string
}

export interface ListSubscriptionsResponse {
  items: Subscription[]
  nextToken?: string
}

const BASE = '/api/ui/v1/sns'

export function listTopics(params?: { nextToken?: string }): Promise<ListTopicsResponse> {
  return api.get<ListTopicsResponse>(`${BASE}/topics`, params as Record<string, string | number>)
}

export function createTopic(req: {
  name: string
  fifo?: boolean
  tags?: Record<string, string>
}): Promise<Topic> {
  return api.post<Topic>(`${BASE}/topics`, req)
}

export function deleteTopic(arn: string): Promise<void> {
  return api.delete<void>(`${BASE}/topics`, { arn })
}

export function getTopic(arn: string): Promise<Topic> {
  return api.get<Topic>(`${BASE}/topics/detail`, { arn })
}

export function listSubscriptionsByTopic(arn: string): Promise<ListSubscriptionsResponse> {
  return api.get<ListSubscriptionsResponse>(`${BASE}/topics/subscriptions`, { arn })
}

export function subscribe(
  arn: string,
  req: { protocol: string; endpoint: string },
): Promise<{ SubscriptionArn: string }> {
  return api.post<{ SubscriptionArn: string }>(`${BASE}/topics/subscribe`, req, { arn })
}

export function unsubscribe(subArn: string): Promise<void> {
  return api.delete<void>(`${BASE}/topics/subscriptions/${encodeURIComponent(subArn)}`)
}

export function publish(arn: string, req: { message: string; subject?: string }): Promise<{ MessageId?: string }> {
  return api.post<{ MessageId?: string }>(`${BASE}/topics/publish`, req, { arn })
}
