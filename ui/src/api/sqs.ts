import { api } from './client'

export interface Queue {
  name: string
  url: string
  arn: string
  type: 'Standard' | 'FIFO'
  messagesAvailable: number
  messagesInFlight: number
  visibilityTimeout: number
  retentionPeriod: number
  maxMessageSize: number
  dlqArn?: string
  dlqMaxReceive?: number
  createdAt: string
  tags?: Record<string, string>
}

export interface ListQueuesResponse {
  items: Queue[]
  nextToken?: string
  total?: number
}

export interface CreateQueueRequest {
  name: string
  type: 'Standard' | 'FIFO'
  visibilityTimeout?: number
  retentionPeriod?: number
  dlqArn?: string
  dlqMaxReceive?: number
  tags?: Record<string, string>
}

export interface SendMessageRequest {
  body: string
  delaySeconds?: number
  messageGroupId?: string
  messageDeduplicationId?: string
}

export interface Message {
  messageId: string
  body: string
  receiptHandle: string
  attributes: Record<string, unknown>
  sentAt: string
}

export interface ReceiveMessagesResponse {
  messages: Message[]
}

export interface DLQSourceQueuesResponse {
  items: Queue[]
  nextToken?: string
}

const BASE = '/api/ui/v1/sqs'

export function listQueues(params?: { pageSize?: number; nextToken?: string }): Promise<ListQueuesResponse> {
  return api.get<ListQueuesResponse>(`${BASE}/queues`, params as Record<string, string | number>)
}

export function getQueue(queueUrl: string): Promise<Queue> {
  return api.get<Queue>(`${BASE}/queues/detail`, { url: queueUrl })
}

export function createQueue(req: CreateQueueRequest): Promise<Queue> {
  return api.post<Queue>(`${BASE}/queues`, req)
}

export function deleteQueue(queueUrl: string): Promise<void> {
  return api.delete<void>(`${BASE}/queues`, { url: queueUrl })
}

export function purgeQueue(queueUrl: string): Promise<void> {
  return api.post<void>(`${BASE}/queues/purge`, undefined, { url: queueUrl })
}

export function sendMessage(queueUrl: string, req: SendMessageRequest): Promise<{ messageId: string }> {
  return api.post<{ messageId: string }>(`${BASE}/queues/messages`, req, { url: queueUrl })
}

export function receiveMessages(queueUrl: string, params?: { maxMessages?: number }): Promise<ReceiveMessagesResponse> {
  const qp: Record<string, string | number> = { url: queueUrl }
  if (params?.maxMessages) qp.maxMessages = params.maxMessages
  return api.get<ReceiveMessagesResponse>(`${BASE}/queues/messages`, qp)
}

export function deleteMessage(queueUrl: string, receiptHandle: string): Promise<void> {
  return api.delete<void>(`${BASE}/queues/messages/receipt`, { url: queueUrl, receiptHandle })
}

export interface PeekedMessage {
  messageId: string
  body: string
  sentAt: string
  receiveCount: number
  status: 'visible' | 'in-flight' | 'delayed'
  groupId?: string
}

export interface PeekMessagesResponse {
  messages: PeekedMessage[]
  total: number
  offset: number
  limit: number
}

export function peekMessages(
  queueUrl: string,
  params?: { offset?: number; limit?: number },
): Promise<PeekMessagesResponse> {
  const qp: Record<string, string | number> = { url: queueUrl }
  if (params?.offset != null) qp.offset = params.offset
  if (params?.limit != null) qp.limit = params.limit
  return api.get<PeekMessagesResponse>(`${BASE}/queues/messages/peek`, qp)
}

export function listDLQSources(queueUrl: string): Promise<DLQSourceQueuesResponse> {
  return api.get<DLQSourceQueuesResponse>(`${BASE}/queues/dlq-sources`, { url: queueUrl })
}

export function getTags(queueUrl: string): Promise<Record<string, string>> {
  return api.get<Record<string, string>>(`${BASE}/queues/tags`, { url: queueUrl })
}
