import { api } from '../client'

const BASE = '/api/ui/v1/gcp/eventarc'

/** One Eventarc event filter row. */
export interface EventFilter {
  attribute: string
  operator?: string
  value: string
}

/** An Eventarc trigger, flattened across locations for the list. */
export interface EventarcTrigger {
  name: string
  location: string
  uid?: string
  etag?: string
  createTime?: string
  updateTime?: string
  labels?: Record<string, string>
  destinationType?: string
  destination?: string
  destinationRegion?: string
  eventFilters?: EventFilter[]
  serviceAccount?: string
  channel?: string
  eventDataContentType?: string
  transportPubsubTopic?: string
  transportPubsubSubscription?: string
  /** The verbatim stored trigger body (drives the edit form's JSON view). */
  config?: Record<string, unknown>
}

export interface ListTriggersResponse {
  triggers: EventarcTrigger[]
  total: number
}

/**
 * The create/edit trigger form body. When `config` is set it is sent verbatim
 * as the trigger body (the JSON escape hatch) and the structured fields are
 * ignored by the server.
 */
export interface TriggerInput {
  name: string
  location: string
  destinationType: string
  destination: string
  destinationRegion: string
  serviceAccount: string
  channel: string
  eventDataContentType: string
  eventFilters: EventFilter[]
  transportPubsubTopic: string
  labels?: Record<string, string>
  config?: Record<string, unknown>
}

/** An Eventarc channel, flattened across locations for the list. */
export interface EventarcChannel {
  name: string
  location: string
  uid?: string
  etag?: string
  activationToken?: string
  pubsubTopic?: string
  state?: string
  createTime?: string
  updateTime?: string
  labels?: Record<string, string>
  provider?: string
  cryptoKeyName?: string
  config?: Record<string, unknown>
}

export interface ListChannelsResponse {
  channels: EventarcChannel[]
  total: number
}

export interface ChannelInput {
  name: string
  location: string
  provider: string
  cryptoKeyName: string
  labels?: Record<string, string>
  config?: Record<string, unknown>
}

export interface IamPolicy {
  bindings?: { role: string; members: string[]; condition?: Record<string, unknown> }[]
  etag?: string
  version?: number
}

const triggerPath = (location: string, trigger: string) =>
  `${BASE}/triggers/${encodeURIComponent(location)}/${encodeURIComponent(trigger)}`

const channelPath = (location: string, channel: string) =>
  `${BASE}/channels/${encodeURIComponent(location)}/${encodeURIComponent(channel)}`

export const listTriggers = () => api.get<ListTriggersResponse>(`${BASE}/triggers`)

export const getTrigger = (location: string, trigger: string) =>
  api.get<EventarcTrigger>(triggerPath(location, trigger))

export const createTrigger = (input: TriggerInput) => api.post<EventarcTrigger>(`${BASE}/triggers`, input)

export const updateTrigger = (location: string, trigger: string, input: TriggerInput) =>
  api.put<EventarcTrigger>(triggerPath(location, trigger), input)

export const deleteTrigger = (location: string, trigger: string) =>
  api.delete<void>(triggerPath(location, trigger))

export const getTriggerIam = (location: string, trigger: string) =>
  api.get<IamPolicy>(`${triggerPath(location, trigger)}/iam`)

export const putTriggerIam = (location: string, trigger: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${triggerPath(location, trigger)}/iam`, policy)

export const listChannels = () => api.get<ListChannelsResponse>(`${BASE}/channels`)

export const getChannel = (location: string, channel: string) =>
  api.get<EventarcChannel>(channelPath(location, channel))

export const createChannel = (input: ChannelInput) => api.post<EventarcChannel>(`${BASE}/channels`, input)

export const updateChannel = (location: string, channel: string, input: ChannelInput) =>
  api.put<EventarcChannel>(channelPath(location, channel), input)

export const deleteChannel = (location: string, channel: string) =>
  api.delete<void>(channelPath(location, channel))

export const getChannelIam = (location: string, channel: string) =>
  api.get<IamPolicy>(`${channelPath(location, channel)}/iam`)

export const putChannelIam = (location: string, channel: string, policy: IamPolicy) =>
  api.put<IamPolicy>(`${channelPath(location, channel)}/iam`, policy)
