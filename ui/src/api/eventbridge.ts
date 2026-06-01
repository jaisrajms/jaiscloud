import { api } from './client'

const BASE = '/api/ui/v1/eventbridge'

export interface Rule {
  name: string
  arn?: string
  eventBusName?: string
  state?: string
  eventPattern?: string
  scheduleExpression?: string
  description?: string
}

export interface Target {
  id: string
  arn: string
}

export interface EventBus {
  name: string
  arn?: string
}

export interface ListRulesResponse {
  items: Rule[]
  total: number
  nextToken?: string
}

export interface ListTargetsResponse {
  items: Target[]
  total: number
}

export interface ListEventBusesResponse {
  items: EventBus[]
  total: number
}

export function listRules(params?: { bus?: string; nextToken?: string }): Promise<ListRulesResponse> {
  const p: Record<string, string> = {}
  if (params?.bus) p.bus = params.bus
  if (params?.nextToken) p.nextToken = params.nextToken
  return api.get(`${BASE}/rules`, p)
}

export function putRule(req: {
  name: string
  eventPattern?: string
  scheduleExpression?: string
  state?: string
  description?: string
  eventBusName?: string
}): Promise<unknown> {
  return api.post(`${BASE}/rules`, req)
}

export function deleteRule(name: string, bus?: string): Promise<void> {
  const p: Record<string, string> = {}
  if (bus) p.bus = bus
  return api.delete(`${BASE}/rules/${encodeURIComponent(name)}`, p)
}

export function enableRule(name: string): Promise<void> {
  return api.post(`${BASE}/rules/${encodeURIComponent(name)}/enable`)
}

export function disableRule(name: string): Promise<void> {
  return api.post(`${BASE}/rules/${encodeURIComponent(name)}/disable`)
}

export function listTargets(ruleName: string): Promise<ListTargetsResponse> {
  return api.get(`${BASE}/rules/${encodeURIComponent(ruleName)}/targets`)
}

export function putTargets(
  ruleName: string,
  targets: { id: string; arn: string }[],
): Promise<unknown> {
  return api.post(`${BASE}/rules/${encodeURIComponent(ruleName)}/targets`, { targets })
}

export function removeTarget(ruleName: string, targetId: string): Promise<void> {
  return api.delete(`${BASE}/rules/${encodeURIComponent(ruleName)}/targets/${encodeURIComponent(targetId)}`)
}

export function putEvents(entries: { source: string; detailType: string; detail: string; bus?: string }[]): Promise<unknown> {
  return api.post(`${BASE}/events`, { entries })
}

export function listEventBuses(): Promise<ListEventBusesResponse> {
  return api.get(`${BASE}/buses`)
}

export function createEventBus(name: string): Promise<unknown> {
  return api.post(`${BASE}/buses`, { name })
}

export function deleteEventBus(name: string): Promise<void> {
  return api.delete(`${BASE}/buses/${encodeURIComponent(name)}`)
}
