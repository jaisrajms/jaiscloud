import { api } from './client'

const BASE = '/api/ui/v1/elbv2'

export interface LoadBalancer { arn: string; name: string; dnsName: string; scheme: string; type: string; state: string }
export interface ListLoadBalancersResponse { items: LoadBalancer[]; total: number }

export const listLoadBalancers = () => api.get<ListLoadBalancersResponse>(`${BASE}/load-balancers`)

export const createLoadBalancer = (req: { name: string; type?: string; scheme?: string }) =>
  api.post<unknown>(`${BASE}/load-balancers`, req)

export const deleteLoadBalancer = (arn: string) =>
  api.delete<void>(`${BASE}/load-balancers/${encodeURIComponent(arn)}`)
