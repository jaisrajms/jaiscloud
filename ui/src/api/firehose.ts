import { api } from './client'

const BASE = '/api/ui/v1/firehose'

export interface DeliveryStream { name: string }
export interface ListDeliveryStreamsResponse { items: DeliveryStream[]; total: number }

export const listDeliveryStreams = () => api.get<ListDeliveryStreamsResponse>(`${BASE}/streams`)

export const createDeliveryStream = (req: { name: string; type?: string }) =>
  api.post<unknown>(`${BASE}/streams`, req)

export const deleteDeliveryStream = (name: string) =>
  api.delete<void>(`${BASE}/streams/${encodeURIComponent(name)}`)
