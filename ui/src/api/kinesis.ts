import { api } from './client'

const BASE = '/api/ui/v1/kinesis'

export interface Stream { name: string; arn: string; status: string; mode: string }
export interface ListStreamsResponse { items: Stream[]; total: number; nextToken: string }

export const listStreams = () => api.get<ListStreamsResponse>(`${BASE}/streams`)

export const createStream = (req: { name: string; shardCount?: number }) =>
  api.post<unknown>(`${BASE}/streams`, req)

export const deleteStream = (name: string) =>
  api.delete<void>(`${BASE}/streams/${encodeURIComponent(name)}`)
