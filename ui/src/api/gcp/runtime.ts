import { api } from '../client'

/** Liveness of one host engine, probed by the server on each request. */
export interface EngineHealth {
  available: boolean
  detail?: string
}

export interface RuntimeHealth {
  docker: EngineHealth
  kubernetes: EngineHealth
}

/** Live host-engine liveness (docker daemon, k8s API server) for the admin view. */
export function getRuntimeHealth(): Promise<RuntimeHealth> {
  return api.get<RuntimeHealth>('/api/ui/v1/gcp/runtime')
}
