import type { EngineMode, ServiceDescriptor } from '../api/services'

/**
 * Runtime tag for an engine-capable service: the active backend
 * ('docker' / 'k8s' / 'native') when one runs, else 'mock'. Undefined only for
 * services with no executor seam at all. This is the implementation axis, never
 * a fidelity tier — the tag is informational and its popover lists every
 * backend, so it renders even when no engine is configured.
 */
export function engineTag(service: ServiceDescriptor): string | undefined {
  const engine = service.engine
  if (!engine) return undefined
  return engine.active && engine.mode ? engine.mode : 'mock'
}

/** Whether a real (non-mock) backend is configured. */
export function engineActive(service: ServiceDescriptor): boolean {
  return service.engine?.active === true
}

/** All backends and their support state, for the availability matrix. */
export function engineModes(service: ServiceDescriptor): EngineMode[] {
  return service.engine?.modes ?? []
}

/** Reachability of the host engines, from GET /api/ui/v1/gcp/runtime. */
export interface EngineReachability {
  docker?: boolean
  kubernetes?: boolean
}

export type EngineStatusColor = 'default' | 'success' | 'warning'

export interface EngineStatus {
  label: string
  color: EngineStatusColor
}

/**
 * The engine status to show for a service: the configured mode plus whether
 * that mode's host engine is actually reachable. Green means "will run", not
 * just "configured" — a configured-but-unreachable backend is a warning, and
 * mock stays neutral (it is the documented default, not a fault). Backends with
 * no host probe (native) report "configured".
 */
export function engineStatus(
  service: ServiceDescriptor,
  reach: EngineReachability,
): EngineStatus {
  const mode = engineTag(service)
  if (!mode || mode === 'mock' || !engineActive(service)) {
    return { label: 'mock', color: 'default' }
  }
  if (mode === 'docker') {
    if (reach.docker === undefined) return { label: 'docker · checking…', color: 'default' }
    return reach.docker
      ? { label: 'docker · reachable', color: 'success' }
      : { label: 'docker · unreachable', color: 'warning' }
  }
  if (mode === 'k8s') {
    if (reach.kubernetes === undefined) return { label: 'k8s · checking…', color: 'default' }
    return reach.kubernetes
      ? { label: 'k8s · reachable', color: 'success' }
      : { label: 'k8s · unreachable', color: 'warning' }
  }
  return { label: `${mode} · configured`, color: 'default' }
}
