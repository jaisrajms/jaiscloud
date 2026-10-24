import { describe, expect, it } from 'vitest'
import type { ServiceDescriptor } from '../api/services'
import { engineActive, engineModes, engineStatus, engineTag } from './engine'

const service = (over: Partial<ServiceDescriptor> = {}): ServiceDescriptor => ({
  id: 'dataproc',
  label: 'Dataproc',
  category: 'Analytics',
  rootPath: '/gcp/dataproc/clusters',
  children: [],
  tier: 'shape',
  ...over,
})

describe('engineTag', () => {
  it('is undefined without an engine and "mock" when inactive', () => {
    expect(engineTag(service())).toBeUndefined()
    expect(engineTag(service({ engine: { active: false, modes: [] } }))).toBe('mock')
  })

  it('returns the active backend mode', () => {
    const tagged = service({ tier: 'full', engine: { active: true, mode: 'k8s', modes: [] } })
    expect(engineTag(tagged)).toBe('k8s')
  })
})

describe('engineActive', () => {
  it('reflects the engine active flag', () => {
    expect(engineActive(service())).toBe(false)
    expect(engineActive(service({ engine: { active: false, modes: [] } }))).toBe(false)
    expect(engineActive(service({ engine: { active: true, mode: 'docker', modes: [] } }))).toBe(true)
  })
})

describe('engineStatus', () => {
  const active = (mode: string) => service({ tier: 'full', engine: { active: true, mode, modes: [] } })

  it('is neutral mock with no engine active', () => {
    expect(engineStatus(service(), {})).toEqual({ label: 'mock', color: 'default' })
    expect(engineStatus(service({ engine: { active: false, modes: [] } }), {})).toEqual({
      label: 'mock',
      color: 'default',
    })
  })

  it('links the mode to host reachability (green only when reachable)', () => {
    expect(engineStatus(active('k8s'), { kubernetes: true })).toEqual({
      label: 'k8s · reachable',
      color: 'success',
    })
    expect(engineStatus(active('k8s'), { kubernetes: false })).toEqual({
      label: 'k8s · unreachable',
      color: 'warning',
    })
    expect(engineStatus(active('docker'), { docker: true })).toEqual({
      label: 'docker · reachable',
      color: 'success',
    })
    expect(engineStatus(active('k8s'), {})).toEqual({
      label: 'k8s · checking…',
      color: 'default',
    })
    // native has no host probe.
    expect(engineStatus(active('native'), {})).toEqual({
      label: 'native · configured',
      color: 'default',
    })
  })
})

describe('engineModes', () => {
  it('is empty without an engine and lists every backend otherwise', () => {
    expect(engineModes(service())).toEqual([])
    const modes = [
      { name: 'mock', supported: true },
      { name: 'docker', supported: false, note: 'not wired' },
    ]
    expect(engineModes(service({ engine: { active: false, modes } }))).toEqual(modes)
  })
})
