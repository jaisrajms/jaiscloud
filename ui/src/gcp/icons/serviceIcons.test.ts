import { describe, expect, it } from 'vitest'
import {
  SERVICE_ACCENTS,
  serviceAccent,
  serviceIconComponent,
  hasServiceIcon,
} from './serviceIcons'

/** The service ids the GCP registrar advertises. */
const SERVICE_IDS = [
  'storage',
  'pubsub',
  'firestore',
  'compute',
  'run',
  'functions',
  'scheduler',
  'tasks',
  'workflows',
  'eventarc',
  'bigquery',
  'dataproc',
  'iam',
  'kms',
  'secretmanager',
  'logging',
  'monitoring',
  'resourcemanager',
]

describe('serviceIcons', () => {
  it('has a dedicated glyph and accent for every registered service', () => {
    for (const id of SERVICE_IDS) {
      expect(hasServiceIcon(id), `missing icon for ${id}`).toBe(true)
      expect(serviceAccent(id), `missing accent for ${id}`).toBe(SERVICE_ACCENTS[id])
      expect(serviceIconComponent(id)).toBeDefined()
    }
  })

  it('falls back to a generic glyph for unknown ids', () => {
    expect(hasServiceIcon('not-a-service')).toBe(false)
    expect(serviceIconComponent('not-a-service')).toBe(serviceIconComponent('another'))
    expect(serviceAccent('not-a-service')).toBe('#5f6368')
  })

  it('gives each known service a distinct glyph', () => {
    const components = SERVICE_IDS.map((id) => serviceIconComponent(id))
    expect(new Set(components).size).toBe(SERVICE_IDS.length)
  })
})
