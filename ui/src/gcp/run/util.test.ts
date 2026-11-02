import { describe, expect, it } from 'vitest'
import {
  applyFormToService,
  buildServiceBody,
  editableService,
  firstImage,
  lastSegment,
  serviceUpdateMask,
  shortDate,
  templateContainers,
} from './util'

describe('lastSegment', () => {
  it('returns the final segment', () => {
    expect(lastSegment('projects/p/locations/us-central1/services/svc')).toBe('svc')
  })

  it('handles trailing slashes and empty names', () => {
    expect(lastSegment('a/b/')).toBe('b')
    expect(lastSegment('')).toBe('')
    expect(lastSegment(undefined)).toBe('')
  })

  it('returns a bare name unchanged', () => {
    expect(lastSegment('svc')).toBe('svc')
  })
})

describe('shortDate', () => {
  it('renders an em dash when absent', () => {
    expect(shortDate()).toBe('—')
  })

  it('passes through an unparseable value', () => {
    expect(shortDate('not-a-date')).toBe('not-a-date')
  })
})

describe('firstImage', () => {
  it('returns the first container image', () => {
    expect(firstImage([{ image: 'gcr.io/p/img:1' }])).toBe('gcr.io/p/img:1')
  })

  it('returns empty for malformed containers', () => {
    expect(firstImage(undefined)).toBe('')
    expect(firstImage([])).toBe('')
    expect(firstImage(['not-an-object'])).toBe('')
    expect(firstImage([{}])).toBe('')
  })
})

describe('templateContainers', () => {
  it('reads containers nested under template', () => {
    expect(templateContainers({ template: { containers: [{ image: 'x' }] } })).toEqual([
      { image: 'x' },
    ])
  })

  it('returns undefined when the template is absent', () => {
    expect(templateContainers({})).toBeUndefined()
    expect(templateContainers(undefined)).toBeUndefined()
  })
})

describe('buildServiceBody', () => {
  it('composes containers, env, ports and limits', () => {
    expect(
      buildServiceBody({
        image: 'nginx:latest',
        command: 'nginx\n-g',
        args: 'daemon off;',
        env: 'A=1\nB=two',
        ports: '8080, 9090',
        cpu: '1',
        memory: '512Mi',
        serviceAccount: 'sa@p.iam.gserviceaccount.com',
        ingress: 'INGRESS_TRAFFIC_ALL',
      }),
    ).toEqual({
      template: {
        containers: [
          {
            image: 'nginx:latest',
            command: ['nginx', '-g'],
            args: ['daemon off;'],
            env: [
              { name: 'A', value: '1' },
              { name: 'B', value: 'two' },
            ],
            ports: [{ containerPort: 8080 }, { containerPort: 9090 }],
            resources: { limits: { cpu: '1', memory: '512Mi' } },
          },
        ],
        serviceAccount: 'sa@p.iam.gserviceaccount.com',
      },
      ingress: 'INGRESS_TRAFFIC_ALL',
    })
  })
})

describe('serviceUpdateMask', () => {
  const before = {
    template: { containers: [{ image: 'nginx:1' }] },
    traffic: [{ percent: 100, latestRevision: true }],
    ingress: 'INGRESS_TRAFFIC_ALL',
    name: 'projects/p/locations/l/services/s',
  }

  it('masks only changed top-level fields', () => {
    const after = {
      template: { containers: [{ image: 'nginx:2' }] },
      traffic: [{ percent: 100, latestRevision: true }],
      ingress: 'INGRESS_TRAFFIC_ALL',
    }
    expect(serviceUpdateMask(before, after)).toEqual(['template'])
  })

  it('ignores rendered system fields and reports no change', () => {
    expect(serviceUpdateMask(before, editableService(before))).toEqual([])
  })
})

describe('applyFormToService', () => {
  it('preserves traffic and unmodelled fields while merging the form', () => {
    const service = {
      name: 'projects/p/locations/us-central1/services/s',
      template: {
        containers: [{ image: 'nginx:1', volumeMounts: [{ name: 'v' }] }],
        volumes: [{ name: 'v' }],
      },
      traffic: [{ percent: 100 }],
      ingress: 'INGRESS_TRAFFIC_ALL',
    }
    const body = applyFormToService(service, {
      image: 'nginx:2',
      env: 'A=1',
      cpu: '2',
      ingress: 'INGRESS_TRAFFIC_INTERNAL_ONLY',
    })
    expect(body.traffic).toEqual([{ percent: 100 }])
    expect(body.ingress).toBe('INGRESS_TRAFFIC_INTERNAL_ONLY')
    const template = body.template as Record<string, unknown>
    expect(template.volumes).toEqual([{ name: 'v' }])
    const container = (template.containers as Array<Record<string, unknown>>)[0]!
    expect(container.image).toBe('nginx:2')
    expect(container.volumeMounts).toEqual([{ name: 'v' }])
    expect(container.env).toEqual([{ name: 'A', value: '1' }])
    expect((container.resources as Record<string, unknown>).limits).toEqual({ cpu: '2' })
    expect((body as Record<string, unknown>).name).toBeUndefined()
  })
})
