import { describe, expect, it } from 'vitest'
import type { ServiceDescriptor } from '../api/services'
import { buildBreadcrumbItems, serviceForPath } from './nav'

const href = (path: string) => `/ui${path}`

const services: ServiceDescriptor[] = [
  {
    id: 's3',
    label: 'S3',
    category: 'Storage',
    rootPath: '/aws/s3',
    children: [{ label: 'Buckets', path: '/aws/s3' }],
    tier: 'full',
  },
  {
    id: 'storage',
    label: 'Cloud Storage',
    category: 'Storage',
    rootPath: '/gcp/storage/buckets',
    children: [{ label: 'Buckets', path: '/gcp/storage/buckets' }],
    tier: 'full',
  },
]

describe('serviceForPath', () => {
  it('resolves AWS and GCP service paths', () => {
    expect(serviceForPath(services, '/aws/s3')?.id).toBe('s3')
    expect(serviceForPath(services, '/gcp/storage/buckets/my-bucket')?.id).toBe('storage')
  })
})

describe('buildBreadcrumbItems', () => {
  it('adds the owning service for any cloud', () => {
    expect(buildBreadcrumbItems(services, '/gcp/storage/buckets', href)).toEqual([
      { text: 'JaisCloud', href: '/ui/' },
      { text: 'Cloud Storage', href: '/ui/gcp/storage/buckets' },
    ])
  })

  it('adds the Admin crumb', () => {
    expect(buildBreadcrumbItems(services, '/admin', href)).toEqual([
      { text: 'JaisCloud', href: '/ui/' },
      { text: 'Admin', href: '/ui/admin' },
    ])
  })

  it('falls back to the console root when nothing matches', () => {
    expect(buildBreadcrumbItems(services, '/favorites', href)).toEqual([
      { text: 'JaisCloud', href: '/ui/' },
    ])
  })
})
