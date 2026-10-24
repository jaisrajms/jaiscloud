import { describe, expect, it } from 'vitest'
import {
  buildVirtualClusterConfig,
  clusterStateColor,
  jobStateColor,
  jobTypeLabel,
  parseJsonObject,
  shortDate,
} from './util'

describe('shortDate', () => {
  it('renders an em dash when absent', () => {
    expect(shortDate()).toBe('—')
  })

  it('passes through an unparseable value', () => {
    expect(shortDate('not-a-date')).toBe('not-a-date')
  })
})

describe('clusterStateColor', () => {
  it('maps known states', () => {
    expect(clusterStateColor('RUNNING')).toBe('success')
    expect(clusterStateColor('STOPPED')).toBe('default')
    expect(clusterStateColor('ERROR')).toBe('error')
  })

  it('warns on a transitional state', () => {
    expect(clusterStateColor('CREATING')).toBe('warning')
    expect(clusterStateColor('STARTING')).toBe('warning')
  })
})

describe('jobStateColor', () => {
  it('maps known states', () => {
    expect(jobStateColor('DONE')).toBe('success')
    expect(jobStateColor('RUNNING')).toBe('info')
    expect(jobStateColor('ERROR')).toBe('error')
    expect(jobStateColor('CANCELLED')).toBe('default')
  })

  it('warns on a pending state', () => {
    expect(jobStateColor('PENDING')).toBe('warning')
  })
})

describe('jobTypeLabel', () => {
  it('humanizes the oneof field name', () => {
    expect(jobTypeLabel('sparkJob')).toBe('Spark')
    expect(jobTypeLabel('pysparkJob')).toBe('PySpark')
    expect(jobTypeLabel('sparkSqlJob')).toBe('Spark SQL')
    expect(jobTypeLabel('hiveJob')).toBe('Hive')
  })

  it('renders an em dash when absent', () => {
    expect(jobTypeLabel()).toBe('—')
  })
})

describe('parseJsonObject', () => {
  it('parses a JSON object', () => {
    expect(parseJsonObject('{"gceClusterConfig":{}}')).toEqual({
      value: { gceClusterConfig: {} },
    })
  })

  it('rejects invalid JSON and non-objects', () => {
    expect(parseJsonObject('{')).toHaveProperty('error')
    expect(parseJsonObject('[]')).toEqual({ error: 'Config must be a JSON object.' })
    expect(parseJsonObject('null')).toEqual({ error: 'Config must be a JSON object.' })
  })
})

describe('buildVirtualClusterConfig', () => {
  it('composes the kubernetes config with a target and namespace', () => {
    expect(
      buildVirtualClusterConfig({
        gkeClusterTarget: 'projects/p/locations/us-central1/clusters/gke-1',
        kubernetesNamespace: 'dataproc',
      }),
    ).toEqual({
      kubernetesClusterConfig: {
        kubernetesNamespace: 'dataproc',
        gkeClusterConfig: {
          gkeClusterTarget: 'projects/p/locations/us-central1/clusters/gke-1',
        },
      },
    })
  })

  it('uses a node pool target when no cluster target is given', () => {
    expect(
      buildVirtualClusterConfig({
        gkeClusterTarget: '',
        kubernetesNamespace: 'dp',
        nodePool: 'projects/p/locations/us-central1/clusters/gke-1/nodePools/default',
      }),
    ).toEqual({
      kubernetesClusterConfig: {
        kubernetesNamespace: 'dp',
        gkeClusterConfig: {
          nodePoolTarget: [
            {
              nodePool: 'projects/p/locations/us-central1/clusters/gke-1/nodePools/default',
              roles: ['DEFAULT'],
            },
          ],
        },
      },
    })
  })
})
