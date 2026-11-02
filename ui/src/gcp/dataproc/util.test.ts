import { describe, expect, it } from 'vitest'
import {
  buildClusterUpdateMask,
  buildJobBody,
  buildVirtualClusterConfig,
  changedJsonPaths,
  clusterStateColor,
  jobStateColor,
  jobTypeLabel,
  parseJsonObject,
  parseKeyValueLines,
  shortDate,
  splitList,
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

describe('buildJobBody', () => {
  it('composes a spark job with placement, args and properties', () => {
    expect(
      buildJobBody({
        clusterName: 'alpha',
        type: 'sparkJob',
        fields: {
          mainClass: 'com.example.Main',
          jarFileUris: 'gs://b/a.jar, gs://b/b.jar',
        },
        args: 'one\ntwo',
        properties: 'spark.executor.memory=2g\n# comment\nspark.app.name=demo',
      }),
    ).toEqual({
      placement: { clusterName: 'alpha' },
      sparkJob: {
        mainClass: 'com.example.Main',
        jarFileUris: ['gs://b/a.jar', 'gs://b/b.jar'],
        args: ['one', 'two'],
        properties: { 'spark.executor.memory': '2g', 'spark.app.name': 'demo' },
      },
    })
  })

  it('splits sparkSql queryList into an array', () => {
    expect(
      buildJobBody({
        clusterName: 'alpha',
        type: 'sparkSqlJob',
        fields: { queryList: 'SELECT 1;\nSELECT a, b FROM t;' },
      }),
    ).toEqual({
      placement: { clusterName: 'alpha' },
      // A SQL query keeps its commas: only newlines separate queries.
      sparkSqlJob: { queryList: ['SELECT 1;', 'SELECT a, b FROM t;'] },
    })
  })

  it('rejects a malformed properties line', () => {
    expect(() =>
      buildJobBody({
        clusterName: 'alpha',
        type: 'sparkJob',
        fields: {},
        properties: 'not-a-pair',
      }),
    ).toThrow('key=value')
  })
})

describe('splitList / parseKeyValueLines', () => {
  it('splits and trims comma and newline separated values', () => {
    expect(splitList(' a, b\n c \n\n')).toEqual(['a', 'b', 'c'])
  })

  it('parses key=value lines and skips comments', () => {
    expect(parseKeyValueLines('a=1\n\n# x\nb = two')).toEqual({ a: '1', b: 'two' })
  })
})

describe('changedJsonPaths', () => {
  it('returns only changed nested leaves', () => {
    const before = { gceClusterConfig: { zoneUri: 'a' }, workerConfig: { numInstances: 2 } }
    const after = { gceClusterConfig: { zoneUri: 'a' }, workerConfig: { numInstances: 3 } }
    expect(changedJsonPaths(before, after, 'config')).toEqual(['config.workerConfig.numInstances'])
  })

  it('treats a changed array as one path', () => {
    expect(changedJsonPaths({ a: [1, 2] }, { a: [1, 2, 3] }, 'config')).toEqual(['config.a'])
  })

  it('returns nothing when unchanged', () => {
    expect(changedJsonPaths({ a: 1 }, { a: 1 }, 'config')).toEqual([])
  })
})

describe('buildClusterUpdateMask', () => {
  it('merges labels and changed config paths', () => {
    expect(
      buildClusterUpdateMask(
        true,
        { workerConfig: { numInstances: 2 }, masterConfig: { numInstances: 1 } },
        { workerConfig: { numInstances: 4 }, masterConfig: { numInstances: 1 } },
      ),
    ).toEqual(['labels', 'config.workerConfig.numInstances'])
  })

  it('is empty when nothing changed', () => {
    expect(buildClusterUpdateMask(false, { a: 1 }, { a: 1 })).toEqual([])
  })
})
