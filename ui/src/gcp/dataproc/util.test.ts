import { describe, expect, it } from 'vitest'
import {
  buildClusterUpdateMask,
  buildJobBody,
  buildStatusTimeline,
  buildVirtualClusterConfig,
  changedJsonPaths,
  clusterStateColor,
  collectGsUris,
  formatDelta,
  gcsBucketPath,
  jobStateColor,
  jobTypeLabel,
  parseGsUri,
  parseJsonObject,
  parseKeyValueLines,
  shortDate,
  sortableDate,
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

describe('sortableDate', () => {
  it('parses a microsecond timestamp and rejects absent/invalid values', () => {
    expect(sortableDate('2026-01-01T00:00:00.5Z')?.getTime()).toBe(
      Date.parse('2026-01-01T00:00:00.500Z'),
    )
    expect(sortableDate()).toBeNull()
    expect(sortableDate('nope')).toBeNull()
  })
})

describe('parseGsUri / gcsBucketPath', () => {
  it('parses a bucket and object', () => {
    expect(parseGsUri('gs://b/dir/a.jar')).toEqual({
      uri: 'gs://b/dir/a.jar',
      bucket: 'b',
      object: 'dir/a.jar',
    })
  })

  it('accepts a bucket-only URI and rejects non-GCS values', () => {
    expect(parseGsUri('gs://b')).toMatchObject({ bucket: 'b', object: '' })
    expect(parseGsUri('https://b/a')).toBeNull()
    expect(parseGsUri('gs://')).toBeNull()
    expect(parseGsUri(undefined)).toBeNull()
  })

  it('opens the bucket at the containing folder', () => {
    expect(gcsBucketPath('b', 'dir/a.jar')).toBe('/gcp/storage/buckets/b?prefix=dir%2F')
    expect(gcsBucketPath('b', 'dir/')).toBe('/gcp/storage/buckets/b?prefix=dir%2F')
    expect(gcsBucketPath('b', 'a.jar')).toBe('/gcp/storage/buckets/b')
    expect(gcsBucketPath('b')).toBe('/gcp/storage/buckets/b')
  })
})

describe('collectGsUris', () => {
  it('collects known fields only, from nested bodies and arrays', () => {
    const job = {
      mainClass: 'Main',
      mainJarFileUri: 'gs://b/app.jar',
      mainRFileUri: 'gs://b/main.R',
      queryFileUri: 'gs://b/query.sql',
      jarFileUris: ['gs://b/a.jar', 'gs://b/b.jar'],
      args: ['gs://not-a-resource'],
      nested: { executableFile: 'gs://b/init.sh' },
      description: 'gs://free-text',
    }
    expect(collectGsUris(job).map((u) => [u.key, u.uri])).toEqual([
      ['mainJarFileUri', 'gs://b/app.jar'],
      ['mainRFileUri', 'gs://b/main.R'],
      ['queryFileUri', 'gs://b/query.sql'],
      ['jarFileUris', 'gs://b/a.jar'],
      ['jarFileUris', 'gs://b/b.jar'],
      ['executableFile', 'gs://b/init.sh'],
    ])
  })

  it('dedupes the same URI and ignores malformed values', () => {
    expect(collectGsUris({ fileUris: ['gs://b/x', 'gs://b/x', 'https://b/x'] })).toHaveLength(1)
  })
})

describe('formatDelta', () => {
  it('renders sub-second, seconds, minutes and hours', () => {
    expect(formatDelta(400)).toBe('0.4s')
    expect(formatDelta(1500)).toBe('1.5s')
    expect(formatDelta(9_949)).toBe('9.9s')
    expect(formatDelta(9_950)).toBe('10s')
    expect(formatDelta(12_000)).toBe('12s')
    expect(formatDelta(65_000)).toBe('1m 5s')
    expect(formatDelta(3_600_000)).toBe('1h')
    expect(formatDelta(0)).toBe('0s')
  })
})

describe('buildStatusTimeline', () => {
  it('appends the current row and computes relative durations', () => {
    const history = [
      { state: 'PENDING', stateStartTime: '2026-01-01T00:00:00.000000Z' },
      { state: 'RUNNING', stateStartTime: '2026-01-01T00:00:00.400000Z' },
    ]
    const current = { state: 'DONE', stateStartTime: '2026-01-01T00:00:01.000000Z' }
    const now = Date.parse('2026-01-01T00:00:02.000000Z')
    expect(buildStatusTimeline(history, current, now).map((r) => [r.state, r.current, r.durationMs])).toEqual([
      ['PENDING', false, 400],
      ['RUNNING', false, 600],
      ['DONE', true, 1000],
    ])
  })

  it('omits durations when timestamps are missing', () => {
    const rows = buildStatusTimeline([], { state: 'ERROR' }, 0)
    expect(rows).toHaveLength(1)
    const row = rows[0]
    expect(row?.current).toBe(true)
    expect(row?.durationMs).toBeUndefined()
  })

  it('omits a negative duration when the timestamps are out of order', () => {
    const rows = buildStatusTimeline(
      [{ state: 'RUNNING', stateStartTime: '2026-01-01T00:00:02.000000Z' }],
      { state: 'DONE', stateStartTime: '2026-01-01T00:00:01.000000Z' },
      Date.parse('2026-01-01T00:00:03.000000Z'),
    )
    expect(rows[0]?.durationMs).toBeUndefined()
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
