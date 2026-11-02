import { describe, expect, it } from 'vitest'
import { rewriteObjectFully, type RewriteResponse } from '../../api/gcp/storage'
import { isKnownEventType, validateEventTypes } from './util'

describe('notification event types', () => {
  it('accepts the documented set', () => {
    for (const et of [
      'OBJECT_FINALIZE',
      'OBJECT_INITIALIZE',
      'OBJECT_METADATA_UPDATE',
      'OBJECT_DELETE',
      'OBJECT_ARCHIVE',
    ]) {
      expect(isKnownEventType(et)).toBe(true)
    }
  })

  it('rejects an unknown type', () => {
    expect(isKnownEventType('OBJECT_RENAME')).toBe(false)
    expect(validateEventTypes(['OBJECT_FINALIZE'])).toBeNull()
    expect(validateEventTypes(['OBJECT_FINALIZE', 'OBJECT_RENAME'])).toBe(
      'Unknown notification event type: OBJECT_RENAME',
    )
  })
})

describe('rewriteObjectFully', () => {
  it('follows rewriteToken until done', async () => {
    const calls: Array<string | undefined> = []
    const pages: RewriteResponse[] = [
      { done: false, rewriteToken: 't1', totalBytesRewritten: '1' },
      { done: false, rewriteToken: 't2', totalBytesRewritten: '2' },
      { done: true, objectSize: '3' },
    ]
    const fake = async (_bucket: string, body: { rewriteToken?: string }) => {
      calls.push(body.rewriteToken)
      return pages.shift()!
    }
    const progress: RewriteResponse[] = []
    const result = await rewriteObjectFully(
      'b1',
      { sourceObject: 'a', destinationObject: 'b' },
      (p) => progress.push(p),
      fake as never,
    )
    expect(calls).toEqual([undefined, 't1', 't2'])
    expect(progress).toHaveLength(3)
    expect(result.done).toBe(true)
    expect(result.objectSize).toBe('3')
  })

  it('stops when the provider completes in one call', async () => {
    const fake = async () => ({ done: true, objectSize: '1' })
    await expect(
      rewriteObjectFully('b1', { sourceObject: 'a', destinationObject: 'b' }, undefined, fake as never),
    ).resolves.toMatchObject({ done: true })
  })
})
