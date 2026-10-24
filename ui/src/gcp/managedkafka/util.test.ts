import { describe, expect, it } from 'vitest'
import { composeAclID, formatKV, parseKV, splitAclID } from './util'

describe('parseKV / formatKV', () => {
  it('parses key=value lines and ignores blanks and comments', () => {
    expect(parseKV('a=1\n\n# c\nb = two words \n')).toEqual({ a: '1', b: 'two words' })
  })

  it('returns undefined for an empty block', () => {
    expect(parseKV('   \n')).toBeUndefined()
  })

  it('round-trips through formatKV', () => {
    expect(parseKV(formatKV({ 'retention.ms': '600000' }))).toEqual({ 'retention.ms': '600000' })
  })
})

describe('ACL id helpers', () => {
  it('composes a prefixed id from a resource prefix and name', () => {
    expect(composeAclID('topic/', ' orders ')).toBe('topic/orders')
  })

  it('leaves a singleton id unchanged', () => {
    expect(composeAclID('allTopics', 'ignored')).toBe('allTopics')
  })

  it('splits a prefixed id back into prefix and name', () => {
    expect(splitAclID('consumerGroupPrefixed/orders-')).toEqual({
      prefix: 'consumerGroupPrefixed/',
      name: 'orders-',
    })
  })

  it('splits a singleton id with no name', () => {
    expect(splitAclID('cluster')).toEqual({ prefix: 'cluster', name: '' })
  })
})
