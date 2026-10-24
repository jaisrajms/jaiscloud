import { describe, expect, it } from 'vitest'
import { keyElementId, keyPathToString, propertiesSummary, shortKey, valueToText } from './key'
import { entityHref, parseKeyParam } from './entityRoute'
import type { KeyRef } from '../../api/gcp/datastore'

describe('datastore key helpers', () => {
  const root: KeyRef = { path: [{ kind: 'Task', name: 'alice' }] }
  const nested: KeyRef = {
    path: [
      { kind: 'Parent', id: '1' },
      { kind: 'Child', name: 'bob' },
    ],
    namespace: 'tenant-a',
  }

  it('renders a key path with id or name', () => {
    expect(keyPathToString(root)).toBe('Task:alice')
    expect(keyPathToString(nested)).toBe('Parent:1/Child:bob')
    expect(shortKey(nested)).toBe('bob')
    expect(keyElementId({ kind: 'Task' })).toBe('')
  })

  it('formats each value variant', () => {
    expect(valueToText({ stringValue: 'x' })).toBe('x')
    expect(valueToText({ integerValue: '42' })).toBe('42')
    expect(valueToText({ doubleValue: 1.5 })).toBe('1.5')
    expect(valueToText({ booleanValue: false })).toBe('false')
    expect(valueToText({ nullValue: 'NULL_VALUE' })).toBe('null')
    expect(valueToText({ arrayValue: { values: [{}, {}] } })).toBe('[2]')
    expect(valueToText({ keyValue: { path: [{ kind: 'Task', name: 'a' }] } })).toBe('Task:a')
  })

  it('summarises properties', () => {
    expect(propertiesSummary({})).toBe('(no properties)')
    expect(propertiesSummary({ name: { stringValue: 'Ada' }, age: { integerValue: '36' } })).toBe(
      'name: Ada · age: 36',
    )
  })
})

describe('entity route', () => {
  it('round-trips a key through the ?key= parameter', () => {
    const key: KeyRef = { path: [{ kind: 'Task', name: 'a' }], namespace: 'ns' }
    const href = entityHref(key)
    expect(href.startsWith('/gcp/datastore/entity?key=')).toBe(true)
    const raw = new URL(href, 'http://localhost').searchParams.get('key')
    const parsed = parseKeyParam(raw)
    expect(parsed).toEqual(key)
  })

  it('rejects malformed keys', () => {
    expect(parseKeyParam(null)).toBeNull()
    expect(parseKeyParam('not json')).toBeNull()
    expect(parseKeyParam('{"path":[]}')).toBeNull()
  })
})
