import { describe, expect, it } from 'vitest'
import {
  collectionId,
  decodeFirestoreId,
  decodeFirestorePath,
  encodeFirestoreId,
  encodeFirestorePath,
  isNestedCollection,
  parentDocument,
  parseJsonObject,
  pathSegments,
  validateCollectionPath,
} from './util'

describe('parseJsonObject', () => {
  it('parses a JSON object', () => {
    const { value, error } = parseJsonObject('{"a":1,"b":true}')
    expect(error).toBeUndefined()
    expect(value).toEqual({ a: 1, b: true })
  })

  it('rejects invalid JSON', () => {
    const { value, error } = parseJsonObject('{')
    expect(value).toBeUndefined()
    expect(error).toBeTruthy()
  })

  it('rejects non-object top-level values', () => {
    for (const text of ['[1,2]', '"str"', '42', 'null']) {
      const { error } = parseJsonObject(text)
      expect(error).toBe('Document data must be a JSON object')
    }
  })
})

describe('firestore path helpers', () => {
  it('splits paths into non-empty segments', () => {
    expect(pathSegments('users/alice/orders')).toEqual(['users', 'alice', 'orders'])
    expect(pathSegments('/users//orders/')).toEqual(['users', 'orders'])
    expect(pathSegments('users')).toEqual(['users'])
  })

  it('returns the leaf collection id', () => {
    expect(collectionId('users')).toBe('users')
    expect(collectionId('users/alice/orders')).toBe('orders')
  })

  it('detects nested collections', () => {
    expect(isNestedCollection('users')).toBe(false)
    expect(isNestedCollection('users/alice/orders')).toBe(true)
    // Even-segment (document) paths are not collections.
    expect(isNestedCollection('users/alice')).toBe(false)
    expect(isNestedCollection('users/alice/orders/o1')).toBe(false)
  })

  it('validates collection paths', () => {
    expect(validateCollectionPath('users')).toBeNull()
    expect(validateCollectionPath('users/alice/orders')).toBeNull()
    expect(validateCollectionPath('')).toBe('Collection ID is required')
    expect(validateCollectionPath('users/alice')).toBeTruthy() // even: a document path
    expect(validateCollectionPath('users//orders')).toBe('Collection path has an empty segment')
    expect(validateCollectionPath('users/../orders')).toBe(
      'Collection path must not contain "." or ".."',
    )
  })

  it('locates the parent document of a nested collection', () => {
    expect(parentDocument('users')).toBeUndefined()
    expect(parentDocument('users/alice')).toBeUndefined()
    expect(parentDocument('users/alice/orders')).toEqual({
      collection: 'users',
      document: 'alice',
    })
    expect(parentDocument('users/alice/orders/o1/invoices')).toEqual({
      collection: 'users/alice/orders',
      document: 'o1',
    })
  })
})

describe('firestore path URL codec', () => {
  it('round-trips a nested collection path', () => {
    expect(encodeFirestorePath('users/alice/orders')).toBe('users~2Falice~2Forders')
    expect(decodeFirestorePath('users~2Falice~2Forders')).toBe('users/alice/orders')
  })

  it('round-trips ids that contain reserved or escape characters', () => {
    const ids = [
      'a%2Fb',
      'a%2fb',
      'a%252Fb',
      'a~2Fb',
      'a~7Eb',
      'a~b',
      'a b',
      'a?b',
      'a#b',
      '100%',
      "a/b'c(d)",
      'café',
      'x🎉y',
      "a!*'()b",
      'a~2Fb/c~7Ed',
    ]
    for (const id of ids) {
      expect(decodeFirestoreId(encodeFirestoreId(id))).toBe(id)
    }
  })

  it('never emits a "%", "/", or a bare "~2F" that React Router could rewrite', () => {
    const encoded = encodeFirestorePath('users/alice/orders')
    expect(encoded).not.toContain('/')
    expect(encoded).not.toContain('%')
    expect(encodeFirestoreId('a%2Fb')).not.toContain('%')

    // A literal `~2F` in an id must not alias a path separator.
    const literal = encodeFirestoreId('users~2Falice')
    expect(literal).not.toContain('~2F')
    expect(decodeFirestorePath(`users~2Falice`)).not.toBe(decodeFirestorePath(literal))
    expect(decodeFirestorePath(literal)).toBe('users~2Falice')
  })

  it('round-trips a full path whose segments use the escape characters', () => {
    const path = 'a%2Fb/c~2Fd/e f'
    expect(decodeFirestorePath(encodeFirestorePath(path))).toBe(path)
  })

  it('drops stray separators when encoding a path', () => {
    expect(encodeFirestorePath('/users//orders/')).toBe('users~2Forders')
    expect(decodeFirestorePath('users~2Forders')).toBe('users/orders')
  })

  it('decodes malformed or foreign input without throwing', () => {
    for (const input of ['a~ZZb', 'bare~', '100%', '%zz', '~', 'a~2Fb~2F']) {
      expect(() => decodeFirestoreId(input)).not.toThrow()
      expect(typeof decodeFirestoreId(input)).toBe('string')
    }
    expect(() => decodeFirestorePath('a~2F~2Fb~')).not.toThrow()
  })
})
