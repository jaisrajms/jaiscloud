import { describe, expect, it } from 'vitest'
import {
  buildStructuredQuery,
  collectionQueryTarget,
  firestoreValueToText,
  parseQueryValue,
  scopeDocumentTarget,
  toFirestoreValue,
  type QuerySpec,
} from './query'

describe('toFirestoreValue', () => {
  it('encodes the primitive scalar types', () => {
    expect(toFirestoreValue(null)).toEqual({ nullValue: null })
    expect(toFirestoreValue(true)).toEqual({ booleanValue: true })
    expect(toFirestoreValue('Ada')).toEqual({ stringValue: 'Ada' })
    expect(toFirestoreValue(42)).toEqual({ integerValue: '42' })
    expect(toFirestoreValue(-7)).toEqual({ integerValue: '-7' })
    expect(toFirestoreValue(1.5)).toEqual({ doubleValue: 1.5 })
  })

  it('encodes arrays and nested maps', () => {
    expect(toFirestoreValue(['a', 1])).toEqual({
      arrayValue: { values: [{ stringValue: 'a' }, { integerValue: '1' }] },
    })
    expect(toFirestoreValue({ a: 1, b: ['x'] })).toEqual({
      mapValue: {
        fields: {
          a: { integerValue: '1' },
          b: { arrayValue: { values: [{ stringValue: 'x' }] } },
        },
      },
    })
  })
})

describe('parseQueryValue', () => {
  it('parses JSON and falls back to the raw string', () => {
    expect(parseQueryValue('42')).toBe(42)
    expect(parseQueryValue('"Ada"')).toBe('Ada')
    expect(parseQueryValue('true')).toBe(true)
    expect(parseQueryValue('Ada')).toBe('Ada')
  })
})

const base: QuerySpec = {
  collectionId: 'users',
  allDescendants: false,
  filters: [],
  orderBy: [],
  limit: '',
  offset: '',
}

describe('collectionQueryTarget', () => {
  it('splits a root collection', () => {
    expect(collectionQueryTarget('users')).toEqual({ collectionId: 'users', scope: '' })
  })

  it('splits a nested collection into scope + id', () => {
    expect(collectionQueryTarget('users/alice/orders')).toEqual({
      collectionId: 'orders',
      scope: 'users/alice',
    })
  })

  it('rejects document paths and empty input', () => {
    expect(collectionQueryTarget('users/alice')).toBeNull()
    expect(collectionQueryTarget('users/alice/orders/o1')).toBeNull()
    expect(collectionQueryTarget('')).toBeNull()
  })
})

describe('firestoreValueToText', () => {
  it('formats scalar types', () => {
    expect(firestoreValueToText({ stringValue: 'Ada' })).toBe('Ada')
    expect(firestoreValueToText({ integerValue: '42' })).toBe('42')
    expect(firestoreValueToText({ doubleValue: 1.5 })).toBe('1.5')
    expect(firestoreValueToText({ booleanValue: true })).toBe('true')
    expect(firestoreValueToText({ nullValue: null })).toBe('null')
    expect(firestoreValueToText({ timestampValue: '2026-01-01T00:00:00Z' })).toBe(
      '2026-01-01T00:00:00Z',
    )
    expect(firestoreValueToText({ referenceValue: 'projects/p/databases/(default)/documents/users/u1' })).toBe(
      'projects/p/databases/(default)/documents/users/u1',
    )
  })

  it('formats geo points, arrays and maps', () => {
    expect(firestoreValueToText({ geoPointValue: { latitude: 1.5, longitude: 2.5 } })).toBe('1.5, 2.5')
    expect(
      firestoreValueToText({
        arrayValue: { values: [{ stringValue: 'a' }, { integerValue: '1' }] },
      }),
    ).toBe('[a, 1]')
    expect(
      firestoreValueToText({
        mapValue: { fields: { a: { integerValue: '1' }, b: { stringValue: 'x' } } },
      }),
    ).toBe('{a: 1, b: x}')
  })

  it('keeps int64 precision as a string', () => {
    expect(firestoreValueToText({ integerValue: '9007199254740993' })).toBe('9007199254740993')
  })

  it('falls back to JSON for an unknown shape', () => {
    expect(firestoreValueToText({ weird: 1 })).toBe('{"weird":1}')
  })
})

describe('scopeDocumentTarget', () => {
  it('splits a document scope into collection path + document id', () => {
    expect(scopeDocumentTarget('cities/SF')).toEqual({ collection: 'cities', document: 'SF' })
    expect(scopeDocumentTarget('users/alice/orders/o1')).toEqual({
      collection: 'users/alice/orders',
      document: 'o1',
    })
  })

  it('rejects empty, collection and too-short scopes', () => {
    expect(scopeDocumentTarget('')).toBeNull()
    expect(scopeDocumentTarget('users')).toBeNull()
    expect(scopeDocumentTarget('users/alice/orders')).toBeNull()
  })
})

describe('buildStructuredQuery', () => {
  it('builds a minimal from clause', () => {
    expect(buildStructuredQuery(base).structuredQuery).toEqual({
      from: [{ collectionId: 'users' }],
    })
  })

  it('sets allDescendants for a collection group', () => {
    const { structuredQuery } = buildStructuredQuery({ ...base, allDescendants: true })
    expect(structuredQuery).toEqual({
      from: [{ collectionId: 'users', allDescendants: true }],
    })
  })

  it('combines filters into an AND composite filter', () => {
    const { structuredQuery } = buildStructuredQuery({
      ...base,
      filters: [
        { field: 'age', operator: 'GREATER_THAN', value: '21' },
        { field: 'name', operator: 'EQUAL', value: 'Ada' },
      ],
    })
    expect(structuredQuery?.where).toEqual({
      compositeFilter: {
        op: 'AND',
        filters: [
          {
            fieldFilter: {
              field: { fieldPath: 'age' },
              op: 'GREATER_THAN',
              value: { integerValue: '21' },
            },
          },
          {
            fieldFilter: {
              field: { fieldPath: 'name' },
              op: 'EQUAL',
              value: { stringValue: 'Ada' },
            },
          },
        ],
      },
    })
  })

  it('supports in/array-contains list values', () => {
    const { structuredQuery } = buildStructuredQuery({
      ...base,
      filters: [{ field: 'status', operator: 'IN', value: '["a","b"]' }],
    })
    expect(structuredQuery?.where).toEqual({
      compositeFilter: {
        op: 'AND',
        filters: [
          {
            fieldFilter: {
              field: { fieldPath: 'status' },
              op: 'IN',
              value: {
                arrayValue: { values: [{ stringValue: 'a' }, { stringValue: 'b' }] },
              },
            },
          },
        ],
      },
    })
  })

  it('adds order by, limit and offset', () => {
    const { structuredQuery } = buildStructuredQuery({
      ...base,
      orderBy: [{ field: 'age', direction: 'DESCENDING' }],
      limit: '10',
      offset: '5',
    })
    expect(structuredQuery).toEqual({
      from: [{ collectionId: 'users' }],
      orderBy: [{ field: { fieldPath: 'age' }, direction: 'DESCENDING' }],
      limit: 10,
      offset: 5,
    })
  })

  it('skips blank filter/order rows', () => {
    const { structuredQuery } = buildStructuredQuery({
      ...base,
      filters: [{ field: '  ', operator: 'EQUAL', value: 'x' }],
      orderBy: [{ field: '', direction: 'ASCENDING' }],
    })
    expect(structuredQuery).toEqual({ from: [{ collectionId: 'users' }] })
  })

  it('rejects an empty or multi-segment collection id', () => {
    expect(buildStructuredQuery({ ...base, collectionId: '  ' }).error).toBe(
      'Collection ID is required',
    )
    expect(buildStructuredQuery({ ...base, collectionId: 'users/alice' }).error).toBeTruthy()
  })

  it('rejects a filter without a value', () => {
    expect(
      buildStructuredQuery({
        ...base,
        filters: [{ field: 'age', operator: 'EQUAL', value: '  ' }],
      }).error,
    ).toBe('Filter on "age" needs a value')
  })

  it('rejects a bad limit or offset', () => {
    expect(buildStructuredQuery({ ...base, limit: '-1' }).error).toBe(
      'Limit must be a non-negative integer',
    )
    expect(buildStructuredQuery({ ...base, offset: 'x' }).error).toBe(
      'Offset must be a non-negative integer',
    )
  })
})
