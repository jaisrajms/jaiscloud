import { describe, expect, it } from 'vitest'
import { formatIndexField, formatIndexFields, indexFieldFromRow } from './indexes'

describe('indexFieldFromRow', () => {
  it('maps directional modes to order', () => {
    expect(indexFieldFromRow({ fieldPath: 'name', mode: 'ASCENDING' })).toEqual({
      fieldPath: 'name',
      order: 'ASCENDING',
    })
    expect(indexFieldFromRow({ fieldPath: 'age', mode: 'DESCENDING' })).toEqual({
      fieldPath: 'age',
      order: 'DESCENDING',
    })
  })

  it('maps CONTAINS to an arrayConfig', () => {
    expect(indexFieldFromRow({ fieldPath: 'tags', mode: 'CONTAINS' })).toEqual({
      fieldPath: 'tags',
      arrayConfig: 'CONTAINS',
    })
  })

  it('trims the path and rejects an empty one', () => {
    expect(indexFieldFromRow({ fieldPath: '  name  ', mode: 'ASCENDING' })).toEqual({
      fieldPath: 'name',
      order: 'ASCENDING',
    })
    expect(indexFieldFromRow({ fieldPath: '   ', mode: 'ASCENDING' })).toBeNull()
  })
})

describe('formatIndexField', () => {
  it('renders order and array config', () => {
    expect(formatIndexField({ fieldPath: 'name', order: 'ASCENDING' })).toBe('name ASCENDING')
    expect(formatIndexField({ fieldPath: 'tags', arrayConfig: 'CONTAINS' })).toBe('tags CONTAINS')
  })
})

describe('formatIndexFields', () => {
  it('joins fields and handles the empty list', () => {
    expect(formatIndexFields([])).toBe('—')
    expect(
      formatIndexFields([
        { fieldPath: 'name', order: 'ASCENDING' },
        { fieldPath: '__name__', order: 'ASCENDING' },
      ]),
    ).toBe('name ASCENDING, __name__ ASCENDING')
  })
})
