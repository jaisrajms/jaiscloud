import { describe, expect, it } from 'vitest'
import {
  cellValue,
  formatMillis,
  parseInsertRows,
  parseJsonObject,
  rowTemplate,
  schemaFields,
} from './util'

describe('parseJsonObject', () => {
  it('accepts an object', () => {
    expect(parseJsonObject('{"a":1}').value).toEqual({ a: 1 })
  })
  it('treats empty text as an empty object', () => {
    expect(parseJsonObject('  ').value).toEqual({})
  })
  it('rejects a top-level array', () => {
    expect(parseJsonObject('[]').error).toBeTruthy()
  })
  it('reports invalid JSON', () => {
    expect(parseJsonObject('{').error).toBeTruthy()
  })
})

describe('formatMillis', () => {
  it('renders a valid epoch-millis timestamp', () => {
    expect(formatMillis('1700000000000')).not.toBe('—')
  })
  it('falls back to the raw value when not numeric', () => {
    expect(formatMillis('nope')).toBe('nope')
  })
  it('renders an em dash for an empty value', () => {
    expect(formatMillis(undefined)).toBe('—')
  })
})

describe('schemaFields', () => {
  it('extracts top-level fields', () => {
    const fields = schemaFields({
      schema: { fields: [{ name: 'id', type: 'STRING', mode: 'REQUIRED' }] },
    })
    expect(fields).toEqual([{ name: 'id', type: 'STRING', mode: 'REQUIRED' }])
  })
  it('returns an empty list when there is no schema', () => {
    expect(schemaFields(undefined)).toEqual([])
    expect(schemaFields({})).toEqual([])
  })
})

describe('cellValue', () => {
  it('renders a primitive', () => {
    expect(cellValue({ v: '42' })).toBe('42')
  })
  it('renders null', () => {
    expect(cellValue({ v: null })).toBe('null')
  })
  it('JSON-encodes nested values', () => {
    expect(cellValue({ v: { f: [] } })).toBe('{"f":[]}')
  })
})

describe('rowTemplate', () => {
  it('maps each named schema field to null', () => {
    expect(
      rowTemplate([
        { name: 'id', type: 'STRING' },
        { name: 'n', type: 'INTEGER' },
      ]),
    ).toEqual({ id: null, n: null })
  })
  it('skips unnamed fields', () => {
    expect(rowTemplate([{ name: '', type: 'STRING' }])).toEqual({})
  })
})

describe('parseInsertRows', () => {
  it('accepts a single row object', () => {
    const parsed = parseInsertRows('{"id":"1"}')
    expect(parsed.error).toBeUndefined()
    expect(parsed.rows).toEqual([{ json: { id: '1' } }])
  })
  it('accepts an array of rows', () => {
    const parsed = parseInsertRows('[{"id":"1"},{"id":"2"}]')
    expect(parsed.rows).toHaveLength(2)
    expect(parsed.rows?.[1]?.json).toEqual({ id: '2' })
  })
  it('rejects a non-object row', () => {
    expect(parseInsertRows('[1]').error).toBeTruthy()
  })
  it('rejects empty input', () => {
    expect(parseInsertRows('   ').error).toBeTruthy()
  })
  it('reports invalid JSON', () => {
    expect(parseInsertRows('{').error).toBeTruthy()
  })
})
