import { describe, expect, it } from 'vitest'
import { filterRows, pageCount, paginate, sortRows } from './pagination'

const rows = [0, 1, 2, 3, 4, 5, 6]

describe('paginate', () => {
  it('slices the requested page', () => {
    expect(paginate(rows, 0, 3)).toEqual([0, 1, 2])
    expect(paginate(rows, 2, 3)).toEqual([6])
  })

  it('returns every row when the size is non-positive', () => {
    expect(paginate(rows, 1, 0)).toEqual(rows)
  })

  it('clamps a negative page to the first page', () => {
    expect(paginate(rows, -1, 3)).toEqual([0, 1, 2])
  })
})

describe('pageCount', () => {
  it('rounds up and never drops below one', () => {
    expect(pageCount(7, 3)).toBe(3)
    expect(pageCount(0, 3)).toBe(1)
    expect(pageCount(7, 0)).toBe(1)
  })
})

describe('filterRows', () => {
  const people = [
    { name: 'Alice', role: 'admin' },
    { name: 'Bob', role: 'viewer' },
  ]

  it('matches case-insensitively over the derived text', () => {
    expect(filterRows(people, 'ali', (p) => p.name)).toEqual([people[0]])
    expect(filterRows(people, 'ADMIN', (p) => p.role)).toEqual([people[0]])
  })

  it('returns all rows for a blank query', () => {
    expect(filterRows(people, '   ', (p) => p.name)).toEqual(people)
  })
})

describe('sortRows', () => {
  const people = [
    { name: 'Charlie', age: 30 },
    { name: 'alice', age: 25 },
    { name: 'Bob', age: 35 },
  ]

  it('sorts strings case-insensitively in both directions', () => {
    expect(sortRows(people, (p) => p.name, 'asc').map((p) => p.name)).toEqual([
      'alice',
      'Bob',
      'Charlie',
    ])
    expect(sortRows(people, (p) => p.name, 'desc').map((p) => p.name)).toEqual([
      'Charlie',
      'Bob',
      'alice',
    ])
  })

  it('sorts numbers numerically', () => {
    expect(sortRows(people, (p) => p.age, 'asc').map((p) => p.age)).toEqual([25, 30, 35])
    expect(sortRows(people, (p) => p.age, 'desc').map((p) => p.age)).toEqual([35, 30, 25])
  })

  it('sorts dates chronologically', () => {
    const rows = [
      { at: new Date('2020-01-03') },
      { at: new Date('2020-01-01') },
      { at: new Date('2020-01-02') },
    ]
    expect(sortRows(rows, (r) => r.at, 'asc').map((r) => r.at.getUTCDate())).toEqual([1, 2, 3])
  })

  it('keeps null/undefined last in both directions and is stable', () => {
    const rows = [
      { name: 'none', extra: null },
      { name: 'two', extra: 2 },
      { name: 'missing', extra: undefined },
      { name: 'one', extra: 1 },
    ]
    expect(sortRows(rows, (r) => r.extra, 'asc').map((r) => r.name)).toEqual([
      'one',
      'two',
      'none',
      'missing',
    ])
    // Nullish rows stay last even descending; their relative order is preserved.
    expect(sortRows(rows, (r) => r.extra, 'desc').map((r) => r.name)).toEqual([
      'two',
      'one',
      'none',
      'missing',
    ])
  })

  it('does not mutate the input array', () => {
    const input = [3, 1, 2]
    expect(sortRows(input, (n) => n, 'asc')).toEqual([1, 2, 3])
    expect(input).toEqual([3, 1, 2])
  })
})
