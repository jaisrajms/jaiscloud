/**
 * Client-side pagination/filter helpers for `GcpDataTable`. Kept framework-free
 * so the table's paging logic is unit-testable without a DOM.
 */

/** Slice one page of rows; `page` is 0-based. A non-positive size returns all. */
export function paginate<T>(rows: T[], page: number, rowsPerPage: number): T[] {
  if (rowsPerPage <= 0) return rows
  const start = Math.max(0, page) * rowsPerPage
  return rows.slice(start, start + rowsPerPage)
}

/** Page count for `count` rows, never below 1 so an empty table has one page. */
export function pageCount(count: number, rowsPerPage: number): number {
  if (rowsPerPage <= 0) return 1
  return Math.max(1, Math.ceil(count / rowsPerPage))
}

/** Case-insensitive substring filter over text derived from each row. */
export function filterRows<T>(rows: T[], query: string, toText: (row: T) => string): T[] {
  const q = query.trim().toLowerCase()
  if (!q) return rows
  return rows.filter((row) => toText(row).toLowerCase().includes(q))
}

export type SortDirection = 'asc' | 'desc'

/**
 * Compare two cell values for sorting. `null`/`undefined` always sort last
 * regardless of direction (handled by the caller negating only the primary
 * comparison), numbers compare numerically, dates chronologically and
 * everything else as case-insensitive strings.
 */
function compareValues(a: unknown, b: unknown): number {
  if (a == null && b == null) return 0
  if (a == null) return 1
  if (b == null) return -1
  if (typeof a === 'number' && typeof b === 'number') return a - b
  if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime()
  return String(a).toLowerCase().localeCompare(String(b).toLowerCase())
}

/**
 * Return a sorted copy of `rows` by the value derived from `getValue`. Sorting
 * is stable and never mutates the input; `null`/`undefined` values sort last.
 */
export function sortRows<T>(
  rows: T[],
  getValue: (row: T) => unknown,
  direction: SortDirection,
): T[] {
  const factor = direction === 'desc' ? -1 : 1
  return [...rows].sort((a, b) => {
    const av = getValue(a)
    const bv = getValue(b)
    // Keep nulls last in both directions, mirroring the console.
    if (av == null || bv == null) return compareValues(av, bv)
    return factor * compareValues(av, bv)
  })
}
