import type { PropertyFilterQuery } from '@cloudscape-design/collection-hooks'

/** Stable key for a table's URL/localStorage state. */
export function tableKey(title: string, urlKey?: string): string {
  if (urlKey) return urlKey
  return title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '')
}

export interface TableUrlState {
  query?: PropertyFilterQuery
  /** 1-based page number, matching useCollection's currentPageIndex. */
  page?: number
  sortColumnId?: string
  sortDescending?: boolean
  pageSize?: number
}

function isQuery(value: unknown): value is PropertyFilterQuery {
  return (
    typeof value === 'object' &&
    value !== null &&
    Array.isArray((value as PropertyFilterQuery).tokens) &&
    typeof (value as PropertyFilterQuery).operation === 'string'
  )
}

/** Read a table's filter/page/sort/size from the URL query string. */
export function readTableUrl(search: URLSearchParams, key: string): TableUrlState {
  const state: TableUrlState = {}

  const filter = search.get(`${key}.f`)
  if (filter) {
    try {
      const parsed: unknown = JSON.parse(filter)
      if (isQuery(parsed)) state.query = parsed
    } catch {
      /* ignore malformed filter */
    }
  }

  const page = Number(search.get(`${key}.p`))
  if (Number.isInteger(page) && page > 1) state.page = page

  const sort = search.get(`${key}.s`)
  if (sort) {
    const [id, dir] = sort.split(':')
    if (id) {
      state.sortColumnId = id
      state.sortDescending = dir === 'desc'
    }
  }

  const size = Number(search.get(`${key}.z`))
  if (Number.isInteger(size) && size > 0) state.pageSize = size

  return state
}

export interface StoredTablePrefs {
  pageSize?: number
  wrapLines?: boolean
  stripedRows?: boolean
  contentDisplay?: { id: string; visible: boolean }[]
}

/** Load persisted column/page preferences for a table. */
export function loadTablePrefs(key: string): StoredTablePrefs {
  try {
    return JSON.parse(localStorage.getItem(`jaiscloud-table:${key}`) ?? '{}') as StoredTablePrefs
  } catch {
    return {}
  }
}

/** Persist column/page preferences for a table. */
export function saveTablePrefs(key: string, prefs: StoredTablePrefs): void {
  try {
    localStorage.setItem(`jaiscloud-table:${key}`, JSON.stringify(prefs))
  } catch {
    /* ignore storage errors */
  }
}
