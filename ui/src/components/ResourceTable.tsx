import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useCollection } from '@cloudscape-design/collection-hooks'
import {
  Box,
  CollectionPreferences,
  Header,
  Pagination,
  PropertyFilter,
  Table,
} from '@cloudscape-design/components'
import type {
  CollectionPreferencesProps,
  TableProps,
} from '@cloudscape-design/components'
import { loadTablePrefs, readTableUrl, saveTablePrefs, tableKey } from '../lib/tableState'
import { FavoriteButton } from './FavoriteButton'
import type { ResourceFavorite } from '../hooks/useResourceFavorites'

export interface ResourceColumn<T> extends TableProps.ColumnDefinition<T> {
  /** Label shown in the filter property dropdown. */
  filterLabel?: string
  /** Text used for client-side filtering; omit to exclude the column from filters. */
  filterValue?: (item: T) => string
}

interface ResourceTableProps<T> {
  items: T[]
  columns: ResourceColumn<T>[]
  trackBy: (item: T) => string
  title: string
  description?: string
  actions?: React.ReactNode
  loading?: boolean
  onRowClick?: (item: T) => void
  emptyTitle: string
  emptyBody?: string
  selectionType?: 'single' | 'multi'
  selectedItems?: T[]
  onSelectionChange?: (items: T[]) => void
  defaultPageSize?: number
  stickyHeader?: boolean
  expandableRows?: TableProps.ExpandableRows<T>
  /** Namespace for URL/localStorage state. Defaults to a slug of `title`. */
  urlKey?: string
  /** Service id used when favoriting rows; enables the leading star column. */
  favoriteService?: string
  /** Resolve a row to a favorite (omit `service`, taken from favoriteService). */
  favorite?: (item: T) => Omit<ResourceFavorite, 'service'> | null
}

/**
 * List table wired up the way the AWS Console does it: a PropertyFilter bar,
 * pagination, column/page-size preferences and optional row selection.
 *
 * Filter, page, sort and page size are mirrored into the URL (shareable) and
 * column/page preferences are persisted per table in localStorage.
 */
export function ResourceTable<T>({
  items,
  columns,
  trackBy,
  title,
  description,
  actions,
  loading,
  onRowClick,
  emptyTitle,
  emptyBody,
  selectionType,
  selectedItems,
  onSelectionChange,
  defaultPageSize = 10,
  stickyHeader = true,
  expandableRows,
  urlKey,
  favoriteService,
  favorite,
}: ResourceTableProps<T>) {
  const [searchParams, setSearchParams] = useSearchParams()
  const key = useMemo(() => tableKey(title, urlKey), [title, urlKey])

  // Read URL + persisted prefs once, on mount.
  const [initial] = useState(() => ({
    url: readTableUrl(searchParams, key),
    prefs: loadTablePrefs(key),
  }))

  const enrichedColumns = columns.map((column) => {
    const col = { ...column }
    // Auto-enable sorting for any filterable column by comparing the text
    // used for filtering (unless the column already defines sorting).
    if (column.filterValue && !column.sortingField && !column.sortingComparator) {
      const filterValue = column.filterValue
      col.sortingComparator = (a: T, b: T) =>
        String(filterValue(a)).localeCompare(String(filterValue(b)), undefined, {
          numeric: true,
          sensitivity: 'base',
        })
    }
    return col
  })

  const [prefs, setPrefs] = useState<CollectionPreferencesProps.Preferences>(() => ({
    pageSize: initial.url.pageSize ?? initial.prefs.pageSize ?? defaultPageSize,
    wrapLines: initial.prefs.wrapLines ?? false,
    stripedRows: initial.prefs.stripedRows ?? false,
    contentDisplay: columns.map((column) => {
      const saved = initial.prefs.contentDisplay?.find((item) => item.id === String(column.id))
      return { id: String(column.id), visible: saved ? saved.visible : true }
    }),
  }))

  const [visibleColumns, setVisibleColumns] = useState<string[]>(() =>
    (prefs.contentDisplay ?? [])
      .filter((item) => item.visible)
      .map((item) => item.id),
  )

  const filteringProperties = enrichedColumns
    .filter((column) => column.filterValue)
    .map((column) => ({
      key: String(column.id),
      propertyLabel: column.filterLabel ?? String(column.header ?? column.id),
      groupValuesLabel: `${column.filterLabel ?? String(column.header ?? column.id)} values`,
    }))

  const defaultSortColumn = initial.url.sortColumnId
    ? enrichedColumns.find((item) => String(item.id) === initial.url.sortColumnId)
    : undefined
  const defaultSorting = defaultSortColumn
    ? { sortingColumn: defaultSortColumn, isDescending: initial.url.sortDescending }
    : undefined

  const {
    items: filteredItems,
    collectionProps,
    propertyFilterProps,
    paginationProps,
  } = useCollection(items, {
    propertyFiltering: {
      filteringProperties,
      empty: (
        <Box textAlign="center" color="inherit">
          <b>No resources</b>
        </Box>
      ),
      noMatch: (
        <Box textAlign="center" color="inherit">
          <b>No matches</b>
          <Box variant="p" color="inherit">
            No resources match the current filter.
          </Box>
        </Box>
      ),
      defaultQuery: initial.url.query,
    },
    pagination: { pageSize: prefs.pageSize ?? defaultPageSize, defaultPage: initial.url.page },
    sorting: { defaultState: defaultSorting },
  })

  const shownColumns: TableProps.ColumnDefinition<T>[] = [
    ...(favorite && favoriteService
      ? [
          {
            id: '__favorite',
            header: '',
            width: 44,
            minWidth: 44,
            cell: (item: T) => {
              const resolved = favorite(item)
              return resolved ? <FavoriteButton service={favoriteService} {...resolved} /> : null
            },
          } as TableProps.ColumnDefinition<T>,
        ]
      : []),
    ...enrichedColumns.filter((column) => visibleColumns.includes(String(column.id))),
  ]

  // Mirror collection state into the URL.
  const filterJson =
    propertyFilterProps.query && propertyFilterProps.query.tokens.length > 0
      ? JSON.stringify(propertyFilterProps.query)
      : ''
  const sortId = (collectionProps.sortingColumn as unknown as { id?: string } | undefined)?.id
  const sortParam = sortId ? `${sortId}:${collectionProps.sortingDescending ? 'desc' : 'asc'}` : ''
  const page = paginationProps.currentPageIndex
  const pageSize = prefs.pageSize ?? defaultPageSize

  useEffect(() => {
    const next = new URLSearchParams(searchParams)
    const setOrDelete = (name: string, value: string) => {
      if (value) next.set(name, value)
      else next.delete(name)
    }
    setOrDelete(`${key}.f`, filterJson)
    setOrDelete(`${key}.p`, page > 1 ? String(page) : '')
    setOrDelete(`${key}.s`, sortParam)
    setOrDelete(`${key}.z`, pageSize !== defaultPageSize ? String(pageSize) : '')
    if (next.toString() !== searchParams.toString()) {
      setSearchParams(next, { replace: true })
    }
  }, [filterJson, page, sortParam, pageSize, key, defaultPageSize, searchParams, setSearchParams])

  return (
    <Table
      {...collectionProps}
      items={filteredItems}
      columnDefinitions={shownColumns}
      loading={loading}
      loadingText="Loading resources"
      trackBy={trackBy}
      stickyHeader={stickyHeader}
      wrapLines={prefs.wrapLines}
      stripedRows={prefs.stripedRows}
      expandableRows={expandableRows}
      selectionType={selectionType}
      selectedItems={selectedItems}
      onSelectionChange={({ detail }) => onSelectionChange?.(detail.selectedItems)}
      onRowClick={onRowClick ? ({ detail }) => onRowClick(detail.item) : undefined}
      filter={
        <PropertyFilter
          {...propertyFilterProps}
          countText={`${filteredItems.length} of ${items.length}`}
        />
      }
      pagination={<Pagination {...paginationProps} />}
      preferences={
        <CollectionPreferences
          title="Preferences"
          confirmLabel="Confirm"
          cancelLabel="Cancel"
          preferences={prefs}
          onConfirm={({ detail }) => {
            setPrefs(detail)
            const display = detail.contentDisplay ?? []
            setVisibleColumns(display.filter((item) => item.visible).map((item) => item.id))
            saveTablePrefs(key, {
              pageSize: detail.pageSize,
              wrapLines: detail.wrapLines,
              stripedRows: detail.stripedRows,
              contentDisplay: display.map((item) => ({ id: item.id, visible: item.visible })),
            })
          }}
          pageSizePreference={{
            title: 'Page size',
            options: [
              { value: 10, label: '10 resources' },
              { value: 25, label: '25 resources' },
              { value: 50, label: '50 resources' },
            ],
          }}
          wrapLinesPreference={{ label: 'Wrap lines', description: 'Allow text to wrap in cells' }}
          stripedRowsPreference={{ label: 'Striped rows', description: 'Alternate row background' }}
          contentDisplayPreference={{
            title: 'Column preferences',
            options: columns.map((column) => ({
              id: String(column.id),
              label: String(column.header ?? column.id),
            })),
          }}
        />
      }
      header={
        <Header description={description} actions={actions} counter={`(${items.length})`}>
          {title}
        </Header>
      }
      empty={
        <Box textAlign="center" color="inherit">
          <b>{emptyTitle}</b>
          {emptyBody && (
            <Box variant="p" color="inherit">
              {emptyBody}
            </Box>
          )}
        </Box>
      }
    />
  )
}
