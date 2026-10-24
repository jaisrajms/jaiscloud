import { useEffect, useState, type MouseEvent, type ReactNode } from 'react'
import {
  Alert,
  Box,
  Checkbox,
  Divider,
  Drawer,
  IconButton,
  Skeleton,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TableSortLabel,
  Typography,
} from '@mui/material'
import CloseIcon from '@mui/icons-material/Close'
import { pageCount, paginate, sortRows, type SortDirection } from './pagination'

export interface GcpColumn<T> {
  /** Stable column key. */
  key: string
  header: ReactNode
  render: (row: T) => ReactNode
  align?: 'left' | 'center' | 'right'
  /** Optional CSS width (number = px). */
  width?: number | string
  /** Enables click-to-sort on this column (requires `sortValue`). */
  sortable?: boolean
  /** Value used for sorting; required when `sortable` is set. */
  sortValue?: (row: T) => string | number | Date | null | undefined
}

export interface GcpDataTableProps<T> {
  columns: GcpColumn<T>[]
  rows: T[]
  getRowKey: (row: T) => string
  loading?: boolean
  /** Error message rendered above the table. */
  error?: string | null
  emptyMessage?: string
  /** Rows per page; 0 renders every row and hides the footer. Defaults to 10. */
  rowsPerPage?: number
  /** Click handler applied to each body row. */
  onRowClick?: (row: T) => void
  /** Skeleton placeholder rows shown while loading. Defaults to 5. */
  skeletonRows?: number
  /** Column key sorted on first render. */
  defaultSortKey?: string
  /** Initial sort direction for `defaultSortKey`. Defaults to ascending. */
  defaultSortDirection?: SortDirection
  /** Renders a checkbox column and select-all header. */
  selectable?: boolean
  /** Controlled selection (row keys returned by `getRowKey`). */
  selectedKeys?: string[]
  onSelectionChange?: (keys: string[]) => void
  /** When set, clicking a row (outside links/buttons) opens a side info panel. */
  renderDetail?: (row: T) => ReactNode
  /** Heading for the info panel; defaults to the first column's rendered cell. */
  detailTitle?: (row: T) => ReactNode
  'aria-label'?: string
}

/**
 * Shared dense data table for console list pages: typed columns, click-to-sort
 * headers, loading skeleton, empty state, error banner, optional row selection
 * and a client-side pagination footer. An optional side info panel shows a row's
 * details without navigating. Server-side `nextPageToken` following is out of
 * scope (UI18).
 */
export function GcpDataTable<T>({
  columns,
  rows,
  getRowKey,
  loading = false,
  error = null,
  emptyMessage = 'No items.',
  rowsPerPage = 10,
  onRowClick,
  skeletonRows = 5,
  defaultSortKey,
  defaultSortDirection = 'asc',
  selectable = false,
  selectedKeys,
  onSelectionChange,
  renderDetail,
  detailTitle,
  'aria-label': ariaLabel,
}: GcpDataTableProps<T>) {
  const [page, setPage] = useState(0)
  const [perPage, setPerPage] = useState(rowsPerPage)
  const [sort, setSort] = useState<{ key: string; direction: SortDirection } | null>(
    defaultSortKey ? { key: defaultSortKey, direction: defaultSortDirection } : null,
  )
  const [detailRow, setDetailRow] = useState<T | null>(null)

  const paginated = perPage > 0
  const sorted = sort
    ? sortRows(
        rows,
        (row) => columns.find((column) => column.key === sort.key)?.sortValue?.(row),
        sort.direction,
      )
    : rows
  const safePage = Math.min(page, pageCount(sorted.length, perPage) - 1)
  const visible = paginated ? paginate(sorted, safePage, perPage) : sorted

  // Return to the first page whenever the sort order changes.
  useEffect(() => {
    setPage(0)
  }, [sort?.key, sort?.direction, rows.length])

  // Close the info panel when its row is no longer in the (filtered) list.
  useEffect(() => {
    if (detailRow === null) return
    const key = getRowKey(detailRow)
    if (!rows.some((row) => getRowKey(row) === key)) setDetailRow(null)
  }, [detailRow, rows, getRowKey])

  const toggleSort = (key: string) => {
    setSort((current) => {
      if (!current || current.key !== key) return { key, direction: 'asc' }
      if (current.direction === 'asc') return { key, direction: 'desc' }
      return null
    })
  }

  const selectedSet = new Set(selectedKeys ?? [])
  const visibleKeys = visible.map(getRowKey)
  const allVisibleSelected = visibleKeys.length > 0 && visibleKeys.every((key) => selectedSet.has(key))
  const someVisibleSelected = visibleKeys.some((key) => selectedSet.has(key))

  const toggleRow = (key: string) => {
    if (!onSelectionChange) return
    const next = new Set(selectedSet)
    if (next.has(key)) next.delete(key)
    else next.add(key)
    onSelectionChange([...next])
  }

  const toggleAllVisible = () => {
    if (!onSelectionChange) return
    const next = new Set(selectedSet)
    if (allVisibleSelected) visibleKeys.forEach((key) => next.delete(key))
    else visibleKeys.forEach((key) => next.add(key))
    onSelectionChange([...next])
  }

  const interactive = Boolean(onRowClick || renderDetail)
  const handleRowClick = (row: T) => (event: MouseEvent<HTMLTableRowElement>) => {
    // Let links, buttons and form controls handle their own clicks.
    if ((event.target as HTMLElement).closest('a, button, input, [role="button"]')) return
    if (onRowClick) onRowClick(row)
    else if (renderDetail) setDetailRow(row)
  }

  const columnCount = columns.length + (selectable ? 1 : 0)

  return (
    <Box>
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small" aria-label={ariaLabel}>
          <TableHead>
            <TableRow>
              {selectable && (
                <TableCell padding="checkbox">
                  <Checkbox
                    size="small"
                    checked={allVisibleSelected}
                    indeterminate={!allVisibleSelected && someVisibleSelected}
                    onChange={toggleAllVisible}
                    slotProps={{ input: { 'aria-label': 'Select all rows' } }}
                  />
                </TableCell>
              )}
              {columns.map((column) => (
                <TableCell
                  key={column.key}
                  align={column.align}
                  sx={{ width: column.width }}
                  sortDirection={sort?.key === column.key ? sort.direction : false}
                >
                  {column.sortable && column.sortValue ? (
                    <TableSortLabel
                      active={sort?.key === column.key}
                      direction={sort?.key === column.key ? sort.direction : 'asc'}
                      onClick={() => toggleSort(column.key)}
                    >
                      {column.header}
                    </TableSortLabel>
                  ) : (
                    column.header
                  )}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {loading &&
              Array.from({ length: skeletonRows }).map((_unused, rowIndex) => (
                <TableRow key={`skeleton-${rowIndex}`}>
                  {selectable && (
                    <TableCell padding="checkbox">
                      <Skeleton variant="circular" width={18} height={18} />
                    </TableCell>
                  )}
                  {columns.map((column) => (
                    <TableCell key={column.key} align={column.align}>
                      <Skeleton variant="text" />
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            {!loading && !error && rows.length === 0 && (
              <TableRow>
                <TableCell
                  colSpan={columnCount}
                  align="center"
                  sx={{ py: 4, color: 'text.secondary' }}
                >
                  {emptyMessage}
                </TableCell>
              </TableRow>
            )}
            {!loading &&
              visible.map((row) => {
                const key = getRowKey(row)
                return (
                  <TableRow
                    key={key}
                    hover
                    selected={selectable && selectedSet.has(key)}
                    onClick={interactive ? handleRowClick(row) : undefined}
                    sx={interactive ? { cursor: 'pointer' } : undefined}
                  >
                    {selectable && (
                      <TableCell padding="checkbox">
                        <Checkbox
                          size="small"
                          checked={selectedSet.has(key)}
                          onChange={() => toggleRow(key)}
                          slotProps={{ input: { 'aria-label': `Select row ${key}` } }}
                        />
                      </TableCell>
                    )}
                    {columns.map((column) => (
                      <TableCell key={column.key} align={column.align}>
                        {column.render(row)}
                      </TableCell>
                    ))}
                  </TableRow>
                )
              })}
          </TableBody>
        </Table>
      </TableContainer>
      {paginated && (
        <TablePagination
          component="div"
          count={sorted.length}
          page={safePage}
          onPageChange={(_event, next) => setPage(next)}
          rowsPerPage={perPage}
          onRowsPerPageChange={(event) => {
            setPerPage(Number(event.target.value))
            setPage(0)
          }}
          rowsPerPageOptions={[10, 25, 50]}
        />
      )}
      {renderDetail && (
        <Drawer anchor="right" open={detailRow !== null} onClose={() => setDetailRow(null)}>
          <Box sx={{ width: { xs: 320, sm: 400 }, p: 2 }}>
            <Stack direction="row" spacing={1} sx={{ alignItems: 'center' }}>
              <Typography variant="h6" sx={{ flexGrow: 1, overflowWrap: 'anywhere' }}>
                {detailRow !== null && (detailTitle ? detailTitle(detailRow) : null)}
              </Typography>
              <IconButton size="small" aria-label="Close details" onClick={() => setDetailRow(null)}>
                <CloseIcon fontSize="small" />
              </IconButton>
            </Stack>
            <Divider sx={{ my: 1.5 }} />
            {detailRow !== null && renderDetail(detailRow)}
          </Box>
        </Drawer>
      )}
    </Box>
  )
}
